import base64
import datetime
import json
import time
import pytest
import httpx
import pydantic

from gql.transport.exceptions import TransportQueryError, TransportServerError

from app.actions.client import (
    execute_gql_query,
    build_request_header,
    get_skylight_events,
    map_event_type,
    _is_token_expired,
    _redact_headers,
    _log_skylight_error_response,
    PullEventsBadConfigException,
    _TOKEN_EXPIRY_SKEW_SECONDS,
    normalize_v2_event,
    search_aois,
)
from app.actions.configurations import ProcessEventsPerAOIConfig, PullEventsConfig, ListAOIsQuery
from app.actions.handlers import (
    action_pull_events,
    action_process_events_per_aoi,
    advance_aoi_cursors,
    batch_events_by_payload_size,
    build_chunk_plan,
    patch_events,
    process_attachments,
    transform,
)
from app.services.state import IntegrationStateManager


def _make_jwt(exp: int) -> str:
    """Build a minimal unsigned JWT with a given exp timestamp."""
    payload = base64.urlsafe_b64encode(json.dumps({"exp": exp}).encode()).rstrip(b"=").decode()
    return f"header.{payload}.sig"

@pytest.fixture
def integration(mocker):
    return mocker.AsyncMock(id="integration_id", base_url="https://gundi-test.com", additional=True)

@pytest.fixture
def auth(mocker):
    return mocker.AsyncMock(username="test_user", password="test_password")

@pytest.fixture
def gql_client(mocker):
    return mocker.MagicMock()

@pytest.fixture
def state_manager(mocker):
    return mocker.AsyncMock(IntegrationStateManager)


@pytest.fixture
def skylight_client():
    import app.actions.client as client
    return client


@pytest.mark.asyncio
async def test_execute_gql_query_success(mocker, gql_client, integration, auth):
    gql_client.execute.return_value = {"data": "response"}
    query = "query"
    params = {}

    response = await execute_gql_query(gql_client, query, params, integration, auth)
    assert response == {"data": "response"}

@pytest.mark.asyncio
async def test_execute_gql_query_clears_token_on_unauthorized(mocker, gql_client, integration, auth, state_manager):
    # On UNAUTHORIZED: state is deleted once (so next run re-auths) and the
    # error is re-raised. No retry — the stale token is still in gql_client's
    # transport headers, so retrying with the same client would fail identically.
    gql_client.execute = mocker.MagicMock(
        side_effect=TransportQueryError(
            "Error",
            errors=[{"extensions": {"message": "UNAUTHORIZED", "code": "UNAUTHORIZED"}}]
        )
    )
    query = "query"
    params = {}

    mocker.patch("app.actions.client.state_manager", state_manager)
    with pytest.raises(TransportQueryError):
        await execute_gql_query(gql_client, query, params, integration, auth)
    assert state_manager.delete_state.call_count == 1


@pytest.mark.asyncio
async def test_execute_gql_query_clears_token_on_unauthenticated(mocker, gql_client, integration, auth, state_manager):
    gql_client.execute = mocker.MagicMock(
        side_effect=TransportQueryError(
            "Error",
            errors=[{"extensions": {"code": "UNAUTHENTICATED"}}]
        )
    )
    query = "query"
    params = {}

    mocker.patch("app.actions.client.state_manager", state_manager)
    with pytest.raises(TransportQueryError):
        await execute_gql_query(gql_client, query, params, integration, auth)
    assert state_manager.delete_state.call_count == 1


@pytest.mark.asyncio
async def test_execute_gql_query_does_not_retry_if_not_unauthorized_code(
        mocker,
        gql_client,
        integration,
        auth,
        state_manager
):
    gql_client.execute = mocker.MagicMock(
        side_effect=TransportQueryError(
            "Error",
            errors=[{"extensions": {"message": "Error", "code": "Error"}}]
        )
    )
    query = "query"
    params = {}

    mocker.patch("app.actions.client.state_manager", state_manager)
    with pytest.raises(TransportQueryError):
        await execute_gql_query(gql_client, query, params, integration, auth)
    assert state_manager.delete_state.call_count == 0


# --- get_skylight_events paging / error paths ---


class _PullCfg:
    """Minimal stand-in for PullEventsConfig (map_event_type is patched, so
    event_types content is irrelevant)."""
    event_types = ["Fishing"]
    aoi_ids = ["aoi1"]
    pageSize = 2
    initial_data_window_days = 1


def _page(items, total, page_size=2, page_num=1, snapshot_id="snap-1"):
    # v2 records use camelCase; get_skylight_events reshapes them to the v1
    # layout (event_id, ...) so downstream assertions use v1 keys.
    records = None if items is None else [{"eventId": i["event_id"]} for i in items]
    return {"searchEventsV2": {"records": records, "meta": {"total": total, "snapshotId": snapshot_id}}}


@pytest.fixture
def patch_skylight_clients(mocker, state_manager):
    """Patch the client/header builders and state so get_skylight_events can be
    driven purely through execute_gql_query."""
    state_manager.get_state.return_value = None
    mocker.patch("app.actions.client.state_manager", state_manager)
    mocker.patch("app.actions.client.build_request_header", return_value=mocker.MagicMock(dict=lambda: {}))
    build_client = mocker.patch("app.actions.client.build_graphql_client", return_value=mocker.MagicMock())
    mocker.patch(
        "app.actions.client.map_event_type",
        return_value=mocker.MagicMock(skylight_event_type="fishing_activity_history"),
    )
    # No AOI list by default: an unusable list means the pull runs unfiltered, so
    # tests here drive paging alone. The validation tests patch this explicitly.
    search_aois_mock = mocker.patch("app.actions.client.search_aois", return_value=[])
    return {
        "state_manager": state_manager,
        "build_client": build_client,
        "search_aois": search_aois_mock,
    }


@pytest.mark.asyncio
async def test_get_skylight_events_stops_at_last_page_via_meta(mocker, integration, auth, patch_skylight_clients):
    # total=3, pageSize=2 -> 2 pages. Loop must stop after page 2 (no empty 3rd request).
    exec_mock = mocker.patch(
        "app.actions.client.execute_gql_query",
        side_effect=[
            _page([{"event_id": "e1"}, {"event_id": "e2"}], total=3, page_num=1),
            _page([{"event_id": "e3"}], total=3, page_num=2),
        ],
    )

    events, _ = await get_skylight_events(integration, _PullCfg(), auth)

    assert exec_mock.call_count == 2
    assert len(events["aoi1"]) == 3


@pytest.mark.asyncio
async def test_get_skylight_events_stops_the_aoi_at_the_first_failed_page(mocker, integration, auth, patch_skylight_clients):
    # total=6, pageSize=2 -> 3 pages, oldest-update-first. Page 2 fails, so the run
    # must stop with only page 1: carrying on to page 3 would push the saved cursor
    # (the newest updatedAt collected) past every page-2 event and lose them for good.
    exec_mock = mocker.patch(
        "app.actions.client.execute_gql_query",
        side_effect=[
            _page([{"event_id": "e1"}, {"event_id": "e2"}], total=6, page_num=1),
            TransportQueryError("boom", errors=[{"message": "boom"}]),
            _page([{"event_id": "e5"}, {"event_id": "e6"}], total=6, page_num=3),
        ],
    )

    events, _ = await get_skylight_events(integration, _PullCfg(), auth)

    assert exec_mock.call_count == 2, "must not request page 3 after page 2 failed"
    collected = [e["event_id"] for e in events["aoi1"]]
    assert collected == ["e1", "e2"], "only the contiguous prefix is kept"


@pytest.mark.asyncio
async def test_get_skylight_events_breaks_when_first_page_fails(mocker, integration, auth, patch_skylight_clients):
    # Page 1 fails before total_pages is known -> give up the AOI (no unbounded loop).
    exec_mock = mocker.patch(
        "app.actions.client.execute_gql_query",
        side_effect=[TransportQueryError("boom", errors=[{"message": "boom"}])],
    )

    events, _ = await get_skylight_events(integration, _PullCfg(), auth)

    assert exec_mock.call_count == 1
    assert events["aoi1"] == []


@pytest.mark.asyncio
async def test_get_skylight_events_logs_and_skips_server_error(mocker, integration, auth, patch_skylight_clients):
    # A 500 (TransportServerError) on a later page is logged and skipped, not swallowed silently.
    exec_mock = mocker.patch(
        "app.actions.client.execute_gql_query",
        side_effect=[
            _page([{"event_id": "e1"}, {"event_id": "e2"}], total=4, page_num=1),
            TransportServerError("500 Internal Server Error"),
        ],
    )
    log = mocker.patch("app.actions.client.logger")

    events, _ = await get_skylight_events(integration, _PullCfg(), auth)

    assert exec_mock.call_count == 2
    assert [e["event_id"] for e in events["aoi1"]] == ["e1", "e2"]
    # The failed page must be logged with attention_needed so it surfaces in activity logs.
    assert any(
        "failed for AOI" in str(call) and call.kwargs.get("extra", {}).get("attention_needed")
        for call in log.error.call_args_list
    )


@pytest.mark.asyncio
async def test_get_skylight_events_resets_none_counter_after_success(mocker, integration, auth, patch_skylight_clients):
    # Non-consecutive Nones must NOT abort: a None on page 1 (recovered via retry)
    # should not count against a later None on page 3.
    exec_mock = mocker.patch(
        "app.actions.client.execute_gql_query",
        side_effect=[
            _page(None, total=6, page_num=1),                                  # page 1: None -> retry
            _page([{"event_id": "e1"}, {"event_id": "e2"}], total=6, page_num=1),  # page 1 retry: ok
            _page([{"event_id": "e3"}, {"event_id": "e4"}], total=6, page_num=2),  # page 2: ok
            _page(None, total=6, page_num=3),                                  # page 3: None -> retry (counter was reset)
            _page([{"event_id": "e5"}, {"event_id": "e6"}], total=6, page_num=3),  # page 3 retry: ok
        ],
    )

    events, _ = await get_skylight_events(integration, _PullCfg(), auth)

    # If the counter weren't reset, the page-3 None would have aborted the AOI.
    assert exec_mock.call_count == 5
    assert [e["event_id"] for e in events["aoi1"]] == ["e1", "e2", "e3", "e4", "e5", "e6"]


@pytest.mark.asyncio
async def test_get_skylight_events_retries_once_on_none_with_new_client(mocker, integration, auth, patch_skylight_clients):
    # First response has None items -> clear token, rebuild client, retry the same page.
    exec_mock = mocker.patch(
        "app.actions.client.execute_gql_query",
        side_effect=[
            _page(None, total=1, page_num=1),
            _page([{"event_id": "e1"}], total=1, page_num=1),
        ],
    )

    events, _ = await get_skylight_events(integration, _PullCfg(), auth)

    assert exec_mock.call_count == 2
    patch_skylight_clients["state_manager"].delete_state.assert_called_once()
    assert len(events["aoi1"]) == 1
    # build_graphql_client: 1 auth client + 1 initial gql client + 1 rebuilt on retry
    assert patch_skylight_clients["build_client"].call_count == 3


@pytest.mark.asyncio
async def test_get_skylight_events_gives_up_after_two_none_responses(mocker, integration, auth, patch_skylight_clients):
    exec_mock = mocker.patch(
        "app.actions.client.execute_gql_query",
        side_effect=[
            _page(None, total=1, page_num=1),
            _page(None, total=1, page_num=1),
        ],
    )

    events, _ = await get_skylight_events(integration, _PullCfg(), auth)

    assert exec_mock.call_count == 2
    assert events["aoi1"] == []


@pytest.mark.asyncio
async def test_get_skylight_events_handles_null_meta(mocker, integration, auth, patch_skylight_clients):
    # A null meta must not crash the AOI or discard events already collected.
    exec_mock = mocker.patch(
        "app.actions.client.execute_gql_query",
        side_effect=[{"searchEventsV2": {"records": [{"eventId": "e1"}], "meta": None}}],
    )

    events, _ = await get_skylight_events(integration, _PullCfg(), auth)

    assert exec_mock.call_count == 1
    assert [e["event_id"] for e in events["aoi1"]] == ["e1"]


@pytest.mark.asyncio
async def test_get_skylight_events_sends_v2_paging_params_and_snapshot(mocker, integration, auth, patch_skylight_clients):
    # total=3, pageSize=2 -> 2 pages. Page 1 has no snapshotId yet; page 2 must
    # echo the snapshotId from page 1 and advance the offset by pageSize.
    exec_mock = mocker.patch(
        "app.actions.client.execute_gql_query",
        side_effect=[
            _page([{"event_id": "e1"}, {"event_id": "e2"}], total=3, snapshot_id="snap-abc"),
            _page([{"event_id": "e3"}], total=3, snapshot_id="snap-abc"),
        ],
    )

    await get_skylight_events(integration, _PullCfg(), auth)

    first, second = [call.args[2] for call in exec_mock.call_args_list]
    assert first["eventTypes"] == ["fishing_activity_history"]
    assert first["aoiId"] == "aoi1"
    assert first["limit"] == 2 and first["offset"] == 0 and first["snapshotId"] is None
    assert second["limit"] == 2 and second["offset"] == 2 and second["snapshotId"] == "snap-abc"
    for params in (first, second):
        assert "pageSize" not in params and "pageNum" not in params
        assert params["updated"] is None   # first run: no cursor yet


@pytest.mark.asyncio
async def test_get_skylight_events_uses_updated_since_cursor(mocker, integration, auth, patch_skylight_clients):
    patch_skylight_clients["state_manager"].get_state.return_value = {"updated_since": "2026-09-08T10:00:00+00:00"}
    exec_mock = mocker.patch("app.actions.client.execute_gql_query", side_effect=[_page([{"event_id": "e1"}], total=1)])

    await get_skylight_events(integration, _PullCfg(), auth)

    params = exec_mock.call_args.args[2]
    assert params["updated"] == {"gte": "2026-09-08T10:00:00+00:00"}
    # The start-time window is still applied as the outer bound (never the cursor).
    assert params["startTime"] < "2026-09-08T10:00:00+00:00" or params["startTime"] > "2026-09-08"


@pytest.mark.asyncio
async def test_get_skylight_events_migrates_legacy_start_time_cursor(mocker, integration, auth, patch_skylight_clients):
    # State written by the previous version only has start_time; use it as the
    # initial `updated` cursor instead of re-pulling the whole window.
    patch_skylight_clients["state_manager"].get_state.return_value = {"start_time": "2026-09-08 09:30:00+00:00"}
    exec_mock = mocker.patch("app.actions.client.execute_gql_query", side_effect=[_page([{"event_id": "e1"}], total=1)])

    await get_skylight_events(integration, _PullCfg(), auth)

    assert exec_mock.call_args.args[2]["updated"] == {"gte": "2026-09-08T09:30:00+00:00"}


def test_query_sorts_oldest_update_first_and_filters_on_updated():
    import app.actions.client as skylight_client
    import inspect
    src = inspect.getsource(skylight_client.get_skylight_events)
    assert "updated: $updated" in src and "sortBy: updated" in src and "sortDirection: asc" in src


def test_latest_update_cursor_picks_newest_and_ignores_missing():
    from app.actions.client import latest_update_cursor
    assert latest_update_cursor([]) is None
    assert latest_update_cursor([{"event_id": "x"}]) is None
    assert latest_update_cursor([
        {"updated_at": "2026-09-08T10:00:00Z"}, {"updated_at": "2026-09-08T12:00:00Z"}, {"event_id": "no-stamp"},
    ]) == "2026-09-08T12:00:00Z"


@pytest.mark.asyncio
async def test_get_skylight_events_stops_on_empty_page_before_total(mocker, integration, auth, patch_skylight_clients):
    # Skylight caps meta.total, so total_pages can overshoot. An empty page must
    # end the AOI instead of requesting the remaining (empty) pages.
    exec_mock = mocker.patch(
        "app.actions.client.execute_gql_query",
        side_effect=[
            _page([{"event_id": "e1"}, {"event_id": "e2"}], total=10),
            _page([], total=10),
            _page([{"event_id": "never"}], total=10),
        ],
    )

    events, _ = await get_skylight_events(integration, _PullCfg(), auth)

    assert exec_mock.call_count == 2
    assert [e["event_id"] for e in events["aoi1"]] == ["e1", "e2"]


@pytest.mark.asyncio
async def test_get_skylight_events_drains_the_backlog_when_total_hits_skylight_cap(mocker, integration, auth, patch_skylight_clients):
    # A backlog at or beyond the cap must NOT skip the AOI: skipping keeps the
    # cursor where it is, so the identical query hits the cap again every run and
    # the AOI never delivers anything. Results are oldest-update-first, so the run
    # keeps this page and the cursor lets the next run continue.
    exec_mock = mocker.patch(
        "app.actions.client.execute_gql_query",
        side_effect=[
            _page([{"event_id": "e1"}, {"event_id": "e2"}], total=10000, page_num=1),
            _page([{"event_id": "e3"}], total=10000, page_num=2),
            _page([], total=10000, page_num=3),
        ],
    )
    log = mocker.patch("app.actions.client.logger")

    events, _ = await get_skylight_events(integration, _PullCfg(), auth)

    assert exec_mock.call_count == 3
    assert [e["event_id"] for e in events["aoi1"]] == ["e1", "e2", "e3"]
    cap_warnings = [
        call for call in log.warning.call_args_list
        if "backlog" in str(call) and call.kwargs.get("extra", {}).get("attention_needed")
    ]
    assert len(cap_warnings) == 1
    assert "Skipping AOI" not in str(cap_warnings[0])


@pytest.mark.asyncio
async def test_get_skylight_events_skips_aoi_ids_the_account_cannot_see(mocker, integration, auth, patch_skylight_clients):
    # Skylight ignores an unknown aoiId and returns worldwide events, so an id the
    # account can't see is dropped before any query is made.
    patch_skylight_clients["search_aois"].return_value = [{"id": "aoi2"}]
    exec_mock = mocker.patch("app.actions.client.execute_gql_query", side_effect=[_page([], total=0)])
    log = mocker.patch("app.actions.client.logger")

    events, _ = await get_skylight_events(integration, _PullCfg(), auth)

    assert "aoi1" not in events
    assert not exec_mock.called
    errors = [
        call for call in log.error.call_args_list
        if "unknown AOI id" in str(call) and call.kwargs.get("extra", {}).get("attention_needed")
    ]
    assert len(errors) == 1


@pytest.mark.asyncio
async def test_get_skylight_events_queries_normally_when_the_aoi_is_known(mocker, integration, auth, patch_skylight_clients):
    patch_skylight_clients["search_aois"].return_value = [{"id": "aoi1"}, {"id": "aoi2"}]
    exec_mock = mocker.patch(
        "app.actions.client.execute_gql_query",
        side_effect=[_page([{"event_id": "e1"}], total=1)],
    )

    events, _ = await get_skylight_events(integration, _PullCfg(), auth)

    assert exec_mock.call_count == 1
    assert [e["event_id"] for e in events["aoi1"]] == ["e1"]


@pytest.mark.asyncio
async def test_get_skylight_events_continues_when_aoi_listing_fails(mocker, integration, auth, patch_skylight_clients):
    # The validation call is a guard, not a gate: if it fails the pull still runs.
    patch_skylight_clients["search_aois"].side_effect = Exception("boom")
    exec_mock = mocker.patch(
        "app.actions.client.execute_gql_query",
        side_effect=[_page([{"event_id": "e1"}], total=1)],
    )

    events, _ = await get_skylight_events(integration, _PullCfg(), auth)

    assert exec_mock.call_count == 1
    assert [e["event_id"] for e in events["aoi1"]] == ["e1"]


@pytest.mark.asyncio
async def test_get_skylight_events_no_cap_warning_below_cap(mocker, integration, auth, patch_skylight_clients):
    mocker.patch(
        "app.actions.client.execute_gql_query",
        side_effect=[_page([{"event_id": "e1"}], total=1)],
    )
    log = mocker.patch("app.actions.client.logger")

    await get_skylight_events(integration, _PullCfg(), auth)

    assert not any("result cap" in str(call) for call in log.warning.call_args_list)


@pytest.mark.asyncio
async def test_get_skylight_events_clamps_last_page_to_result_cap(mocker, integration, auth, patch_skylight_clients):
    # Skylight rejects offset + limit > 10000. A total just under the cap (so the
    # skip-the-AOI guardrail does not fire) with page size 3000 still spans four
    # pages, and the last must ask for limit 1000 at offset 9000, not 3000.
    class _BigPageCfg(_PullCfg):
        pageSize = 3000

    def page(n):
        return _page([{"event_id": f"e{n}"}], total=9999)
    exec_mock = mocker.patch("app.actions.client.execute_gql_query", side_effect=[page(1), page(2), page(3), page(4)])

    await get_skylight_events(integration, _BigPageCfg(), auth)

    requests = [(c.args[2]["offset"], c.args[2]["limit"]) for c in exec_mock.call_args_list]
    assert requests == [(0, 3000), (3000, 3000), (6000, 3000), (9000, 1000)]
    assert all(offset + limit <= 10000 for offset, limit in requests)


# --- normalize_v2_event (v2 record -> v1 layout) ---


def _v2_vessel(**overrides):
    vessel = {
        "vesselId": "412416076", "name": "23839", "mmsi": 412416076, "imo": None,
        "countryCode": ["CHN"], "trackId": "B:412416076:1697504108", "category": "fishing",
        "subcategory": None, "vesselType": "FISHING", "gfwVesselId": "78450769f", "displayCountry": "China",
        "length": None,
    }
    vessel.update(overrides)
    return vessel


def test_normalize_v2_event_maps_record_and_vessel_to_v1_keys():
    record = {
        "eventId": "12bb22aa", "eventType": "fishing_activity_history",
        "createdAt": "2026-09-08T22:37:36Z", "updatedAt": "2026-09-08T22:37:37Z",
        "start": {"point": {"lat": 31.1, "lon": 126.1}, "time": "2026-09-08T10:53:18Z"},
        "end": {"point": {"lat": 30.9, "lon": 126.3}, "time": "2026-09-08T19:56:52Z"},
        "vessels": {"vessel0": _v2_vessel(), "vessel1": None},
        "eventDetails": {"__typename": "FishingEventDetails", "fishingScore": 0.87},
    }

    result = normalize_v2_event(record)

    assert result["event_id"] == "12bb22aa"
    assert result["event_type"] == "fishing_activity_history"
    assert result["updated_at"] == "2026-09-08T22:37:37Z"   # top-level copy used for the pull cursor
    assert result["start"] == record["start"] and result["end"] == record["end"]
    # v1 vessel key names, keyed vessel_0; vessel_1 omitted when v2 returned none.
    assert set(result["vessels"]) == {"vessel_0"}
    vessel = result["vessels"]["vessel_0"]
    assert vessel["vessel_id"] == "412416076"
    assert vessel["name"] == "23839"
    assert vessel["mmsi"] == 412416076
    assert vessel["type"] == "FISHING"
    assert vessel["display_country"] == "China"
    assert vessel["category"] == "fishing"
    assert vessel["country_filter"] == ["CHN"]   # v1 country_filter == v2 countryCode
    # v2-only vessel fields pass through in snake_case.
    assert vessel["track_id"] == "B:412416076:1697504108"
    assert vessel["gfw_vessel_id"] == "78450769f"
    assert "vessel_type" not in vessel and "vesselId" not in vessel
    # v2-only detail fields pass through; GraphQL meta is dropped.
    assert result["event_details"]["fishing_score"] == 0.87
    assert "visit_type" not in result["event_details"]   # v1 constant carried no info; not emitted
    assert result["event_details"]["created_at"] == "2026-09-08T22:37:36Z"
    assert result["event_details"]["updated_at"] == "2026-09-08T22:37:37Z"
    assert "__typename" not in result["event_details"]


def test_normalize_v2_event_maps_speed_range_and_aoi_visit_details_to_v1_keys():
    speed = normalize_v2_event({
        "eventId": "s1", "eventType": "speed_range", "vessels": {"vessel0": None, "vessel1": None},
        "eventDetails": {"averageSpeed": 3.22, "distance": 25.5, "durationSec": 16013},
    })
    assert speed["event_details"] == {"average_speed": 3.22, "distance": 25.5, "duration": 16013}

    visit = normalize_v2_event({
        "eventId": "a1", "eventType": "aoi_visit", "vessels": {"vessel0": None, "vessel1": None},
        "eventDetails": {"entrySpeed": 11.2, "entryHeading": 132, "endHeading": None},
    })
    # v1 serialised these as strings; end_heading None stays None (transform drops it).
    assert visit["event_details"] == {"entry_speed": "11.2", "entry_heading": "132", "end_heading": None}


def test_normalize_v2_event_detection_sets_v1_data_source_correlated_and_image_url():
    dark = normalize_v2_event({
        "eventId": "d1", "eventType": "eo_sentinel2", "vessels": {"vessel0": None, "vessel1": None},
        "eventDetails": {"imageUrl": "https://cdn/x.png", "dataSource": "sentinel2", "detectionType": "dark", "score": 0.99, "radianceNw": None},
    })
    details = dark["event_details"]
    assert details["image_url"] == "https://cdn/x.png"
    assert details["data_source"] == "sentinel2"
    assert details["correlated"] is False
    assert details["detection_type"] == "dark"
    assert details["score"] == 0.99
    # No vessel -> vessel_0 is None so transform fills EMPTY_VESSEL_DICT, as in v1.
    assert dark["vessels"] == {"vessel_0": None}

    ais = normalize_v2_event({
        "eventId": "d2", "eventType": "viirs", "vessels": {"vessel0": _v2_vessel(), "vessel1": None},
        "eventDetails": {"imageUrl": "https://cdn/y.jpeg", "dataSource": "noaa_21", "detectionType": "ais_correlated", "radianceNw": 26.5},
    })
    assert ais["event_details"]["correlated"] is True
    assert ais["event_details"]["data_source"] == "noaa_21"
    assert ais["event_details"]["radiance_nw"] == 26.5
    assert ais["vessels"]["vessel_0"]["name"] == "23839"


def test_normalize_v2_event_rendezvous_keeps_second_vessel_as_vessel_1():
    # The empty event_details below is correct, not a gap: StandardRendezvousEventDetails
    # exposes no fields in v2, and v1 returned average_speed/distance/duration as null
    # for this type anyway (verified live, 2026-09-10). See the query in client.py.
    result = normalize_v2_event({
        "eventId": "r1", "eventType": "standard_rendezvous",
        "vessels": {"vessel0": _v2_vessel(name="BIEN DONG"), "vessel1": _v2_vessel(name="LANH LX", mmsi=574345679)},
        "eventDetails": {"__typename": "StandardRendezvousEventDetails"},
    })
    assert result["vessels"]["vessel_0"]["name"] == "BIEN DONG"
    assert result["vessels"]["vessel_1"]["name"] == "LANH LX"
    assert result["vessels"]["vessel_1"]["mmsi"] == 574345679
    assert result["event_details"] == {}


def test_normalize_v2_event_handles_missing_vessels_and_details():
    result = normalize_v2_event({"eventId": "x", "eventType": "fishing_activity_history"})
    assert result["vessels"] == {"vessel_0": None}
    assert result["event_details"] == {}
    assert result["start"] is None and result["end"] is None


def test_transform_of_normalized_v2_event_keeps_v1_er_keys(skylight_client):
    # End-to-end contract: a v2 record, once normalized, produces the same
    # event_details key names EarthRanger received from the v1 API.
    record = {
        "eventId": "S1C_...SAFE_31", "eventType": "sar_sentinel1",
        "start": {"point": {"lat": 43.04, "lon": 31.63}, "time": "2026-09-08T15:51:23Z"},
        "end": {"point": {"lat": 43.04, "lon": 31.63}, "time": "2026-09-08T15:51:23Z"},
        "vessels": {"vessel0": _v2_vessel(name="PSV YESILKOY", imo=9709130), "vessel1": None},
        "eventDetails": {"imageUrl": "https://cdn/s1.png", "dataSource": "sentinel1", "detectionType": "ais_correlated", "score": 0.99},
    }
    config = [{"skylight_event_type": ["viirs", "sar_sentinel1"], "event_title": "Vessel Detection", "event_type": "detection_alert_rep"}]

    result = transform(config, normalize_v2_event(record))

    assert result["title"] == "Vessel Detection"
    assert result["event_type"] == "detection_alert_rep"
    details = result["event_details"]
    assert details["vessel_0_name"] == "PSV YESILKOY"
    assert details["vessel_0_mmsi"] == 412416076
    assert details["vessel_0_display_country"] == "China"
    assert details["vessel_0_type"] == "FISHING"
    assert details["vessel_0_imo"] == 9709130
    assert details["image_url"] == "https://cdn/s1.png"
    assert details["data_source"] == "sentinel1"
    assert details["correlated"] is True
    assert details["event_id"] == "S1C_...SAFE_31"
    assert "entry_link" in details
    assert not any(k.startswith("vessel_1_") for k in details)


# --- map_event_type ---

def test_map_event_type_valid_returns_eventtype(mocker):
    integration = mocker.MagicMock(additional=None)  # falls back to DEFAULT_EVENT_MAPPING
    result = map_event_type(integration, "fishing")
    assert result.event_type == "fishing_alert_rep"
    assert result.skylight_event_type == "fishing_activity_history"


def test_map_event_type_unknown_raises(mocker):
    integration = mocker.MagicMock(additional=None)
    with pytest.raises(PullEventsBadConfigException):
        map_event_type(integration, "not_a_real_event_type")


# --- error-response logging hook ---

def _make_response(mocker, status_code):
    request = mocker.MagicMock(
        method="POST",
        url="https://api.skylight.earth/graphql",
        headers={"Authorization": "Bearer super-secret-token", "Content-Type": "application/json"},
        content=b'{"query": "getRendedzvousExternal"}',
    )
    return mocker.MagicMock(
        status_code=status_code,
        headers={"content-type": "application/json"},
        text="Internal Server Error",
        request=request,
    )


def test_redact_headers_masks_authorization():
    redacted = _redact_headers({"Authorization": "Bearer secret-123", "Content-Type": "application/json"})
    assert redacted["Authorization"] == "Bearer ***redacted***"
    assert redacted["Content-Type"] == "application/json"


def test_redact_headers_is_case_insensitive():
    assert _redact_headers({"authorization": "Bearer x"})["authorization"] == "Bearer ***redacted***"


def test_log_skylight_error_response_logs_full_detail_on_error(mocker):
    response = _make_response(mocker, 500)
    log = mocker.patch("app.actions.client.logger")

    _log_skylight_error_response(response)

    response.read.assert_called_once()
    log.error.assert_called_once()
    # The bearer token must be redacted, never logged in the clear.
    logged = str(log.error.call_args)
    assert "super-secret-token" not in logged
    assert "***redacted***" in logged


def test_log_skylight_error_response_silent_on_success(mocker):
    response = _make_response(mocker, 200)
    log = mocker.patch("app.actions.client.logger")

    _log_skylight_error_response(response)

    log.error.assert_not_called()
    response.read.assert_not_called()


# --- _is_token_expired ---

def test_is_token_expired_returns_false_for_valid_token():
    future_exp = int(time.time()) + 3600  # expires in 1 hour
    token = _make_jwt(future_exp)
    assert _is_token_expired(token) is False


def test_is_token_expired_returns_true_within_skew_window():
    # Token expires in 30s — within the 60s skew, so should be treated as expired
    soon_exp = int(time.time()) + (_TOKEN_EXPIRY_SKEW_SECONDS - 30)
    token = _make_jwt(soon_exp)
    assert _is_token_expired(token) is True


def test_is_token_expired_returns_true_for_expired_token():
    past_exp = int(time.time()) - 60
    token = _make_jwt(past_exp)
    assert _is_token_expired(token) is True


def test_is_token_expired_returns_true_for_unparsable_token():
    assert _is_token_expired("not.a.jwt") is True


def test_is_token_expired_returns_true_for_empty_token():
    assert _is_token_expired("") is True


# --- build_request_header ---

@pytest.mark.asyncio
async def test_build_request_header_reuses_valid_cached_token(mocker, integration, auth, gql_client, state_manager):
    future_exp = int(time.time()) + 3600
    cached = {"access_token": _make_jwt(future_exp), "expires_in": 3600, "token_type": "Bearer"}
    state_manager.get_state.return_value = cached
    mocker.patch("app.actions.client.state_manager", state_manager)
    get_auth = mocker.patch("app.actions.client.get_authentication_token")

    header = await build_request_header(integration, auth, gql_client)

    get_auth.assert_not_called()
    assert "Bearer" in header.Authorization


@pytest.mark.asyncio
async def test_build_request_header_refreshes_expired_token(mocker, integration, auth, gql_client, state_manager):
    past_exp = int(time.time()) - 60
    cached = {"access_token": _make_jwt(past_exp), "expires_in": 3600, "token_type": "Bearer"}
    state_manager.get_state.return_value = cached
    mocker.patch("app.actions.client.state_manager", state_manager)

    from app.actions.client import SkylightGetTokenResponse
    new_token = SkylightGetTokenResponse(
        access_token=_make_jwt(int(time.time()) + 3600),
        expires_in=3600,
        token_type="Bearer",
    )
    get_auth = mocker.patch("app.actions.client.get_authentication_token", return_value=new_token)

    header = await build_request_header(integration, auth, gql_client)

    get_auth.assert_called_once()
    state_manager.set_state.assert_called_once()
    assert "Bearer" in header.Authorization


@pytest.mark.asyncio
async def test_build_request_header_fetches_token_when_no_cache(mocker, integration, auth, gql_client, state_manager):
    state_manager.get_state.return_value = None
    mocker.patch("app.actions.client.state_manager", state_manager)

    from app.actions.client import SkylightGetTokenResponse
    new_token = SkylightGetTokenResponse(
        access_token=_make_jwt(int(time.time()) + 3600),
        expires_in=3600,
        token_type="Bearer",
    )
    get_auth = mocker.patch("app.actions.client.get_authentication_token", return_value=new_token)

    header = await build_request_header(integration, auth, gql_client)

    get_auth.assert_called_once()
    state_manager.set_state.assert_called_once()
    assert "Bearer" in header.Authorization


@pytest.mark.asyncio
async def test_action_process_events_per_aoi_success(mocker, integration, process_events_config, mock_publish_event):
    mocker.patch("app.services.state.IntegrationStateManager.get_state", return_value=None)
    mocker.patch("app.services.state.IntegrationStateManager.set_state", return_value=None)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mocker.patch("app.services.action_scheduler.publish_event", mock_publish_event)
    mocker.patch("app.actions.handlers.transform", return_value={"event_id": "event1"})
    mocker.patch(
        "app.actions.handlers.gundi_tools.send_events_to_gundi",
        return_value=[{"object_id": "event1"}, {"object_id": "event2"}],
    )
    mocker.patch("app.actions.handlers.process_attachments", return_value=None)
    mocker.patch("app.actions.handlers.save_events_state", return_value=None)

    result = await action_process_events_per_aoi(integration, process_events_config)
    assert result["events_processed"] == 2


@pytest.mark.asyncio
async def test_action_process_events_per_aoi_filters_empty_transforms(mocker, integration, process_events_config, mock_publish_event):
    # transform() returns {} for skipped events; those must not leak into the
    # Gundi batch. process_events_config has two events; the first is skipped.
    mocker.patch("app.services.state.IntegrationStateManager.get_state", return_value=None)
    mocker.patch("app.services.state.IntegrationStateManager.set_state", return_value=None)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mocker.patch("app.services.action_scheduler.publish_event", mock_publish_event)
    mocker.patch("app.actions.handlers.transform", side_effect=[{}, {"event_id": "event2"}])
    send_mock = mocker.patch(
        "app.actions.handlers.gundi_tools.send_events_to_gundi", return_value=[{"object_id": "event2"}]
    )
    mocker.patch("app.actions.handlers.process_attachments", return_value=None)
    mocker.patch("app.actions.handlers.save_events_state", return_value=None)

    await action_process_events_per_aoi(integration, process_events_config)

    sent_events = send_mock.call_args.kwargs["events"]
    assert {} not in sent_events
    assert sent_events == [{"event_id": "event2"}]

@pytest.mark.asyncio
async def test_action_process_events_per_aoi_failure(mocker, integration, process_events_config, mock_publish_event):
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mocker.patch("app.services.action_scheduler.publish_event", mock_publish_event)
    mocker.patch("app.actions.handlers.transform", return_value={"event_id": "event1"})
    mocker.patch("app.actions.handlers.gundi_tools.send_events_to_gundi", side_effect=httpx.HTTPError("Error"))

    with pytest.raises(httpx.HTTPError):
        await action_process_events_per_aoi(integration, process_events_config)

@pytest.mark.asyncio
async def test_action_pull_events_triggers_process_events_per_aoi(mocker, integration, pull_events_config, mock_publish_event):
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mocker.patch("app.services.action_scheduler.publish_event", mock_publish_event)
    mocker.patch("app.actions.client.get_skylight_events", return_value=({"aoi": [{"event_id": "event1"}]}, []))
    mocker.patch("app.actions.client.get_auth_config", return_value=None)
    mocker.patch("app.services.state.IntegrationStateManager.get_state", return_value=None)
    mocker.patch("app.services.state.IntegrationStateManager.set_state", return_value=None)
    mock_trigger_action = mocker.patch("app.actions.handlers.trigger_action", return_value=None)

    result = await action_pull_events(integration, pull_events_config)
    assert result["process_events_per_aoi_action_triggered"] == 1
    config = mock_trigger_action.call_args.kwargs["config"]
    assert (config.integration_id, config.aoi, config.events, config.updated_config_data) == (
        integration.id, "aoi", [{"event_id": "event1"}], []
    )
    # The batch is handed out with the id it must report delivery under, and
    # that id is the one recorded in the AOI's chunk plan.
    assert config.chunk_id
    assert result["details"]["chunk_plan"] == {"aoi": [config.chunk_id]}


@pytest.mark.asyncio
async def test_action_pull_events_records_a_plan_and_never_advances_its_own_cursor(
        mocker, integration, pull_events_config, mock_publish_event
):
    # Triggering a sub-action only confirms the command was published, not that
    # its events reached EarthRanger. So this run writes no cursor at all: it
    # records the plan it handed out, and the NEXT run advances the cursor over
    # whatever the sub-actions confirmed.
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.actions.client.get_skylight_events", return_value=({
        "aoi1": [{"event_id": "a", "updated_at": "2026-09-08T10:00:00Z"}, {"event_id": "b", "updated_at": "2026-09-08T11:00:00Z"}],
        "aoi2": [{"event_id": "c"}],   # no update stamps -> nothing to advance to
    }, []))
    mocker.patch("app.actions.client.get_auth_config", return_value=None)
    mocker.patch("app.services.state.IntegrationStateManager.get_state", return_value=None)
    set_state = mocker.patch("app.services.state.IntegrationStateManager.set_state", return_value=None)
    mocker.patch("app.actions.handlers.trigger_action", return_value=None)

    result = await action_pull_events(integration, pull_events_config)

    written = {call.args[3]: call.args[2] for call in set_state.call_args_list}
    assert set(written) == {"_plan.aoi1", "_plan.aoi2"}
    assert not any("updated_since" in state for state in written.values())
    assert written["_plan.aoi1"]["final"] == "2026-09-08T11:00:00Z"
    assert [chunk["cursor"] for chunk in written["_plan.aoi1"]["chunks"]] == ["2026-09-08T11:00:00Z"]
    assert written["_plan.aoi2"]["final"] is None
    assert "cursors" not in result["details"]


@pytest.mark.asyncio
async def test_action_pull_events_gives_each_aoi_distinct_chunk_ids(
        mocker, integration, pull_events_config, mock_publish_event
):
    # Completion markers live in one flat keyspace, so if two AOIs numbered
    # their chunks from zero under the same run id they would read each other's
    # markers and a delivered chunk in one AOI would advance the other's cursor.
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.actions.client.get_skylight_events", return_value=({
        "aoi1": [{"event_id": "a", "updated_at": "2026-09-08T10:00:00Z"}],
        "aoi2": [{"event_id": "b", "updated_at": "2026-09-08T10:00:00Z"}],
    }, []))
    mocker.patch("app.actions.client.get_auth_config", return_value=None)
    mocker.patch("app.services.state.IntegrationStateManager.get_state", return_value=None)
    mocker.patch("app.services.state.IntegrationStateManager.set_state", return_value=None)
    mocker.patch("app.actions.handlers.trigger_action", return_value=None)

    result = await action_pull_events(integration, pull_events_config)

    ids = [chunk_id for chunk_ids in result["details"]["chunk_plan"].values() for chunk_id in chunk_ids]
    assert len(ids) == 2
    assert len(set(ids)) == 2


def test_batch_events_by_payload_size_keeps_small_lists_in_one_batch():
    events = [{"event_id": "event1"}, {"event_id": "event2"}]

    assert batch_events_by_payload_size(events, max_payload_bytes=10_000) == [events]


def test_batch_events_by_payload_size_splits_events_over_limit():
    events = [{"event_id": f"event{i}", "data": "x" * 100} for i in range(5)]
    event_size = len(json.dumps(events[0]).encode("utf-8"))

    batches = batch_events_by_payload_size(events, max_payload_bytes=event_size * 2)

    assert len(batches) == 3
    assert all(len(batch) <= 2 for batch in batches)
    assert [event for batch in batches for event in batch] == events


def test_batch_events_by_payload_size_gives_oversized_event_its_own_batch():
    events = [{"event_id": "event1", "data": "x" * 500}, {"event_id": "event2"}]

    batches = batch_events_by_payload_size(events, max_payload_bytes=100)

    assert batches == [[events[0]], [events[1]]]


@pytest.mark.asyncio
async def test_action_pull_events_splits_large_aoi_into_multiple_triggers(
        mocker, integration, pull_events_config, mock_publish_event
):
    # A single PubSub command message embedding every event for an AOI can
    # exceed PubSub's 10MB publish limit (400 Bad Request in production), so
    # large AOIs must be dispatched as multiple bounded messages.
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mocker.patch("app.services.action_scheduler.publish_event", mock_publish_event)
    events = [{"event_id": f"event{i}", "data": "x" * 200} for i in range(10)]
    mocker.patch("app.actions.client.get_skylight_events", return_value=({"aoi": events}, []))
    mocker.patch("app.actions.client.get_auth_config", return_value=None)
    mocker.patch("app.services.state.IntegrationStateManager.get_state", return_value=None)
    mocker.patch("app.services.state.IntegrationStateManager.set_state", return_value=None)
    mock_trigger_action = mocker.patch("app.actions.handlers.trigger_action", return_value=None)
    mocker.patch("app.actions.handlers.MAX_TRIGGER_PAYLOAD_BYTES", 500)

    result = await action_pull_events(integration, pull_events_config)

    assert mock_trigger_action.call_count > 1
    dispatched_events = []
    for call in mock_trigger_action.call_args_list:
        config = call.kwargs["config"]
        assert config.aoi == "aoi"
        dispatched_events.extend(config.events)
    assert dispatched_events == events
    assert result["events_extracted"] == 10
    assert result["process_events_per_aoi_action_triggered"] == mock_trigger_action.call_count
    # Every dispatched batch is in the plan, in the same order, under the id the
    # batch was told to report against.
    dispatched_chunk_ids = [call.kwargs["config"].chunk_id for call in mock_trigger_action.call_args_list]
    assert result["details"]["chunk_plan"]["aoi"] == dispatched_chunk_ids


@pytest.mark.asyncio
async def test_process_attachments_success(mocker, integration):
    transformed_data = [{"event_details": {"image_url": "https://example.com/image.png"}}]
    response = [{"object_id": "event1"}]
    mock_image_content = b"image data"

    mock_read_img = mocker.patch("httpx.AsyncClient.get", return_value=mocker.AsyncMock(status_code=200, aread=mocker.AsyncMock(return_value=mock_image_content)))
    mock_send_event_attachments_to_gundi = mocker.patch("app.actions.handlers.gundi_tools.send_event_attachments_to_gundi", return_value=None)

    await process_attachments(transformed_data, response, integration)

    mock_read_img.assert_called_once_with("https://example.com/image.png")
    mock_send_event_attachments_to_gundi.assert_called_once_with(
        event_id="event1",
        attachments=[("image.png", mock_image_content)],
        integration_id=integration.id
    )

@pytest.mark.asyncio
async def test_process_attachments_403_error(mocker, integration):
    transformed_data = [{"event_details": {"image_url": "https://example.com/image.png"}}]
    response = [{"object_id": "event1"}]

    mock_read_img = mocker.patch("httpx.AsyncClient.get", side_effect=httpx.HTTPStatusError("403 Forbidden", request=mocker.Mock(), response=mocker.Mock(status_code=403)))
    mock_log_action_activity = mocker.patch("app.actions.handlers.log_action_activity", return_value=None)

    await process_attachments(transformed_data, response, integration)

    mock_read_img.assert_called_once_with("https://example.com/image.png")
    mock_log_action_activity.assert_called_once()

def test_transform_with_vessel_info():
    data = {
        "event_type": "some_event_type",
        "event_details": {},
        "end": {
            'point':
                {
                    'lat': 5.883240159715233,
                    'lon': 115.69985442805672
                },
            'time': '2025-02-28T02:46:18.489582Z'
        },
        "vessels": {
            "vessel1": {
                "detail1": "value1",
                "detail2": "value2"
            }
        }
    }
    config = [{"skylight_event_type": "some_event_type", "event_title": "Test Event", "event_type": "test_event"}]
    result = transform(config, data)
    assert result["event_details"]["vessel1_detail1"] == "value1"
    assert result["event_details"]["vessel1_detail2"] == "value2"

def test_transform_with_empty_vessel_detail(skylight_client):
    data = {
        "event_type": "some_event_type",
        "event_details": {},
        "end": {
            'point':
                {
                    'lat': 5.883240159715233,
                    'lon': 115.69985442805672
                },
            'time': '2025-02-28T02:46:18.489582Z'
        },
        "vessels": {
            "vessel_0": None
        }
    }
    skylight_client.EMPTY_VESSEL_DICT = {"id": "N/A", "name": "N/A"}
    config = [{"skylight_event_type": "some_event_type", "event_title": "Test Event", "event_type": "test_event"}]
    result = transform(config, data)
    assert result["event_details"]["vessel_0_id"] == "N/A"
    assert result["event_details"]["vessel_0_name"] == "N/A"

def test_transform_without_vessel_info(skylight_client):
    data = {
        "event_type": "some_event_type",
        "event_details": {},
        "end": {
            'point':
                {
                    'lat': 5.883240159715233,
                    'lon': 115.69985442805672
                },
            'time': '2025-02-28T02:46:18.489582Z'
        }
    }
    skylight_client.EMPTY_VESSEL_DICT = {"id": "N/A", "name": "N/A"}
    config = [{"skylight_event_type": "some_event_type", "event_title": "Test Event", "event_type": "test_event"}]
    result = transform(config, data)
    assert result["event_details"]["vessel_0_id"] == "N/A"
    assert result["event_details"]["vessel_0_name"] == "N/A"


# --- Entry alert (aoi_visit) transform ---

_ENTRY_ALERT_CONFIG = [{"skylight_event_type": "aoi_visit", "event_title": "Marine Entry", "event_type": "entry_alert_rep"}]


def test_transform_entry_alert_start_and_end():
    """location and recorded_at come from start; exit_date and duration_in_area computed from end."""
    data = {
        "event_id": "evt1",
        "event_type": "aoi_visit",
        "event_details": {},
        "vessels": {},
        "start": {"point": {"lat": 1.0, "lon": 104.0}, "time": "2026-06-01T00:00:00Z"},
        "end": {"point": {"lat": 1.1, "lon": 104.1}, "time": "2026-06-01T02:30:00Z"},
    }
    result = transform(_ENTRY_ALERT_CONFIG, data)

    assert result["recorded_at"].isoformat().startswith("2026-06-01T00:00:00")
    assert result["location"] == {"lat": 1.0, "lon": 104.0}
    assert result["event_details"]["exit_date"] == "2026-06-01T02:30:00Z"
    assert result["event_details"]["duration_in_area"] == 2.5


def test_transform_entry_alert_start_only_pending():
    """When vessel has not exited, exit_date and duration_in_area are 'Pending'."""
    data = {
        "event_id": "evt2",
        "event_type": "aoi_visit",
        "event_details": {},
        "vessels": {},
        "start": {"point": {"lat": 2.0, "lon": 105.0}, "time": "2026-06-01T06:00:00Z"},
    }
    result = transform(_ENTRY_ALERT_CONFIG, data)

    assert result["recorded_at"].isoformat().startswith("2026-06-01T06:00:00")
    assert result["location"] == {"lat": 2.0, "lon": 105.0}
    assert result["event_details"]["exit_date"] == "Pending"
    assert result["event_details"]["duration_in_area"] == "Pending"


def test_transform_entry_alert_no_start_returns_empty():
    """An entry alert with no start point is invalid — transform returns {}."""
    data = {
        "event_id": "evt3",
        "event_type": "aoi_visit",
        "event_details": {},
        "vessels": {},
        "end": {"point": {"lat": 1.0, "lon": 104.0}, "time": "2026-06-01T02:00:00Z"},
    }
    result = transform(_ENTRY_ALERT_CONFIG, data)
    assert result == {}


# --- v1/v2 parity on real Skylight samples ---
# Captured live on 2026-09-08 from both APIs for the same event ids (staging AOI).
# Guards the contract: what EarthRanger receives from a v2 record must equal
# what it received from the v1 record, for every key v1 produced.

_V1_AOI_VISIT = {
    "event_id": "B:511101710:1755308361:2868795:994615_d8f26fe4-4d81-4365-adf5-b8d3b2a8fcc8_1788890029_aoi_visit",
    "event_type": "aoi_visit",
    "start": {"point": {"lat": -0.7890933333333333, "lon": 107.67064666666667}, "time": "2026-09-08T17:53:49Z"},
    "end": None,
    "vessels": {"vessel_0": {"category": "cargo", "class": "vessel", "country_filter": ["PLW"], "display_country": "Palau",
                             "mmsi": 511101710, "name": "GREEN BAY", "length": 79, "type": "CARGO", "vessel_id": "511101710"}},
    "event_details": {"average_speed": None, "data_source": None, "distance": None, "duration": None, "correlated": None,
                      "image_url": None, "entry_speed": "7.800000190734863", "entry_heading": "307", "end_heading": None,
                      "visit_type": "end"},
}
_V2_AOI_VISIT = {
    "eventId": _V1_AOI_VISIT["event_id"], "eventType": "aoi_visit",
    "start": _V1_AOI_VISIT["start"], "end": None,
    "vessels": {"vessel0": {"vesselId": "511101710", "name": "GREEN BAY", "mmsi": 511101710, "imo": 9373175, "countryCode": ["PLW"],
                            "trackId": "B:511101710:1755308361:2868795:994615", "category": "cargo", "subcategory": None,
                            "vesselType": "CARGO", "gfwVesselId": None, "displayCountry": "Palau", "length": 79, "class": "vessel"},
                "vessel1": None},
    "eventDetails": {"__typename": "AoiVisitEventDetails", "entrySpeed": 7.800000190734863, "entryHeading": 307, "endHeading": None},
}
_V1_SPEED_RANGE = {
    "event_id": "B:563074580:1638492069:2663937:1087021_6a2e736f87c4e6d991424fe3_1788893817.0_speed_range",
    "event_type": "speed_range",
    "start": {"point": {"lat": 1.2555016666666667, "lon": 103.72687333333333}, "time": "2026-09-08T17:54:45Z"},
    "end": {"point": {"lat": 1.2157116666666667, "lon": 103.68442666666667}, "time": "2026-09-08T20:25:26Z"},
    "vessels": {"vessel_0": {"category": "service", "class": "vessel", "country_filter": ["SGP"], "display_country": "Singapore",
                             "mmsi": 563074580, "name": "VISION 227", "length": 24, "type": "TUG", "vessel_id": "563074580"}},
    "event_details": {"average_speed": 1.389130423898282, "data_source": None, "distance": 6.577, "duration": 9221, "correlated": None,
                      "image_url": None, "entry_speed": None, "entry_heading": None, "end_heading": None, "visit_type": "end"},
}
_V2_SPEED_RANGE = {
    "eventId": _V1_SPEED_RANGE["event_id"], "eventType": "speed_range",
    "start": _V1_SPEED_RANGE["start"], "end": _V1_SPEED_RANGE["end"],
    "vessels": {"vessel0": {"vesselId": "563074580", "name": "VISION 227", "mmsi": 563074580, "imo": 9907770, "countryCode": ["SGP"],
                            "trackId": "B:563074580:1638492069:2663937:1087021", "category": "service", "subcategory": None,
                            "vesselType": "TUG", "gfwVesselId": None, "displayCountry": "Singapore", "length": 24, "class": "vessel"},
                "vessel1": None},
    "eventDetails": {"__typename": "SpeedRangeEventDetails", "averageSpeed": 1.389130423898282, "distance": 6.577, "durationSec": 9221},
}
_V1_VIIRS = {
    "event_id": "VJ102DNB_NRT.A2026251.1830.021.2026251220648_3.356_103.939", "event_type": "viirs",
    "start": {"point": {"lat": 3.356, "lon": 103.939}, "time": "2026-09-08T18:30:00Z"},
    "end": {"point": {"lat": 3.356, "lon": 103.939}, "time": "2026-09-08T18:30:00Z"},
    "vessels": {"vessel_0": None},
    "event_details": {"average_speed": None, "data_source": "noaa", "distance": None, "duration": None, "correlated": False,
                      "image_url": "https://cdn.sky-prod-a.skylight.earth/sat-service/viirs-noaa/detections/2026/09/08/x.jpeg",
                      "entry_speed": None, "entry_heading": None, "end_heading": None, "visit_type": "end"},
}
_V2_VIIRS = {
    "eventId": _V1_VIIRS["event_id"], "eventType": "viirs", "start": _V1_VIIRS["start"], "end": _V1_VIIRS["end"],
    "vessels": {"vessel0": None, "vessel1": None},
    "eventDetails": {"__typename": "ViirsEventDetails", "imageUrl": _V1_VIIRS["event_details"]["image_url"], "dataSource": "noaa",
                     "detectionType": "dark", "estimatedLength": None, "frameIds": ["abc"], "heading": None, "radianceNw": 16.67},
}
_PARITY_CONFIG = [
    {"skylight_event_type": "aoi_visit", "event_title": "Marine Entry", "event_type": "entry_alert_rep"},
    {"skylight_event_type": "speed_range", "event_title": "Speed Range", "event_type": "speed_range_alert_rep"},
    {"skylight_event_type": ["viirs", "sar_sentinel1", "eo_sentinel2", "eo_landsat_8_9"], "event_title": "Vessel Detection", "event_type": "detection_alert_rep"},
]


@pytest.mark.parametrize("v1_item, v2_record", [
    (_V1_AOI_VISIT, _V2_AOI_VISIT),
    (_V1_SPEED_RANGE, _V2_SPEED_RANGE),
    (_V1_VIIRS, _V2_VIIRS),
])
def test_v2_record_transforms_to_the_same_er_event_as_v1(v1_item, v2_record):
    from_v1 = transform(_PARITY_CONFIG, v1_item)
    from_v2 = transform(_PARITY_CONFIG, normalize_v2_event(v2_record))

    assert from_v2["title"] == from_v1["title"]
    assert from_v2["event_type"] == from_v1["event_type"]
    assert from_v2["recorded_at"] == from_v1["recorded_at"]
    assert from_v2["location"] == from_v1["location"]
    # Every key v1 produced is present with the identical value (same type too);
    # v2 may add extra keys on top (imo, track_id, radiance_nw, ...).
    for key, value in from_v1["event_details"].items():
        if key == "visit_type":
            continue  # v1 constant "end" on every event; deliberately dropped (no v2 source, no information)
        assert key in from_v2["event_details"], f"missing v1 key {key}"
        assert from_v2["event_details"][key] == value, key
        assert type(from_v2["event_details"][key]) is type(value), key


@pytest.mark.asyncio
async def test_handlers_never_log_the_integration_or_action_config(mocker, integration, pull_events_config, mock_publish_event):
    # On the ephemeral path the integration model and the action config carry the
    # draft's submitted credentials verbatim, so neither may be interpolated into
    # a log line. Pin the entry logs of both operator-triggered actions.
    from app.actions.handlers import action_auth, action_pull_events
    mocker.patch("app.actions.client.build_graphql_client", return_value=mocker.MagicMock())
    mocker.patch("app.actions.client.get_authentication_token", return_value=None)
    mocker.patch("app.actions.client.get_skylight_events", return_value=({}, []))
    mocker.patch("app.actions.client.get_auth_config", return_value=None)
    mocker.patch("app.services.state.IntegrationStateManager.get_state", return_value=None)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    log = mocker.patch("app.actions.handlers.logger")

    await action_auth(integration, mocker.MagicMock())
    await action_pull_events(integration, pull_events_config)

    logged = " ".join(str(c) for c in log.mock_calls)
    assert "action_config" not in logged
    for forbidden in ("configurations", "AuthenticateConfig(", "password"):
        assert forbidden not in logged, forbidden


# --- action_auth: no secrets in logs or responses ---


@pytest.mark.asyncio
async def test_action_auth_success_does_not_log_token(mocker, integration):
    from app.actions.handlers import action_auth
    from app.actions.client import SkylightGetTokenResponse
    mocker.patch("app.actions.client.build_graphql_client", return_value=mocker.MagicMock())
    mocker.patch(
        "app.actions.client.get_authentication_token",
        return_value=SkylightGetTokenResponse(access_token="SECRET-TOKEN-XYZ", expires_in=3600, token_type="Bearer"),
    )
    log = mocker.patch("app.actions.handlers.logger")

    result = await action_auth(integration, mocker.MagicMock())

    assert result == {"valid_credentials": True}
    assert "SECRET-TOKEN-XYZ" not in " ".join(str(c) for c in log.mock_calls)


@pytest.mark.asyncio
async def test_action_auth_reports_invalid_credentials_without_echoing_input(mocker, integration):
    # Skylight answers a bad password with UNAUTHENTICATED; the portal should see
    # a definitive "invalid" plus a readable reason, and never the raw error.
    from app.actions.handlers import action_auth
    from gql.transport.exceptions import TransportQueryError
    mocker.patch("app.actions.client.build_graphql_client", return_value=mocker.MagicMock())
    mocker.patch(
        "app.actions.client.get_authentication_token",
        side_effect=TransportQueryError("boom", errors=[{"message": "Invalid username or password",
                                                         "extensions": {"code": "UNAUTHENTICATED"}}]),
    )

    result = await action_auth(integration, mocker.MagicMock())

    assert result == {"valid_credentials": False, "error": "Skylight rejected the username or password."}


@pytest.mark.asyncio
async def test_action_auth_failure_returns_redacted_error(mocker, integration):
    from app.actions.handlers import action_auth
    mocker.patch("app.actions.client.build_graphql_client", return_value=mocker.MagicMock())
    mocker.patch(
        "app.actions.client.get_authentication_token",
        side_effect=httpx.ConnectError("boom https://api.skylight.earth/graphql?user=x&password=hunter2"),
    )

    result = await action_auth(integration, mocker.MagicMock())

    assert result["valid_credentials"] is None
    assert "hunter2" not in result["error"] and "graphql" not in result["error"]
    assert result["error"]  # still says something useful (classified text or the exception type)


# --- list_aois reference action ---


def _aoi(aoi_id, name, status="active", area=None, description=""):
    return {"id": aoi_id, "status": status, "createdAt": "2026-06-12T09:02:16Z", "updatedAt": "2026-06-12T09:02:16Z",
            "properties": {"name": name, "description": description, "areaKm2": area}}


def _aoi_page(records, total):
    return {"searchAOIs": {"records": records, "meta": {"total": total}}}


@pytest.mark.asyncio
async def test_search_aois_pages_through_total(mocker, integration, auth, patch_skylight_clients):
    mocker.patch("app.actions.client.AOI_SEARCH_PAGE_SIZE", 2)
    exec_mock = mocker.patch(
        "app.actions.client.execute_gql_query",
        side_effect=[
            _aoi_page([_aoi("a1", "Alpha"), _aoi("a2", "Bravo")], total=3),
            _aoi_page([_aoi("a3", "Charlie")], total=3),
        ],
    )

    aois = await search_aois(integration, auth)

    assert [a["id"] for a in aois] == ["a1", "a2", "a3"]
    assert exec_mock.call_count == 2
    first, second = [call.args[2] for call in exec_mock.call_args_list]
    assert first == {"limit": 2, "offset": 0}
    assert second == {"limit": 2, "offset": 2}


@pytest.mark.asyncio
async def test_search_aois_stops_on_empty_page(mocker, integration, auth, patch_skylight_clients):
    exec_mock = mocker.patch(
        "app.actions.client.execute_gql_query",
        side_effect=[_aoi_page([], total=5)],
    )

    aois = await search_aois(integration, auth)

    assert aois == []
    assert exec_mock.call_count == 1


@pytest.mark.asyncio
async def test_action_list_aois_returns_portal_options(mocker, integration):
    from app.actions.handlers import action_list_aois
    mocker.patch("app.actions.client.get_auth_config", return_value=mocker.MagicMock())
    mocker.patch(
        "app.actions.client.search_aois",
        return_value=[
            _aoi("d8f26fe4", "Bintan - Nikoi Island", area=276338.1),
            _aoi("0000-arch", "Archived Zone", status="archived", description="old"),
            _aoi("no-name", None),
        ],
    )

    result = await action_list_aois(integration, ListAOIsQuery())

    assert result["cache_ttl_seconds"] == 300 and result["truncated"] is False
    options = result["options"]
    # Sorted by label; the id is the value so pull_events.aoi_ids keeps storing ids.
    assert [o["value"] for o in options] == ["0000-arch", "d8f26fe4", "no-name"]
    by_value = {o["value"]: o for o in options}
    assert by_value["d8f26fe4"]["label"] == "Bintan - Nikoi Island"
    assert by_value["d8f26fe4"]["description"] == "276,338 km²"
    assert by_value["0000-arch"]["description"] == "archived, old"
    assert by_value["no-name"]["label"] == "no-name" and by_value["no-name"]["description"] is None


def test_event_types_render_as_checkboxes_with_an_inlined_enum():
    # The portal renders this field from the registered schema. A $ref inside
    # array items is not resolved there (falls back to a grey multi-select), and
    # the checkbox list needs an explicit widget, so both are pinned here.
    from app.actions.configurations import SkylightEventType

    schema = PullEventsConfig.schema()
    items = schema["properties"]["event_types"]["items"]
    assert "$ref" not in items, "a $ref here silently degrades the portal form"
    assert items == {"type": "string", "enum": [e.value for e in SkylightEventType]}
    assert "SkylightEventType" not in (schema.get("definitions") or {})
    assert PullEventsConfig.ui_schema()["event_types"]["ui:widget"] == "checkboxes"
    # The inherited schedule toggle must keep rendering alongside it.
    assert "run_on_schedule" in schema["properties"]


def test_event_types_labels_still_validate_into_mapping_keys():
    # The displayed labels must survive the validator and resolve to a key in
    # DEFAULT_EVENT_MAPPING, otherwise the form saves values the pull rejects.
    from app.actions.client import DEFAULT_EVENT_MAPPING
    from app.actions.configurations import SkylightEventType

    config = PullEventsConfig(aoi_ids=["aoi"], event_types=[e.value for e in SkylightEventType])

    assert all(key in DEFAULT_EVENT_MAPPING for key in config.event_types)


def test_pull_events_aoi_ids_carry_reference_annotation_for_registered_action():
    # Mirrors the portal contract: annotation on the array items, no ui:widget
    # (older portals keep the text input), free text allowed so pasted ids work,
    # and the referenced action exists and is a reference action.
    from app.actions.core import ReferenceActionConfiguration, discover_actions

    ui = PullEventsConfig.ui_schema()
    items = ui["aoi_ids"]["items"]
    ref = items["gundi:reference"]
    assert ref == {"action": "list_aois", "target": "self", "params": {}, "allow_free_text": True}
    assert "ui:widget" not in items

    handlers = discover_actions(module_name="app.actions.handlers", prefix="action_")
    _, config_model, _ = handlers["list_aois"]
    assert issubclass(config_model, ReferenceActionConfiguration)
    # aoi_ids itself is unchanged: a plain list of strings in the JSON schema.
    assert PullEventsConfig.schema()["properties"]["aoi_ids"]["items"] == {"type": "string"}


@pytest.mark.asyncio
async def test_process_events_per_aoi_pairs_state_with_the_event_actually_sent(
    mocker, integration, process_events_config, mock_publish_event
):
    # Regression: responses follow the *sorted* transformed list, so pairing them
    # back against action_config.events crossed the event -> Gundi object mapping
    # and a later patch would overwrite the wrong EarthRanger event.
    import datetime as _dt

    mocker.patch("app.services.state.IntegrationStateManager.get_state", return_value=None)
    mocker.patch("app.services.state.IntegrationStateManager.set_state", return_value=None)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mocker.patch("app.services.action_scheduler.publish_event", mock_publish_event)
    # event1 is older, so sorting by recorded_at descending reverses the input.
    mocker.patch(
        "app.actions.handlers.transform",
        side_effect=[
            {"event_id": "t1", "recorded_at": _dt.datetime(2024, 1, 1)},
            {"event_id": "t2", "recorded_at": _dt.datetime(2024, 1, 2)},
        ],
    )
    send_mock = mocker.patch(
        "app.actions.handlers.gundi_tools.send_events_to_gundi",
        return_value=[{"object_id": "obj2"}, {"object_id": "obj1"}],
    )
    mocker.patch("app.actions.handlers.process_attachments", return_value=None)
    save_state = mocker.patch("app.actions.handlers.save_events_state", return_value=None)

    await action_process_events_per_aoi(integration, process_events_config)

    assert [e["event_id"] for e in send_mock.call_args.kwargs["events"]] == ["t2", "t1"]
    responses, events, _ = save_state.call_args.args
    assert responses == [{"object_id": "obj2"}, {"object_id": "obj1"}]
    # event2 produced t2, which was sent first, so it must pair with obj2.
    assert [e["event_id"] for e in events] == ["event2", "event1"]


@pytest.mark.asyncio
async def test_process_events_per_aoi_skips_state_when_response_count_differs(
    mocker, integration, process_events_config, mock_publish_event
):
    # A guessed mapping is worse than none: it would make a later patch overwrite
    # a different event. Two events sent, one response back -> nothing saved.
    mocker.patch("app.services.state.IntegrationStateManager.get_state", return_value=None)
    mocker.patch("app.services.state.IntegrationStateManager.set_state", return_value=None)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mocker.patch("app.services.action_scheduler.publish_event", mock_publish_event)
    mocker.patch("app.actions.handlers.transform", return_value={"event_id": "t"})
    mocker.patch(
        "app.actions.handlers.gundi_tools.send_events_to_gundi", return_value=[{"object_id": "obj1"}]
    )
    mocker.patch("app.actions.handlers.process_attachments", return_value=None)
    save_state = mocker.patch("app.actions.handlers.save_events_state", return_value=None)
    log = mocker.patch("app.actions.handlers.logger")

    await action_process_events_per_aoi(integration, process_events_config)

    save_state.assert_called_once_with([], [], integration)
    assert any("Skipping the event-state mapping" in str(c) for c in log.warning.call_args_list)


@pytest.mark.asyncio
async def test_pull_events_checks_every_event_against_saved_state(
    mocker, integration, pull_events_config, mock_publish_event
):
    # Regression: removing from the list being iterated shifted the index, so the
    # event right after an already-known one was never checked and was re-sent to
    # Gundi as new. Both events here are known, so neither may be re-sent.
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mocker.patch("app.services.action_scheduler.publish_event", mock_publish_event)
    mocker.patch(
        "app.actions.client.get_skylight_events",
        return_value=({"aoi": [{"event_id": "event1"}, {"event_id": "event2"}]}, []),
    )
    mocker.patch("app.actions.client.get_auth_config", return_value=None)
    mocker.patch(
        "app.services.state.IntegrationStateManager.get_state",
        # Only the per-event dedupe keys answer; plan/cursor lookups find nothing.
        side_effect=lambda *args, **kwargs: (
            {"object_id": f"obj_{args[2]}"} if args[2].startswith("event") else None
        ),
    )
    mocker.patch("app.services.state.IntegrationStateManager.set_state", return_value=None)
    trigger = mocker.patch("app.actions.handlers.trigger_action", return_value=None)
    patch_mock = mocker.patch("app.actions.handlers.patch_events", return_value=[{}, {}])

    result = await action_pull_events(integration, pull_events_config)

    assert not trigger.called
    assert result["events_extracted"] == 0
    patched_ids = [event["event_id"] for _, event in patch_mock.call_args.args[0]]
    assert patched_ids == ["event1", "event2"]


@pytest.mark.asyncio
async def test_patch_events_refreshes_the_dedupe_ttl(mocker, integration):
    # The cursor is inclusive, so the boundary event returns every run and lands
    # on the patch path. Without refreshing the 72h key it would expire on a quiet
    # AOI, look new again, and be duplicated in EarthRanger.
    mocker.patch("app.actions.handlers.transform", return_value={"event_id": "t1"})
    mocker.patch("app.actions.handlers.gundi_tools.update_gundi_event", return_value={"object_id": "obj1"})
    set_state = mocker.patch("app.actions.handlers.state_manager.set_state", return_value=None)

    await patch_events([("obj1", {"event_id": "event1"})], [], integration)

    set_state.assert_called_once()
    assert set_state.call_args.kwargs["source_id"] == "event1"
    assert set_state.call_args.kwargs["state"] == {"object_id": "obj1"}
    assert set_state.call_args.kwargs["expire"] == 259200


def test_pull_events_config_rejects_a_zero_page_size():
    # pageSize 0 made every AOI yield nothing and the run report success.
    with pytest.raises(pydantic.ValidationError):
        PullEventsConfig(aoi_ids=["aoi1"], event_types=["Fishing"], pageSize=0)


@pytest.mark.asyncio
async def test_get_skylight_events_warns_when_paging_is_not_pinned(mocker, integration, auth, patch_skylight_clients):
    # No snapshotId means the pages aren't pinned to a snapshot; a concurrent
    # update can shift the window and the event at that offset is never returned,
    # with an updatedAt already below the saved cursor. Paging still continues.
    exec_mock = mocker.patch(
        "app.actions.client.execute_gql_query",
        side_effect=[
            _page([{"event_id": "e1"}, {"event_id": "e2"}], total=3, page_num=1, snapshot_id=None),
            _page([{"event_id": "e3"}], total=3, page_num=2, snapshot_id=None),
        ],
    )
    log = mocker.patch("app.actions.client.logger")

    events, _ = await get_skylight_events(integration, _PullCfg(), auth)

    assert exec_mock.call_count == 2
    assert [e["event_id"] for e in events["aoi1"]] == ["e1", "e2", "e3"]
    warnings = [
        call for call in log.warning.call_args_list
        if "no snapshotId" in str(call) and call.kwargs.get("extra", {}).get("attention_needed")
    ]
    assert len(warnings) == 1


@pytest.mark.asyncio
async def test_get_skylight_events_no_snapshot_warning_when_pinned(mocker, integration, auth, patch_skylight_clients):
    mocker.patch(
        "app.actions.client.execute_gql_query",
        side_effect=[_page([{"event_id": "e1"}], total=1)],
    )
    log = mocker.patch("app.actions.client.logger")

    await get_skylight_events(integration, _PullCfg(), auth)

    assert not any("no snapshotId" in str(call) for call in log.warning.call_args_list)


# --- Option A: the cursor only moves over work that reached EarthRanger ---
#
# Publishing a sub-action command is not delivery. If the parent advanced the
# cursor at trigger time, a sub-action that exhausted its retries would leave
# its events stranded behind the new cursor: permanent, silent loss. So the
# parent records the chunk plan it handed out, each sub-action marks its own
# chunk once its events are in EarthRanger, and the *next* parent run walks the
# plan in order and advances the cursor to the end of the last contiguous
# completed chunk. The parent is the only writer of the cursor.


def _plan_state(chunks, final):
    return {"chunks": chunks, "final": final}


def _state_reader(states):
    """get_state stand-in backed by a {source_id: value} dict."""
    async def _get_state(integration_id, action_id, source_id="no-source"):
        return states.get(source_id)
    return _get_state


@pytest.mark.asyncio
async def test_advance_aoi_cursors_moves_to_the_end_of_the_last_completed_chunk(mocker, integration):
    # Chunks 0 and 1 delivered, chunk 2 did not. The cursor stops at the end of
    # chunk 1: advancing to chunk 2's end would strand its events forever.
    states = {
        "_plan.aoi1": _plan_state(
            [
                {"id": "run-0", "cursor": "2026-09-08T10:00:00Z", "events": 2},
                {"id": "run-1", "cursor": "2026-09-08T11:00:00Z", "events": 2},
                {"id": "run-2", "cursor": "2026-09-08T12:00:00Z", "events": 2},
            ],
            final="2026-09-08T12:00:00Z",
        ),
        "_chunk.run-0": {"delivered_at": "x"},
        "_chunk.run-1": {"delivered_at": "x"},
    }
    mocker.patch("app.actions.handlers.state_manager.get_state", side_effect=_state_reader(states))
    set_state = mocker.patch("app.actions.handlers.state_manager.set_state", return_value=None)
    mocker.patch("app.actions.handlers.state_manager.delete_state", return_value=None)

    advanced = await advance_aoi_cursors(integration, ["aoi1"])

    assert advanced == {"aoi1": "2026-09-08T11:00:00Z"}
    set_state.assert_called_once_with(
        str(integration.id), "pull_events", {"updated_since": "2026-09-08T11:00:00Z"}, "aoi1"
    )


@pytest.mark.asyncio
async def test_advance_aoi_cursors_stops_at_the_first_gap_not_the_last_marker(mocker, integration):
    # Chunk 1 is missing but chunk 2 completed. The walk must stop at the gap:
    # taking the newest marker instead would jump the cursor over chunk 1.
    states = {
        "_plan.aoi1": _plan_state(
            [
                {"id": "run-0", "cursor": "2026-09-08T10:00:00Z", "events": 1},
                {"id": "run-1", "cursor": "2026-09-08T11:00:00Z", "events": 1},
                {"id": "run-2", "cursor": "2026-09-08T12:00:00Z", "events": 1},
            ],
            final="2026-09-08T12:00:00Z",
        ),
        "_chunk.run-0": {"delivered_at": "x"},
        "_chunk.run-2": {"delivered_at": "x"},
    }
    mocker.patch("app.actions.handlers.state_manager.get_state", side_effect=_state_reader(states))
    mocker.patch("app.actions.handlers.state_manager.set_state", return_value=None)
    mocker.patch("app.actions.handlers.state_manager.delete_state", return_value=None)

    advanced = await advance_aoi_cursors(integration, ["aoi1"])

    assert advanced == {"aoi1": "2026-09-08T10:00:00Z"}


@pytest.mark.asyncio
async def test_advance_aoi_cursors_holds_the_cursor_when_the_first_chunk_never_arrived(mocker, integration):
    states = {
        "_plan.aoi1": _plan_state(
            [{"id": "run-0", "cursor": "2026-09-08T10:00:00Z", "events": 1}],
            final="2026-09-08T10:00:00Z",
        ),
    }
    mocker.patch("app.actions.handlers.state_manager.get_state", side_effect=_state_reader(states))
    set_state = mocker.patch("app.actions.handlers.state_manager.set_state", return_value=None)
    mocker.patch("app.actions.handlers.state_manager.delete_state", return_value=None)

    advanced = await advance_aoi_cursors(integration, ["aoi1"])

    assert advanced == {}
    assert not set_state.called


@pytest.mark.asyncio
async def test_advance_aoi_cursors_reaches_final_only_when_every_chunk_delivered(mocker, integration):
    # `final` is the newest updatedAt of everything the run pulled, including
    # events the parent patched itself after the last chunk. It is only safe
    # once nothing is outstanding.
    states = {
        "_plan.aoi1": _plan_state(
            [{"id": "run-0", "cursor": "2026-09-08T10:00:00Z", "events": 1}],
            final="2026-09-08T13:00:00Z",
        ),
        "_chunk.run-0": {"delivered_at": "x"},
    }
    mocker.patch("app.actions.handlers.state_manager.get_state", side_effect=_state_reader(states))
    mocker.patch("app.actions.handlers.state_manager.set_state", return_value=None)
    mocker.patch("app.actions.handlers.state_manager.delete_state", return_value=None)

    advanced = await advance_aoi_cursors(integration, ["aoi1"])

    assert advanced == {"aoi1": "2026-09-08T13:00:00Z"}


@pytest.mark.asyncio
async def test_advance_aoi_cursors_moves_a_patch_only_run_with_no_chunks(mocker, integration):
    # Reason the cursor lives in the parent at all (b): a run that finds only
    # already-known events starts no sub-action, so there is no chunk to wait
    # on. The parent patched those events synchronously, so `final` is reached.
    states = {"_plan.aoi1": _plan_state([], final="2026-09-08T11:00:00Z")}
    mocker.patch("app.actions.handlers.state_manager.get_state", side_effect=_state_reader(states))
    mocker.patch("app.actions.handlers.state_manager.set_state", return_value=None)
    mocker.patch("app.actions.handlers.state_manager.delete_state", return_value=None)

    advanced = await advance_aoi_cursors(integration, ["aoi1"])

    assert advanced == {"aoi1": "2026-09-08T11:00:00Z"}


@pytest.mark.asyncio
async def test_advance_aoi_cursors_never_moves_the_cursor_backwards(mocker, integration):
    # A stale or replayed plan must not rewind a cursor that is already ahead,
    # which would re-pull and re-patch a whole window for nothing.
    states = {
        "_plan.aoi1": _plan_state(
            [{"id": "run-0", "cursor": "2026-09-08T10:00:00Z", "events": 1}],
            final="2026-09-08T10:00:00Z",
        ),
        "_chunk.run-0": {"delivered_at": "x"},
        "aoi1": {"updated_since": "2026-09-09T00:00:00Z"},
    }
    mocker.patch("app.actions.handlers.state_manager.get_state", side_effect=_state_reader(states))
    set_state = mocker.patch("app.actions.handlers.state_manager.set_state", return_value=None)
    mocker.patch("app.actions.handlers.state_manager.delete_state", return_value=None)

    advanced = await advance_aoi_cursors(integration, ["aoi1"])

    assert advanced == {}
    assert not set_state.called


@pytest.mark.asyncio
async def test_advance_aoi_cursors_compares_cursors_as_timestamps_not_text(mocker, integration):
    # Skylight has returned both `...Z` and `...+00:00`. "2026-09-08T11:00:00Z"
    # sorts *after* "2026-09-08T12:00:00+00:00" as text, so a string compare
    # would let an older cursor overwrite a newer one.
    states = {
        "_plan.aoi1": _plan_state(
            [{"id": "run-0", "cursor": "2026-09-08T11:00:00Z", "events": 1}],
            final="2026-09-08T11:00:00Z",
        ),
        "_chunk.run-0": {"delivered_at": "x"},
        "aoi1": {"updated_since": "2026-09-08T12:00:00+00:00"},
    }
    mocker.patch("app.actions.handlers.state_manager.get_state", side_effect=_state_reader(states))
    set_state = mocker.patch("app.actions.handlers.state_manager.set_state", return_value=None)
    mocker.patch("app.actions.handlers.state_manager.delete_state", return_value=None)

    assert await advance_aoi_cursors(integration, ["aoi1"]) == {}
    assert not set_state.called


@pytest.mark.asyncio
async def test_advance_aoi_cursors_clears_the_plan_and_its_markers(mocker, integration):
    # The plan is one run's bookkeeping. Leaving it behind would let the next
    # run read a marker from two runs ago; leaving markers behind would make a
    # recycled id look already delivered.
    states = {
        "_plan.aoi1": _plan_state(
            [
                {"id": "run-0", "cursor": "2026-09-08T10:00:00Z", "events": 1},
                {"id": "run-1", "cursor": "2026-09-08T11:00:00Z", "events": 1},
            ],
            final="2026-09-08T11:00:00Z",
        ),
        "_chunk.run-0": {"delivered_at": "x"},
    }
    mocker.patch("app.actions.handlers.state_manager.get_state", side_effect=_state_reader(states))
    mocker.patch("app.actions.handlers.state_manager.set_state", return_value=None)
    delete_state = mocker.patch("app.actions.handlers.state_manager.delete_state", return_value=None)

    await advance_aoi_cursors(integration, ["aoi1"])

    deleted = [call.args[2] for call in delete_state.call_args_list]
    assert deleted == ["_plan.aoi1", "_chunk.run-0", "_chunk.run-1"]


@pytest.mark.asyncio
async def test_advance_aoi_cursors_is_a_no_op_without_a_plan(mocker, integration):
    mocker.patch("app.actions.handlers.state_manager.get_state", side_effect=_state_reader({}))
    set_state = mocker.patch("app.actions.handlers.state_manager.set_state", return_value=None)
    delete_state = mocker.patch("app.actions.handlers.state_manager.delete_state", return_value=None)

    assert await advance_aoi_cursors(integration, ["aoi1", "aoi2"]) == {}
    assert not set_state.called and not delete_state.called


def test_build_chunk_plan_orders_chunks_oldest_update_first():
    # The whole scheme rests on this: "chunks 0..k delivered" may only mean
    # "everything up to chunk k's cursor delivered" if the chunks are in
    # updatedAt order. Skylight already returns them that way; this pins it so
    # a change upstream cannot silently break the cursor.
    events = [
        {"event_id": "c", "updated_at": "2026-09-08T12:00:00Z"},
        {"event_id": "a", "updated_at": "2026-09-08T10:00:00Z"},
        {"event_id": "b", "updated_at": "2026-09-08T11:00:00Z"},
    ]

    chunks, batches = build_chunk_plan("run-0", events, max_payload_bytes=60)

    assert [event["event_id"] for batch in batches for event in batch] == ["a", "b", "c"]
    cursors = [chunk["cursor"] for chunk in chunks]
    assert cursors == sorted(cursors)
    assert [chunk["id"] for chunk in chunks] == [f"run-0-{i}" for i in range(len(chunks))]
    assert sum(chunk["events"] for chunk in chunks) == 3


def test_build_chunk_plan_puts_events_without_an_update_stamp_first():
    # An event with no updatedAt cannot move the cursor. Sorting it first keeps
    # it behind every real cursor value rather than capping a chunk at None.
    events = [
        {"event_id": "a", "updated_at": "2026-09-08T10:00:00Z"},
        {"event_id": "b"},
    ]

    _, batches = build_chunk_plan("run-0", events, max_payload_bytes=10_000)

    assert [event["event_id"] for event in batches[0]] == ["b", "a"]


@pytest.mark.asyncio
async def test_action_pull_events_settles_the_previous_run_before_fetching(
        mocker, integration, pull_events_config, mock_publish_event
):
    # The fetch reads the cursor that reconciliation writes, so reconciliation
    # has to run first or the run would query from the stale value.
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    calls = []
    async def _advance(_integration, _aoi_ids):
        calls.append("advance")
        return {"aoi1": "2026-09-08T09:00:00Z"}
    async def _fetch(**kwargs):
        calls.append("fetch")
        return {}, []
    mocker.patch("app.actions.handlers.advance_aoi_cursors", side_effect=_advance)
    mocker.patch("app.actions.client.get_skylight_events", side_effect=_fetch)
    mocker.patch("app.actions.client.get_auth_config", return_value=None)

    result = await action_pull_events(integration, pull_events_config)

    assert calls == ["advance", "fetch"]
    assert result["details"]["cursors"] == {"aoi1": "2026-09-08T09:00:00Z"}


@pytest.mark.asyncio
async def test_action_process_events_per_aoi_marks_its_chunk_delivered(
        mocker, integration, process_events_config, mock_publish_event
):
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    process_events_config.chunk_id = "run-0-0"
    mocker.patch("app.actions.handlers.transform", side_effect=lambda c, e: {"event_id": e["event_id"]})
    mocker.patch(
        "app.actions.handlers.gundi_tools.send_events_to_gundi",
        return_value=[{"object_id": "o1"}, {"object_id": "o2"}],
    )
    mocker.patch("app.actions.handlers.process_attachments", return_value=None)
    mocker.patch("app.actions.handlers.save_events_state", return_value=None)
    set_state = mocker.patch("app.actions.handlers.state_manager.set_state", return_value=None)

    result = await action_process_events_per_aoi(integration, process_events_config)

    assert result["details"]["chunk_delivered"] is True
    set_state.assert_called_once()
    assert set_state.call_args.args[3] == "_chunk.run-0-0"
    assert set_state.call_args.kwargs["expire"] == 604800


@pytest.mark.asyncio
async def test_action_process_events_per_aoi_marks_a_chunk_that_transformed_to_nothing(
        mocker, integration, process_events_config, mock_publish_event
):
    # Every event was skipped by transform (unsupported type, entry alert with
    # no start point). There is nothing left to deliver, so the chunk must not
    # hold the AOI cursor back forever.
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    process_events_config.chunk_id = "run-0-0"
    mocker.patch("app.actions.handlers.transform", return_value={})
    send_mock = mocker.patch("app.actions.handlers.gundi_tools.send_events_to_gundi", return_value=[])
    set_state = mocker.patch("app.actions.handlers.state_manager.set_state", return_value=None)

    result = await action_process_events_per_aoi(integration, process_events_config)

    assert not send_mock.called
    assert result["details"]["chunk_delivered"] is True
    assert set_state.call_args.args[3] == "_chunk.run-0-0"


@pytest.mark.asyncio
async def test_action_process_events_per_aoi_leaves_the_chunk_unmarked_when_gundi_returns_nothing(
        mocker, integration, process_events_config, mock_publish_event
):
    # An empty response means those events are not in EarthRanger. Without a
    # marker the cursor stops short of this chunk and the next run re-pulls it.
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    process_events_config.chunk_id = "run-0-0"
    mocker.patch("app.actions.handlers.transform", side_effect=lambda c, e: {"event_id": e["event_id"]})
    mocker.patch("app.actions.handlers.gundi_tools.send_events_to_gundi", return_value=[])
    mocker.patch("app.actions.handlers.save_events_state", return_value=None)
    set_state = mocker.patch("app.actions.handlers.state_manager.set_state", return_value=None)

    result = await action_process_events_per_aoi(integration, process_events_config)

    assert result["details"]["chunk_delivered"] is False
    assert not set_state.called


@pytest.mark.asyncio
async def test_action_process_events_per_aoi_leaves_the_chunk_unmarked_when_it_raises(
        mocker, integration, process_events_config, mock_publish_event
):
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mocker.patch("app.services.action_scheduler.publish_event", mock_publish_event)
    process_events_config.chunk_id = "run-0-0"
    mocker.patch("app.actions.handlers.transform", return_value={"event_id": "event1"})
    mocker.patch("app.actions.handlers.gundi_tools.send_events_to_gundi", side_effect=httpx.HTTPError("boom"))
    set_state = mocker.patch("app.actions.handlers.state_manager.set_state", return_value=None)

    with pytest.raises(httpx.HTTPError):
        await action_process_events_per_aoi(integration, process_events_config)

    assert not set_state.called


@pytest.mark.asyncio
async def test_process_events_per_aoi_without_a_chunk_id_writes_no_marker(
        mocker, integration, process_events_config, mock_publish_event
):
    # A command queued by an older revision carries no chunk_id. It must still
    # run; it simply has no plan entry to report against.
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    assert process_events_config.chunk_id is None
    mocker.patch("app.actions.handlers.transform", side_effect=lambda c, e: {"event_id": e["event_id"]})
    mocker.patch(
        "app.actions.handlers.gundi_tools.send_events_to_gundi",
        return_value=[{"object_id": "o1"}, {"object_id": "o2"}],
    )
    mocker.patch("app.actions.handlers.process_attachments", return_value=None)
    mocker.patch("app.actions.handlers.save_events_state", return_value=None)
    set_state = mocker.patch("app.actions.handlers.state_manager.set_state", return_value=None)

    result = await action_process_events_per_aoi(integration, process_events_config)

    assert result["events_processed"] == 2
    assert not set_state.called


@pytest.mark.asyncio
async def test_cursor_survives_a_sub_action_that_never_delivers(
        mocker, integration, pull_events_config, mock_publish_event
):
    # End to end for the [P1]: run 1 hands out two chunks and the second one
    # never lands. Run 2 must resume from the end of chunk 1, not from the end
    # of the batch, so the lost events are fetched again instead of skipped.
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.actions.client.get_auth_config", return_value=None)
    mocker.patch("app.actions.handlers.trigger_action", return_value=None)

    store = {}
    async def _get_state(integration_id, action_id, source_id="no-source"):
        return store.get(source_id)
    async def _set_state(integration_id, action_id, state, source_id="no-source", expire=None):
        store[source_id] = state
    async def _delete_state(integration_id, action_id, source_id="no-source"):
        store.pop(source_id, None)
    mocker.patch("app.actions.handlers.state_manager.get_state", side_effect=_get_state)
    mocker.patch("app.actions.handlers.state_manager.set_state", side_effect=_set_state)
    mocker.patch("app.actions.handlers.state_manager.delete_state", side_effect=_delete_state)

    events = [
        {"event_id": "a", "updated_at": "2026-09-08T10:00:00Z", "pad": "x" * 200},
        {"event_id": "b", "updated_at": "2026-09-08T11:00:00Z", "pad": "x" * 200},
    ]
    mocker.patch("app.actions.client.get_skylight_events", return_value=({"aoi1": events}, []))
    mocker.patch("app.actions.handlers.MAX_TRIGGER_PAYLOAD_BYTES", 250)

    run1 = await action_pull_events(integration, pull_events_config)
    chunk_ids = run1["details"]["chunk_plan"]["aoi1"]
    assert len(chunk_ids) == 2

    # Only the first chunk reaches EarthRanger; the second exhausts its retries.
    store[f"_chunk.{chunk_ids[0]}"] = {"delivered_at": "x"}

    run2 = await action_pull_events(integration, pull_events_config)

    # Resumed at event "a", so event "b" is still in range of the next fetch.
    assert run2["details"]["cursors"] == {"aoi1": "2026-09-08T10:00:00Z"}
    assert store["aoi1"] == {"updated_since": "2026-09-08T10:00:00Z"}

