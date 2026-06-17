import base64
import json
import time
import pytest
import httpx

from gql.transport.exceptions import TransportQueryError, TransportServerError

from app.actions.client import execute_gql_query, build_request_header, get_skylight_events, _is_token_expired, _TOKEN_EXPIRY_SKEW_SECONDS
from app.actions.configurations import ProcessEventsPerAOIConfig
from app.actions.handlers import action_pull_events, action_process_events_per_aoi, process_attachments, transform
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


def _page(items, total, page_size=2, page_num=1):
    return {"events": {"items": items, "meta": {"total": total, "pageSize": page_size, "pageNum": page_num}}}


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
    return {"state_manager": state_manager, "build_client": build_client}


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
async def test_get_skylight_events_skips_failed_page_and_continues(mocker, integration, auth, patch_skylight_clients):
    # total=6, pageSize=2 -> 3 pages. Page 2 fails; page 1 and 3 are still collected.
    exec_mock = mocker.patch(
        "app.actions.client.execute_gql_query",
        side_effect=[
            _page([{"event_id": "e1"}, {"event_id": "e2"}], total=6, page_num=1),
            TransportQueryError("boom", errors=[{"message": "boom"}]),
            _page([{"event_id": "e5"}, {"event_id": "e6"}], total=6, page_num=3),
        ],
    )

    events, _ = await get_skylight_events(integration, _PullCfg(), auth)

    assert exec_mock.call_count == 3
    collected = [e["event_id"] for e in events["aoi1"]]
    assert collected == ["e1", "e2", "e5", "e6"]


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

    events, _ = await get_skylight_events(integration, _PullCfg(), auth)

    assert exec_mock.call_count == 2
    assert [e["event_id"] for e in events["aoi1"]] == ["e1", "e2"]


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
    mocker.patch("app.actions.handlers.generate_batches", return_value=[[{"event_id": "event1"}]])
    mocker.patch("app.actions.handlers.gundi_tools.send_events_to_gundi", return_value=[{"object_id": "event1"}])
    mocker.patch("app.actions.handlers.process_attachments", return_value=None)
    mocker.patch("app.actions.handlers.save_events_state", return_value=None)

    result = await action_process_events_per_aoi(integration, process_events_config)
    assert result["events_processed"] == 1

@pytest.mark.asyncio
async def test_action_process_events_per_aoi_failure(mocker, integration, process_events_config, mock_publish_event):
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mocker.patch("app.services.action_scheduler.publish_event", mock_publish_event)
    mocker.patch("app.actions.handlers.transform", return_value={"event_id": "event1"})
    mocker.patch("app.actions.handlers.generate_batches", return_value=[[{"event_id": "event1"}]])
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
    mock_trigger_action = mocker.patch("app.actions.handlers.trigger_action", return_value=None)

    result = await action_pull_events(integration, pull_events_config)
    assert result["process_events_per_aoi_action_triggered"] == 1
    mock_trigger_action.assert_called_once_with(
        integration.id,
        "process_events_per_aoi",
        config=ProcessEventsPerAOIConfig(
            integration_id=integration.id,
            aoi="aoi",
            events=[{"event_id": "event1"}],
            updated_config_data=[]
        )
    )


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
