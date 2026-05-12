import pytest
import httpx

from gql.transport.exceptions import TransportQueryError
from unittest.mock import AsyncMock, MagicMock

from app.actions.client import execute_gql_query, get_skylight_events
from app.actions.configurations import ProcessEventsPerAOIConfig, PullEventsConfig
from app.actions.handlers import action_pull_events, action_process_events_per_aoi, process_attachments, transform
from app.services.state import IntegrationStateManager


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def integration(mocker):
    return mocker.AsyncMock(id="integration_id", base_url="https://gundi-test.com", additional=None)

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

@pytest.fixture
def pull_events_config():
    return PullEventsConfig(
        aoi_ids=["aoi1"],
        event_types=["fishing", "dark_rendezvous"],
        pageSize=100,
        initial_data_window_days=7
    )

@pytest.fixture
def pull_events_config_no_aoi():
    return PullEventsConfig(
        aoi_ids=[],
        event_types=["fishing"],
        pageSize=100,
        initial_data_window_days=7
    )

@pytest.fixture
def mock_publish_event(mocker):
    return mocker.AsyncMock()

@pytest.fixture
def process_events_config():
    return ProcessEventsPerAOIConfig(
        integration_id="integration_id",
        aoi="global",
        events=[{"eventId": "event1"}, {"eventId": "event2"}],
        updated_config_data=[]
    )

@pytest.fixture
def skylight_fishing_event():
    return {
        "eventId": "evt-fishing-001",
        "eventType": "fishing_activity_history",
        "createdAt": "2025-04-01T00:00:00Z",
        "updatedAt": "2025-04-01T01:00:00Z",
        "start": {"point": {"lat": -40.0, "lon": -65.0}, "time": "2025-04-01T00:00:00Z"},
        "end": {"point": {"lat": -40.1, "lon": -65.1}, "time": "2025-04-01T01:00:00Z"},
        "vessels": {
            "vessel0": {
                "name": "Pescador I", "mmsi": "701234567", "imo": "IMO9876543",
                "countryCode": "AR", "trackId": "track-001",
                "category": "fishing", "length": 45
            },
            "vessel1": None
        },
        "eventDetails": {"fishingScore": 0.92}
    }

@pytest.fixture
def skylight_dark_rendezvous_event():
    return {
        "eventId": "evt-dark-001",
        "eventType": "dark_rendezvous",
        "createdAt": "2025-04-01T00:00:00Z",
        "updatedAt": "2025-04-01T01:00:00Z",
        "start": {"point": {"lat": -38.0, "lon": -57.0}, "time": "2025-04-01T00:00:00Z"},
        "end": {"point": {"lat": -38.1, "lon": -57.1}, "time": "2025-04-01T02:00:00Z"},
        "vessels": {
            "vessel0": {
                "name": "Dark Ship", "mmsi": "701111111", "imo": None,
                "countryCode": "AR", "trackId": "track-002",
                "category": "cargo", "length": 120
            },
            "vessel1": None
        },
        "eventDetails": {"osrScore": 0.87}
    }

@pytest.fixture
def skylight_standard_rendezvous_event():
    return {
        "eventId": "evt-std-001",
        "eventType": "standard_rendezvous",
        "createdAt": "2025-04-01T00:00:00Z",
        "updatedAt": "2025-04-01T01:00:00Z",
        "start": {"point": {"lat": -42.0, "lon": -60.0}, "time": "2025-04-01T00:00:00Z"},
        "end": {"point": {"lat": -42.0, "lon": -60.0}, "time": "2025-04-01T01:00:00Z"},
        "vessels": {
            "vessel0": {
                "name": "Vessel A", "mmsi": "701222222", "imo": "IMO1111111",
                "countryCode": "AR", "trackId": "track-003",
                "category": "tanker", "length": 200
            },
            "vessel1": {
                "name": "Vessel B", "mmsi": "701333333", "imo": "IMO2222222",
                "countryCode": "CN", "displayCountry": "China", "trackId": "track-004",
                "category": "tanker", "length": 180
            }
        },
        "eventDetails": {"__typename": "StandardRendezvousEventDetails"}
    }

@pytest.fixture
def skylight_speed_range_event():
    return {
        "eventId": "evt-speed-001",
        "eventType": "speed_range",
        "createdAt": "2025-04-01T00:00:00Z",
        "updatedAt": "2025-04-01T01:00:00Z",
        "start": {"point": {"lat": -41.0, "lon": -63.0}, "time": "2025-04-01T00:00:00Z"},
        "end": {"point": {"lat": -41.5, "lon": -63.5}, "time": "2025-04-01T02:00:00Z"},
        "vessels": {
            "vessel0": {
                "name": "Fast Ship", "mmsi": "701444444", "imo": None,
                "countryCode": "AR", "trackId": "track-005",
                "category": "fishing", "length": 30
            },
            "vessel1": None
        },
        "eventDetails": {"averageSpeed": 14.5, "distance": 28.9, "durationSec": 7200.0}
    }

@pytest.fixture
def skylight_aoi_visit_event():
    return {
        "eventId": "evt-aoi-001",
        "eventType": "aoi_visit",
        "createdAt": "2025-04-01T00:00:00Z",
        "updatedAt": "2025-04-01T01:00:00Z",
        "start": {"point": {"lat": -40.5, "lon": -62.0}, "time": "2025-04-01T06:00:00Z"},
        "end": {"point": {"lat": -40.6, "lon": -62.1}, "time": "2025-04-01T08:00:00Z"},
        "vessels": {
            "vessel0": {
                "name": "Entry Vessel", "mmsi": "701555555", "imo": None,
                "countryCode": "AR", "trackId": "track-006",
                "category": "fishing", "length": 25
            },
            "vessel1": None
        },
        "eventDetails": {"entrySpeed": 6.2, "entryHeading": 270, "endHeading": 90}
    }

@pytest.fixture
def skylight_imagery_event():
    return {
        "eventId": "evt-sar-001",
        "eventType": "sar_sentinel1",
        "createdAt": "2025-04-01T00:00:00Z",
        "updatedAt": "2025-04-01T01:00:00Z",
        "start": {"point": {"lat": -39.0, "lon": -61.0}, "time": "2025-04-01T10:00:00Z"},
        "end": None,
        "vessels": {"vessel0": None, "vessel1": None},
        "eventDetails": {
            "imageUrl": "https://example.com/sat/image.png",
            "detectionType": "dark",
            "score": 0.78,
            "estimatedLength": 95.0,
            "estimatedSpeedKts": 4.1,
            "estimatedVesselCategory": "fishing",
            "frameIds": ["frame-001"],
            "heading": 180,
            "distanceToCoastM": 52000,
            "orientation": 45.0,
            "metersPerPixel": 10.0
        }
    }

@pytest.fixture
def skylight_viirs_event():
    return {
        "eventId": "evt-viirs-001",
        "eventType": "viirs",
        "createdAt": "2025-04-01T00:00:00Z",
        "updatedAt": "2025-04-01T01:00:00Z",
        "start": {"point": {"lat": -43.0, "lon": -64.0}, "time": "2025-04-01T03:00:00Z"},
        "end": None,
        "vessels": {"vessel0": None, "vessel1": None},
        "eventDetails": {
            "imageUrl": "https://example.com/viirs/image.png",
            "detectionType": "dark",
            "estimatedLength": None,
            "estimatedSpeedKts": None,
            "estimatedVesselCategory": None,
            "frameIds": ["frame-viirs-001"],
            "heading": None,
            "radianceNw": 3.14
        }
    }

@pytest.fixture
def mock_skylight_gql_response(skylight_fishing_event, skylight_dark_rendezvous_event):
    return {
        "searchEventsV2": {
            "records": [skylight_fishing_event, skylight_dark_rendezvous_event],
            "meta": {"snapshotId": "snap-abc-123", "total": 2}
        }
    }

CONFIG = [
    {"skylight_event_type": "fishing_activity_history", "event_title": "Fishing", "event_type": "fishing_alert_rep"},
    {"skylight_event_type": "dark_rendezvous", "event_title": "Dark Rendezvous", "event_type": "dark_rendezvous_alert_rep"},
    {"skylight_event_type": "standard_rendezvous", "event_title": "Standard Rendezvous", "event_type": "standard_rendezvous_alert_rep"},
    {"skylight_event_type": "speed_range", "event_title": "Speed Range", "event_type": "speed_range_alert_rep"},
    {"skylight_event_type": "aoi_visit", "event_title": "Marine Entry", "event_type": "entry_alert_rep"},
    {"skylight_event_type": ["sar_sentinel1", "eo_sentinel2", "eo_landsat_8_9"], "event_title": "Vessel Detection", "event_type": "detection_alert_rep"},
    {"skylight_event_type": "viirs", "event_title": "Vessel Detection", "event_type": "detection_alert_rep"},
]


# ---------------------------------------------------------------------------
# execute_gql_query
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_execute_gql_query_success(mocker, gql_client, integration, auth):
    gql_client.execute.return_value = {"data": "response"}
    response = await execute_gql_query(gql_client, "query", {}, integration, auth)
    assert response == {"data": "response"}


@pytest.mark.asyncio
async def test_execute_gql_query_retry_on_unauthorized(mocker, gql_client, integration, auth, state_manager):
    gql_client.execute = mocker.MagicMock(
        side_effect=TransportQueryError(
            "Error",
            errors=[{"extensions": {"message": "UNAUTHORIZED", "code": "UNAUTHORIZED"}}]
        )
    )
    mocker.patch("app.actions.client.state_manager", state_manager)
    with pytest.raises(TransportQueryError):
        await execute_gql_query(gql_client, "query", {}, integration, auth)
    assert state_manager.delete_state.call_count == 3


@pytest.mark.asyncio
async def test_execute_gql_query_delete_state_on_unauthenticated(mocker, gql_client, integration, auth, state_manager):
    gql_client.execute = mocker.MagicMock(
        side_effect=TransportQueryError(
            "Error",
            errors=[{"extensions": {"code": "UNAUTHENTICATED"}}]
        )
    )
    mocker.patch("app.actions.client.state_manager", state_manager)
    with pytest.raises(TransportQueryError):
        await execute_gql_query(gql_client, "query", {}, integration, auth)
    assert state_manager.delete_state.call_count == 3


@pytest.mark.asyncio
async def test_execute_gql_query_does_not_retry_on_other_errors(mocker, gql_client, integration, auth, state_manager):
    gql_client.execute = mocker.MagicMock(
        side_effect=TransportQueryError(
            "Error",
            errors=[{"extensions": {"message": "Error", "code": "OTHER_ERROR"}}]
        )
    )
    mocker.patch("app.actions.client.state_manager", state_manager)
    with pytest.raises(TransportQueryError):
        await execute_gql_query(gql_client, "query", {}, integration, auth)
    assert state_manager.delete_state.call_count == 0


# ---------------------------------------------------------------------------
# get_skylight_events
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_skylight_events_with_aoi(mocker, integration, auth, pull_events_config, mock_skylight_gql_response):
    mocker.patch("app.actions.client.state_manager.get_state", return_value=None)
    mocker.patch("app.actions.client.build_request_header", return_value=MagicMock(dict=lambda: {}))
    mocker.patch("app.actions.client.build_graphql_client")
    mock_execute = mocker.patch(
        "app.actions.client.execute_gql_query",
        side_effect=[mock_skylight_gql_response, {"searchEventsV2": {"records": [], "meta": {"snapshotId": None, "total": 0}}}]
    )

    result, _, _end_time = await get_skylight_events(integration, pull_events_config, auth)

    assert "global" in result
    assert len(result["global"]) == 2
    call_params = mock_execute.call_args_list[0][0][2]
    assert call_params["aoiId"] == "aoi1"


@pytest.mark.asyncio
async def test_get_skylight_events_without_aoi_raises(mocker, integration, auth, pull_events_config_no_aoi):
    from app.actions.client import PullEventsBadConfigException
    mocker.patch("app.actions.client.state_manager.get_state", return_value=None)
    mocker.patch("app.actions.client.build_request_header", return_value=MagicMock(dict=lambda: {}))
    mocker.patch("app.actions.client.build_graphql_client")

    with pytest.raises(PullEventsBadConfigException):
        await get_skylight_events(integration, pull_events_config_no_aoi, auth)


@pytest.mark.asyncio
async def test_get_skylight_events_multiple_aois(mocker, integration, auth):
    config = PullEventsConfig(aoi_ids=["aoi-a", "aoi-b"], event_types=["fishing"], pageSize=100)
    page_a = {"searchEventsV2": {"records": [{"eventId": "evt-1"}], "meta": {"snapshotId": "s1", "total": 1}}}
    page_a_empty = {"searchEventsV2": {"records": [], "meta": {"snapshotId": "s1", "total": 1}}}
    page_b = {"searchEventsV2": {"records": [{"eventId": "evt-2"}], "meta": {"snapshotId": "s2", "total": 1}}}
    page_b_empty = {"searchEventsV2": {"records": [], "meta": {"snapshotId": "s2", "total": 1}}}

    mocker.patch("app.actions.client.state_manager.get_state", return_value=None)
    mocker.patch("app.actions.client.build_request_header", return_value=MagicMock(dict=lambda: {}))
    mocker.patch("app.actions.client.build_graphql_client")
    mocker.patch(
        "app.actions.client.execute_gql_query",
        side_effect=[page_a, page_a_empty, page_b, page_b_empty]
    )

    result, _, _end_time = await get_skylight_events(integration, config, auth)

    assert len(result["global"]) == 2
    assert result["global"][0]["eventId"] == "evt-1"
    assert result["global"][1]["eventId"] == "evt-2"


@pytest.mark.asyncio
async def test_get_skylight_events_pagination_uses_snapshot_id(mocker, integration, auth, pull_events_config):
    page1 = {"searchEventsV2": {"records": [{"eventId": "evt-1"}], "meta": {"snapshotId": "snap-xyz", "total": 2}}}
    page2 = {"searchEventsV2": {"records": [{"eventId": "evt-2"}], "meta": {"snapshotId": "snap-xyz", "total": 2}}}
    page3 = {"searchEventsV2": {"records": [], "meta": {"snapshotId": "snap-xyz", "total": 2}}}

    mocker.patch("app.actions.client.state_manager.get_state", return_value=None)
    mocker.patch("app.actions.client.build_request_header", return_value=MagicMock(dict=lambda: {}))
    mocker.patch("app.actions.client.build_graphql_client")
    mock_execute = mocker.patch(
        "app.actions.client.execute_gql_query",
        side_effect=[page1, page2, page3]
    )

    result, _, _end_time = await get_skylight_events(integration, pull_events_config, auth)

    assert len(result["global"]) == 2
    # Second and third calls should include snapshotId
    assert mock_execute.call_args_list[1][0][2].get("snapshotId") == "snap-xyz"
    assert mock_execute.call_args_list[2][0][2].get("snapshotId") == "snap-xyz"


@pytest.mark.asyncio
async def test_get_skylight_events_uses_saved_state_as_start_time(mocker, integration, auth, pull_events_config):
    saved_state = {"start_time": "2025-04-01T12:00:00+00:00"}
    empty_response = {"searchEventsV2": {"records": [], "meta": {"snapshotId": None, "total": 0}}}

    mocker.patch("app.actions.client.state_manager.get_state", return_value=saved_state)
    mocker.patch("app.actions.client.build_request_header", return_value=MagicMock(dict=lambda: {}))
    mocker.patch("app.actions.client.build_graphql_client")
    mock_execute = mocker.patch("app.actions.client.execute_gql_query", return_value=empty_response)

    _, _, _end_time = await get_skylight_events(integration, pull_events_config, auth)

    call_params = mock_execute.call_args_list[0][0][2]
    assert "2025-04-01" in call_params["startTime"]


# ---------------------------------------------------------------------------
# transform
# ---------------------------------------------------------------------------

def test_transform_fishing_event(skylight_fishing_event):
    result = transform(CONFIG, skylight_fishing_event)
    assert result["event_type"] == "fishing_alert_rep"
    assert result["event_details"]["fishing_score"] == 0.92
    assert result["event_details"]["vessel_name"] == "Pescador I"
    assert result["event_details"]["mmsi"] == "701234567"
    assert result["location"] == {"lat": -40.1, "lon": -65.1}


def test_transform_dark_rendezvous_event(skylight_dark_rendezvous_event):
    result = transform(CONFIG, skylight_dark_rendezvous_event)
    assert result["event_type"] == "dark_rendezvous_alert_rep"
    assert result["event_details"]["osr_score"] == 0.87
    vessels = result["event_details"]["vessels"]
    assert vessels[0]["vessel_name"] == "Dark Ship"


def test_transform_standard_rendezvous_with_vessel1(skylight_standard_rendezvous_event):
    result = transform(CONFIG, skylight_standard_rendezvous_event)
    assert result["event_type"] == "standard_rendezvous_alert_rep"
    vessels = result["event_details"]["vessels"]
    assert len(vessels) == 2
    assert vessels[0]["vessel_name"] == "Vessel A"
    assert vessels[1]["vessel_name"] == "Vessel B"
    assert vessels[1]["mmsi"] == "701333333"
    assert vessels[1]["country"] == "China"


def test_transform_speed_range_event(skylight_speed_range_event):
    result = transform(CONFIG, skylight_speed_range_event)
    assert result["event_type"] == "speed_range_alert_rep"
    assert result["event_details"]["average_speed_range"] == 14.5
    assert result["event_details"]["distance"] == 28.9
    assert result["event_details"]["duration_in_seconds"] == 7200.0


def test_transform_aoi_visit_event(skylight_aoi_visit_event):
    result = transform(CONFIG, skylight_aoi_visit_event)
    assert result["event_type"] == "entry_alert_rep"
    assert result["event_details"]["entry_speed"] == 6.2
    assert result["event_details"]["entry_heading"] == 270
    assert result["event_details"]["end_heading"] == 90


def test_transform_imagery_event_dark(skylight_imagery_event):
    # detectionType == "dark" → dark_detection_rep
    result = transform(CONFIG, skylight_imagery_event)
    assert result["event_type"] == "dark_detection_rep"
    assert result["title"] == "Dark Vessel Detection"
    assert result["event_details"]["detection_type"] == "Sentinel-1 Radar"
    assert result["event_details"]["score"] == 0.78
    assert result["event_details"]["estimated_length"] == 95.0
    assert result["event_details"]["distance_to_coast_m"] == 52000
    assert "imageUrl" not in result["event_details"]  # sent as attachment


def test_transform_imagery_event_ais_correlated(skylight_imagery_event):
    # detectionType == "ais_correlated" → ais_correlated_detection_rep
    event = dict(skylight_imagery_event)
    event["eventDetails"] = dict(event["eventDetails"])
    event["eventDetails"]["detectionType"] = "ais_correlated"
    result = transform(CONFIG, event)
    assert result["event_type"] == "ais_correlated_detection_rep"
    assert result["title"] == "AIS Correlated Vessel Detection"
    assert result["event_details"]["detection_type"] == "Sentinel-1 Radar"


def test_transform_viirs_event_dark(skylight_viirs_event):
    # detectionType == "dark" → dark_detection_rep
    result = transform(CONFIG, skylight_viirs_event)
    assert result["event_type"] == "dark_detection_rep"
    assert result["title"] == "Dark Vessel Detection"
    assert result["event_details"]["radiance"] == 3.14
    assert result["event_details"]["detection_type"] == "Night Lights (VIIRS)"
    assert "imageUrl" not in result["event_details"]  # sent as attachment, not a field


def test_transform_viirs_event_ais_correlated():
    event = {
        "eventId": "evt-viirs-ais-001",
        "eventType": "viirs",
        "createdAt": "2025-04-01T00:00:00Z",
        "updatedAt": "2025-04-01T01:00:00Z",
        "start": {"point": {"lat": -43.0, "lon": -64.0}, "time": "2025-04-01T03:00:00Z"},
        "end": None,
        "vessels": {
            "vessel0": {
                "name": "AIS Ship", "mmsi": "701999999", "imo": None,
                "countryCode": "CN", "trackId": "track-viirs-ais",
                "category": "cargo", "length": 90
            },
            "vessel1": None
        },
        "eventDetails": {
            "imageUrl": "https://example.com/viirs/ais.png",
            "detectionType": "ais_correlated",
            "estimatedLength": 88.0,
            "radianceNw": 5.5
        }
    }
    result = transform(CONFIG, event)
    assert result["event_type"] == "ais_correlated_detection_rep"
    assert result["title"] == "AIS Correlated Vessel Detection"
    assert result["event_details"]["detection_type"] == "Night Lights (VIIRS)"
    assert result["event_details"]["vessel_name"] == "AIS Ship"
    assert result["event_details"]["mmsi"] == "701999999"


def test_transform_uses_end_point_for_location(skylight_fishing_event):
    result = transform(CONFIG, skylight_fishing_event)
    assert result["location"]["lat"] == -40.1
    assert result["location"]["lon"] == -65.1


def test_transform_falls_back_to_start_point_when_no_end():
    data = {
        "eventType": "fishing_activity_history",
        "eventDetails": {"fishingScore": 0.5},
        "start": {"point": {"lat": 10.0, "lon": 20.0}, "time": "2025-04-01T00:00:00Z"},
        "end": None,
        "vessels": {}
    }
    result = transform(CONFIG, data)
    assert result["location"] == {"lat": 10.0, "lon": 20.0}


def test_transform_unknown_event_type_returns_empty():
    data = {
        "eventType": "unknown_type",
        "eventDetails": {},
        "end": {"point": {"lat": 0.0, "lon": 0.0}, "time": "2025-04-01T00:00:00Z"},
        "vessels": {}
    }
    result = transform(CONFIG, data)
    assert result == {}


def test_transform_adds_event_id_and_entry_link(skylight_fishing_event):
    result = transform(CONFIG, skylight_fishing_event)
    assert "skylight_link" in result["event_details"]


def test_transform_with_vessel_info():
    data = {
        "eventType": "standard_rendezvous",
        "eventDetails": {},
        "end": {"point": {"lat": 5.88, "lon": 115.69}, "time": "2025-02-28T02:46:18.489582Z"},
        "vessels": {"vessel1": {"name": "Vessel B", "mmsi": "701333333", "displayCountry": "China"}}
    }
    result = transform(CONFIG, data)
    vessels = result["event_details"]["vessels"]
    assert vessels[0]["vessel_name"] == "Vessel B"
    assert vessels[0]["mmsi"] == "701333333"
    assert vessels[0]["country"] == "China"


def test_transform_with_empty_vessel_detail(skylight_client):
    data = {
        "eventType": "fishing_activity_history",
        "eventDetails": {},
        "end": {"point": {"lat": 5.88, "lon": 115.69}, "time": "2025-02-28T02:46:18.489582Z"},
        "vessels": {"vessel0": None}
    }
    result = transform(CONFIG, data)
    assert "vessel0_id" not in result["event_details"]
    assert "vessel0_name" not in result["event_details"]


def test_transform_without_vessel_info(skylight_client):
    data = {
        "eventType": "fishing_activity_history",
        "eventDetails": {},
        "end": {"point": {"lat": 5.88, "lon": 115.69}, "time": "2025-02-28T02:46:18.489582Z"},
    }
    result = transform(CONFIG, data)
    assert "vessel0_id" not in result["event_details"]
    assert "vessel0_name" not in result["event_details"]


# ---------------------------------------------------------------------------
# action_process_events_per_aoi
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_action_process_events_per_aoi_success(mocker, integration, process_events_config, mock_publish_event):
    mocker.patch("app.services.state.IntegrationStateManager.get_state", return_value=None)
    mocker.patch("app.services.state.IntegrationStateManager.set_state", return_value=None)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mocker.patch("app.services.action_scheduler.publish_event", mock_publish_event)
    mocker.patch("app.actions.handlers.transform", return_value={"eventId": "event1"})
    mocker.patch("app.actions.handlers.generate_batches", return_value=[[{"eventId": "event1"}]])
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
    mocker.patch("app.actions.handlers.transform", return_value={"eventId": "event1"})
    mocker.patch("app.actions.handlers.generate_batches", return_value=[[{"eventId": "event1"}]])
    mocker.patch("app.actions.handlers.gundi_tools.send_events_to_gundi", side_effect=httpx.HTTPError("Error"))

    with pytest.raises(httpx.HTTPError):
        await action_process_events_per_aoi(integration, process_events_config)


# ---------------------------------------------------------------------------
# action_pull_events
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_action_pull_events_triggers_process_events_per_aoi(mocker, integration, pull_events_config, mock_publish_event):
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mocker.patch("app.services.action_scheduler.publish_event", mock_publish_event)
    mocker.patch("app.actions.client.get_skylight_events", return_value=({"global": [{"eventId": "event1"}]}, [], "2025-04-01T12:00:00+00:00"))
    mocker.patch("app.actions.client.get_auth_config", return_value=None)
    mocker.patch("app.services.state.IntegrationStateManager.get_state", return_value=None)
    mocker.patch("app.services.state.IntegrationStateManager.set_state", return_value=None)
    mock_trigger_action = mocker.patch("app.actions.handlers.trigger_action", return_value=None)

    result = await action_pull_events(integration, pull_events_config)
    assert result["process_events_per_aoi_action_triggered"] == 1
    mock_trigger_action.assert_called_once_with(
        integration.id,
        "process_events_per_aoi",
        config=ProcessEventsPerAOIConfig(
            integration_id=integration.id,
            aoi="global",
            events=[{"eventId": "event1"}],
            updated_config_data=[]
        )
    )


@pytest.mark.asyncio
async def test_action_pull_events_patches_existing_events(mocker, integration, pull_events_config, mock_publish_event):
    saved_event = {"object_id": "gundi-obj-001"}
    raw_event = {"eventId": "evt-001"}

    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mocker.patch("app.services.action_scheduler.publish_event", mock_publish_event)
    mocker.patch("app.actions.client.get_skylight_events", return_value=({"global": [raw_event]}, [], "2025-04-01T12:00:00+00:00"))
    mocker.patch("app.actions.client.get_auth_config", return_value=None)
    mocker.patch("app.services.state.IntegrationStateManager.get_state", return_value=saved_event)
    mocker.patch("app.services.state.IntegrationStateManager.set_state", return_value=None)
    mock_patch = mocker.patch("app.actions.handlers.patch_events", return_value=[{"object_id": "gundi-obj-001"}])
    mocker.patch("app.actions.handlers.trigger_action", return_value=None)

    result = await action_pull_events(integration, pull_events_config)

    mock_patch.assert_called_once()
    assert result.get("events_updated") == 1


@pytest.mark.asyncio
async def test_action_pull_events_no_events_returns_early(mocker, integration, pull_events_config, mock_publish_event):
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mocker.patch("app.services.action_scheduler.publish_event", mock_publish_event)
    mocker.patch("app.actions.client.get_skylight_events", return_value=({"global": []}, [], "2025-04-01T12:00:00+00:00"))
    mocker.patch("app.actions.client.get_auth_config", return_value=None)
    mocker.patch("app.services.state.IntegrationStateManager.get_state", return_value=None)
    mocker.patch("app.services.state.IntegrationStateManager.set_state", return_value=None)

    result = await action_pull_events(integration, pull_events_config)

    assert result["events_extracted"] == 0
    assert result["process_events_per_aoi_action_triggered"] == 0


# ---------------------------------------------------------------------------
# process_attachments
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_process_attachments_success(mocker, integration):
    transformed_data = [{"event_details": {"imageUrl": "https://example.com/image.png"}}]
    response = [{"object_id": "event1"}]
    mock_image_content = b"image data"

    mocker.patch("httpx.AsyncClient.get", return_value=mocker.AsyncMock(
        status_code=200, aread=mocker.AsyncMock(return_value=mock_image_content)
    ))
    mock_send = mocker.patch("app.actions.handlers.gundi_tools.send_event_attachments_to_gundi", return_value=None)

    await process_attachments(transformed_data, response, integration)

    mock_send.assert_called_once_with(
        event_id="event1",
        attachments=[("image.png", mock_image_content)],
        integration_id=integration.id
    )


@pytest.mark.asyncio
async def test_process_attachments_403_error(mocker, integration):
    transformed_data = [{"event_details": {"imageUrl": "https://example.com/image.png"}}]
    response = [{"object_id": "event1"}]

    mocker.patch("httpx.AsyncClient.get", side_effect=httpx.HTTPStatusError(
        "403 Forbidden", request=mocker.Mock(), response=mocker.Mock(status_code=403)
    ))
    mock_log = mocker.patch("app.actions.handlers.log_action_activity", return_value=None)

    await process_attachments(transformed_data, response, integration)

    mock_log.assert_called_once()


@pytest.mark.asyncio
async def test_process_attachments_skips_events_without_image(mocker, integration):
    transformed_data = [{"event_details": {"fishingScore": 0.9}}]
    response = [{"object_id": "event1"}]

    mock_send = mocker.patch("app.actions.handlers.gundi_tools.send_event_attachments_to_gundi", return_value=None)

    await process_attachments(transformed_data, response, integration)

    mock_send.assert_not_called()


# ---------------------------------------------------------------------------
# AOI deduplication
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_skylight_events_deduplicates_same_event_across_aois(mocker, integration, auth):
    config = PullEventsConfig(aoi_ids=["aoi-a", "aoi-b"], event_types=["fishing"], pageSize=100)
    # Same base event returned for both AOIs with different suffixes
    event_aoi_a = {"eventId": "evt-abc;aoi-a"}
    event_aoi_b = {"eventId": "evt-abc;aoi-b"}
    page_a = {"searchEventsV2": {"records": [event_aoi_a], "meta": {"snapshotId": "s1", "total": 1}}}
    page_a_empty = {"searchEventsV2": {"records": [], "meta": {"snapshotId": "s1", "total": 1}}}
    page_b = {"searchEventsV2": {"records": [event_aoi_b], "meta": {"snapshotId": "s2", "total": 1}}}
    page_b_empty = {"searchEventsV2": {"records": [], "meta": {"snapshotId": "s2", "total": 1}}}

    mocker.patch("app.actions.client.state_manager.get_state", return_value=None)
    mocker.patch("app.actions.client.build_request_header", return_value=MagicMock(dict=lambda: {}))
    mocker.patch("app.actions.client.build_graphql_client")
    mocker.patch(
        "app.actions.client.execute_gql_query",
        side_effect=[page_a, page_a_empty, page_b, page_b_empty]
    )

    result, _, _end_time = await get_skylight_events(integration, config, auth)

    assert len(result["global"]) == 1
    assert result["global"][0]["eventId"] == "evt-abc;aoi-a"


@pytest.mark.asyncio
async def test_get_skylight_events_returns_end_time(mocker, integration, auth, pull_events_config):
    empty_response = {"searchEventsV2": {"records": [], "meta": {"snapshotId": None, "total": 0}}}
    mocker.patch("app.actions.client.state_manager.get_state", return_value=None)
    mocker.patch("app.actions.client.build_request_header", return_value=MagicMock(dict=lambda: {}))
    mocker.patch("app.actions.client.build_graphql_client")
    mocker.patch("app.actions.client.execute_gql_query", return_value=empty_response)

    _, _, end_time = await get_skylight_events(integration, pull_events_config, auth)

    assert end_time is not None
    assert "T" in end_time  # ISO format


# ---------------------------------------------------------------------------
# transform — entry alert (aoi_visit) changes
# ---------------------------------------------------------------------------

def test_transform_aoi_visit_uses_start_for_location(skylight_aoi_visit_event):
    result = transform(CONFIG, skylight_aoi_visit_event)
    assert result["location"] == {"lat": -40.5, "lon": -62.0}  # start point, not end


def test_transform_aoi_visit_uses_start_for_recorded_at(skylight_aoi_visit_event):
    result = transform(CONFIG, skylight_aoi_visit_event)
    assert "2025-04-01T06" in result["recorded_at"].isoformat()  # start time


def test_transform_aoi_visit_includes_exit_datetime(skylight_aoi_visit_event):
    result = transform(CONFIG, skylight_aoi_visit_event)
    assert result["event_details"]["exit_date"] == "2025-04-01T08:00:00Z"
    assert result["event_details"]["duration_in_area"] == 2.0


def test_transform_aoi_visit_no_end_shows_pending():
    event = {
        "eventId": "evt-aoi-noend",
        "eventType": "aoi_visit",
        "start": {"point": {"lat": -40.5, "lon": -62.0}, "time": "2025-04-01T06:00:00Z"},
        "end": None,
        "vessels": {},
        "eventDetails": {"entrySpeed": 5.0, "entryHeading": 180, "endHeading": None}
    }
    result = transform(CONFIG, event)
    assert result["location"] == {"lat": -40.5, "lon": -62.0}
    assert result["event_details"]["exit_date"] == "Pending"
    assert result["event_details"]["duration_in_area"] == "Pending"



def test_transform_fishing_includes_skylight_link(skylight_fishing_event):
    result = transform(CONFIG, skylight_fishing_event)
    assert "skylight_link" in result["event_details"]
    assert result["event_details"]["fishing_score"] == 0.92


def test_transform_excludes_typename_from_event_details(skylight_standard_rendezvous_event):
    result = transform(CONFIG, skylight_standard_rendezvous_event)
    assert "__typename" not in result["event_details"]


def test_transform_non_entry_event_includes_duration(skylight_dark_rendezvous_event):
    result = transform(CONFIG, skylight_dark_rendezvous_event)
    assert result["event_details"]["duration_hours"] == 2.0


def test_transform_non_entry_event_no_duration_when_no_end():
    data = {
        "eventType": "fishing_activity_history",
        "eventDetails": {"fishingScore": 0.5},
        "start": {"point": {"lat": 10.0, "lon": 20.0}, "time": "2025-04-01T00:00:00Z"},
        "end": None,
        "vessels": {}
    }
    result = transform(CONFIG, data)
    assert "duration_hours" not in result["event_details"]


# ---------------------------------------------------------------------------
# action_pull_events — cursor saved to state
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_action_pull_events_saves_cursor_to_state(mocker, integration, pull_events_config, mock_publish_event):
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mocker.patch("app.services.action_scheduler.publish_event", mock_publish_event)
    mocker.patch("app.actions.client.get_skylight_events", return_value=({"global": []}, [], "2025-05-01T10:00:00+00:00"))
    mocker.patch("app.actions.client.get_auth_config", return_value=None)
    mocker.patch("app.services.state.IntegrationStateManager.get_state", return_value=None)
    mock_set_state = mocker.patch("app.services.state.IntegrationStateManager.set_state", return_value=None)

    await action_pull_events(integration, pull_events_config)

    mock_set_state.assert_called_once_with(
        str(integration.id),
        "pull_events",
        {"start_time": "2025-05-01T10:00:00+00:00"},
        "global"
    )


# ---------------------------------------------------------------------------
# Bug fixes
# ---------------------------------------------------------------------------

def test_get_clean_event_id_handles_none_event_id():
    from app.actions.handlers import get_clean_event_id
    event = {"eventId": None}
    # Should not raise; falls back to None rather than crashing
    result = get_clean_event_id(event)
    assert result is None


def test_get_clean_event_id_handles_missing_event_id():
    from app.actions.handlers import get_clean_event_id
    result = get_clean_event_id({})
    assert result is None


def test_action_process_events_sort_does_not_crash_with_null_timestamp(mocker, integration, mock_publish_event):
    """Sort must not raise TypeError when an event has a null time field (naive vs aware datetime)."""
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    event_with_null_time = {
        "eventId": "evt-nulltime",
        "eventType": "fishing_activity_history",
        "eventDetails": {"fishingScore": 0.8},
        "start": {"point": {"lat": 10.0, "lon": 20.0}, "time": None},
        "end": None,
        "vessels": {},
    }
    event_normal = {
        "eventId": "evt-normal",
        "eventType": "fishing_activity_history",
        "eventDetails": {"fishingScore": 0.5},
        "start": {"point": {"lat": 10.0, "lon": 20.0}, "time": "2025-04-01T00:00:00Z"},
        "end": None,
        "vessels": {},
    }
    config = ProcessEventsPerAOIConfig(
        integration_id="integration_id",
        aoi="global",
        events=[event_with_null_time, event_normal],
        updated_config_data=list(CONFIG),
    )
    mocker.patch("app.actions.handlers.gundi_tools.send_events_to_gundi", new_callable=AsyncMock, return_value=[{"object_id": "obj-1"}])
    mocker.patch("app.actions.handlers.gundi_tools.send_event_attachments_to_gundi", new_callable=AsyncMock)
    mocker.patch("app.actions.handlers.state_manager.set_state", new_callable=AsyncMock)

    import asyncio
    # Should not raise TypeError
    asyncio.get_event_loop().run_until_complete(
        action_process_events_per_aoi(integration=integration, action_config=config)
    )


@pytest.mark.asyncio
async def test_action_process_events_state_saved_with_correct_event_ids(mocker, integration, mock_publish_event):
    """State must be saved aligned with the sorted order, not the original event order."""
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)

    older_event = {
        "eventId": "evt-older;aoi1",
        "eventType": "fishing_activity_history",
        "eventDetails": {"fishingScore": 0.5},
        "start": {"point": {"lat": 10.0, "lon": 20.0}, "time": "2025-01-01T00:00:00Z"},
        "end": None,
        "vessels": {},
    }
    newer_event = {
        "eventId": "evt-newer;aoi1",
        "eventType": "fishing_activity_history",
        "eventDetails": {"fishingScore": 0.9},
        "start": {"point": {"lat": 11.0, "lon": 21.0}, "time": "2025-06-01T00:00:00Z"},
        "end": None,
        "vessels": {},
    }
    config = ProcessEventsPerAOIConfig(
        integration_id="integration_id",
        aoi="global",
        events=[older_event, newer_event],  # older first
        updated_config_data=list(CONFIG),
    )
    # Gundi returns object IDs in the order events are sent (newer first after sort)
    mocker.patch(
        "app.actions.handlers.gundi_tools.send_events_to_gundi",
        new_callable=AsyncMock,
        return_value=[{"object_id": "obj-newer"}, {"object_id": "obj-older"}]
    )
    mocker.patch("app.actions.handlers.gundi_tools.send_event_attachments_to_gundi", new_callable=AsyncMock)
    mock_set_state = mocker.patch("app.actions.handlers.state_manager.set_state", new_callable=AsyncMock)

    await action_process_events_per_aoi(integration=integration, action_config=config)

    saved_calls = {call.kwargs["source_id"]: call.kwargs["state"] for call in mock_set_state.call_args_list}
    assert saved_calls["evt-newer"]["object_id"] == "obj-newer"
    assert saved_calls["evt-older"]["object_id"] == "obj-older"
