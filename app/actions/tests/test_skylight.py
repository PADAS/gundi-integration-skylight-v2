import pytest
import httpx

from gql.transport.exceptions import TransportQueryError

from app.actions.client import execute_gql_query
from app.actions.configurations import ProcessEventsPerAOIConfig
from app.actions.handlers import action_pull_events, action_process_events_per_aoi, process_attachments, transform
from app.services.state import IntegrationStateManager

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
async def test_execute_gql_query_retry_on_unauthorized(mocker, gql_client, integration, auth, state_manager):
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
    assert state_manager.delete_state.call_count == 3


@pytest.mark.asyncio
async def test_execute_gql_query_delete_state_on_retry(mocker, gql_client, integration, auth, state_manager):
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
    assert state_manager.delete_state.call_count == 3


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
    skylight_client.EMPTY_VESSEL_DICT = {"vessel_0_id": "N/A", "vessel_0_name": "N/A"}
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
    skylight_client.EMPTY_VESSEL_DICT = {"vessel_0_id": "N/A", "vessel_0_name": "N/A"}
    config = [{"skylight_event_type": "some_event_type", "event_title": "Test Event", "event_type": "test_event"}]
    result = transform(config, data)
    assert result["event_details"]["vessel_0_id"] == "N/A"
    assert result["event_details"]["vessel_0_name"] == "N/A"
