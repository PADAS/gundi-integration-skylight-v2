import pytest
import httpx

from gql.transport.exceptions import TransportQueryError

from app.actions.client import execute_gql_query
from app.actions.configurations import ProcessEventsPerAOIConfig
from app.actions.handlers import action_pull_events, action_process_events_per_aoi
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
async def test_action_process_events_per_aoi_success(mocker, integration, process_events_config):
    mocker.patch("app.actions.handlers.transform", return_value={"event_id": "event1"})
    mocker.patch("app.actions.handlers.generate_batches", return_value=[[{"event_id": "event1"}]])
    mocker.patch("app.actions.handlers.gundi_tools.send_events_to_gundi", return_value=[{"object_id": "event1"}])
    mocker.patch("app.actions.handlers.process_attachments", return_value=None)
    mocker.patch("app.actions.handlers.save_events_state", return_value=None)

    result = await action_process_events_per_aoi(integration, process_events_config)
    assert result["events_processed"] == 1

@pytest.mark.asyncio
async def test_action_process_events_per_aoi_failure(mocker, integration, process_events_config):
    mocker.patch("app.actions.handlers.transform", return_value={"event_id": "event1"})
    mocker.patch("app.actions.handlers.generate_batches", return_value=[[{"event_id": "event1"}]])
    mocker.patch("app.actions.handlers.gundi_tools.send_events_to_gundi", side_effect=httpx.HTTPError("Error"))

    with pytest.raises(httpx.HTTPError):
        await action_process_events_per_aoi(integration, process_events_config)

@pytest.mark.asyncio
async def test_action_pull_events_triggers_process_events_per_aoi(mocker, integration, pull_events_config):
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
