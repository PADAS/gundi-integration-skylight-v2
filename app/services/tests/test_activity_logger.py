import aiohttp
import pytest
from types import SimpleNamespace
from unittest.mock import ANY
from gundi_core.events import (
    LogLevel,
    IntegrationActionStarted,
    IntegrationActionComplete,
    IntegrationActionFailed,
    IntegrationActionCustomLog,
    IntegrationWebhookStarted,
    IntegrationWebhookComplete,
    IntegrationWebhookFailed
)
from app import settings
from app.services.activity_logger import publish_event, activity_logger, webhook_activity_logger, log_activity
from app.webhooks import GenericJsonPayload, GenericJsonTransformConfig


@pytest.mark.parametrize(
    "system_event",
    [
        "action_started_event", "action_complete_event", "action_failed_event", "custom_activity_log_event",
        "webhook_started_event", "webhook_complete_event", "webhook_failed_event", "webhook_custom_activity_log_event"
    ],
    indirect=["system_event"])
@pytest.mark.asyncio
async def test_publish_event(
        mocker, mock_pubsub_client, integration_event_pubsub_message, gcp_pubsub_publish_response,
        system_event
):
    mocker.patch("app.services.activity_logger.pubsub", mock_pubsub_client)

    response = await publish_event(
        event=system_event,
        topic_name=settings.INTEGRATION_EVENTS_TOPIC
    )

    assert response == gcp_pubsub_publish_response
    assert mock_pubsub_client.PublisherClient.called
    assert mock_pubsub_client.PubsubMessage.called
    assert mock_pubsub_client.PublisherClient.called
    assert mock_pubsub_client.PublisherClient.return_value.publish.called
    mock_pubsub_client.PublisherClient.return_value.publish.assert_any_call(
        f"projects/{settings.GCP_PROJECT_ID}/topics/{settings.INTEGRATION_EVENTS_TOPIC}",
        [integration_event_pubsub_message],
    )


def _client_response_error(status, message):
    request_info = SimpleNamespace(real_url="https://pubsub.googleapis.com/v1/topics/test:publish")
    return aiohttp.ClientResponseError(
        request_info=request_info, history=(), status=status, message=message
    )


@pytest.fixture
def fast_retries(mocker):
    # stamina instantiates tenacity.AsyncRetrying at call time (via
    # stamina._core._t), and tenacity binds asyncio.sleep as its default
    # sleep. Inject an instant sleep so retry waits don't slow tests down.
    import stamina._core

    real_async_retrying = stamina._core._t.AsyncRetrying

    async def _instant_sleep(_delay, result=None):
        return result

    mocker.patch.object(
        stamina._core._t,
        "AsyncRetrying",
        lambda **kwargs: real_async_retrying(sleep=_instant_sleep, **kwargs),
    )


@pytest.mark.asyncio
async def test_publish_event_does_not_retry_client_errors(
        mocker, mock_pubsub_client, action_started_event, fast_retries
):
    # A 4xx (e.g. an oversized message rejected with 400) will never succeed
    # on retry, so publish_event must fail fast instead of burning 5 attempts.
    mocker.patch("app.services.activity_logger.pubsub", mock_pubsub_client)
    publish_mock = mock_pubsub_client.PublisherClient.return_value.publish
    publish_mock.side_effect = _client_response_error(400, "Bad Request")

    with pytest.raises(aiohttp.ClientResponseError):
        await publish_event(
            event=action_started_event,
            topic_name=settings.INTEGRATION_EVENTS_TOPIC,
        )

    assert publish_mock.call_count == 1


@pytest.mark.parametrize("status", [500, 503, 429])
@pytest.mark.asyncio
async def test_publish_event_retries_transient_errors(
        mocker, mock_pubsub_client, gcp_pubsub_publish_response, action_started_event, fast_retries, status
):
    mocker.patch("app.services.activity_logger.pubsub", mock_pubsub_client)
    publish_mock = mock_pubsub_client.PublisherClient.return_value.publish

    async def _ok():
        return gcp_pubsub_publish_response

    publish_mock.side_effect = [
        _client_response_error(status, "Transient error"),
        _client_response_error(status, "Transient error"),
        _ok(),
    ]

    response = await publish_event(
        event=action_started_event,
        topic_name=settings.INTEGRATION_EVENTS_TOPIC,
    )

    assert response == gcp_pubsub_publish_response
    assert publish_mock.call_count == 3


@pytest.mark.asyncio
async def test_activity_logger_decorator(
        mocker, mock_publish_event, integration_v2, pull_observations_config
):

    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)

    @activity_logger()
    async def action_pull_observations(integration, action_config):
        return {"observations_extracted": 10}

    await action_pull_observations(
        integration=integration_v2,
        action_config=pull_observations_config
    )

    # Two events expected: One on start and one on completion
    assert mock_publish_event.call_count == 2
    assert isinstance(mock_publish_event.call_args_list[0].kwargs.get("event"), IntegrationActionStarted)
    assert isinstance(mock_publish_event.call_args_list[1].kwargs.get("event"), IntegrationActionComplete)


@pytest.mark.asyncio
async def test_webhook_activity_logger(
        mocker, mock_publish_event, integration_v2_with_webhook_generic,
        mock_webhook_request_payload_for_dynamic_schema, mock_generic_webhook_config
):

    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)

    @webhook_activity_logger()
    async def webhook_handler(payload: GenericJsonPayload, integration=None, webhook_config: GenericJsonTransformConfig = None):
        return {"observations_extracted": 10}

    await webhook_handler(
        payload=GenericJsonPayload(data=mock_webhook_request_payload_for_dynamic_schema),
        integration=integration_v2_with_webhook_generic,
        webhook_config=GenericJsonTransformConfig(**mock_generic_webhook_config)
    )

    # Two events expected: One on start and one on completion
    assert mock_publish_event.call_count == 2
    assert isinstance(mock_publish_event.call_args_list[0].kwargs.get("event"), IntegrationWebhookStarted)
    assert isinstance(mock_publish_event.call_args_list[1].kwargs.get("event"), IntegrationWebhookComplete)


@pytest.mark.asyncio
async def test_webhook_activity_logger_on_error(
        mocker, mock_publish_event, integration_v2_with_webhook_generic,
        mock_webhook_request_payload_for_dynamic_schema, mock_generic_webhook_config
):
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)

    @webhook_activity_logger()
    async def webhook_handler(payload: GenericJsonPayload, integration=None,
                              webhook_config: GenericJsonTransformConfig = None):
        raise Exception("Something went wrong")

    with pytest.raises(Exception):
        await webhook_handler(
            payload=GenericJsonPayload(data=mock_webhook_request_payload_for_dynamic_schema),
            integration=integration_v2_with_webhook_generic,
            webhook_config=GenericJsonTransformConfig(**mock_generic_webhook_config)
        )

    # Two events expected: One on start and one on error
    assert mock_publish_event.call_count == 2
    assert isinstance(mock_publish_event.call_args_list[0].kwargs.get("event"), IntegrationWebhookStarted)
    assert isinstance(mock_publish_event.call_args_list[1].kwargs.get("event"), IntegrationWebhookFailed)


@pytest.mark.asyncio
async def test_activity_logger_decorator_with_arguments(
        mocker, mock_publish_event, integration_v2, pull_observations_config
):

    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)

    @activity_logger(on_start=False, on_completion=True, on_error=False)
    async def action_pull_observations(integration, action_config):
        return {"observations_extracted": 10}

    await action_pull_observations(
        integration=integration_v2,
        action_config=pull_observations_config
    )

    # Only one event expected, on completion
    assert mock_publish_event.call_count == 1
    assert isinstance(mock_publish_event.call_args_list[0].kwargs.get("event"), IntegrationActionComplete)


@pytest.mark.asyncio
async def test_activity_logger_decorator_on_error(
        mocker, mock_publish_event, integration_v2, pull_observations_config
):

    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)

    @activity_logger()
    async def action_pull_observations(integration, action_config):
        raise Exception("Something went wrong")

    with pytest.raises(Exception):
        await action_pull_observations(
            integration=integration_v2,
            action_config=pull_observations_config
        )

    # Two events expected: One on start and one on error
    assert mock_publish_event.call_count == 2
    assert isinstance(mock_publish_event.call_args_list[0].kwargs.get("event"), IntegrationActionStarted)
    assert isinstance(mock_publish_event.call_args_list[1].kwargs.get("event"), IntegrationActionFailed)


@pytest.mark.asyncio
async def test_log_activity_with_debug_level(mocker, integration_v2, pull_observations_config, mock_publish_event):
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    await log_activity(
        integration_id=integration_v2.id,
        action_id="pull_observations",
        level=LogLevel.DEBUG,
        title="Extracted 10 observations from 2 devices",
        data={"devices": ["deviceid1", "deviceid2"]},
        config_data=pull_observations_config.dict()
    )
    assert mock_publish_event.call_count == 1
    assert isinstance(mock_publish_event.call_args_list[0].kwargs.get("event"), IntegrationActionCustomLog)


@pytest.mark.asyncio
async def test_log_activity_with_info_level(mocker, integration_v2, mock_publish_event, pull_observations_config):
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    await log_activity(
        integration_id=integration_v2.id,
        action_id="pull_observations",
        level=LogLevel.INFO,
        title="Extracting observations with filter..",
        data={"start_date": "2024-01-01", "end_date": "2024-01-31"},
        config_data=pull_observations_config.dict()
    )
    assert mock_publish_event.call_count == 1
    assert isinstance(mock_publish_event.call_args_list[0].kwargs.get("event"), IntegrationActionCustomLog)


@pytest.mark.asyncio
async def test_log_activity_with_warning_level(mocker, integration_v2, mock_publish_event, pull_observations_config):
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    await log_activity(
        integration_id=integration_v2.id,
        action_id="pull_observations",
        level=LogLevel.WARNING,
        title="Skipping end_date because it's greater than today. Please review your configuration.",
        config_data=pull_observations_config.dict()
    )
    assert mock_publish_event.call_count == 1
    assert isinstance(mock_publish_event.call_args_list[0].kwargs.get("event"), IntegrationActionCustomLog)


@pytest.mark.asyncio
async def test_log_activity_with_error_level(mocker, integration_v2, mock_publish_event, pull_observations_config):
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    await log_activity(
        integration_id=integration_v2.id,
        action_id="pull_observations",
        level=LogLevel.ERROR,
        title="Error getting data from System X",
        data={"error": "Connection error with host 'systemx.com'"},
        config_data=pull_observations_config.dict()
    )
    assert mock_publish_event.call_count == 1
    assert isinstance(mock_publish_event.call_args_list[0].kwargs.get("event"), IntegrationActionCustomLog)

