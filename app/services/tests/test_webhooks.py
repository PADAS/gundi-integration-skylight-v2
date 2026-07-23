import logging
from unittest.mock import ANY

import httpx
import pytest
from fastapi.testclient import TestClient

from app.conftest import MockWebhookPayloadModel, MockWebhookConfigModel
from app.main import app
from app.services.webhooks import forward_payload_to_diagnostic_url
from app.webhooks import GenericJsonTransformConfig

api_client = TestClient(app)


@pytest.fixture
def mock_config_manager_for_webhooks(mocker):
    return mocker.AsyncMock()


@pytest.mark.asyncio
async def test_process_webhook_request_with_fixed_schema(
        mocker, integration_v2_with_webhook, mock_config_manager_for_webhooks, mock_publish_event,
        mock_get_webhook_handler_for_fixed_json_payload, mock_webhook_handler,
        mock_webhook_request_headers_onyesha, mock_webhook_request_payload_for_fixed_schema
):
    mock_config_manager_for_webhooks.get_integration_details.return_value = integration_v2_with_webhook
    mocker.patch("app.services.webhooks.get_webhook_handler", mock_get_webhook_handler_for_fixed_json_payload)
    mocker.patch("app.services.webhooks.config_manager", mock_config_manager_for_webhooks)

    response = api_client.post(
        "/webhooks",
        headers=mock_webhook_request_headers_onyesha,
        json=mock_webhook_request_payload_for_fixed_schema,
    )

    assert response.status_code == 200
    assert mock_config_manager_for_webhooks.get_integration_details.called
    assert mock_get_webhook_handler_for_fixed_json_payload.called
    expected_payload = MockWebhookPayloadModel.parse_obj(mock_webhook_request_payload_for_fixed_schema)
    expected_config = MockWebhookConfigModel.parse_obj(integration_v2_with_webhook.webhook_configuration.data)
    mock_webhook_handler.assert_called_once_with(
        payload=expected_payload,
        integration=integration_v2_with_webhook,
        webhook_config=expected_config
    )


@pytest.mark.asyncio
async def test_process_webhook_request_with_dynamic_schema(
        mocker, integration_v2_with_webhook_generic, mock_config_manager_for_webhooks, mock_publish_event,
        mock_get_webhook_handler_for_generic_json_payload, mock_webhook_handler,
        mock_webhook_request_headers_onyesha, mock_webhook_request_payload_for_dynamic_schema
):
    mock_config_manager_for_webhooks.get_integration_details.return_value = integration_v2_with_webhook_generic
    mocker.patch("app.services.webhooks.get_webhook_handler", mock_get_webhook_handler_for_generic_json_payload)
    mocker.patch("app.services.webhooks.config_manager", mock_config_manager_for_webhooks)

    response = api_client.post(
        "/webhooks",
        headers=mock_webhook_request_headers_onyesha,
        json=mock_webhook_request_payload_for_dynamic_schema,
    )

    assert response.status_code == 200
    assert mock_config_manager_for_webhooks.get_integration_details.called
    assert mock_get_webhook_handler_for_generic_json_payload.called
    expected_config = GenericJsonTransformConfig.parse_obj(integration_v2_with_webhook_generic.webhook_configuration.data)
    mock_webhook_handler.assert_called_once_with(
        payload=ANY,
        integration=integration_v2_with_webhook_generic,
        webhook_config=expected_config
    )


@pytest.mark.asyncio
async def test_process_webhook_request_without_integration_is_acked(
        mocker, mock_config_manager_for_webhooks, mock_publish_event,
        mock_get_webhook_handler_for_generic_json_payload, mock_webhook_handler,
        mock_webhook_request_payload_for_dynamic_schema
):
    mocker.patch("app.services.webhooks.get_webhook_handler", mock_get_webhook_handler_for_generic_json_payload)
    mocker.patch("app.services.webhooks.config_manager", mock_config_manager_for_webhooks)

    response = api_client.post(
        "/webhooks",
        headers={"x-consumer-username": "anonymous"},
        json=mock_webhook_request_payload_for_dynamic_schema,
    )

    assert response.status_code == 200
    assert not mock_config_manager_for_webhooks.get_integration_details.called
    assert not mock_webhook_handler.called


@pytest.mark.asyncio
async def test_process_webhook_request_forwards_payload_to_diagnostic_url(
        mocker, integration_v2_with_diagnostic_webhook, mock_config_manager_for_webhooks, mock_publish_event,
        mock_get_webhook_handler_for_generic_json_payload, mock_webhook_handler,
        mock_webhook_request_headers_onyesha, mock_webhook_request_payload_for_dynamic_schema
):
    mock_config_manager_for_webhooks.get_integration_details.return_value = integration_v2_with_diagnostic_webhook
    mocker.patch("app.services.webhooks.get_webhook_handler", mock_get_webhook_handler_for_generic_json_payload)
    mocker.patch("app.services.webhooks.config_manager", mock_config_manager_for_webhooks)
    mock_forward = mocker.patch(
        "app.services.webhooks.forward_payload_to_diagnostic_url", mocker.AsyncMock()
    )

    response = api_client.post(
        "/webhooks",
        headers=mock_webhook_request_headers_onyesha,
        json=mock_webhook_request_payload_for_dynamic_schema,
    )

    assert response.status_code == 200
    mock_forward.assert_called_once_with(
        destination_url="https://diagnostics.example.com/webhook-dump",
        integration_id=str(integration_v2_with_diagnostic_webhook.id),
        json_content=mock_webhook_request_payload_for_dynamic_schema,
    )
    assert mock_webhook_handler.called


@pytest.mark.asyncio
async def test_forward_payload_failure_log_does_not_leak_url_secrets(mocker, caplog):
    # httpx exception messages embed the full request URL, so logging str(e)
    # would leak query-string credentials that _redact_url() exists to hide.
    destination_url = "https://partner.example.com/hook?token=SUPERSECRET"
    mocker.patch("app.services.webhooks._validate_diagnostic_url", mocker.AsyncMock())
    request = httpx.Request("POST", destination_url)
    response = httpx.Response(500, request=request)
    error = httpx.HTTPStatusError(
        f"Server error '500 Internal Server Error' for url '{destination_url}'",
        request=request,
        response=response,
    )
    mock_client = mocker.MagicMock()
    mock_client.post = mocker.AsyncMock(side_effect=error)
    mocker.patch("app.services.webhooks._get_diagnostic_client", return_value=mock_client)

    with caplog.at_level(logging.WARNING, logger="app.services.webhooks"):
        await forward_payload_to_diagnostic_url(
            destination_url=destination_url,
            integration_id="integration_id",
            json_content={"some": "payload"},
        )

    assert "SUPERSECRET" not in caplog.text
    assert "partner.example.com" in caplog.text
    assert "HTTPStatusError" in caplog.text
    assert "500" in caplog.text
