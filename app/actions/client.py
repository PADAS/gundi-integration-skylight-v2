import base64
import json
import logging
import pydantic

import app.settings.integration as settings

from app.actions.configurations import (
    AuthenticateConfig,
    PullEventsConfig
)

from dateparser import parse as dp
from datetime import datetime, timedelta, timezone
from enum import Enum

from gql import Client as GQLClient, gql
from gql.transport.exceptions import TransportError, TransportQueryError
from gql.transport.httpx import HTTPXAsyncTransport, HTTPXTransport

from app.services.errors import ConfigurationNotFound
from app.services.utils import find_config_for_action
from app.services.state import IntegrationStateManager

from typing import Any


logger = logging.getLogger(__name__)
state_manager = IntegrationStateManager()

DEFAULT_SKYLIGHT_API_URL = 'https://api.skylight.earth/graphql'
GRAPHQL_EXECUTE_TIMEOUT_SECONDS = 60

# Refresh tokens this many seconds before their actual expiry to avoid races.
_TOKEN_EXPIRY_SKEW_SECONDS = 60

# For use in case an event doesn't have a vessel dict
EMPTY_VESSEL_DICT = {
    "category": "N/A",
    "class": "N/A",
    "country_filter": "N/A",
    "display_country": "N/A",
    "mmsi": "N/A",
    "name": "N/A",
    "subcategory": "N/A",
    "type": "N/A",
}

# Default mapping values (for ER destinations)
# Maps a Skylight event type (the snake_case keys here) to its EarthRanger
# event type and title. The keys MUST match the values produced by
# PullEventsConfig.format_string_case (configurations.py), which lower/snake-cases
# the SkylightEventType enum labels selected in the portal. Three things stay in
# lockstep: the SkylightEventType enum labels, that validator, and these keys.
# When adding an event type, update all three.
DEFAULT_EVENT_MAPPING = {
    "fishing": {
        "event_type": "fishing_alert_rep",
        "event_title": "Fishing",
        "skylight_event_type": "fishing_activity_history"
    },
    "vessel_detection": {
        "event_type": "detection_alert_rep",
        "event_title": "Vessel Detection",
        "skylight_event_type": ["viirs", "sar_sentinel1", "eo_sentinel2", "eo_landsat_8_9"]
    },
    "speed_range": {
        "event_type": "speed_range_alert_rep",
        "event_title": "Speed Range",
        "skylight_event_type": "speed_range"
    },
    "marine_entry": {
        "event_type": "entry_alert_rep",
        "event_title": "Marine Entry",
        "skylight_event_type": "aoi_visit"
    },
    "dark_rendezvous": {
        "event_type": "dark_rendezvous_alert_rep",
        "event_title": "Dark Rendezvous",
        "skylight_event_type": "dark_rendezvous"
    },
    "standard_rendezvous": {
        "event_type": "standard_rendezvous_alert_rep",
        "event_title": "Standard Rendezvous",
        "skylight_event_type": "standard_rendezvous"
    }
}


# Pydantic Models
class ERSkylightEventTypes(str, Enum):
    dark_rendezvous_alert_rep = 'dark_rendezvous_alert_rep'
    detection_alert_rep = 'detection_alert_rep'
    fishing_alert_rep = 'fishing_alert_rep'
    speed_range_alert_rep = 'speed_range_alert_rep'
    standard_rendezvous_alert_rep = 'standard_rendezvous_alert_rep'
    entry_alert_rep = 'entry_alert_rep'


class EventType(pydantic.BaseModel):
    skylight_event_type: Any = pydantic.Field(
        title='Skylight Event Type',
    )
    event_type: ERSkylightEventTypes = pydantic.Field(
        title='Provider Event Type ID',
    )
    event_title: str = pydantic.Field(
        title='Event Title',
    )

    class Config:
        use_enum_values = True


# Endpoint call models (request/response)
class SkylightRequestHeader(pydantic.BaseModel):
    Authorization: str


class SkylightGetTokenResponse(pydantic.BaseModel):
    access_token: str
    expires_in: int
    token_type: str


class PullEventsBadConfigException(Exception):
    def __init__(self, message: str, status_code=422):
        self.status_code = status_code
        self.message = message
        super().__init__(f'{self.status_code}: {self.message}')


def get_auth_config(integration):
    # Look for the login credentials, needed for any action
    auth_config = find_config_for_action(
        configurations=integration.configurations,
        action_id="auth"
    )
    if not auth_config:
        raise ConfigurationNotFound(
            f"Authentication settings for integration {str(integration.id)} "
            f"are missing. Please fix the integration setup in the portal."
        )
    return AuthenticateConfig.parse_obj(auth_config.data)


def get_pull_config(integration):
    # Look for the login credentials, needed for any action
    pull_config = find_config_for_action(
        configurations=integration.configurations,
        action_id="pull_events"
    )
    if not pull_config:
        raise ConfigurationNotFound(
            f"Authentication settings for integration {str(integration.id)} "
            f"are missing. Please fix the integration setup in the portal."
        )
    return PullEventsConfig.parse_obj(pull_config.data)


def _redact_headers(headers):
    """Copy headers with the Authorization token masked, so we can log full
    request detail without leaking bearer tokens into the logs."""
    redacted = {}
    for key, value in headers.items():
        redacted[key] = "Bearer ***redacted***" if key.lower() == "authorization" else value
    return redacted


def _log_skylight_error_response(response):
    """httpx response hook: whenever Skylight returns an error (4xx/5xx), log
    the full request and response (URL, headers, bodies) so the failure is
    fully diagnosable from the activity logs. Successful responses are not
    logged here, to avoid noise and dumping large payloads."""
    if response.status_code < 400:
        return
    response.read()
    request = response.request
    logger.error(
        'Skylight returned an error response.\n'
        'Request: %s %s\nRequest headers: %s\nRequest body: %s\n'
        'Response status: %s\nResponse headers: %s\nResponse body: %s',
        request.method,
        request.url,
        _redact_headers(request.headers),
        request.content.decode("utf-8", "replace"),
        response.status_code,
        dict(response.headers),
        response.text,
        extra={"attention_needed": True},
    )


def build_graphql_client(transport_dict):
    transport = HTTPXTransport(**transport_dict)

    # Create a GraphQL client using the defined transport
    gql_client = GQLClient(
        transport=transport,
        execute_timeout=GRAPHQL_EXECUTE_TIMEOUT_SECONDS,
        fetch_schema_from_transport=False
    )
    return gql_client


def _is_token_expired(access_token: str) -> bool:
    """Best-effort JWT exp check. Returns True on any parse failure (fail-safe)."""
    try:
        payload = access_token.split(".")[1]
        padded = payload + "=" * ((-len(payload)) % 4)
        claims = json.loads(base64.urlsafe_b64decode(padded))
        exp = claims.get("exp")
        if not exp:
            return True
        return datetime.now(tz=timezone.utc).timestamp() >= (exp - _TOKEN_EXPIRY_SKEW_SECONDS)
    except Exception:
        return True


async def build_request_header(integration, auth, gql_client):
    token_dict = await state_manager.get_state(str(integration.id), "pull_events", auth.username)

    if token_dict and not _is_token_expired(token_dict.get("access_token", "")):
        token_dict = SkylightGetTokenResponse.parse_obj(token_dict)
    else:
        if token_dict:
            logger.info(f"Skylight token expired for '{auth.username}', refreshing.")
        token_dict = await get_authentication_token(integration, auth, gql_client)
        await state_manager.set_state(
            str(integration.id),
            "pull_events",
            token_dict.dict(),
            auth.username,
        )

    return SkylightRequestHeader.parse_obj(
        {
            "Authorization": "{} {}".format(token_dict.token_type, token_dict.access_token)
        }
    )


async def get_authentication_token(integration, auth, gql_client):
    try:
        query = gql(
            """
            query getToken($username: String!, $password: String!) {
                getToken(username: $username, password: $password){
                    access_token
                    expires_in
                    token_type
                }
            }
            """
        )
        params = {
            "username": auth.username,
            "password": auth.password.get_secret_value()
        }
        response = gql_client.execute(query, variable_values=params)

        token_response = response['getToken']

        return SkylightGetTokenResponse.parse_obj(token_response)
    except pydantic.ValidationError as ve:
        message = f'Validation error in Skylight "SkylightGetTokenResponse" model. {ve.json()}'
        logger.exception(
            message,
            extra={
                "integration_id": str(integration.id),
                "attention_needed": True
            }
        )
        raise ve
    except TransportQueryError as te:
        message = f"TransportQueryError on 'get_authentication_token'. message: {te.errors[0].get('message')}"
        logger.exception(
            message,
            extra={
                "integration_id": str(integration.id),
                "attention_needed": True
            }
        )
        raise te


def map_event_type(integration, event_type):
    events_mapping = integration.additional or DEFAULT_EVENT_MAPPING
    try:
        return EventType.parse_obj(events_mapping.get(event_type, {}))
    except pydantic.ValidationError as e:
        message = f'Failed to map "{event_type}". Either is invalid or unsupported. {e}'
        logger.warning(message)
        raise PullEventsBadConfigException(message)


_AUTH_ERROR_CODES = {"UNAUTHENTICATED", "UNAUTHORIZED"}


def _gql_error_code(te: TransportQueryError):
    """Safely extract the error code from a GraphQL TransportQueryError."""
    try:
        return (te.errors[0] or {}).get("extensions", {}).get("code")
    except (IndexError, AttributeError, TypeError):
        return None


async def execute_gql_query(gql_client, query, params, integration, auth):
    try:
        return gql_client.execute(query, variable_values=params)
    except TransportQueryError as te:
        code = _gql_error_code(te)
        if code in _AUTH_ERROR_CODES:
            # Delete the cached token so the next action run re-authenticates.
            # We do not retry here: the gql_client transport headers already
            # hold the stale token, so retrying with the same client would fail
            # identically. The proactive _is_token_expired check in
            # build_request_header prevents this path in the common case.
            logger.warning(f'"getRendedzvousExternal" query returned {code}, clearing token for next run.')
            await state_manager.delete_state(str(integration.id), "pull_events", auth.username)
        raise


async def get_skylight_events(integration, config_data, auth):
    # Check if data mapping dict is set in the integration
    if not integration.additional:
        msg = f'Data map JSON not found. Will use default ER map for integration ID: "{str(integration.id)}"'
        logger.warning(msg)

    default_transport_dict = dict(
        url=DEFAULT_SKYLIGHT_API_URL,
        verify=True,
    )
    auth_client = build_graphql_client(default_transport_dict)
    headers = await build_request_header(integration, auth, auth_client)
    # Log full request/response detail whenever Skylight returns an error.
    error_log_hooks = {"response": [_log_skylight_error_response]}
    gql_client = build_graphql_client(
        {**default_transport_dict, 'headers': headers.dict(), 'event_hooks': error_log_hooks}
    )

    events = {}

    # GetEvents query (external)
    query = gql(
        """
        query getRendedzvousExternal(
            $eventTypes:[EventType]!
            $aoiId: String
            $startTime: String
            $pageSize: Int
            $pageNum: Int
        )
        {
            events(
                eventTypes: $eventTypes,
                aoiId: $aoiId,
                startTime: $startTime
                pageSize: $pageSize
                pageNum: $pageNum
            ) {
                items {
                    event_id
                    event_type  
                    start {
                        point {
                            lat
                            lon
                        }
                        time
                    }
                    vessels {
                        vessel_0 {
                            category
                            class
                            country_filter
                            display_country
                            mmsi
                            name
                            length
                            type
                            vessel_id
                        }
                    }
                    event_details {
                        average_speed
                        data_source
                        distance
                        duration
                        correlated
                        image_url
                        entry_speed
                        entry_heading
                        end_heading
                        visit_type
                    }
                    end {
                        point {
                            lat
                            lon
                        }
                        time
                    }
                }
                meta {
                    total
                    pageSize
                    pageNum
                }
            }
        }
        """
    )

    try:
        mapped_event_types = [map_event_type(integration, et) for et in config_data.event_types]
    except PullEventsBadConfigException:
        logger.error(f'Invalid config received for integration ID: {str(integration.id)}')
        raise

    # Get variables from mapped event types
    event_types = []
    for mapped_event in mapped_event_types:
        if isinstance(mapped_event.skylight_event_type, list):
            event_types.extend(mapped_event.skylight_event_type)
        else:
            event_types.append(mapped_event.skylight_event_type)
    aoi_ids = config_data.aoi_ids
    page_size = config_data.pageSize
    initial_data_window_days = config_data.initial_data_window_days or settings.DEFAULT_WINDOW_DAYS

    for aoi in aoi_ids:
        try:
            response_list = []
            none_retries = 0
            saved_aoi_start_time = await state_manager.get_state(str(integration.id), "pull_events", aoi)
            if saved_aoi_start_time:
                start_time = dp(saved_aoi_start_time.get("start_time")).isoformat()
            else:
                start_time = (
                        datetime.now(tz=timezone.utc).replace(hour=0, minute=0, second=0) -
                        timedelta(days=initial_data_window_days)
                ).isoformat()

            page_num = 1
            total_pages = None

            while total_pages is None or page_num <= total_pages:
                params = {
                    "eventTypes": event_types,
                    "aoiId": aoi,
                    "startTime": start_time,
                    "pageSize": page_size,
                    "pageNum": page_num
                }

                logger.info(f'Sending "getRendedzvousExternal" query request. Params: "{params}"...')

                try:
                    response = await execute_gql_query(gql_client, query, params, integration, auth)
                except TransportError as te:
                    # Catch the TransportError base so every Skylight transport
                    # failure is logged — query errors, 500s (TransportServerError),
                    # protocol errors, etc.
                    logger.error(
                        f'"getRendedzvousExternal" page {page_num} failed for AOI "{aoi}": '
                        f'{type(te).__name__}: {te}. '
                        f'Request params: {params}. '
                        f'Keeping {len(response_list)} events collected so far.',
                        extra={
                            "integration_id": str(integration.id),
                            "aoi": aoi,
                            "attention_needed": True,
                        }
                    )
                    # The loop is bounded by total_pages, which we can only learn
                    # from a SUCCESSFUL response's meta.total — and the first
                    # success is normally page 1. If we fail before ever getting
                    # that bound (total_pages is None, i.e. page 1 itself failed),
                    # there is nothing to stop the loop, so we must give up this
                    # AOI rather than retry an unknown number of pages forever.
                    # Once total_pages IS known, a later failed page is safe to
                    # skip — page_num is still capped by the while condition.
                    if total_pages is None:
                        break
                    page_num += 1
                    continue

                events_response = response['events']['items']

                if events_response is None:
                    if none_retries >= 1:
                        logger.error(
                            f'"getRendedzvousExternal" returned None twice for AOI "{aoi}", giving up.',
                            extra={"integration_id": str(integration.id), "aoi": aoi, "attention_needed": True}
                        )
                        break
                    logger.info(f'"getRendedzvousExternal" query returned None, retrying with a new token...')
                    await state_manager.delete_state(str(integration.id), "pull_events", auth.username)
                    headers = await build_request_header(integration, auth, auth_client)
                    gql_client = build_graphql_client(
                        {**default_transport_dict, 'headers': headers.dict(), 'event_hooks': error_log_hooks}
                    )
                    none_retries += 1
                    continue

                meta = response['events']['meta']
                if total_pages is None:
                    total = meta['total'] or 0
                    total_pages = (total + meta['pageSize'] - 1) // meta['pageSize']

                response_list.extend(events_response)
                page_num += 1
            events.update({aoi: response_list})
        except pydantic.ValidationError as ve:
            message = f'Validation error in Skylight "getRendedzvousExternal" endpoint. {ve.json()}'
            logger.exception(
                message,
                extra={
                    "integration_id": str(integration.id),
                    "attention_needed": True
                }
            )
            raise ve
        except TransportQueryError as te:
            message = f"TransportQueryError. message: {te.errors[0].get('message')}"
            logger.exception(
                message,
                extra={
                    "aoi": aoi,
                    "integration_id": str(integration.id),
                    "attention_needed": True
                }
            )
            continue
        except Exception as e:
            message = f"Unhandled exception occurred. Exception: {e}"
            logger.exception(
                message,
                extra={
                    "aoi": aoi,
                    "integration_id": str(integration.id),
                    "attention_needed": True
                }
            )
            continue

    return events, mapped_event_types
