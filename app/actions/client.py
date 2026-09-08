import base64
import json
import logging
import re
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

# searchEventsV2 refuses offset + limit > 10000 and caps meta.total there. An
# unknown/invalid AOI id is silently ignored by Skylight, which then returns
# worldwide events, so hitting this cap almost always means a misconfigured AOI.
SKYLIGHT_RESULT_CAP = 10000

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

# --- searchEventsV2 -> v1 record shape -------------------------------------
# The connector was written against Skylight's v1 `events` query. Everything
# downstream of get_skylight_events (handlers.transform, get_clean_event_id,
# attachments) and the EarthRanger event schemas expect that v1 layout:
# snake_case keys and vessels keyed `vessel_0` / `vessel_1`. The v2
# `searchEventsV2` query returns camelCase fields and `vessel0` / `vessel1`.
# To swap the API without changing what EarthRanger receives, every v2 record
# is reshaped here into the v1 layout: fields with a v1 equivalent are written
# under their v1 key, and v2-only fields are passed through in snake_case.

# v1 vessel key <- v2 vessel field. v2 fields not listed here (imo,
# countryCode, trackId, gfwVesselId) pass through as snake_case extras.
# v1 `class` and `country_filter` have no v2 equivalent and are not emitted.
_V2_VESSEL_FIELD_MAP = {
    "vessel_id": "vesselId",
    "name": "name",
    "mmsi": "mmsi",
    "category": "category",
    "subcategory": "subcategory",
    "type": "vesselType",
    "display_country": "displayCountry",
    "length": "length",
}

# v1 event_details key <- v2 eventDetails field. v2-only fields (fishingScore,
# osrScore, detectionType, score, radianceNw, ...) pass through as snake_case.
# v1 `visit_type` has no v2 equivalent and is not emitted.
_V2_DETAILS_FIELD_MAP = {
    "average_speed": "averageSpeed",
    "distance": "distance",
    "duration": "durationSec",
    "image_url": "imageUrl",
    "entry_speed": "entrySpeed",
    "entry_heading": "entryHeading",
    "end_heading": "endHeading",
}

# Satellite detection event types. In v1 these carried `data_source` (the
# sensor) and `correlated` (AIS-correlated or not); v2 expresses the same via
# eventType and eventDetails.detectionType ("dark" | "ais_correlated").
_DETECTION_EVENT_TYPES = {"viirs", "sar_sentinel1", "eo_sentinel2", "eo_landsat_8_9"}


def _camel_to_snake(name: str) -> str:
    return re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()


def _reshape_v2_fields(source, field_map: dict) -> dict:
    """Rename v2 fields to their v1 keys per `field_map`; pass every other
    field through in snake_case. GraphQL meta fields (`__typename`) are dropped."""
    source = source or {}
    mapped_v2_names = set(field_map.values())
    out = {v1_key: source[v2_key] for v1_key, v2_key in field_map.items() if v2_key in source}
    for key, value in source.items():
        if key in mapped_v2_names or key.startswith("__"):
            continue
        out[_camel_to_snake(key)] = value
    return out


def normalize_v2_event(record: dict) -> dict:
    """Reshape one `searchEventsV2` record into the v1 `events` item layout."""
    event_type = record.get("eventType")
    v2_details = record.get("eventDetails") or {}
    details = _reshape_v2_fields(v2_details, _V2_DETAILS_FIELD_MAP)
    if event_type in _DETECTION_EVENT_TYPES:
        details["data_source"] = event_type
        detection_type = v2_details.get("detectionType")
        if detection_type is not None:
            details["correlated"] = detection_type == "ais_correlated"
    for key in ("createdAt", "updatedAt"):
        if record.get(key) is not None:
            details[_camel_to_snake(key)] = record[key]

    v2_vessels = record.get("vessels") or {}
    vessel0 = v2_vessels.get("vessel0")
    # vessel_0 is always present (None -> EMPTY_VESSEL_DICT in transform, as in
    # v1). vessel_1 is only emitted when v2 actually returned a second vessel,
    # so single-vessel events don't gain a set of "N/A" vessel_1 fields.
    vessels = {"vessel_0": _reshape_v2_fields(vessel0, _V2_VESSEL_FIELD_MAP) if vessel0 else None}
    vessel1 = v2_vessels.get("vessel1")
    if vessel1:
        vessels["vessel_1"] = _reshape_v2_fields(vessel1, _V2_VESSEL_FIELD_MAP)

    return {
        "event_id": record.get("eventId"),
        "event_type": event_type,
        "start": record.get("start"),
        "end": record.get("end"),
        "vessels": vessels,
        "event_details": details,
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
    logged here, to avoid noise and dumping large payloads.

    Note: this is a synchronous hook for HTTPXTransport (sync). If the transport
    is ever switched to HTTPXAsyncTransport, this must become async and use
    `await response.aread()`."""
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


def build_events_client(base_transport_dict, headers):
    """Build the authenticated events GraphQL client, with the error-logging
    hook attached so any Skylight 4xx/5xx is fully captured."""
    return build_graphql_client({
        **base_transport_dict,
        'headers': headers.dict(),
        'event_hooks': {"response": [_log_skylight_error_response]},
    })


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
            logger.warning(f'"searchEventsV2" query returned {code}, clearing token for next run.')
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
    gql_client = build_events_client(default_transport_dict, headers)

    events = {}

    # searchEventsV2 query. Records are reshaped into the v1 layout by
    # normalize_v2_event before they leave this function.
    query = gql(
        """
        query searchSkylightEventsV2(
            $eventTypes: [String!]!
            $aoiId: String
            $startTime: String
            $limit: Int
            $offset: Int
            $snapshotId: String
        )
        {
            searchEventsV2(input: {
                eventType: { inc: $eventTypes }
                intersectsAoiId: $aoiId
                startTime: { gte: $startTime }
                limit: $limit
                offset: $offset
                snapshotId: $snapshotId
            }) {
                records {
                    eventId
                    eventType
                    createdAt
                    updatedAt
                    start {
                        point {
                            lat
                            lon
                        }
                        time
                    }
                    end {
                        point {
                            lat
                            lon
                        }
                        time
                    }
                    vessels {
                        vessel0 {
                            vesselId
                            name
                            mmsi
                            imo
                            countryCode
                            trackId
                            category
                            subcategory
                            vesselType
                            gfwVesselId
                            displayCountry
                            length
                        }
                        vessel1 {
                            vesselId
                            name
                            mmsi
                            imo
                            countryCode
                            trackId
                            category
                            subcategory
                            vesselType
                            gfwVesselId
                            displayCountry
                            length
                        }
                    }
                    eventDetails {
                        ... on FishingEventDetails {
                            fishingScore
                        }
                        ... on DarkRendezvousEventDetails {
                            osrScore
                        }
                        ... on SpeedRangeEventDetails {
                            averageSpeed
                            distance
                            durationSec
                        }
                        ... on AoiVisitEventDetails {
                            entrySpeed
                            entryHeading
                            endHeading
                        }
                        ... on ImageryMetadataEventDetails {
                            imageUrl
                            detectionType
                            score
                            estimatedLength
                            estimatedSpeedKts
                            estimatedVesselCategory
                            frameIds
                            heading
                            distanceToCoastM
                            orientation
                            metersPerPixel
                        }
                        ... on ViirsEventDetails {
                            imageUrl
                            detectionType
                            estimatedLength
                            estimatedSpeedKts
                            estimatedVesselCategory
                            frameIds
                            heading
                            radianceNw
                        }
                    }
                }
                meta {
                    snapshotId
                    total
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

            logger.info(
                f'Fetching Skylight events for AOI "{aoi}" since {start_time}. '
                f'Event types: {event_types}. Page size: {page_size}.'
            )
            pages_fetched = 0
            reported_total = None
            page_num = 1
            total_pages = None
            # v2 pins paging to a snapshot so results don't shift between
            # pages. The id comes back with the first page and is echoed on
            # every following one.
            snapshot_id = None

            while total_pages is None or page_num <= total_pages:
                params = {
                    "eventTypes": event_types,
                    "aoiId": aoi,
                    "startTime": start_time,
                    "limit": page_size,
                    "offset": (page_num - 1) * page_size,
                    "snapshotId": snapshot_id,
                }

                logger.info(
                    f'"searchEventsV2" page {page_num}'
                    f'{f"/{total_pages}" if total_pages else ""} (offset {params["offset"]}) for AOI "{aoi}"...'
                )

                try:
                    response = await execute_gql_query(gql_client, query, params, integration, auth)
                except TransportError as te:
                    # Catch the TransportError base so every Skylight transport
                    # failure is logged — query errors, 500s (TransportServerError),
                    # protocol errors, etc.
                    logger.error(
                        f'"searchEventsV2" page {page_num} failed for AOI "{aoi}": '
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

                search_response = response['searchEventsV2'] or {}
                events_response = search_response.get('records')

                if events_response is None:
                    if none_retries >= 1:
                        logger.error(
                            f'"searchEventsV2" returned None twice for AOI "{aoi}", giving up.',
                            extra={"integration_id": str(integration.id), "aoi": aoi, "attention_needed": True}
                        )
                        break
                    logger.info(f'"searchEventsV2" query returned None, retrying with a new token...')
                    await state_manager.delete_state(str(integration.id), "pull_events", auth.username)
                    headers = await build_request_header(integration, auth, auth_client)
                    gql_client = build_events_client(default_transport_dict, headers)
                    none_retries += 1
                    continue

                # Successful page: reset the None counter so only *consecutive*
                # None responses (not Nones scattered across the AOI) trigger the abort.
                none_retries = 0

                # Guard against a null/absent meta: without this, meta.get()
                # would raise AttributeError, fall through to the broad except
                # below, and drop the AOI *including events already collected*.
                meta = search_response.get('meta') or {}
                if snapshot_id is None:
                    snapshot_id = meta.get('snapshotId')
                if total_pages is None:
                    total = meta.get('total') or 0
                    reported_total = total
                    # Divide by our requested page size (always >= 1), which is
                    # also the offset step, so total_pages matches the offsets.
                    total_pages = (total + page_size - 1) // page_size
                    if total >= SKYLIGHT_RESULT_CAP:
                        logger.warning(
                            f'Skylight reported {total} events for AOI "{aoi}" since {start_time}, '
                            f'which is its result cap ({SKYLIGHT_RESULT_CAP}). Skylight ignores unknown '
                            f'AOI ids and returns worldwide events, so check that this AOI id exists '
                            f'in the Skylight account. Only the newest {SKYLIGHT_RESULT_CAP} events '
                            f'can be fetched.',
                            extra={
                                "integration_id": str(integration.id),
                                "aoi": aoi,
                                "attention_needed": True,
                            }
                        )

                if not events_response:
                    # Nothing left even though total_pages said otherwise
                    # (Skylight caps meta.total). Don't request empty pages.
                    break

                response_list.extend(normalize_v2_event(record) for record in events_response)
                pages_fetched += 1
                page_num += 1
            logger.info(
                f'Fetched {len(response_list)} events for AOI "{aoi}" in {pages_fetched} page(s). '
                f'Skylight reported total: {reported_total}. Window start: {start_time}.'
            )
            events.update({aoi: response_list})
        except pydantic.ValidationError as ve:
            message = f'Validation error in Skylight "searchEventsV2" endpoint. {ve.json()}'
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
