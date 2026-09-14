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
from gundi_core.schemas.v2 import LogLevel
from gql.transport.httpx import HTTPXAsyncTransport, HTTPXTransport

from app.services.activity_logger import log_action_activity
from app.services.errors import ConfigurationNotFound
from app.services.utils import find_config_for_action
from app.services.state import IntegrationStateManager

from typing import Any, Optional


logger = logging.getLogger(__name__)

# gql's httpx transport logs every request payload at DEBUG, which would put the
# plaintext Skylight password from the `getToken` mutation into the logs whenever
# LOGGING_LEVEL is DEBUG. Nothing here needs that detail, so hold it at INFO.
logging.getLogger("gql.transport.httpx").setLevel(logging.INFO)
state_manager = IntegrationStateManager()

DEFAULT_SKYLIGHT_API_URL = 'https://api.skylight.earth/graphql'
GRAPHQL_EXECUTE_TIMEOUT_SECONDS = 60

# searchEventsV2 refuses offset + limit > 10000 and caps meta.total there. An
# unknown/invalid AOI id is silently ignored by Skylight, which then returns
# worldwide events, so hitting this cap almost always means a misconfigured AOI.
SKYLIGHT_RESULT_CAP = 10000

# searchEventsV2 rejects a startTime older than a rolling retention boundary
# with a 400 ("start_time.gte must be greater than ..."), which fails the whole
# run for that AOI. Verified live on 2026-09-10: 540 days back was accepted and
# 545 was not, and the boundary tracks the clock (two probes 38 seconds apart
# moved it by 38 seconds), so it is a rolling window rather than a fixed date.
# initial_data_window_days is operator-configurable with no upper bound, so it
# is clamped to this. Conservative on purpose: the exact interval is Skylight's
# to change, and asking for slightly less history is harmless where a 400 is not.
SKYLIGHT_MAX_WINDOW_DAYS = 540

# Page size for the searchAOIs reference lookup (accounts typically have a handful).
AOI_SEARCH_PAGE_SIZE = 100

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

# v1 vessel key <- v2 vessel field. Every v1 vessel key has a v2 source
# (verified 2026-09-08 on 1,973 identical events: 0 mismatches). v2 fields not
# listed here (imo, trackId, gfwVesselId) pass through as snake_case extras.
_V2_VESSEL_FIELD_MAP = {
    "vessel_id": "vesselId",
    "name": "name",
    "mmsi": "mmsi",
    "category": "category",
    "subcategory": "subcategory",
    "class": "class",
    "country_filter": "countryCode",
    "type": "vesselType",
    "display_country": "displayCountry",
    "length": "length",
}

# v1 event_details key <- v2 eventDetails field (same verification). v2-only
# fields (fishingScore, osrScore, detectionType, score, radianceNw, ...) pass
# through as snake_case.
_V2_DETAILS_FIELD_MAP = {
    "average_speed": "averageSpeed",
    "distance": "distance",
    "duration": "durationSec",   # v1 `duration` was already in seconds
    "image_url": "imageUrl",
    "data_source": "dataSource",  # e.g. "sentinel2", "noaa_21"
    "entry_speed": "entrySpeed",
    "entry_heading": "entryHeading",
    "end_heading": "endHeading",
}

# v1 serialised these three as strings ("7.800000190734863", "307"); v2 returns
# numbers. Stringified so EarthRanger keeps receiving the same value types.
_V1_STRING_DETAILS = ("entry_speed", "entry_heading", "end_heading")

# v1 also returned `visit_type: "end"` on every event of every type; it carried
# no information and v2 has no such field, so it is intentionally not emitted.

# Satellite detection event types. v1 carried `correlated` (AIS-correlated or
# not); v2 expresses the same via eventDetails.detectionType ("dark" |
# "ais_correlated"). Verified equal on 556/556 detections.
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
    for key in _V1_STRING_DETAILS:
        if details.get(key) is not None:
            details[key] = str(details[key])
    if event_type in _DETECTION_EVENT_TYPES:
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
        # Top-level copy for the pull cursor (transform ignores unknown keys).
        "updated_at": record.get("updatedAt"),
    }


def _updated_after(candidate, current) -> bool:
    """True when `candidate` carries a strictly newer `updated_at` than `current`.

    Parsed rather than compared as text: Skylight has returned both `...Z` and
    `...+00:00`, which sort differently as strings.
    """
    raw_new, raw_old = candidate.get("updated_at"), current.get("updated_at")
    if not raw_new:
        return False
    if not raw_old:
        return True
    # dateparser returns None rather than raising on an unparseable value.
    parsed_new, parsed_old = dp(raw_new), dp(raw_old)
    if parsed_new is None:
        return False
    if parsed_old is None:
        return True
    if (parsed_new.tzinfo is None) != (parsed_old.tzinfo is None):
        # One side lacked an offset; treat a naive stamp as UTC so the two are
        # comparable at all.
        parsed_new = parsed_new.replace(tzinfo=parsed_new.tzinfo or timezone.utc)
        parsed_old = parsed_old.replace(tzinfo=parsed_old.tzinfo or timezone.utc)
    return parsed_new > parsed_old


def latest_update_cursor(events) -> Optional[str]:
    """The newest `updated_at` among normalized events, or None.

    Saved per AOI as the pull cursor: the next run asks Skylight for events
    *updated* at or after it. Skylight publishes many events hours after
    their start time (satellite detections ~6 h, some entries/speed events a
    day later), so a cursor on event start time silently skipped them; one on
    update time does not, and it also picks up edits (e.g. an entry alert
    gaining its exit time) for the patch path.
    """
    stamps = [e.get("updated_at") for e in events if e.get("updated_at")]
    return max(stamps) if stamps else None


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
            logger.warning(f'Skylight query returned {code}, clearing token for next run.')
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
            $updated: DateFilter
            $limit: Int
            $offset: Int
            $snapshotId: String
        )
        {
            searchEventsV2(input: {
                eventType: { inc: $eventTypes }
                intersectsAoiId: $aoiId
                startTime: { gte: $startTime }
                updated: $updated
                sortBy: updated
                sortDirection: asc
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
                            class
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
                            class
                        }
                    }
                    eventDetails {
                        # Reviewed 2026-09-10: fishing, dark rendezvous and standard
                        # rendezvous look thin next to v1, which asked for
                        # average_speed/distance/duration on every type. They are not
                        # a regression — v1 *returned* those as null for these types
                        # (checked live over 200 events of each; only visit_type, a
                        # constant, was ever populated). v2 has no field for them
                        # either: probing ~75 candidate names against the schema
                        # validator found only the ones selected here, and
                        # StandardRendezvousEventDetails exposes nothing at all, which
                        # is why its fragment is absent. Introspection is disabled on
                        # the Skylight API, so re-check by probing, not by reading a
                        # schema. (DarkRendezvousEventDetails also accepts
                        # `fishingScore`; not requested, as v1 never carried it.)
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
                            dataSource
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
                            dataSource
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
    if initial_data_window_days > SKYLIGHT_MAX_WINDOW_DAYS:
        logger.warning(
            f'Configured window of {initial_data_window_days} days exceeds the furthest back '
            f'Skylight will accept ({SKYLIGHT_MAX_WINDOW_DAYS} days); asking for '
            f'{SKYLIGHT_MAX_WINDOW_DAYS} instead. Without this the query is rejected outright '
            f'and the integration returns nothing at all.',
            extra={
                "integration_id": str(integration.id),
                "attention_needed": True,
            }
        )
        initial_data_window_days = SKYLIGHT_MAX_WINDOW_DAYS

    # Skylight silently ignores an unknown aoiId and returns *worldwide* events,
    # so a single typo would flood the destination. Every configured id is
    # therefore checked against the account's AOIs before it is queried.
    #
    # This is a gate, not a guard: an id that could not be validated is deferred,
    # not queried. A failed or empty listing is an unusable answer rather than
    # permission. Deferring costs one run, which the next run recovers; querying
    # an unvalidated id can flood the destination with worldwide events and
    # cannot be undone. The result-cap rule below is a second line of defence
    # rather than the first — it drops an AOI that answers with 10,000 events —
    # but it only catches a bad id that happens to be that busy, so the gate
    # still has to hold on its own.
    try:
        known_aoi_ids = {record.get("id") for record in await search_aois(integration, auth)}
    except Exception as e:
        logger.error(
            f'The configured AOI id(s) could not be validated: listing this Skylight '
            f'account\'s AOIs failed ({type(e).__name__}: {e}). Skylight ignores an unknown '
            f'aoiId and would return worldwide events, so nothing is queried this run. '
            f'This recovers on its own once the listing works again.',
            extra={
                "integration_id": str(integration.id),
                "attention_needed": True,
            }
        )
        return events, mapped_event_types
    if not known_aoi_ids:
        logger.error(
            f'The configured AOI id(s) could not be validated: this Skylight account lists '
            f'no AOIs at all. Skylight ignores an unknown aoiId and would return worldwide '
            f'events, so nothing is queried this run. Check that the credentials belong to '
            f'the account that owns the configured AOIs.',
            extra={
                "integration_id": str(integration.id),
                "attention_needed": True,
            }
        )
        return events, mapped_event_types
    unknown_aoi_ids = [aoi for aoi in aoi_ids if aoi not in known_aoi_ids]
    if unknown_aoi_ids:
        logger.error(
            f'Skipping unknown AOI id(s) {unknown_aoi_ids}: they are not visible to this '
            f'Skylight account. Skylight ignores an unknown aoiId and would return worldwide '
            f'events, so these are not queried. Correct the ids in the integration configuration.',
            extra={
                "integration_id": str(integration.id),
                "attention_needed": True,
            }
        )
        aoi_ids = [aoi for aoi in aoi_ids if aoi in known_aoi_ids]

    for aoi in aoi_ids:
        try:
            response_list = []
            none_retries = 0
            # Outer bound on how old an event may be: only events that STARTED
            # inside the configured window are considered, on every run.
            start_time = (
                    datetime.now(tz=timezone.utc).replace(hour=0, minute=0, second=0) -
                    timedelta(days=initial_data_window_days)
            ).isoformat()
            # Cursor: events UPDATED at/after the newest update seen last run
            # (see latest_update_cursor). Results are sorted oldest-update-first
            # so a run that hits Skylight's result cap keeps the oldest events
            # and the next run continues from where it stopped. A pre-existing
            # `start_time` (the old start-time cursor) is used as the starting
            # point once, then replaced by `updated_since`.
            saved_state = await state_manager.get_state(str(integration.id), "pull_events", aoi) or {}
            updated_since = saved_state.get("updated_since") or saved_state.get("start_time")
            if updated_since:
                updated_since = dp(updated_since).isoformat()

            logger.info(
                f'Fetching Skylight events for AOI "{aoi}" started since {start_time}'
                f'{f" and updated since {updated_since}" if updated_since else " (no cursor yet)"}. '
                f'Event types: {event_types}. Page size: {page_size}.'
            )
            pages_fetched = 0
            reported_total = None
            page_num = 1
            # Set when the AOI matches Skylight's whole result cap, which drops
            # the AOI for this run (see the cap branch below).
            aoi_capped = False
            # v2 *should* pin paging to a snapshot so results don't shift between
            # pages, but Skylight returns no snapshotId in practice. Paging is
            # therefore by cursor, not by offset: each page asks for events
            # updated at or after the newest stamp the previous page returned.
            #
            # Offsets cannot be used without a snapshot. Sorting is by `updated`,
            # so an event updated mid-run moves to the end of the ordering and
            # every event behind it shifts down one place — the record that slides
            # into an already-passed offset is never returned, and since the run's
            # cursor ends up past its updatedAt it is never fetched again either.
            # A value cursor has no such hole: an event that moves is simply met
            # again later in the scan.
            snapshot_id = None
            # Events at the boundary stamp come back on the page after their own
            # (the filter is `>=`, inclusive), so pages overlap by design and
            # repeats are dropped here rather than handed downstream twice.
            # Position in response_list of each event id already collected, so a
            # newer copy arriving on a later page replaces the older one in place
            # rather than being dropped (see the replacement branch below).
            seen_event_positions = {}
            page_cursor = updated_since

            while True:
                # Also the cap: stop once this run holds everything Skylight will
                # return for one query.
                limit = min(page_size, SKYLIGHT_RESULT_CAP - len(response_list))
                if limit <= 0:
                    # Same rule as the meta.total branch below, for the case
                    # where Skylight gives no usable total: this AOI has handed
                    # back the entire result cap, which is a configuration
                    # problem rather than a backlog. Drop it whole rather than
                    # sending 10,000 events into EarthRanger.
                    cap_message = (
                        f'AOI {aoi} skipped to avoid sending {len(response_list)} events to '
                        f'EarthRanger. Check the configuration in Gundi.'
                    )
                    # The integration is named in the message itself, not only in
                    # `extra`: this line gets read in GCP on its own, away from
                    # its structured fields, and "some AOI was skipped" is not
                    # actionable without knowing whose.
                    logger.error(
                        f'Integration "{integration.name}" ({str(integration.id)}): {cap_message}',
                        extra={
                            "integration_id": str(integration.id),
                            "aoi": aoi,
                            "attention_needed": True,
                        }
                    )
                    await log_action_activity(
                        integration_id=integration.id,
                        action_id="pull_events",
                        level=LogLevel.ERROR,
                        title=cap_message,
                        data={
                            "message": cap_message,
                            "aoi": aoi,
                            "integration_name": integration.name,
                            "events_matched": len(response_list),
                            "result_cap": SKYLIGHT_RESULT_CAP,
                            "start_time": start_time,
                            "updated_since": updated_since,
                        }
                    )
                    aoi_capped = True
                    break
                params = {
                    "eventTypes": event_types,
                    "aoiId": aoi,
                    "startTime": start_time,
                    "updated": {"gte": page_cursor} if page_cursor else None,
                    "limit": limit,
                    # Always zero: the cursor in `updated` does the paging.
                    "offset": 0,
                    "snapshotId": snapshot_id,
                }

                logger.info(
                    f'"searchEventsV2" page {page_num} for AOI "{aoi}" '
                    f'(updated since {page_cursor or "the window start"})...'
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
                        f'Stopping this AOI and keeping the {len(response_list)} events '
                        f'collected so far; the rest are picked up by the next run.',
                        extra={
                            "integration_id": str(integration.id),
                            "aoi": aoi,
                            "attention_needed": True,
                        }
                    )
                    # Stop the AOI here rather than skipping to the next page.
                    # Pages come back oldest-update-first, and the cursor saved
                    # after the run is the newest updatedAt collected, so keeping
                    # a later page while dropping this one would advance the
                    # cursor past every event on the failed page and they would
                    # never be fetched again. Keeping only the contiguous prefix
                    # leaves the cursor just before the gap, so the next run
                    # resumes exactly where this one stopped.
                    break

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
                    if snapshot_id is None and page_num == 1:
                        # Expected: Skylight has not returned a snapshotId on any
                        # observed query. Noted at debug level because paging does
                        # not depend on one — the cursor below is what keeps the
                        # scan whole. If Skylight ever starts returning ids, it is
                        # echoed back on later pages as a bonus.
                        logger.debug(
                            f'Skylight returned no snapshotId on the first page for AOI "{aoi}". '
                            f'Paging is by update cursor, which does not rely on a pinned snapshot.'
                        )
                if reported_total is None:
                    total = meta.get('total') or 0
                    reported_total = total
                    if total >= SKYLIGHT_RESULT_CAP:
                        # An AOI matching Skylight's entire result cap is a
                        # configuration problem, not a backlog to work through.
                        # Ten thousand events is far more than one AOI is meant
                        # to deliver, and sending them would hit EarthRanger hard
                        # and flood the map with data nobody asked for. So the
                        # AOI is dropped whole: nothing is pulled, nothing is
                        # sent, and the cursor is left exactly where it was.
                        #
                        # The same error then repeats on every run, which is the
                        # intent — only a person can fix this (a wrong AOI id, a
                        # window that is too wide, too many event types), and the
                        # integration should keep saying so until they do. Once
                        # the configuration is narrowed the events are picked up
                        # from the unchanged cursor, so nothing is skipped.
                        cap_message = (
                            f'AOI {aoi} skipped to avoid sending {total} events to EarthRanger. '
                            f'Check the configuration in Gundi.'
                        )
                        # The integration is named in the message itself, not only
                        # in `extra`: this line gets read in GCP on its own, away
                        # from its structured fields, and "some AOI was skipped"
                        # is not actionable without knowing whose.
                        logger.error(
                            f'Integration "{integration.name}" ({str(integration.id)}): {cap_message}',
                            extra={
                                "integration_id": str(integration.id),
                                "aoi": aoi,
                                "attention_needed": True,
                            }
                        )
                        # Also surfaced in the portal: this AOI is delivering
                        # nothing at all until it is reconfigured, and the person
                        # who can reconfigure it does not read GCP logs.
                        await log_action_activity(
                            integration_id=integration.id,
                            action_id="pull_events",
                            level=LogLevel.ERROR,
                            title=cap_message,
                            data={
                                "message": cap_message,
                                "aoi": aoi,
                                "integration_name": integration.name,
                                "events_matched": total,
                                "result_cap": SKYLIGHT_RESULT_CAP,
                                "start_time": start_time,
                                "updated_since": updated_since,
                            }
                        )
                        aoi_capped = True
                        break

                if not events_response:
                    # Nothing left (Skylight caps meta.total, so an empty page can
                    # arrive before the reported total is reached).
                    break

                page_events = [normalize_v2_event(record) for record in events_response]
                new_events = []
                for event in page_events:
                    event_id = event.get("event_id")
                    position = seen_event_positions.get(event_id) if event_id else None
                    if position is None:
                        if event_id:
                            seen_event_positions[event_id] = len(response_list)
                        response_list.append(event)
                        new_events.append(event)
                    elif _updated_after(event, response_list[position]):
                        # Same event, updated between the page that first returned
                        # it and this one. Keep the newer copy: the page cursor
                        # advances over repeats (below), so the saved cursor ends
                        # up past this new stamp and the update would otherwise
                        # never be fetched again. Not counted as new — the page
                        # still yielded no unseen event, which is what the stall
                        # guard below is measuring.
                        response_list[position] = event
                pages_fetched += 1
                page_num += 1

                # Advance over the whole page, repeats included: an event that was
                # updated since it was first seen comes back with a newer stamp,
                # and that stamp is legitimately the furthest this page reached.
                next_cursor = latest_update_cursor(page_events)

                if len(events_response) < limit:
                    # A short page is the end of the result set.
                    break
                if not new_events:
                    # A full page containing nothing new means the cursor cannot
                    # move: more events share this one updatedAt than fit in a
                    # page, so every request returns the same ones. Stop rather
                    # than loop forever, and say so — the events beyond this
                    # timestamp need a bigger page size to reach.
                    logger.error(
                        f'AOI "{aoi}": a full page of {len(events_response)} event(s) contained '
                        f'nothing new, so paging cannot advance past {next_cursor}. More events '
                        f'share that same update timestamp than fit in one page (page size '
                        f'{page_size}). Keeping the {len(response_list)} event(s) collected so far; '
                        f'raise the page size for this integration to get past it.',
                        extra={
                            "integration_id": str(integration.id),
                            "aoi": aoi,
                            "attention_needed": True,
                        }
                    )
                    break
                if not next_cursor:
                    # No usable stamp on the page, so there is nothing to page by.
                    logger.error(
                        f'AOI "{aoi}": no updatedAt on any event of page {page_num - 1}, so the '
                        f'paging cursor cannot advance. Keeping the {len(response_list)} event(s) '
                        f'collected so far.',
                        extra={
                            "integration_id": str(integration.id),
                            "aoi": aoi,
                            "attention_needed": True,
                        }
                    )
                    break
                page_cursor = next_cursor

            if aoi_capped:
                # Dropped whole: no events for this AOI, and no entry in
                # `events`, so no chunk plan is written for it and the next run
                # leaves its cursor untouched.
                continue

            logger.info(
                f'Fetched {len(response_list)} events for AOI "{aoi}" in {pages_fetched} page(s). '
                f'Skylight reported total: {reported_total}. Window start: {start_time}. '
                f'Cursor: {updated_since}.'
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


async def search_aois(integration, auth) -> list:
    """List the AOIs visible to the Skylight account behind `auth` (searchAOIs).

    Returns raw records: {id, status, createdAt, updatedAt, properties {name,
    description, areaKm2}}. Pages through meta.total; read-only, no state.
    """
    default_transport_dict = dict(
        url=DEFAULT_SKYLIGHT_API_URL,
        verify=True,
    )
    auth_client = build_graphql_client(default_transport_dict)
    headers = await build_request_header(integration, auth, auth_client)
    gql_client = build_events_client(default_transport_dict, headers)

    query = gql(
        """
        query searchSkylightAOIs($limit: Int, $offset: Int) {
            searchAOIs(input: { limit: $limit, offset: $offset }) {
                records {
                    id
                    status
                    createdAt
                    updatedAt
                    properties {
                        name
                        description
                        areaKm2
                    }
                }
                meta {
                    total
                }
            }
        }
        """
    )

    aois = []
    offset = 0
    total = None
    while total is None or offset < total:
        params = {"limit": AOI_SEARCH_PAGE_SIZE, "offset": offset}
        logger.info(f'"searchAOIs" query (offset {offset}) for integration "{str(integration.id)}"...')
        response = await execute_gql_query(gql_client, query, params, integration, auth)
        search_response = response.get("searchAOIs") or {}
        records = search_response.get("records") or []
        if total is None:
            total = (search_response.get("meta") or {}).get("total") or 0
        if not records:
            break
        aois.extend(records)
        offset += len(records)
    logger.info(f'"searchAOIs" returned {len(aois)} AOI(s) for integration "{str(integration.id)}".')
    return aois
