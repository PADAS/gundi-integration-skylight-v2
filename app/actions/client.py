import backoff
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
from gql.transport.exceptions import TransportQueryError
from gql.transport.httpx import HTTPXAsyncTransport, HTTPXTransport

from app.services.errors import ConfigurationNotFound
from app.services.utils import find_config_for_action
from app.services.state import IntegrationStateManager

from typing import Any


logger = logging.getLogger(__name__)
state_manager = IntegrationStateManager()

DEFAULT_SKYLIGHT_API_URL = 'https://api.skylight.earth/graphql'

# Refresh tokens this many seconds before their actual expiry,
# to avoid races where the token expires mid-request.
_TOKEN_EXPIRY_SKEW_SECONDS = 60


# Default mapping values (for ER destinations)
DEFAULT_EVENT_MAPPING = {
    "fishing": {
        "event_type": "fishing_alert_rep",
        "event_title": "Fishing",
        "skylight_event_type": "fishing_activity_history"
    },
    "viirs_detection": {
        "event_type": "detection_alert_rep",
        "event_title": "Night Lights Detection",
        "skylight_event_type": "viirs"
    },
    "sar_detection": {
        "event_type": "detection_alert_rep",
        "event_title": "SAR Detection",
        "skylight_event_type": "sar_sentinel1"
    },
    "eo_detection": {
        "event_type": "detection_alert_rep",
        "event_title": "Optical Detection",
        "skylight_event_type": ["eo_sentinel2", "eo_landsat_8_9"]
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
    "vessel_detection": {
        "event_type": "detection_alert_rep",
        "event_title": "Vessel Detection",
        "skylight_event_type": ["viirs", "sar_sentinel1", "eo_sentinel2", "eo_landsat_8_9"]
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
    fishing_alert_rep = 'fishing_alert_rep'
    speed_range_alert_rep = 'speed_range_alert_rep'
    standard_rendezvous_alert_rep = 'standard_rendezvous_alert_rep'
    entry_alert_rep = 'entry_alert_rep'
    detection_alert_rep = 'detection_alert_rep'



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


def build_graphql_client(transport_dict):
    transport = HTTPXTransport(**transport_dict)

    # Create a GraphQL client using the defined transport
    gql_client = GQLClient(
        transport=transport,
        execute_timeout=60,
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
    try:
        events_mapping = integration.additional or DEFAULT_EVENT_MAPPING
        parsed_obj = EventType.parse_obj(events_mapping.get(event_type, {}))
    except pydantic.ValidationError as e:
        message = f'Failed to map "{event_type}". Either is invalid or unsupported. {e}'
        logger.warning(message)
        return "error"
    else:
        return parsed_obj


@backoff.on_exception(
    backoff.expo,
    TransportQueryError,
    max_tries=3,
    jitter=backoff.full_jitter,
    giveup=lambda e: e.errors[0]["extensions"].get("code") not in ["UNAUTHENTICATED", "UNAUTHORIZED"]
)
async def execute_gql_query(gql_client, query, params, integration, auth):
    try:
        result = gql_client.execute(query, variable_values=params)
    except TransportQueryError as te:
        if te.errors[0]["extensions"].get("code") in ["UNAUTHENTICATED", "UNAUTHORIZED"]:
            logger.warning(f'"searchEventsV2" query returned {te.errors[0]["extensions"].get("code")}, retrying with a new token...')
            await state_manager.delete_state(
                str(integration.id),
                "pull_events",
                auth.username
            )
        raise te
    else:
        return result


async def get_skylight_events(integration, config_data, auth):
    if not integration.additional:
        msg = f'Data map JSON not found. Will use default ER map for integration ID: "{str(integration.id)}"'
        logger.warning(msg)

    default_transport_dict = dict(
        url=DEFAULT_SKYLIGHT_API_URL,
        verify=True,
    )
    gql_client = build_graphql_client(default_transport_dict)
    headers = await build_request_header(integration, auth, gql_client)
    transport_dict_with_header = {**default_transport_dict, 'headers': headers.dict()}
    gql_client = build_graphql_client(transport_dict_with_header)

    query = gql(
        """
        query searchSkylightEventsV2(
            $eventTypes: [String!]!
            $startTime: String
            $endTime: String
            $aoiId: String
            $limit: Int
            $offset: Int
            $snapshotId: String
        ) {
            searchEventsV2(input: {
                eventType: { inc: $eventTypes }
                startTime: { gte: $startTime, lte: $endTime }
                intersectsAoiId: $aoiId
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
                        point { lat lon }
                        time
                    }
                    end {
                        point { lat lon }
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
                        ... on StandardRendezvousEventDetails {
                            __typename
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
                            detectionType
                            estimatedLength
                            estimatedSpeedKts
                            estimatedVesselCategory
                            frameIds
                            heading
                            imageUrl
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

    mapped_event_types = [map_event_type(integration, event_type) for event_type in config_data.event_types]
    if "error" in mapped_event_types:
        msg = f'Invalid config received for integration ID: {str(integration.id)}'
        logger.error(msg)
        raise PullEventsBadConfigException(msg)

    event_types = []
    for mapped_event in mapped_event_types:
        if isinstance(mapped_event.skylight_event_type, list):
            event_types.extend(mapped_event.skylight_event_type)
        else:
            event_types.append(mapped_event.skylight_event_type)

    limit = config_data.pageSize
    initial_data_window_days = config_data.initial_data_window_days or settings.DEFAULT_WINDOW_DAYS

    try:
        saved_state = await state_manager.get_state(str(integration.id), "pull_events", "global")
        if saved_state:
            start_time = dp(saved_state.get("start_time")).isoformat()
        else:
            start_time = (
                datetime.now(tz=timezone.utc).replace(hour=0, minute=0, second=0) -
                timedelta(days=initial_data_window_days)
            ).isoformat()

        end_time = datetime.now(tz=timezone.utc).isoformat()
        response_list = []
        aoi_ids = config_data.aoi_ids
        if not aoi_ids:
            msg = f'No AOI IDs configured for integration "{str(integration.id)}". At least one AOI ID is required.'
            logger.error(msg)
            raise PullEventsBadConfigException(msg)

        for aoi_id in aoi_ids:
            offset = 0
            snapshot_id = None
            has_data = True

            while has_data:
                params = {
                    "eventTypes": event_types,
                    "startTime": start_time,
                    "endTime": end_time,
                    "aoiId": aoi_id,
                    "limit": limit,
                    "offset": offset,
                }
                if snapshot_id:
                    params["snapshotId"] = snapshot_id

                logger.debug(f'Sending "searchEventsV2" query. AOI: "{aoi_id}". Params: "{params}"...')
                response = await execute_gql_query(gql_client, query, params, integration, auth)
                search_response = response['searchEventsV2']
                records = search_response.get('records') or []

                logger.debug(f'"searchEventsV2" returned {len(records)} records for AOI "{aoi_id}" at offset {offset}.')

                if not snapshot_id:
                    snapshot_id = (search_response.get('meta') or {}).get('snapshotId')

                if not records:
                    has_data = False
                else:
                    response_list.extend(records)
                    offset += limit

            logger.info(f'Fetched {offset} records for AOI "{aoi_id}" (time window: {start_time} → {end_time}).')

    except pydantic.ValidationError as ve:
        message = f'Validation error in Skylight "searchEventsV2" endpoint. {ve.json()}'
        logger.exception(message, extra={"integration_id": str(integration.id), "attention_needed": True})
        raise ve
    except TransportQueryError as te:
        message = f"TransportQueryError. message: {te.errors[0].get('message')}"
        logger.exception(message, extra={"integration_id": str(integration.id), "attention_needed": True})
        raise te
    except Exception as e:
        message = f"Unhandled exception occurred. Exception: {e}"
        logger.exception(message, extra={"integration_id": str(integration.id), "attention_needed": True})
        raise e

    seen = set()
    deduped = []
    for record in response_list:
        raw_eid = record.get("eventId") or ""
        eid = ";".join(raw_eid.split(";")[:-1]) or raw_eid
        if eid not in seen:
            seen.add(eid)
            deduped.append(record)

    return {"global": deduped}, mapped_event_types, end_time
