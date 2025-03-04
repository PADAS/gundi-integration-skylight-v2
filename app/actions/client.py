import backoff
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

# For use in case an event doesn't have a vessel dict
EMPTY_VESSEL_DICT = {
    "vessel_0_category": "N/A",
    "vessel_0_class": "N/A",
    "vessel_0_country_filter": "N/A",
    "vessel_0_display_country": "N/A",
    "vessel_0_mmsi": "N/A",
    "vessel_0_name": "N/A",
    "vessel_0_subcategory": "N/A",
    "vessel_0_type": "N/A",
}

# Default mapping values (for ER destinations)
DEFAULT_EVENT_MAPPING = {
    "fishing_alert": {
        "event_type": "fishing_alert_rep",
        "event_title": "Fishing Alert",
        "skylight_event_type": "fishing_activity_history"
    },
    "vessel_detection_alert": {
        "event_type": "detection_alert_rep",
        "event_title": "Vessel Detection Alert",
        "skylight_event_type": ["viirs", "sar_sentinel1", "eo_sentinel2"]
    },
    "speed_range_alert": {
        "event_type": "speed_range_alert_rep",
        "event_title": "Speed Range Alert",
        "skylight_event_type": "speed_range"
    },
    "marine_entry_alert": {
        "event_type": "entry_alert_rep",
        "event_title": "Marine Entry Alert",
        "skylight_event_type": "aoi_visit"
    },
    "dark_activity_alert": {
        "event_type": "dark_activity_alert_rep",
        "event_title": "Dark Activity Alert",
        "skylight_event_type": "dark_activity"
    },
    "dark_rendezvous_alert": {
        "event_type": "dark_rendezvous_alert_rep",
        "event_title": "Dark Rendezvous Alert",
        "skylight_event_type": "dark_rendezvous"
    },
    "standard_rendezvous_alert": {
        "event_type": "standard_rendezvous_alert_rep",
        "event_title": "Standard Rendezvous Alert",
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
    dark_activity_alert_rep = 'dark_activity_alert_rep'


class SkylightEventTypes(str, Enum):
    dark_rendezvous = 'dark_rendezvous'
    detection = 'detection'
    fishing_activity_history = 'fishing_activity_history'
    speed_range = 'speed_range'
    standard_rendezvous = 'standard_rendezvous'
    aoi_visit = 'aoi_visit'
    dark_activity = 'dark_activity'


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


async def build_request_header(integration, auth, gql_client):
    token_dict = await state_manager.get_state(str(integration.id), "pull_events", auth.username)

    if token_dict:
        token_dict = SkylightGetTokenResponse.parse_obj(token_dict)
    else:
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
            # Currently, if we receive None in the response, there was an unknown error in Skylight API
            # Will delete token and try again
            logger.info(f'"getRendedzvousExternal" query returned {te.errors[0]["extensions"].get("code")}, retrying with a new token...')
            await state_manager.delete_state(
                str(integration.id),
                "pull_events",
                auth.username
            )
        raise te
    else:
        return result


async def get_skylight_events(integration, config_data, auth):
    # Check if data mapping dict is set in the integration
    if not integration.additional:
        msg = f'Data map JSON not found. Will use default ER map for integration ID: "{str(integration.id)}"'
        logger.warning(msg)

    # GraphQL Client
    default_transport_dict = dict(
        url=integration.base_url or DEFAULT_SKYLIGHT_API_URL,
        verify=True,
    )
    gql_client = build_graphql_client(default_transport_dict)

    # get Token
    headers = await build_request_header(integration, auth, gql_client)

    # Add 'header' to GraphQL client
    transport_dict_with_header = default_transport_dict
    transport_dict_with_header['headers'] = headers.dict()
    gql_client = build_graphql_client(transport_dict_with_header)

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

    # Map event types
    mapped_event_types = [map_event_type(integration, event_type) for event_type in config_data.event_types]

    if "error" in mapped_event_types:
        msg = f'Invalid config received for integration ID: {str(integration.id)}'
        logger.error(msg)
        raise PullEventsBadConfigException(msg)

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
            saved_aoi_start_time = await state_manager.get_state(str(integration.id), "pull_events", aoi)
            if saved_aoi_start_time:
                start_time = dp(saved_aoi_start_time.get("start_time")).isoformat()
            else:
                start_time = (
                        datetime.now(tz=timezone.utc).replace(hour=0, minute=0, second=0) -
                        timedelta(days=initial_data_window_days)
                ).isoformat()

            page_num = 1
            has_data = True

            while has_data:
                params = {
                    "eventTypes": event_types,
                    "aoiId": aoi,
                    "startTime": start_time,
                    "pageSize": page_size,
                    "pageNum": page_num
                }

                logger.info(f'Sending "getRendedzvousExternal" query request. Params: "{params}"...')

                response = await execute_gql_query(gql_client, query, params, integration, auth)

                events_response = response['events']['items']

                logger.info(f'"getRendedzvousExternal" query returned: "{events_response}"...')

                if events_response is None:
                    # Currently, if we receive None in the response, there was an unknown error in Skylight API
                    # Will delete token and try again
                    logger.info(f'"getRendedzvousExternal" query returned None, retrying with a new token...')
                    await state_manager.delete_state(
                        str(integration.id),
                        "pull_events",
                        auth.username
                    )
                    return await get_skylight_events(integration, config_data, auth)

                if not events_response:
                    has_data = False

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
