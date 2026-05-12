import datetime
import httpx
import logging
import re
import stamina

import app.actions.client as client
import app.services.gundi as gundi_tools
import app.settings.integration as settings

from copy import deepcopy
from dateparser import parse as dp

from gql.transport.exceptions import TransportQueryError

from app.actions.configurations import AuthenticateConfig, PullEventsConfig, ProcessEventsPerAOIConfig
from app.services.action_scheduler import trigger_action
from app.services.activity_logger import activity_logger, log_action_activity
from app.services.state import IntegrationStateManager
from app.services.utils import generate_batches

from gundi_core.schemas.v2 import LogLevel


logger = logging.getLogger(__name__)


state_manager = IntegrationStateManager()


def _to_snake(name: str) -> str:
    return re.sub(r'(?<!^)(?=[A-Z])', '_', name).lower()


# Explicit field mappings — vessel info is sent separately as a "vessels" array
# Fields with "Pending" fallback will show "Pending" when absent (e.g. no exit yet)
_AOI_VISIT_FIELD_MAP = {
    "entry_speed": ("entrySpeed", None),
    "entry_heading": ("entryHeading", None),
    "end_heading": ("endHeading", None),
    "exit_date": ("end_time", "Pending"),
    "duration_in_area": ("duration_hours", "Pending"),
    "skylight_link": ("entry_link", None),
}

_SPEED_RANGE_FIELD_MAP = {
    "average_speed_range": ("averageSpeed", None),
    "duration_in_seconds": ("durationSec", None),
    "distance": ("distance", None),
    "skylight_link": ("entry_link", None),
}

_FISHING_FIELD_MAP = {
    "fishing_score": ("fishingScore", None),
    "skylight_link": ("entry_link", None),
}

_RENDEZVOUS_FIELD_MAP = {
    "osr_score": ("osrScore", None),
    "duration_hours": ("duration_hours", None),
    "skylight_link": ("entry_link", None),
}

_VIIRS_FIELD_MAP = {
    "detection_type": ("detectionType", None),
    "estimated_length": ("estimatedLength", None),
    "heading": ("heading", None),
    "radiance": ("radianceNw", None),
    "skylight_link": ("entry_link", None),
}

_IMAGERY_FIELD_MAP = {
    "detection_type": ("detectionType", None),
    "estimated_length": ("estimatedLength", None),
    "estimated_speed": ("estimatedSpeedKts", None),
    "estimated_vessel_category": ("estimatedVesselCategory", None),
    "heading": ("heading", None),
    "score": ("score", None),
    "distance_to_coast_m": ("distanceToCoastM", None),
    "orientation": ("orientation", None),
    "meters_per_pixel": ("metersPerPixel", None),
    "skylight_link": ("entry_link", None),
}

# Fields to extract per vessel when building the vessels array
_VESSEL_ARRAY_FIELDS = [
    ("vessel_name", "name"),
    ("country", "displayCountry"),
    ("mmsi", "mmsi"),
    ("vessel_type", "vesselType"),
    ("imo", "imo"),
    ("length", "length"),
]


def _build_vessels_list(vessels: dict) -> list:
    result = []
    for vessel_key in ("vessel0", "vessel1"):
        vessel = (vessels or {}).get(vessel_key)
        if vessel:
            obj = {}
            for out_key, raw_key in _VESSEL_ARRAY_FIELDS:
                val = vessel.get(raw_key)
                if val is not None:
                    obj[out_key] = val
            if obj:
                result.append(obj)
    return result


def _map_details(field_map: dict, details: dict, value_maps: dict = None) -> dict:
    mapped = {}
    for output_key, (raw_key, fallback) in field_map.items():
        value = details.get(raw_key, fallback)
        if value is not None:
            if value_maps and output_key in value_maps:
                value = value_maps[output_key].get(value, value)
            mapped[output_key] = value
    return mapped


# Keyed by Skylight event type: (field_map, value_maps, fixed_fields)
# fixed_fields hardcode a display value regardless of what the API returns
# Vessel data is handled separately via _build_vessels_list() in transform()
_EVENT_TYPE_FIELD_MAPS = {
    "aoi_visit":                (_AOI_VISIT_FIELD_MAP,   None, None),
    "speed_range":              (_SPEED_RANGE_FIELD_MAP, None, None),
    "fishing_activity_history": (_FISHING_FIELD_MAP,     None, None),
    "dark_rendezvous":          (_RENDEZVOUS_FIELD_MAP,  None, None),
    "standard_rendezvous":      (_RENDEZVOUS_FIELD_MAP,  None, None),
    "viirs":                    (_VIIRS_FIELD_MAP,    None, {"detection_type": "Night Lights (VIIRS)"}),
    "sar_sentinel1":            (_IMAGERY_FIELD_MAP,  None, {"detection_type": "Sentinel-1 Radar"}),
    "eo_sentinel2":             (_IMAGERY_FIELD_MAP,  None, {"detection_type": "Sentinel-2 Optical"}),
    "eo_landsat_8_9":           (_IMAGERY_FIELD_MAP,  None, {"detection_type": "Landsat 8/9 Optical"}),
}


def _get_mapped_details(skylight_event_type: str, details: dict) -> dict:
    config = _EVENT_TYPE_FIELD_MAPS.get(skylight_event_type)
    if config:
        field_map, value_maps, fixed_fields = config
        mapped = _map_details(field_map, details, value_maps)
        if fixed_fields:
            mapped.update(fixed_fields)
        return mapped
    return details


# Skylight event types that represent vessel detections; ER event type/title
# is determined by the detectionType field in eventDetails, not the sensor type.
_DETECTION_SKYLIGHT_TYPES = {"viirs", "sar_sentinel1", "eo_sentinel2", "eo_landsat_8_9"}

_DETECTION_ER_OVERRIDES = {
    "ais_correlated": ("detection_alert_rep", "AIS Correlated Vessel Detection"),
}
_DETECTION_ER_DEFAULT = ("detection_alert_rep", "Dark Vessel Detection")


def get_clean_event_id(event):
    event_id = ";".join([x for x in (event.get("eventId") or "").split(";")[:-1]])
    if not event_id:
        event_id = event.get("eventId")
    return event_id


def transform(config, data: dict) -> dict:
    event_type = data.get("eventType")
    event_config = None

    try:
        for conf in config:
            if isinstance(conf.get("skylight_event_type"), list):
                if event_type in conf.get("skylight_event_type"):
                    event_config = conf
                    break
            else:
                if event_type == conf.get("skylight_event_type"):
                    event_config = conf
                    break
        if not event_config:
            message = f"'{event_type}' event type is not supported at the moment."
            logger.warning(message)
            return {}
    except Exception:
        message = f"'{event_type}' event type is not supported at the moment."
        logger.warning(message)
        return {}
    else:
        full_event_details = {}

        event_details = deepcopy(data.get("eventDetails", {}))
        for key, detail in event_details.items():
            if detail is not None and key != "__typename":
                full_event_details.update({key: detail})

        for field in ("createdAt", "updatedAt"):
            if data.get(field) is not None:
                full_event_details[field] = data[field]

        vessels = deepcopy(data.get("vessels", {}))
        for vessel_name, vessel_detail in (vessels or {}).items():
            if vessel_detail:
                for key, detail in vessel_detail.items():
                    if detail is not None:
                        full_event_details.update({f"{_to_snake(key)}_{vessel_name}": detail})

        full_event_details["eventId"] = data.get("eventId")
        full_event_details["entry_link"] = settings.ENTRY_LINK_URL.format(
            event_id=full_event_details["eventId"]
        )

        is_entry_alert = event_config.get("skylight_event_type") == "aoi_visit"
        if is_entry_alert:
            event_time_and_location = data.get('start')
            end = data.get('end')
            if not event_time_and_location:
                message = f"Entry alert '{data.get('eventId')}' has no start point, skipping."
                logger.warning(message)
                return {}
            if end:
                full_event_details["end_time"] = end.get('time')
                end_point = end.get('point') or {}
                full_event_details["end_lat"] = end_point.get('lat')
                full_event_details["end_lon"] = end_point.get('lon')
                start_dt = dp(event_time_and_location.get('time'))
                end_dt = dp(end.get('time'))
                if start_dt and end_dt:
                    full_event_details["duration_hours"] = round((end_dt - start_dt).total_seconds() / 3600, 2)
        else:
            event_time_and_location = data.get('end') or data.get('start')
            end = None
            if not event_time_and_location:
                message = f"Event '{data.get('eventId')}' has no start or end point, skipping."
                logger.warning(message)
                return {}
            start = data.get('start')
            if start:
                full_event_details["start_time"] = start.get('time')
                start_point = start.get('point') or {}
                full_event_details["start_lat"] = start_point.get('lat')
                full_event_details["start_lon"] = start_point.get('lon')
            if data.get('end') and start:
                start_dt = dp(start.get('time'))
                end_dt = dp(data['end'].get('time'))
                if start_dt and end_dt:
                    full_event_details["duration_hours"] = round((end_dt - start_dt).total_seconds() / 3600, 2)

        skylight_type = data.get("eventType")
        if skylight_type in _DETECTION_SKYLIGHT_TYPES:
            raw_detection_type = (data.get("eventDetails") or {}).get("detectionType")
            er_event_type, er_event_title = _DETECTION_ER_OVERRIDES.get(
                raw_detection_type, _DETECTION_ER_DEFAULT
            )
        else:
            er_event_type = event_config.get("event_type")
            er_event_title = event_config.get("event_title")

        time_str = event_time_and_location.get('time')
        event_details = _get_mapped_details(skylight_type, full_event_details)

        # Add vessels array for all event types (empty for dark detections)
        vessels = _build_vessels_list(data.get("vessels"))
        event_details["vessels"] = vessels

        transformed = dict(
            title=er_event_title,
            event_type=er_event_type,
            recorded_at=dp(time_str) if time_str else None,
            location={
                "lat": (event_time_and_location.get('point') or {}).get('lat'),
                "lon": (event_time_and_location.get('point') or {}).get('lon')
            },
            event_details=event_details
        )

        return transformed


async def action_auth(integration, action_config: AuthenticateConfig):
    logger.info(
        f"Executing auth action with integration {integration} and action_config {action_config}..."
    )
    try:
        # GraphQL Client
        default_transport_dict = dict(
            url=client.DEFAULT_SKYLIGHT_API_URL,
            verify=True,
        )
        gql_client = client.build_graphql_client(default_transport_dict)
        token = await client.get_authentication_token(integration, action_config, gql_client)
        if not token:
            logger.error(f"Auth unsuccessful for integration '{integration.id}'.")
            return {"valid_credentials": False}

        logger.info(f"Auth successful for integration '{integration.id}'.")
        return {"valid_credentials": True}
    except Exception as e:
        logger.info(f"An error occurred while fetching token for integration '{integration.id}'")
        return {"valid_credentials": None, "error": str(e)}


@activity_logger()
async def action_pull_events(integration, action_config: PullEventsConfig):
    logger.debug(
        f"Executing pull_events action with integration {integration} and action_config {action_config}..."
    )
    result = {"events_extracted": 0, "process_events_per_aoi_action_triggered": 0, "details": {}}
    try:
        async for attempt in stamina.retry_context(
                on=httpx.HTTPError,
                attempts=3,
                wait_initial=datetime.timedelta(seconds=10),
                wait_max=datetime.timedelta(seconds=30),
                wait_jitter=datetime.timedelta(seconds=3)
        ):
            with attempt:
                events, updated_config_data, end_time = await client.get_skylight_events(
                    integration=integration,
                    config_data=action_config,
                    auth=client.get_auth_config(integration)
                )

    except TransportQueryError as te:
        message = f"TransportQueryError. message: {te.errors[0].get('message')}"
        await log_action_activity(
            integration_id=integration.id,
            action_id="pull_events",
            level=LogLevel.WARNING,
            title="Error executing 'get_skylight_events' GraphQL query (TransportQueryError)",
            data={"message": message}
        )
        raise te
    except httpx.HTTPError as e:
        msg = f"pull_observations action returned error. Integration: {str(integration.id)}. Exception: {e}"
        logger.exception(msg, extra={
            "integration_id": str(integration.id),
            "attention_needed": True
        })
        await log_action_activity(
            integration_id=integration.id,
            action_id="pull_events",
            level=LogLevel.WARNING,
            title=msg,
            data={"message": msg}
        )
        raise e
    except Exception as e:
        message = f"Unhandled exception occurred. Exception: {e}"
        await log_action_activity(
            integration_id=integration.id,
            action_id="pull_events",
            level=LogLevel.WARNING,
            title="Unhandled error while executing 'get_skylight_events' GraphQL query",
            data={"message": message}
        )
        raise e
    else:
        if all([len(items) == 0 for items in events.values()]):
            logger.info(f"No events were pulled for integration: '{str(integration.id)}'.")
            result["message"] = f"No events were pulled for integration: '{str(integration.id)}'."
            await state_manager.set_state(
                str(integration.id), "pull_events", {"start_time": end_time}, "global"
            )
            return result

        async def get_skylight_events_to_patch():
            patch_these_events = []
            for aoi, events_list in events.items():
                new_events = []
                for event in events_list:
                    event_id = get_clean_event_id(event)
                    if saved_event := await state_manager.get_state(str(integration.id), "pull_events", event_id):
                        patch_these_events.append((saved_event.get("object_id"), event))
                    else:
                        new_events.append(event)
                events[aoi] = new_events
            return events, patch_these_events

        events, events_to_patch = await get_skylight_events_to_patch()

        # trigger "process_events_per_aoi" action for each AOI
        for aoi, aoi_events in events.items():
            if aoi_events:
                result["events_extracted"] += len(aoi_events)
                logger.info(f"Triggering 'process_events_per_aoi' action for AOI: '{aoi}' Events: '{len(aoi_events)}'")
                parsed_config = ProcessEventsPerAOIConfig(
                    integration_id=str(integration.id),
                    aoi=aoi,
                    events=aoi_events,
                    updated_config_data=[config.dict() for config in updated_config_data],
                    # max_events_per_type=action_config.max_events_per_type,  # TESTING ONLY
                )
                await trigger_action(integration.id, "process_events_per_aoi", config=parsed_config)
                result["process_events_per_aoi_action_triggered"] += 1

        if events_to_patch:
            response = await patch_events(
                events_to_patch,
                [config.dict() for config in updated_config_data],
                integration,
            )
            result["events_updated"] = len(response)
            result["details"]["updated"] = response

        await state_manager.set_state(
            str(integration.id), "pull_events", {"start_time": end_time}, "global"
        )
        return result


@activity_logger()
async def action_process_events_per_aoi(integration, action_config: ProcessEventsPerAOIConfig):
    result = {"events_processed": 0, "details": {}}
    all_responses = []

    _tz_min = datetime.datetime.min.replace(tzinfo=datetime.timezone.utc)
    pairs = [
        (transform(action_config.updated_config_data, event), event)
        for event in action_config.events
    ]
    pairs = [(t, e) for t, e in pairs if t]
    pairs.sort(key=lambda x: x[0].get("recorded_at") or _tz_min, reverse=True)

    # # TESTING ONLY — uncomment to limit events sent to ER per event type
    # if action_config.max_events_per_type:
    #     counts: dict = {}
    #     filtered = []
    #     for t, e in pairs:
    #         et = t.get("event_type")
    #         if counts.get(et, 0) < action_config.max_events_per_type:
    #             filtered.append((t, e))
    #             counts[et] = counts.get(et, 0) + 1
    #     pairs = filtered

    transformed_data = [t for t, _ in pairs]
    source_events = [e for _, e in pairs]

    if transformed_data:
        # Send transformed data to Sensors API V2
        try:
            for i, batch in enumerate(generate_batches(transformed_data, 200)):
                logger.info(f'Sending observations batch #{i}: {len(batch)} observations. AOI: {action_config.aoi}')
                response = await gundi_tools.send_events_to_gundi(
                    events=batch,
                    integration_id=integration.id
                )

                if response:
                    all_responses.extend(response)
                    result["events_processed"] += len(response)
                    # Send images as attachments (if available)
                    await process_attachments(batch, response, integration)
            await save_events_state(all_responses, source_events, integration)
        except (httpx.ConnectTimeout, httpx.ReadTimeout) as e:
            msg = (f'Timeout exception. AOI: {action_config.aoi}. Integration: {str(integration.id)}. '
                   f'Exception: {e}, Type: {str(type(e))}, Request: {str(e.request)}')
            logger.exception(
                msg,
                extra={
                    'needs_attention': True,
                    'integration_id': str(integration.id),
                    "aoi": action_config.aoi,
                    'action_id': "pull_events"
                }
            )
            raise e
        else:
            return result
    else:
        return result


async def process_attachments(transformed_data, response, integration):
    for data, event_id in zip(transformed_data, response):
        try:
            image_url = data["event_details"].get("imageUrl", None)
            if image_url:
                filename = (
                        image_url.split("/")[-1]
                        or
                        f"skylight_att_{str(integration.id)}_default.png"
                )
                logger.info(
                    f"Processing attachment '{filename}' for event ID '{event_id['object_id']}'",
                    extra={
                        "integration_id": str(integration.id)
                    }
                )
                async with httpx.AsyncClient(timeout=120, verify=False) as session:
                    image_response = await session.get(image_url)
                    image_response.raise_for_status()

                img = await image_response.aread()

                await gundi_tools.send_event_attachments_to_gundi(
                    event_id=event_id["object_id"],
                    attachments=[(filename, img)],
                    integration_id=integration.id
                )
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 403:
                message = f"HTTP 403 Forbidden response while reading event attachment for event ID '{event_id['object_id']}'. Exception: {e}"
            else:
                message = f"Error while processing event attachment for event ID '{event_id['object_id']}'. Exception: {e}"

            request = {
                "event_id": event_id["object_id"],
                "filename": filename,
                "integration_id": integration.id
            }
            logger.exception(message, extra={
                "integration_id": str(integration.id),
                "attention_needed": True
            })
            log_data = {"message": message, "request": request}
            if server_response := getattr(e, "response", None):
                log_data["server_response_body"] = server_response.text
            await log_action_activity(
                integration_id=integration.id,
                action_id="pull_events",
                level=LogLevel.WARNING,
                title=message,
                data=log_data
            )
            continue


async def patch_events(events, updated_config_data, integration):
    logger.info(f"Patching {len(events)} existing events for integration '{integration.id}'.")
    responses = []
    for event in events:
        gundi_object_id = event[0]
        new_event = event[1]
        transformed_data = transform(updated_config_data, new_event)
        if transformed_data:
            response = await gundi_tools.update_gundi_event(
                event=transformed_data,
                integration_id=integration.id,
                event_id=gundi_object_id
            )
            responses.append(response)
    logger.info(f"Patched {len(responses)}/{len(events)} events for integration '{integration.id}'.")
    return responses


async def save_events_state(response, events, integration):
    for saved_event, event in zip(response, events):
        try:
            event_id = get_clean_event_id(event)
            await state_manager.set_state(
                integration_id=str(integration.id),
                action_id="pull_events",
                state=saved_event,
                source_id=event_id,
                expire=259200 # 72 hrs
            )
        except Exception as e:
            message = f"Error while saving event ID '{event.get('eventId')}'. Exception: {e}."
            logger.exception(message, extra={
                "integration_id": str(integration.id),
                "attention_needed": True
            })
            raise e
