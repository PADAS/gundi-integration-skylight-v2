import datetime
import httpx
import logging
import stamina

import app.actions.client as client
import app.services.gundi as gundi_tools
import app.settings.integration as settings

from copy import deepcopy
from dateparser import parse as dp

from app.actions.configurations import AuthenticateConfig, PullEventsConfig, ProcessEventsPerAOIConfig
from app.services.action_scheduler import trigger_action
from app.services.activity_logger import activity_logger, log_action_activity
from app.services.state import IntegrationStateManager
from app.services.utils import generate_batches

from gundi_core.schemas.v2 import LogLevel


logger = logging.getLogger(__name__)


state_manager = IntegrationStateManager()


def get_clean_event_id(event):
    # This logic is to extract and remove timestamps from event_id
    event_id = ";".join([x for x in event.get("event_id").split(";")[:-1]])
    if not event_id:
        # no timestamp attached to event_ids, using event_id as it is
        event_id = event.get("event_id")
    return event_id


def transform(config, data: dict) -> dict:
    event_type = data.get("event_type")
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
            logger.info(message)
            return {}
    except:
        message = f"'{event_type}' event type is not supported at the moment."
        logger.info(message)
        return {}
    else:
        full_event_details = {}

        # Get all available event_details
        event_details = deepcopy(data.get("event_details", {}))
        for key, detail in event_details.items():
            if detail is not None:
                full_event_details.update({key: detail})

        # Get all available vessels info
        vessels = deepcopy(data.get("vessels", {}))
        if not vessels:
            full_event_details.update(**client.EMPTY_VESSEL_DICT)
        else:
            for vessel_name, vessel_detail in vessels.items():
                for key, detail in vessel_detail.items():
                    if detail is not None:
                        full_event_details.update({vessel_name + "_" + key: detail})

        full_event_details["event_id"] = data.get("event_id")
        full_event_details["entry_link"] = settings.ENTRY_LINK_URL.format(
            event_id=full_event_details["event_id"]
        )

        # Get end and/or start info
        event_time_and_location = data.get('end') or data.get('start')

        return dict(
            title=event_config.get("event_title"),
            event_type=event_config.get("event_type"),
            recorded_at=dp(event_time_and_location.get('time')),
            location={
                "lat": event_time_and_location["point"].get('lat'),
                "lon": event_time_and_location["point"].get('lon')
            },
            event_details=full_event_details
        )


async def action_auth(integration, action_config: AuthenticateConfig):
    logger.info(
        f"Executing auth action with integration {integration} and action_config {action_config}..."
    )
    try:
        # GraphQL Client
        default_transport_dict = dict(
            url=integration.base_url or client.DEFAULT_SKYLIGHT_API_URL,
            verify=True,
        )
        gql_client = client.build_graphql_client(default_transport_dict)
        token = await client.get_authentication_token(integration, action_config, gql_client)
        if not token:
            logger.error(f"Auth unsuccessful for integration '{integration.id}'.")
            return {"valid_credentials": False}

        logger.info(f"Auth successful for integration '{integration.id}'. Token: {token.access_token}")
        return {"valid_credentials": True}
    except Exception as e:
        logger.info(f"An error occurred while fetching token for integration '{integration.id}'")
        return {"valid_credentials": None, "error": str(e)}


@activity_logger()
async def action_pull_events(integration, action_config: PullEventsConfig):
    logger.info(
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
                events, updated_config_data = await client.get_skylight_events(
                    integration=integration,
                    config_data=action_config,
                    auth=client.get_auth_config(integration)
                )

    except httpx.HTTPError as e:
        msg = f"pull_observations action returned error. Integration: {str(integration.id)}. Exception: {e}"
        logger.exception(msg, extra={
            "integration_id": str(integration.id),
            "attention_needed": True
        })
        raise e
    else:
        if all([len(items) == 0 for items in events.values()]):
            logger.info(f"No events were pulled for integration: '{str(integration.id)}'.")
            result["message"] = f"No events were pulled for integration: '{str(integration.id)}'."
            return result

        event_ids = []
        async def get_skylight_events_to_patch():
            # Get through the events and check if state_manager has it recorded from a previous execution
            patch_these_events = []
            for aoi, events_list in events.items():
                for event in events_list:
                    event_id = get_clean_event_id(event)
                    event_ids.append(event_id)
                    if saved_event := await state_manager.get_state(str(integration.id), "pull_events", event_id):
                        # Event already exists, will patch it
                        patch_these_events.append((saved_event.get("object_id"), event))
                        events_list.remove(event)
                events[aoi] = events_list
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
                    updated_config_data=[config.dict() for config in updated_config_data]
                )
                await trigger_action(integration.id, "process_events_per_aoi", config=parsed_config)
                result["process_events_per_aoi_action_triggered"] += 1
                logger.info(f"Triggered 'process_events_per_aoi' action for AOI: '{aoi}'")

        if events_to_patch:
            # Process events to patch
            response = await patch_events(events_to_patch, updated_config_data, integration)
            result["events_updated"] = len(response)
            result["details"]["updated"] = response

        return result


@activity_logger()
async def action_process_events_per_aoi(integration, action_config: ProcessEventsPerAOIConfig):
    result = {"events_processed": 0, "details": {}}
    all_responses = []

    transformed_data = sorted(
        [transform(action_config.updated_config_data, event) for event in action_config.events],
        key=lambda x: x.get("recorded_at") or datetime.datetime.min, reverse=True
    )

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
                    # Process events to patch
            await save_events_state(all_responses, action_config.events, integration)
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
            # Update states
            state = {
                "start_time": transformed_data[0].get("recorded_at")
            }
            await state_manager.set_state(
                str(integration.id),
                "pull_events",
                state,
                action_config.aoi
            )
            return result
    else:
        return result


async def process_attachments(transformed_data, response, integration):
    for data, event_id in zip(transformed_data, response):
        try:
            image_url = data["event_details"].get("image_url", None)
            if image_url:
                filename = (
                        image_url.split("/")[-1]
                        or
                        f"skylight_att_{str(integration.id)}_{data['event_details'].get('data_source', 'default')}.png"
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
            message = f"Error while saving event ID '{event.get('event_id')}'. Exception: {e}."
            logger.exception(message, extra={
                "integration_id": str(integration.id),
                "attention_needed": True
            })
            raise e
