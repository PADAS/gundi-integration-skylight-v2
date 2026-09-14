import datetime
import httpx
import json
import logging
import stamina
import uuid

import app.actions.client as client
import app.services.gundi as gundi_tools
import app.settings.integration as settings

from copy import deepcopy
from dateparser import parse as dp

from gql.transport.exceptions import TransportQueryError

from app.actions.core import action_title
from app.actions.configurations import AuthenticateConfig, PullEventsConfig, ProcessEventsPerAOIConfig, ListAOIsQuery, ReferenceDataResponse, ReferenceOption
from app.services.action_scheduler import trigger_action
from app.services.activity_logger import activity_logger, log_action_activity
from app.services.errors import classify_error, format_classified_error
from app.services.state import IntegrationStateManager
from app.services.utils import generate_batches

from gundi_core.schemas.v2 import LogLevel


logger = logging.getLogger(__name__)


state_manager = IntegrationStateManager()

# PubSub rejects publish requests over 10MB (400 Bad Request). The
# RunIntegrationAction command message embeds the raw events plus config, and
# PubSub base64-encodes message data (~33% overhead), so the events payload
# per message must stay well under that limit.
MAX_TRIGGER_PAYLOAD_BYTES = 2 * 1024 * 1024

# How long a chunk plan and its completion markers survive. They only need to
# outlive the gap between two pull_events runs; the generous window is so a
# paused or slow-scheduled integration still gets its cursor advanced instead
# of silently re-pulling the same window forever.
CHUNK_PLAN_TTL_SECONDS = 7 * 24 * 60 * 60


def _plan_source_id(aoi):
    # Prefixed so it can't collide with the AOI cursor key (bare aoi id), the
    # per-event dedupe keys (bare Skylight event id) or the token key
    # (the Skylight username), which all share this action's key namespace.
    return f"_plan.{aoi}"


def _chunk_source_id(chunk_id):
    return f"_chunk.{chunk_id}"


def _updated_at(event):
    """The event's Skylight updatedAt as a comparable, timezone-aware datetime."""
    raw = event.get("updated_at") if isinstance(event, dict) else None
    parsed = dp(raw) if raw else None
    if parsed is None:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=datetime.timezone.utc)
    return parsed


def _is_after(candidate, current):
    """True when the candidate cursor is strictly newer than the current one.

    Cursors are compared as parsed timestamps, not as strings: Skylight has
    returned both `...Z` and `...+00:00`, which sort differently as text.
    """
    if not candidate:
        return False
    if not current:
        return True
    new = _updated_at({"updated_at": candidate})
    old = _updated_at({"updated_at": current})
    if new is None:
        return False
    if old is None:
        return True
    return new > old


def build_chunk_plan(chunk_prefix, aoi_events, max_payload_bytes):
    """Split an AOI's new events into ordered chunks and describe each one.

    Returns (chunks, batches) where chunks[i] describes batches[i]:
    an id the sub-action reports completion under, and the newest updatedAt
    the chunk contains. Events are sorted oldest-update-first so that
    "every chunk up to here is delivered" also means "every event up to this
    timestamp is delivered" — the property the cursor relies on.

    `chunk_prefix` must be unique per AOI per run: completion markers live in
    one flat keyspace, so two AOIs numbering their chunks from zero under the
    same prefix would read each other's markers.
    """
    ordered = sorted(
        aoi_events,
        key=lambda event: _updated_at(event) or datetime.datetime.min.replace(tzinfo=datetime.timezone.utc)
    )
    chunks = []
    batches = batch_events_by_payload_size(ordered, max_payload_bytes)
    for index, batch in enumerate(batches):
        chunks.append({
            "id": f"{chunk_prefix}-{index}",
            "cursor": client.latest_update_cursor(batch),
            "events": len(batch),
        })
    return chunks, batches


async def advance_aoi_cursors(integration, aoi_ids):
    """Move each AOI cursor over the work the *previous* run actually delivered.

    pull_events never advances the cursor for its own run: triggering a
    sub-action only confirms the command was published, not that its events
    reached EarthRanger, so a sub-action that exhausts its retries would leave
    its events stranded behind an already-advanced cursor. Instead the run
    records the chunk plan it handed out, each sub-action writes a completion
    marker once its events are delivered, and this function — the only writer
    of the cursor — walks the plan in order, stops at the first chunk with no
    marker, and advances to the end of the last contiguous completed chunk.

    Consequence, accepted: the cursor lags one run behind, so each run re-fetches
    the previous run's events. Delivered ones are recognised by their per-event
    state and patched rather than duplicated.
    """
    advanced = {}
    for aoi in aoi_ids:
        plan = await state_manager.get_state(str(integration.id), "pull_events", _plan_source_id(aoi))
        if not plan:
            continue
        chunks = plan.get("chunks") or []
        completed_through = None
        pending = None
        for chunk in chunks:
            marker = await state_manager.get_state(
                str(integration.id), "pull_events", _chunk_source_id(chunk.get("id"))
            )
            if not marker:
                pending = chunk
                break
            completed_through = chunk.get("cursor") or completed_through
        if pending is None:
            # Every chunk delivered, so the whole of the previous run is
            # accounted for: the cursor may pass its last event. `final` covers
            # the tail of events the parent handled itself (patches), including
            # the case of a run that had no new events and so no chunks at all.
            new_cursor = plan.get("final") or completed_through
        else:
            new_cursor = completed_through
            logger.warning(
                f'AOI "{aoi}": the previous pull left chunk {pending.get("id")} '
                f'({pending.get("events")} event(s)) undelivered, so the cursor stops at '
                f'{new_cursor or "its previous value"}. Those events are fetched again next run.',
                extra={
                    "integration_id": str(integration.id),
                    "aoi": aoi,
                    "attention_needed": True,
                }
            )

        saved_state = await state_manager.get_state(str(integration.id), "pull_events", aoi) or {}
        current = saved_state.get("updated_since") or saved_state.get("start_time")
        if _is_after(new_cursor, current):
            await state_manager.set_state(
                str(integration.id), "pull_events", {"updated_since": new_cursor}, aoi
            )
            advanced[aoi] = new_cursor
            logger.info(f'AOI "{aoi}": cursor advanced to {new_cursor}.')

        # The plan is always cleared, complete or not. A chunk still in flight
        # is covered by the next run's own plan, because the cursor did not move
        # past it and its events are fetched again.
        await state_manager.delete_state(str(integration.id), "pull_events", _plan_source_id(aoi))
        for chunk in chunks:
            await state_manager.delete_state(
                str(integration.id), "pull_events", _chunk_source_id(chunk.get("id"))
            )
    return advanced


async def save_chunk_plan(integration, aoi, chunks, final_cursor):
    await state_manager.set_state(
        str(integration.id),
        "pull_events",
        {
            "chunks": [{k: v for k, v in chunk.items()} for chunk in chunks],
            # The newest updatedAt across everything this run pulled for the
            # AOI, new and already-known alike. Reached only once every chunk
            # has reported in.
            "final": final_cursor,
        },
        _plan_source_id(aoi),
        expire=CHUNK_PLAN_TTL_SECONDS,
    )


async def mark_chunk_delivered(integration, action_config):
    """Record that this batch's events reached EarthRanger.

    One sub-action per chunk id, so there is exactly one writer per key and no
    race. The next pull_events run reads these to decide how far the AOI cursor
    may move.
    """
    if not action_config.chunk_id:
        return
    await state_manager.set_state(
        str(integration.id),
        "pull_events",
        {"delivered_at": datetime.datetime.now(tz=datetime.timezone.utc).isoformat()},
        _chunk_source_id(action_config.chunk_id),
        expire=CHUNK_PLAN_TTL_SECONDS,
    )


def batch_events_by_payload_size(events, max_payload_bytes):
    # Splits events into ordered batches whose serialized size stays under
    # max_payload_bytes. A single event larger than the limit gets its own
    # batch (it can't be split further).
    batches = []
    current_batch = []
    current_size = 0
    for event in events:
        event_size = len(json.dumps(event, default=str).encode("utf-8"))
        if current_batch and current_size + event_size > max_payload_bytes:
            batches.append(current_batch)
            current_batch = []
            current_size = 0
        current_batch.append(event)
        current_size += event_size
    if current_batch:
        batches.append(current_batch)
    return batches


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
            full_event_details.update(
                {
                    f"vessel_0_{key}": value
                    for key, value in client.EMPTY_VESSEL_DICT.items()
                }
            )
        else:
            for vessel_name, vessel_detail in vessels.items():
                if vessel_detail:
                    for key, detail in vessel_detail.items():
                        if detail is not None:
                            full_event_details.update({vessel_name + "_" + key: detail})
                else:
                    full_event_details.update(
                        {
                            f"{vessel_name}_{key}": value
                            for key, value in client.EMPTY_VESSEL_DICT.items()
                        }
                    )

        full_event_details["event_id"] = data.get("event_id")
        full_event_details["entry_link"] = settings.ENTRY_LINK_URL.format(
            event_id=full_event_details["event_id"]
        )

        _skylight_type = event_config.get("skylight_event_type")
        is_entry_alert = (
            _skylight_type == "aoi_visit"
            or (isinstance(_skylight_type, list) and "aoi_visit" in _skylight_type)
        )

        if is_entry_alert:
            event_time_and_location = data.get('start')
            if not event_time_and_location:
                logger.warning(f"Entry alert '{data.get('event_id')}' has no start point, skipping.")
                return {}
            end = data.get('end')
            if end:
                full_event_details["exit_date"] = end.get('time')
                start_dt = dp(event_time_and_location.get('time'))
                end_dt = dp(end.get('time'))
                if start_dt and end_dt:
                    full_event_details["duration_in_area"] = round(
                        (end_dt - start_dt).total_seconds() / 3600, 2
                    )
                else:
                    full_event_details["duration_in_area"] = "Pending"
            else:
                full_event_details["exit_date"] = "Pending"
                full_event_details["duration_in_area"] = "Pending"
        else:
            event_time_and_location = data.get('end') or data.get('start')

        if not event_time_and_location:
            logger.warning(f"Event '{data.get('event_id')}' has no start or end point, skipping.")
            return {}

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
        # Never interpolate the integration or the action config: on the
        # ephemeral path both carry the draft's submitted credentials verbatim.
        f"Executing auth action for integration '{integration.id}'..."
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

        # Never log the token itself: this handler also runs on the ephemeral
        # (draft-credentials) path and logs are persistent.
        logger.info(f"Auth successful for integration '{integration.id}'.")
        return {"valid_credentials": True}
    except Exception as e:
        # Full detail stays server-side. The caller gets a fixed, non-echoing
        # message, never str(e): httpx messages can embed the request URL.
        logger.exception(f"An error occurred while fetching token for integration '{integration.id}'")
        if isinstance(e, TransportQueryError) and client._gql_error_code(e) in client._AUTH_ERROR_CODES:
            # Skylight rejected the credentials outright, so this is a definitive
            # answer ("invalid"), not an inconclusive one ("could not check").
            return {
                "valid_credentials": False,
                "error": "Skylight rejected the username or password.",
            }
        classified = classify_error(e)
        error_text = format_classified_error(classified, include_message=False) if classified else type(e).__name__
        return {"valid_credentials": None, "error": error_text}


@action_title("List AOIs")
async def action_list_aois(integration, action_config: ListAOIsQuery):
    """Reference action: AOI options for the portal's aoi_ids dropdown.

    Read-only and stateless (safe on the ephemeral/draft path). Option values
    are the Skylight AOI ids that pull_events.aoi_ids already stores, so
    existing configurations keep working and hand-typed ids remain valid.
    """
    auth = client.get_auth_config(integration)
    aois = await client.search_aois(integration, auth)
    options = []
    for aoi in aois:
        aoi_id = aoi.get("id")
        if not aoi_id:
            continue
        props = aoi.get("properties") or {}
        details = []
        if aoi.get("status") and aoi["status"] != "active":
            details.append(aoi["status"])
        if props.get("areaKm2"):
            details.append(f"{props['areaKm2']:,.0f} km²")
        if props.get("description"):
            details.append(props["description"])
        options.append(ReferenceOption(
            value=aoi_id,
            label=props.get("name") or aoi_id,
            description=", ".join(details) or None,
        ))
    options.sort(key=lambda option: (option.label or "").lower())
    return ReferenceDataResponse(options=options).dict()


@activity_logger()
async def action_pull_events(integration, action_config: PullEventsConfig):
    logger.info(
        # Same here: the integration model embeds every action's config data.
        f"Executing pull_events action for integration '{integration.id}'..."
    )
    result = {"events_extracted": 0, "process_events_per_aoi_action_triggered": 0, "details": {}}

    # Before anything is fetched: settle the previous run. This reads the chunk
    # plan that run recorded plus the completion markers its sub-actions wrote,
    # and advances each AOI cursor over the work that actually reached
    # EarthRanger. It has to happen first because the fetch below reads the
    # cursor it writes.
    advanced = await advance_aoi_cursors(integration, action_config.aoi_ids)
    if advanced:
        result["details"]["cursors"] = advanced

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
            return result

        # The far end of everything this run pulled per AOI, taken from the raw
        # batch (before the patch split below removes already-known events).
        # Recorded in the chunk plan as `final`; the next run only advances the
        # cursor this far once every chunk has reported delivery.
        final_cursors = {aoi: client.latest_update_cursor(aoi_events) for aoi, aoi_events in events.items()}

        event_ids = []
        async def get_skylight_events_to_patch():
            # Get through the events and check if state_manager has it recorded from a previous execution
            patch_these_events = []
            for aoi, events_list in events.items():
                # Build a keep-list rather than removing from the list being
                # iterated: removing shifts the index, so the event right after
                # an already-known one was never checked and would be re-sent to
                # Gundi as new. With the `updated >= cursor` boundary that is
                # routine — Skylight batch-updates give several events the same
                # updatedAt, so the boundary regularly holds more than one.
                new_events = []
                for event in events_list:
                    event_id = get_clean_event_id(event)
                    event_ids.append(event_id)
                    if saved_event := await state_manager.get_state(str(integration.id), "pull_events", event_id):
                        # Event already exists, will patch it
                        patch_these_events.append((saved_event.get("object_id"), event))
                    else:
                        new_events.append(event)
                events[aoi] = new_events
            return events, patch_these_events

        events, events_to_patch = await get_skylight_events_to_patch()

        # Plan the work per AOI, then hand it out. Each chunk carries the id it
        # must report completion under; the plan is saved after the patches
        # below so a run that fails partway leaves the cursor where it was.
        run_id = uuid.uuid4().hex[:12]
        plans = {}
        for aoi_index, (aoi, aoi_events) in enumerate(events.items()):
            chunks, batches = build_chunk_plan(
                f"{run_id}-{aoi_index}", aoi_events, MAX_TRIGGER_PAYLOAD_BYTES
            )
            plans[aoi] = chunks
            if not batches:
                continue
            result["events_extracted"] += len(aoi_events)
            logger.info(f"Triggering 'process_events_per_aoi' action for AOI: '{aoi}' Events: '{len(aoi_events)}'")
            for chunk, events_batch in zip(chunks, batches):
                parsed_config = ProcessEventsPerAOIConfig(
                    integration_id=str(integration.id),
                    aoi=aoi,
                    events=events_batch,
                    updated_config_data=[config.dict() for config in updated_config_data],
                    chunk_id=chunk["id"],
                )
                await trigger_action(integration.id, "process_events_per_aoi", config=parsed_config)
                result["process_events_per_aoi_action_triggered"] += 1
            logger.info(f"Triggered 'process_events_per_aoi' action for AOI: '{aoi}'")

        if events_to_patch:
            # Process events to patch
            response = await patch_events(
                events_to_patch,
                [config.dict() for config in updated_config_data],
                integration
            )
            result["events_updated"] = len(response)
            result["details"]["updated"] = response

        # Record the plan, but do not touch the cursor: at this point the
        # sub-actions have only been *published*, not confirmed. The next run
        # reads this back alongside the markers they write and moves the cursor
        # over whatever actually arrived. Saved after the patches so a failed
        # patch leaves no plan and the cursor simply stays put.
        # An AOI with no new events still gets a plan (zero chunks, `final`
        # set), which is what lets a patch-only run move its cursor forward.
        for aoi, chunks in plans.items():
            await save_chunk_plan(integration, aoi, chunks, final_cursors.get(aoi))
        result["details"]["chunk_plan"] = {
            aoi: [chunk["id"] for chunk in chunks] for aoi, chunks in plans.items()
        }

        # Logged here (not only returned) because the HTTP response is lost when
        # a long run outlives the caller's timeout.
        logger.info(
            f"pull_events finished for integration '{str(integration.id)}': "
            f"{result['events_extracted']} events extracted, "
            f"{result['process_events_per_aoi_action_triggered']} process_events_per_aoi action(s) triggered, "
            f"{result.get('events_updated', 0)} events updated."
        )
        return result


@activity_logger()
async def action_process_events_per_aoi(integration, action_config: ProcessEventsPerAOIConfig):
    result = {"events_processed": 0, "details": {}}
    all_responses = []
    # Cleared by any batch Gundi did not accept. Only a fully delivered chunk
    # gets its completion marker, and only marked chunks let the next
    # pull_events run move the AOI cursor past them.
    delivered = True
    # Kept aligned 1:1 so the event -> Gundi object mapping is saved against the
    # event that actually produced each response. The transformed list is both
    # sorted and filtered below, so pairing responses back against
    # `action_config.events` would cross the mappings and later patches would
    # overwrite the wrong EarthRanger event.
    state_responses = []
    state_events = []

    # An event that already carries a saved Gundi mapping was delivered by an
    # earlier sub-action. pull_events makes this same check when it splits the
    # work, but a chunk can be re-issued while the original is still in flight —
    # a missing completion marker does not prove the original died — and the
    # original may well finish first. Re-checking here stops the replacement
    # creating a second EarthRanger event. Skipping is the right outcome: the
    # replacement carries the same Skylight event the original already created.
    # This does not close a true dead heat (both chunks in flight, neither has
    # written its mappings yet); that needs a create claim and is deliberately
    # left out of this change.
    events_to_send = []
    already_delivered = 0
    for event in action_config.events:
        if await state_manager.get_state(
            str(integration.id), "pull_events", get_clean_event_id(event)
        ):
            already_delivered += 1
        else:
            events_to_send.append(event)
    if already_delivered:
        logger.info(
            f'Skipping {already_delivered} of {len(action_config.events)} event(s) already '
            f'delivered by an earlier sub-action. AOI: {action_config.aoi}.',
            extra={"integration_id": str(integration.id), "aoi": action_config.aoi}
        )

    # Filter out falsy results: transform() returns {} for events it skips
    # (e.g. an entry alert with no start point). An empty dict is truthy inside
    # a list, so it must be filtered here or it would leak into the Gundi batch.
    # Each transformed event is carried with the raw Skylight event it came from
    # so identity survives the filtering and sorting.
    transformed_pairs = sorted(
        [
            (transformed, event)
            for event in events_to_send
            if (transformed := transform(action_config.updated_config_data, event))
        ],
        key=lambda pair: pair[0].get("recorded_at") or datetime.datetime.min, reverse=True
    )
    transformed_data = [transformed for transformed, _ in transformed_pairs]

    if transformed_data:
        # Send transformed data to Sensors API V2
        try:
            for i, batch_pairs in enumerate(generate_batches(transformed_pairs, 200)):
                batch = [transformed for transformed, _ in batch_pairs]
                logger.info(f'Sending observations batch #{i}: {len(batch)} observations. AOI: {action_config.aoi}')
                response = await gundi_tools.send_events_to_gundi(
                    events=batch,
                    integration_id=integration.id
                )

                if not response:
                    # Nothing came back for a non-empty batch, so these events
                    # did not reach EarthRanger. Leave the chunk unmarked: the
                    # AOI cursor then stops short of it and the next run pulls
                    # these events again.
                    delivered = False
                    logger.warning(
                        f'Gundi returned no response for batch #{i} of {len(batch_pairs)} event(s). '
                        f'Treating this chunk as undelivered; its events are pulled again next run.',
                        extra={
                            "integration_id": str(integration.id),
                            "aoi": action_config.aoi,
                            "attention_needed": True,
                        }
                    )
                if response:
                    all_responses.extend(response)
                    result["events_processed"] += len(response)
                    # Send images as attachments (if available)
                    await process_attachments(batch, response, integration)
                    # Process events to patch
                    if len(response) == len(batch_pairs):
                        state_responses.extend(response)
                        state_events.extend(event for _, event in batch_pairs)
                    else:
                        # Without one response per event sent there is no way to
                        # tell which event each object id belongs to. Saving a
                        # guessed mapping is worse than saving none: a wrong
                        # mapping makes a later patch overwrite another event.
                        # The chunk is also not delivered: at least one event is
                        # unaccounted for, so the cursor must not pass it.
                        delivered = False
                        logger.warning(
                            f'Gundi returned {len(response)} response(s) for a batch of '
                            f'{len(batch_pairs)} event(s). Skipping the event-state mapping for '
                            f'this batch and treating the chunk as undelivered; its events are '
                            f'pulled again next run.',
                            extra={
                                "integration_id": str(integration.id),
                                "aoi": action_config.aoi,
                                "attention_needed": True,
                            }
                        )
            await save_events_state(state_responses, state_events, integration)
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
        # The per-AOI cursor is owned by action_pull_events. This only reports
        # that the chunk's events are in EarthRanger; the next pull_events run
        # decides what that means for the cursor.
        if delivered:
            await mark_chunk_delivered(integration, action_config)
        result["details"]["chunk_delivered"] = delivered
        return result

    # Nothing left to send: either nothing survived transform() (unsupported
    # types, entry alerts with no start point) or every event was already
    # delivered by an earlier sub-action. Either way the chunk is complete and
    # must not hold the cursor back.
    await mark_chunk_delivered(integration, action_config)
    result["details"]["chunk_delivered"] = True
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
            # Refresh the dedupe key's 72h TTL. The cursor is inclusive
            # (`updated >= cursor`), so the boundary event comes back on every
            # run and lands here. Without this refresh its key would expire on a
            # quiet AOI, the event would look new again, and a duplicate would be
            # created in EarthRanger.
            await state_manager.set_state(
                integration_id=str(integration.id),
                action_id="pull_events",
                state={"object_id": gundi_object_id},
                source_id=get_clean_event_id(new_event),
                expire=259200  # 72 hrs
            )
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
