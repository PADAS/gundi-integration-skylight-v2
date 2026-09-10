from enum import Enum
from re import sub
from app.actions.core import (
    AuthActionConfiguration,
    ExecutableActionMixin,
    InternalActionConfiguration,
    PullActionConfiguration,
    ReferenceActionConfiguration,
)
from app.services.utils import GlobalUISchemaOptions
from typing import List, Optional
from pydantic import BaseModel, Field, validator, SecretStr


# Labels shown in the portal's event-type picker. The format_string_case
# validator on PullEventsConfig.event_types snake_cases these labels (e.g.
# "Dark Rendezvous" -> "dark_rendezvous"), and the result MUST match a key in
# DEFAULT_EVENT_MAPPING (client.py). "Dark Activity" is intentionally omitted
# here: it is deprecated and no longer offered.
class SkylightEventType(str, Enum):
    dark_rendezvous = "Dark Rendezvous"
    vessel_detection = "Vessel Detection"
    fishing = "Fishing"
    speed_range = "Speed Range"
    standard_rendezvous = "Standard Rendezvous"
    marine_entry = "Marine Entry"


class AuthenticateConfig(AuthActionConfiguration, ExecutableActionMixin):
    username: str
    password: SecretStr = Field(..., format="password")

    ui_global_options: GlobalUISchemaOptions = GlobalUISchemaOptions(
        order=[
            "username",
            "password",
        ],
    )


# --- Reference data (portal live dropdowns) ---------------------------------
# Response contract for reference actions, as consumed by the Gundi portal:
# {options: [{value, label?, description?, group?}], cache_ttl_seconds, truncated}.

class ReferenceOption(BaseModel):
    value: str
    label: Optional[str] = None        # portal defaults label to value
    description: Optional[str] = None  # tooltip / help text
    group: Optional[str] = None        # optional grouping for long lists


class ReferenceDataResponse(BaseModel):
    options: List[ReferenceOption]
    cache_ttl_seconds: int = 300       # portal-side cache hint
    truncated: bool = False            # true if the list was capped


class ListAOIsQuery(ReferenceActionConfiguration):
    """Reference action: the AOIs visible to the integration's Skylight account.
    Takes no parameters; the portal calls it when the AOI dropdown opens."""


def _reference(action: str, params: Optional[dict] = None) -> dict:
    """Build a `gundi:reference` ui_schema annotation. Deliberately does NOT
    set ui:widget: portals without reference support keep rendering the plain
    text field, and allow_free_text keeps hand-typed AOI ids valid."""
    return {"action": action, "target": "self", "params": params or {}, "allow_free_text": True}


class ProcessEventsPerAOIConfig(InternalActionConfiguration):
    integration_id: str
    aoi: str
    events: List[dict]
    updated_config_data: List[dict]
    # Identifies this batch inside the chunk plan pull_events recorded for the
    # AOI. The sub-action writes a completion marker under it once its events
    # have reached EarthRanger, and the next pull_events run uses those markers
    # to decide how far the AOI cursor may move. Optional so a command already
    # queued by an older revision still deserializes.
    chunk_id: Optional[str] = None


class PullEventsConfig(PullActionConfiguration):
    aoi_ids: List[str] = Field(
        title='Area of Interest (AOI) IDs',
        description='Skylight AOIs to pull events for. Pick from the list or paste an AOI id.',
    )
    event_types: List[SkylightEventType] = Field(
        title='Event Types to Fetch',
        description='The list of EventTypes the integration will use.',
        uniqueItems=True,
    )
    pageSize: int = Field(
        1000,
        ge=1,
        title='Number of records per Skylight call',
        description='Number of records the integration will fetch per API call.',
    )
    initial_data_window_days: int = Field(
        30,
        title='Days to fetch data from',
        description='Number of days the integration will get data from if no startTime set.',
    )

    @classmethod
    def schema(cls, **kwargs):
        # Inline the SkylightEventType enum into the array items. Pydantic emits
        # `items: {"$ref": "#/definitions/..."}`, but the portal does not resolve
        # a $ref inside array items, so the field falls back to a plain grey
        # multi-select instead of the labelled checkbox list.
        json_schema = super().schema(**kwargs)
        event_types = (json_schema.get("properties") or {}).get("event_types") or {}
        ref = (event_types.get("items") or {}).get("$ref")
        if ref:
            name = ref.rsplit("/", 1)[-1]
            definition = (json_schema.get("definitions") or {}).get(name) or {}
            if definition.get("enum"):
                event_types["items"] = {"type": "string", "enum": list(definition["enum"])}
                json_schema["definitions"] = {
                    k: v for k, v in json_schema.get("definitions", {}).items() if k != name
                }
        return json_schema

    @classmethod
    def ui_schema(cls):
        # aoi_ids stays a plain list of Skylight AOI ids (existing integrations
        # are untouched); the annotation only tells the portal it can offer a
        # live dropdown fed by the list_aois reference action.
        ui = super().ui_schema()
        ui.setdefault("aoi_ids", {}).setdefault("items", {})["gundi:reference"] = _reference("list_aois")
        # Without this the portal renders the event-type list as a multi-select
        # box; the operator-facing form has always shown checkboxes.
        ui.setdefault("event_types", {})["ui:widget"] = "checkboxes"
        return ui

    @validator('event_types')
    def format_string_case(cls, v):
        # The portal stores the SkylightEventType display labels (e.g. "Dark Rendezvous").
        # This validator converts them to snake_case keys (e.g. "dark_rendezvous") that
        # match DEFAULT_EVENT_MAPPING in client.py.
        #
        # This behaviour predates this PR: the portal JSON schema was manually edited to
        # use these same display labels as enum values long before SkylightEventType was
        # added to code. The enum simply formalises what was already stored in production.
        # Verified: all 6 display labels round-trip correctly through this validator and
        # resolve to a valid DEFAULT_EVENT_MAPPING entry.
        format_string_case_list = [
            '_'.join(
                sub('([A-Z][a-z]+)', r' \1',
                    sub('([A-Z]+)', r' \1',
                        val.replace('-', ' '))).split()
            ).lower()
            for val in v
        ]
        return format_string_case_list
