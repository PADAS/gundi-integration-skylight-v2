from enum import Enum
from re import sub
from app.actions.core import AuthActionConfiguration, PullActionConfiguration, ExecutableActionMixin, InternalActionConfiguration
from app.services.utils import GlobalUISchemaOptions
from typing import List
from pydantic import Field, validator, SecretStr


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


class ProcessEventsPerAOIConfig(InternalActionConfiguration):
    integration_id: str
    aoi: str
    events: List[dict]
    updated_config_data: List[dict]


class PullEventsConfig(PullActionConfiguration):
    aoi_ids: List[str] = Field(
        title='Area of Interest (AOI) IDs',
        description='IDs of the desired areas.',
    )
    event_types: List[SkylightEventType] = Field(
        title='Event Types to Fetch',
        description='The list of EventTypes the integration will use.',
        uniqueItems=True,
    )
    pageSize: int = Field(
        1000,
        title='Number of records per Skylight call',
        description='Number of records the integration will fetch per API call.',
    )
    initial_data_window_days: int = Field(
        30,
        title='Days to fetch data from',
        description='Number of days the integration will get data from if no startTime set.',
    )

    @validator('event_types')
    def format_string_case(cls, v):
        format_string_case_list = [
            '_'.join(
                sub('([A-Z][a-z]+)', r' \1',
                    sub('([A-Z]+)', r' \1',
                        val.replace('-', ' '))).split()
            ).lower()
            for val in v
        ]
        return format_string_case_list
