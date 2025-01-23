from re import sub
from .core import ActionConfiguration
from app.services.utils import GlobalUISchemaOptions
from typing import List
from pydantic import Field, validator, SecretStr


class AuthenticateConfig(ActionConfiguration):
    username: str
    password: SecretStr = Field(..., format="password")

    ui_global_options: GlobalUISchemaOptions = GlobalUISchemaOptions(
        order=[
            "username",
            "password",
        ],
    )


class PullEventsConfig(ActionConfiguration):
    aoi_ids: List[str] = Field(
        title='Area of Interest (AOI) IDs',
        description='IDs of the desired areas.',
    )
    event_types: List[str] = Field(
        title='Event Types to Fetch',
        description='The list of EventTypes the integration will use.',
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
