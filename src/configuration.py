import logging
from pydantic import BaseModel, Field, ValidationError
from keboola.component.exceptions import UserException
from enum import Enum


class Units(str, Enum):
    metric = "metric"
    imperial = "imperial"


class Configuration(BaseModel):
    debug: bool = False
    api_token: str = Field(alias="#api_token")
    location_column: str = Field(default=None)
    units: Units = Units.metric

    def __init__(self, **data):
        try:
            super().__init__(**data)
        except ValidationError as e:
            error_messages = [f"{err['loc'][0]}: {err['msg']}" for err in e.errors()]
            raise UserException(f"Validation Error: {', '.join(error_messages)}")

        if self.debug:
            logging.debug("Component will run in Debug mode")
