# MODULES
from typing import Optional

# PYDANTIC
from pydantic import BaseModel, ConfigDict, Field

# MODELS
from tests.models.schemas.city import CityRead


class AddressRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    street: str
    zip_code: int

    city: Optional[CityRead] = Field(default=None)
