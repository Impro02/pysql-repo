# PYDANTIC
from typing import List
from pydantic import BaseModel, ConfigDict, Field

# MODELS
from tests.models.schemas.address import AddressRead


class UserCreate(BaseModel):
    email: str
    hashed_password: str
    full_name: str


class UserRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    email: str
    hashed_password: str
    full_name: str
    is_active: bool

    addresses: List[AddressRead] = Field(default_factory=lambda: [])
