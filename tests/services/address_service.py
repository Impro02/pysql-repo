# MODULES
from logging import Logger
from typing import List, Optional, Tuple

# SQLALCHEMY
from sqlalchemy.orm import Session

# PYSQL_REPO
from pysql_repo import Service

# REPOSITORIES
from tests.repositories.address_repository import AddressRepository

# MODELS
from tests.models.schemas.address import AddressRead


class AddressService(Service[AddressRepository]):
    def __init__(
        self,
        address_repository: AddressRepository,
        logger: Logger,
    ) -> None:
        super().__init__(
            repository=address_repository,
        )
        self._logger = logger

    def get_addresses(
        self,
        __session__: Session,
        /,
        ids: Optional[List[int]] = None,
        streets: Optional[List[str]] = None,
        street_like: Optional[str] = None,
        street: Optional[str] = None,
        zip_codes: Optional[List[str]] = None,
        order_by: Optional[List[str]] = None,
        direction: Optional[List[str]] = None,
    ) -> List[AddressRead]:
        addresses = self._repository.get_all(
            __session__,
            ids=ids,
            streets=streets,
            street_like=street_like,
            street=street,
            zip_codes=zip_codes,
            order_by=order_by,
            direction=direction,
        )

        return [AddressRead.model_validate(item) for item in addresses]

    def get_addresses_paginate(
        self,
        __session__: Session,
        /,
        page: int,
        per_page: int,
        ids: Optional[List[int]] = None,
        streets: Optional[List[str]] = None,
        street_like: Optional[str] = None,
        street: Optional[str] = None,
        zip_codes: Optional[List[str]] = None,
        order_by: Optional[List[str]] = None,
        direction: Optional[List[str]] = None,
    ) -> Tuple[List[AddressRead], str]:
        addresses, pagination = self._repository.get_paginate(
            __session__,
            page=page,
            per_page=per_page,
            ids=ids,
            streets=streets,
            street_like=street_like,
            street=street,
            zip_codes=zip_codes,
            order_by=order_by,
            direction=direction,
        )

        return [AddressRead.model_validate(item) for item in addresses], pagination

    def get_address_by_id(
        self,
        __session__: Session,
        /,
        id: int,
    ) -> Optional[AddressRead]:
        address = self._repository.get_by_id(
            __session__,
            id=id,
        )

        if address is None:
            return None

        return AddressRead.model_validate(address)
