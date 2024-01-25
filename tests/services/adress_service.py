# MODULES
from logging import Logger
from typing import List, Optional, Tuple

# SQLALCHEMY
from sqlalchemy.orm import Session

# PYSQL_REPO
from pysql_repo import SessionService, with_session

# REPOSITORIES
from tests.repositories.address_repository import AddressRepository

# MODELS
from tests.models.schemas.adress import AddressRead


class AddressService(SessionService[AddressRepository]):
    def __init__(
        self,
        address_repository: AddressRepository,
        logger: Logger,
    ) -> None:
        super().__init__(
            repository=address_repository,
            logger=logger,
        )

    @with_session
    def get_addresses(
        self,
        ids: Optional[List[int]] = None,
        streets: Optional[List[str]] = None,
        street_like: Optional[str] = None,
        street: Optional[str] = None,
        zip_codes: Optional[List[str]] = None,
        order_by: Optional[List[str]] = None,
        direction: Optional[List[str]] = None,
        current_session: Optional[Session] = None,
    ) -> List[AddressRead]:
        cities = self._repository.get_all(
            ids=ids,
            streets=streets,
            street_like=street_like,
            street=street,
            zip_codes=zip_codes,
            order_by=order_by,
            direction=direction,
            current_session=current_session,
        )

        return [AddressRead.model_validate(item) for item in cities]

    @with_session
    def get_addresses_paginate(
        self,
        page: int,
        per_page: int,
        ids: Optional[List[int]] = None,
        streets: Optional[List[str]] = None,
        street_like: Optional[str] = None,
        street: Optional[str] = None,
        zip_codes: Optional[List[str]] = None,
        order_by: Optional[List[str]] = None,
        direction: Optional[List[str]] = None,
        current_session: Optional[Session] = None,
    ) -> Tuple[List[AddressRead], str]:
        adresses, pagination = self._repository.get_paginate(
            page=page,
            per_page=per_page,
            ids=ids,
            streets=streets,
            street_like=street_like,
            street=street,
            zip_codes=zip_codes,
            order_by=order_by,
            direction=direction,
            current_session=current_session,
        )

        adresses = [AddressRead.model_validate(item) for item in adresses]

        return adresses, pagination

    @with_session
    def get_address_by_id(
        self,
        id: int,
        current_session: Optional[Session] = None,
    ) -> AddressRead:
        address = self._repository.get_by_id(
            id=id,
            current_session=current_session,
        )

        if address is None:
            return None

        return AddressRead.model_validate(address)
