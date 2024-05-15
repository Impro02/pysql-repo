# MODULES
from typing import Callable, List, Optional, Sequence, Tuple

# SQLALCHEMY
from sqlalchemy.orm import Session

# PYSQL_REPO
from pysql_repo import Operators, Repository
from pysql_repo._utils import FilterType

# CONTEXTLIB
from contextlib import AbstractContextManager

# MODEL
from tests.models.database.database import Address, City


class AddressRepository(Repository):
    def __init__(
        self,
        session_factory: Callable[..., AbstractContextManager[Session]],
    ) -> None:
        super().__init__(session_factory)

    @classmethod
    def __get_filters(
        cls,
        ids: Optional[List[int]] = None,
        streets: Optional[List[str]] = None,
        street_like: Optional[str] = None,
        street: Optional[str] = None,
        zip_codes: Optional[List[str]] = None,
        city: Optional[str] = None,
    ) -> FilterType:
        return {
            Address.id: {
                Operators.IN: ids,
            },
            Address.street: {
                Operators.IIN: streets,
                Operators.ILIKE: street_like,
                Operators.IEQUAL: street,
            },
            Address.zip_code: {
                Operators.IN: zip_codes,
            },
            Address.city: {
                Operators.HAS: {
                    City.name: {
                        Operators.IEQUAL: city,
                    }
                },
            },
        }

    def get_all(
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
    ) -> Sequence[Address]:
        addresses = self._select_all(
            __session__,
            model=Address,
            optional_filters=self.__get_filters(
                ids=ids,
                streets=streets,
                street_like=street_like,
                street=street,
                zip_codes=zip_codes,
            ),
            order_by=order_by,
            direction=direction,
        )

        return addresses

    def get_paginate(
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
    ) -> Tuple[Sequence[Address], str]:
        addresses, pagination = self._select_paginate(
            __session__,
            model=Address,
            optional_filters=self.__get_filters(
                ids=ids,
                streets=streets,
                street_like=street_like,
                street=street,
                zip_codes=zip_codes,
            ),
            order_by=order_by,
            direction=direction,
            page=page,
            per_page=per_page,
        )

        return addresses, pagination

    def get_by_id(
        self,
        __session__: Session,
        /,
        id: int,
    ) -> Optional[Address]:
        address = self._select(
            __session__,
            model=Address,
            filters={
                Address.id: {
                    Operators.EQUAL: id,
                },
            },
        )

        return address
