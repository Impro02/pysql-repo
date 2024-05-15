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
from tests.models.database.database import City


class CityRepository(Repository):
    def __init__(
        self,
        session_factory: Callable[..., AbstractContextManager[Session]],
    ) -> None:
        super().__init__(session_factory)

    @classmethod
    def __get_filters(
        cls,
        ids: Optional[List[int]] = None,
        names: Optional[List[str]] = None,
        name_like: Optional[str] = None,
        name: Optional[str] = None,
    ) -> FilterType:
        return {
            City.id: {
                Operators.IN: ids,
            },
            City.name: {
                Operators.IN: names,
                Operators.ILIKE: name_like,
                Operators.IEQUAL: name,
            },
        }

    def get_all(
        self,
        __session__: Session,
        /,
        ids: Optional[List[int]] = None,
        names: Optional[List[str]] = None,
        name_like: Optional[str] = None,
        name: Optional[str] = None,
        order_by: Optional[List[str]] = None,
        direction: Optional[List[str]] = None,
    ) -> Sequence[City]:
        cities = self._select_all(
            __session__,
            model=City,
            optional_filters=self.__get_filters(
                ids=ids,
                names=names,
                name_like=name_like,
                name=name,
            ),
            order_by=order_by,
            direction=direction,
        )

        return cities

    def get_paginate(
        self,
        __session__: Session,
        /,
        page: int,
        per_page: int,
        ids: Optional[List[int]] = None,
        names: Optional[List[str]] = None,
        name_like: Optional[str] = None,
        name: Optional[str] = None,
        order_by: Optional[List[str]] = None,
        direction: Optional[List[str]] = None,
    ) -> Tuple[Sequence[City], str]:
        cities, pagination = self._select_paginate(
            __session__,
            model=City,
            optional_filters=self.__get_filters(
                ids=ids,
                names=names,
                name_like=name_like,
                name=name,
            ),
            order_by=order_by,
            direction=direction,
            page=page,
            per_page=per_page,
        )

        return cities, pagination

    def get_by_id(
        self,
        __session__: Session,
        /,
        id: int,
    ) -> Optional[City]:
        city = self._select(
            __session__,
            model=City,
            filters={
                City.id: {
                    Operators.EQUAL: id,
                },
            },
        )

        return city
