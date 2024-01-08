# MODULES
from typing import Any, Callable, Dict, List, Optional, Tuple

# SQLALCHEMY
from sqlalchemy import Column
from sqlalchemy.orm import Session

# SESSION_REPOSITORY
from session_repository import Operators, SessionRepository

# CONTEXTLIB
from contextlib import AbstractContextManager

# MODEL
from tests.models.database.database import City


class CityRepository(SessionRepository):
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
    ) -> Dict[Column, Any]:
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
        ids: Optional[List[int]] = None,
        names: Optional[List[int]] = None,
        name_like: Optional[str] = None,
        name: Optional[str] = None,
        order_by: Optional[List[str]] = None,
        direction: Optional[List[str]] = None,
        current_session: Optional[Session] = None,
    ) -> List[City]:
        cities = self._select_all(
            session=current_session,
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
        page: int,
        per_page: int,
        ids: Optional[List[int]] = None,
        names: Optional[List[int]] = None,
        name_like: Optional[str] = None,
        name: Optional[str] = None,
        order_by: Optional[List[str]] = None,
        direction: Optional[List[str]] = None,
        current_session: Optional[Session] = None,
    ) -> Tuple[List[City], str]:
        cities, pagination = self._select_paginate(
            session=current_session,
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
        id: int,
        current_session: Optional[Session] = None,
    ) -> Optional[City]:
        city = self._select(
            session=current_session,
            model=City,
            filters={
                City.id: {
                    Operators.EQUAL: id,
                },
            },
        )

        return city
