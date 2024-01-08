# MODULES
from logging import Logger
from typing import List, Optional, Tuple

# SQLALCHEMY
from sqlalchemy.orm import Session

# SESSION_REPOSITORY
from session_repository import SessionService, with_session

# REPOSITORIES
from tests.repositories.city_repository import CityRepository

# MODELS
from tests.models.schemas.city import CityRead


class CityService(SessionService[CityRepository]):
    def __init__(
        self,
        city_repository: CityRepository,
        logger: Logger,
    ) -> None:
        super().__init__(
            repository=city_repository,
            logger=logger,
        )

    @with_session
    def get_cities(
        self,
        ids: Optional[List[int]] = None,
        names: Optional[List[int]] = None,
        name_like: Optional[str] = None,
        name: Optional[str] = None,
        order_by: Optional[List[str]] = None,
        direction: Optional[List[str]] = None,
        current_session: Optional[Session] = None,
    ) -> List[CityRead]:
        cities = self._repository.get_all(
            ids=ids,
            names=names,
            name_like=name_like,
            name=name,
            order_by=order_by,
            direction=direction,
            current_session=current_session,
        )

        return [CityRead.model_validate(item) for item in cities]

    @with_session
    def get_cities_paginate(
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
    ) -> Tuple[List[CityRead], str]:
        cities, pagination = self._repository.get_paginate(
            page=page,
            per_page=per_page,
            ids=ids,
            names=names,
            name_like=name_like,
            name=name,
            order_by=order_by,
            direction=direction,
            current_session=current_session,
        )

        cities = [CityRead.model_validate(item) for item in cities]

        return cities, pagination

    @with_session
    def get_city_by_id(
        self,
        id: int,
        current_session: Optional[Session] = None,
    ) -> CityRead:
        city = self._repository.get_by_id(
            id=id,
            current_session=current_session,
        )

        if city is None:
            return None

        return CityRead.model_validate(city)
