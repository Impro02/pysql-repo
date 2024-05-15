# MODULES
from logging import Logger
from typing import List, Optional, Tuple

# SQLALCHEMY
from sqlalchemy.orm import Session

# PYSQL_REPO
from pysql_repo import Service

# REPOSITORIES
from tests.repositories.city_repository import CityRepository

# MODELS
from tests.models.schemas.city import CityRead


class CityService(Service[CityRepository]):
    def __init__(
        self,
        city_repository: CityRepository,
        logger: Logger,
    ) -> None:
        super().__init__(
            repository=city_repository,
        )
        self._logger = logger

    def get_cities(
        self,
        __session__: Session,
        /,
        ids: Optional[List[int]] = None,
        names: Optional[List[str]] = None,
        name_like: Optional[str] = None,
        name: Optional[str] = None,
        order_by: Optional[List[str]] = None,
        direction: Optional[List[str]] = None,
    ) -> List[CityRead]:
        cities = self._repository.get_all(
            __session__,
            ids=ids,
            names=names,
            name_like=name_like,
            name=name,
            order_by=order_by,
            direction=direction,
        )

        return [CityRead.model_validate(item) for item in cities]

    def get_cities_paginate(
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
    ) -> Tuple[List[CityRead], str]:
        cities, pagination = self._repository.get_paginate(
            __session__,
            page=page,
            per_page=per_page,
            ids=ids,
            names=names,
            name_like=name_like,
            name=name,
            order_by=order_by,
            direction=direction,
        )

        return [CityRead.model_validate(item) for item in cities], pagination

    def get_city_by_id(
        self,
        __session__: Session,
        /,
        id: int,
    ) -> Optional[CityRead]:
        city = self._repository.get_by_id(
            __session__,
            id=id,
        )

        if city is None:
            return None

        return CityRead.model_validate(city)
