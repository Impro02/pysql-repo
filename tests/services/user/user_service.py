# MODULES
from logging import Logger
from typing import List, Optional, Tuple

# SQLALCHEMY
from sqlalchemy.orm import Session

# PYSQL_REPO
from pysql_repo import Service

# REPOSITORIES
from tests.repositories.user.user_repository import UserRepository

# MODELS
from tests.models.schemas.user import UserCreate, UserRead


class UserService(Service[UserRepository]):
    def __init__(
        self,
        user_repository: UserRepository,
        logger: Logger,
    ) -> None:
        super().__init__(
            repository=user_repository,
        )
        self._logger = logger

    def get_users(
        self,
        __session__: Session,
        /,
        ids_in: Optional[List[int]] = None,
        ids_not_in: Optional[List[int]] = None,
        emails_iin: Optional[List[str]] = None,
        emails_in: Optional[List[str]] = None,
        emails_not_iin: Optional[List[str]] = None,
        emails_not_in: Optional[List[str]] = None,
        email_ilike: Optional[str] = None,
        email_like: Optional[str] = None,
        email_not_ilike: Optional[str] = None,
        email_not_like: Optional[str] = None,
        email_equal: Optional[str] = None,
        email_iequal: Optional[str] = None,
        email_different: Optional[str] = None,
        email_idifferent: Optional[str] = None,
        zip_codes_in: Optional[List[int]] = None,
        zip_codes_not_in: Optional[List[int]] = None,
        is_active_equal: Optional[bool] = None,
        load_addresses: bool = True,
        load_city: bool = True,
        order_by: Optional[List[str]] = None,
        direction: Optional[List[str]] = None,
    ) -> List[UserRead]:
        users = self._repository.get_all(
            __session__,
            ids_in=ids_in,
            ids_not_in=ids_not_in,
            emails_iin=emails_iin,
            emails_in=emails_in,
            emails_not_iin=emails_not_iin,
            emails_not_in=emails_not_in,
            email_ilike=email_ilike,
            email_like=email_like,
            email_not_ilike=email_not_ilike,
            email_not_like=email_not_like,
            email_equal=email_equal,
            email_iequal=email_iequal,
            email_different=email_different,
            email_idifferent=email_idifferent,
            zip_codes_in=zip_codes_in,
            zip_codes_not_in=zip_codes_not_in,
            is_active_equal=is_active_equal,
            load_addresses=load_addresses,
            load_city=load_city,
            order_by=order_by,
            direction=direction,
        )

        return [UserRead.model_validate(item) for item in users]

    def get_users_paginate(
        self,
        __session__: Session,
        /,
        page: int,
        per_page: int,
        ids_in: Optional[List[int]] = None,
        ids_not_in: Optional[List[int]] = None,
        emails_iin: Optional[List[str]] = None,
        emails_in: Optional[List[str]] = None,
        emails_not_iin: Optional[List[str]] = None,
        emails_not_in: Optional[List[str]] = None,
        email_ilike: Optional[str] = None,
        email_like: Optional[str] = None,
        email_not_ilike: Optional[str] = None,
        email_not_like: Optional[str] = None,
        email_equal: Optional[str] = None,
        email_iequal: Optional[str] = None,
        email_different: Optional[str] = None,
        email_idifferent: Optional[str] = None,
        zip_codes_in: Optional[List[int]] = None,
        zip_codes_not_in: Optional[List[int]] = None,
        is_active_equal: Optional[bool] = None,
        load_addresses: bool = True,
        load_city: bool = True,
        order_by: Optional[List[str]] = None,
        direction: Optional[List[str]] = None,
    ) -> Tuple[List[UserRead], str]:
        users, pagination = self._repository.get_paginate(
            __session__,
            page=page,
            per_page=per_page,
            ids_in=ids_in,
            ids_not_in=ids_not_in,
            emails_iin=emails_iin,
            emails_in=emails_in,
            emails_not_iin=emails_not_iin,
            emails_not_in=emails_not_in,
            email_ilike=email_ilike,
            email_like=email_like,
            email_not_ilike=email_not_ilike,
            email_not_like=email_not_like,
            email_equal=email_equal,
            email_iequal=email_iequal,
            email_different=email_different,
            email_idifferent=email_idifferent,
            zip_codes_in=zip_codes_in,
            zip_codes_not_in=zip_codes_not_in,
            is_active_equal=is_active_equal,
            load_addresses=load_addresses,
            load_city=load_city,
            order_by=order_by,
            direction=direction,
        )

        return [UserRead.model_validate(item) for item in users], pagination

    def get_user_by_id(
        self,
        __session__: Session,
        /,
        id: int,
    ) -> Optional[UserRead]:
        user = self._repository.get_by_id(
            __session__,
            id=id,
        )

        if user is None:
            return None

        return UserRead.model_validate(user)

    def create_user(
        self,
        __session__: Session,
        /,
        data: UserCreate,
    ) -> UserRead:
        user = self._repository.create(
            __session__,
            data=data,
            flush=True,
        )

        return UserRead.model_validate(user)

    def create_users(
        self,
        __session__: Session,
        /,
        data: List[UserCreate],
    ) -> List[UserRead]:
        users = self._repository.create_all(
            __session__,
            data=data,
            flush=True,
        )

        return [UserRead.model_validate(user) for user in users]

    def patch_email(
        self,
        __session__: Session,
        /,
        id: int,
        email: str,
    ) -> UserRead:
        user = self._repository.patch_email(
            __session__,
            id=id,
            email=email,
            flush=True,
        )

        return UserRead.model_validate(user)

    def bulk_patch_email(
        self,
        __session__: Session,
        /,
        data: List[Tuple[int, str]],
    ) -> List[UserRead]:
        users = self._repository.bulk_patch_email(
            __session__,
            data=data,
            flush=True,
        )

        return [UserRead.model_validate(user) for user in users]

    def patch_disable(
        self,
        __session__: Session,
        /,
        ids: List[int],
    ) -> List[UserRead]:
        users = self._repository.patch_disable(
            __session__,
            ids=ids,
            flush=True,
        )

        return [UserRead.model_validate(user) for user in users]

    def delete_by_id(
        self,
        __session__: Session,
        /,
        id: int,
    ) -> bool:
        return self._repository.delete(
            __session__,
            id=id,
            flush=True,
        )

    def delete_by_ids(
        self,
        __session__: Session,
        /,
        ids: List[int],
    ) -> bool:
        return self._repository.delete_all(
            __session__,
            ids=ids,
            flush=True,
        )
