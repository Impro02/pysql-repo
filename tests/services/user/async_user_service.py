# MODULES
from logging import Logger
from typing import List, Optional, Tuple

# SQLALCHEMY
from sqlalchemy.ext.asyncio import AsyncSession

# PYSQL_REPO
from pysql_repo.asyncio import AsyncService

# REPOSITORIES
from tests.repositories.user.async_user_repository import AsyncUserRepository

# MODELS
from tests.models.schemas.user import UserCreate, UserRead


class AsyncUserService(AsyncService[AsyncUserRepository]):
    def __init__(
        self,
        user_repository: AsyncUserRepository,
        logger: Logger,
    ) -> None:
        super().__init__(
            repository=user_repository,
        )
        self._logger = logger

    async def get_users(
        self,
        __session__: AsyncSession,
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
        users = await self._repository.get_all(
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

    async def get_users_paginate(
        self,
        __session__: AsyncSession,
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
        users, pagination = await self._repository.get_paginate(
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

    async def get_user_by_id(
        self,
        __session__: AsyncSession,
        /,
        id: int,
    ) -> Optional[UserRead]:
        user = await self._repository.get_by_id(
            __session__,
            id=id,
        )

        if user is None:
            return None

        return UserRead.model_validate(user)

    async def create_user(
        self,
        __session__: AsyncSession,
        /,
        data: UserCreate,
    ) -> UserRead:
        user = await self._repository.create(
            __session__,
            data=data,
            flush=True,
        )

        return UserRead.model_validate(user)

    async def create_users(
        self,
        __session__: AsyncSession,
        /,
        data: List[UserCreate],
    ) -> List[UserRead]:
        users = await self._repository.create_all(
            __session__,
            data=data,
            flush=True,
        )

        return [UserRead.model_validate(user) for user in users]

    async def patch_email(
        self,
        __session__: AsyncSession,
        /,
        id: int,
        email: str,
    ) -> UserRead:
        user = await self._repository.patch_email(
            __session__,
            id=id,
            email=email,
            flush=True,
        )

        return UserRead.model_validate(user)

    async def patch_disable(
        self,
        __session__: AsyncSession,
        /,
        ids: List[int],
    ) -> List[UserRead]:
        users = await self._repository.patch_disable(
            __session__,
            ids=ids,
            flush=True,
        )

        return [UserRead.model_validate(user) for user in users]

    async def delete_by_id(
        self,
        __session__: AsyncSession,
        /,
        id: int,
    ) -> bool:
        return await self._repository.delete(
            __session__,
            id=id,
            flush=True,
        )

    async def delete_by_ids(
        self,
        __session__: AsyncSession,
        /,
        ids: List[int],
    ) -> bool:
        return await self._repository.delete_all(
            __session__,
            ids=ids,
            flush=True,
        )
