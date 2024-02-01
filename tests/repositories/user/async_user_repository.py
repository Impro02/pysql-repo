# MODULES
from typing import Callable, List, Optional, Tuple

# SQLALCHEMY
from sqlalchemy import Sequence
from sqlalchemy.ext.asyncio import AsyncSession

# PYSQL_REPO
from pysql_repo import Operators
from pysql_repo.asyncio import AsyncRepository

# CONTEXTLIB
from contextlib import AbstractAsyncContextManager

# MODEL
from tests.repositories.user._base import UserRepositoryBase as _UserRepositoryBase
from tests.models.database.database import User
from tests.models.schemas.user import UserCreate


class AsyncUserRepository(AsyncRepository, _UserRepositoryBase):
    def __init__(
        self,
        session_factory: Callable[..., AbstractAsyncContextManager[AsyncSession]],
    ) -> None:
        super().__init__(session_factory)

    async def get_all(
        self,
        ids_in: Optional[List[int]] = None,
        ids_not_in: Optional[List[int]] = None,
        emails_iin: Optional[List[str]] = None,
        emails_in: Optional[List[str]] = None,
        emails_not_iin: Optional[List[str]] = None,
        emails_not_in: Optional[List[str]] = None,
        email_ilike: Optional[List[str]] = None,
        email_like: Optional[List[str]] = None,
        email_not_ilike: Optional[List[str]] = None,
        email_not_like: Optional[List[str]] = None,
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
        session: Optional[AsyncSession] = None,
    ) -> Sequence[User]:
        users = await self._select_all(
            session=session,
            model=User,
            optional_filters=self.get_filters(
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
            ),
            relationship_options=self.get_relationship_options(
                load_addresses=load_addresses,
                load_city=load_city,
                zip_codes_in=zip_codes_in,
                zip_codes_not_in=zip_codes_not_in,
            ),
            order_by=order_by,
            direction=direction,
        )

        return users

    async def get_paginate(
        self,
        page: int,
        per_page: int,
        ids_in: Optional[List[int]] = None,
        ids_not_in: Optional[List[int]] = None,
        emails_iin: Optional[List[str]] = None,
        emails_in: Optional[List[str]] = None,
        emails_not_iin: Optional[List[str]] = None,
        emails_not_in: Optional[List[str]] = None,
        email_ilike: Optional[List[str]] = None,
        email_like: Optional[List[str]] = None,
        email_not_ilike: Optional[List[str]] = None,
        email_not_like: Optional[List[str]] = None,
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
        session: Optional[AsyncSession] = None,
    ) -> Tuple[Sequence[User], str]:
        users, pagination = await self._select_paginate(
            session=session,
            model=User,
            optional_filters=self.get_filters(
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
            ),
            relationship_options=self.get_relationship_options(
                load_addresses=load_addresses,
                load_city=load_city,
                zip_codes_in=zip_codes_in,
                zip_codes_not_in=zip_codes_not_in,
            ),
            order_by=order_by,
            direction=direction,
            page=page,
            per_page=per_page,
        )

        return users, pagination

    async def get_by_id(
        self,
        id: int,
        session: Optional[AsyncSession] = None,
    ) -> Optional[User]:
        user = await self._select(
            session=session,
            model=User,
            filters={
                User.id: {
                    Operators.EQUAL: id,
                },
            },
        )

        return user

    async def create(
        self,
        data: UserCreate,
        flush: bool = False,
        commit: bool = True,
        session: Optional[AsyncSession] = None,
    ) -> User:
        user = await self._add(
            session=session,
            model=User,
            values={
                User.email.key: data.email,
                User.hashed_password.key: data.hashed_password,
                User.full_name.key: data.full_name,
            },
            flush=flush,
            commit=commit,
        )

        return user

    async def create_all(
        self,
        data: List[UserCreate],
        flush: bool = False,
        commit: bool = True,
        session: Optional[AsyncSession] = None,
    ) -> Sequence[User]:
        users = await self._add_all(
            session=session,
            model=User,
            values=[
                {
                    User.email.key: item.email,
                    User.hashed_password.key: item.hashed_password,
                    User.full_name.key: item.full_name,
                }
                for item in data
            ],
            flush=flush,
            commit=commit,
        )

        return users

    async def patch_email(
        self,
        id: int,
        email: str,
        flush: bool = False,
        commit: bool = True,
        session: Optional[AsyncSession] = None,
    ) -> User:
        user = await self._update(
            session=session,
            model=User,
            values={
                User.email.key: email,
            },
            filters={
                User.id: {
                    Operators.EQUAL: id,
                },
            },
            flush=flush,
            commit=commit,
        )

        return user

    async def patch_disable(
        self,
        ids: List[int],
        flush: bool = False,
        commit: bool = True,
        session: Optional[AsyncSession] = None,
    ) -> List[User]:
        users = await self._update_all(
            session=session,
            model=User,
            values={
                User.is_active.key: False,
            },
            filters={
                User.id: {
                    Operators.IN: ids,
                },
            },
            flush=flush,
            commit=commit,
        )

        return users

    async def delete(
        self,
        id: int,
        flush: bool = False,
        commit: bool = True,
        session: Optional[AsyncSession] = None,
    ) -> bool:
        is_deleted = await self._delete(
            session=session,
            model=User,
            filters={
                User.id: {
                    Operators.EQUAL: id,
                },
            },
            flush=flush,
            commit=commit,
        )

        return is_deleted

    async def delete_all(
        self,
        ids: List[int],
        flush: bool = False,
        commit: bool = True,
        session: Optional[AsyncSession] = None,
    ) -> bool:
        is_deleted = await self._delete_all(
            session=session,
            model=User,
            filters={
                User.id: {
                    Operators.IN: ids,
                },
            },
            flush=flush,
            commit=commit,
        )

        return is_deleted
