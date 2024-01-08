# MODULES
from logging import Logger
from typing import List, Optional, Tuple

# SQLALCHEMY
from sqlalchemy.orm import Session

# SESSION_REPOSITORY
from session_repository.decorators import with_session
from session_repository.models.service import SessionService

# REPOSITORIES
from tests.repositories.user_repositoy import UserRepository

# MODELS
from tests.models.schemas.user import UserRead


class UserService(SessionService[UserRepository]):
    def __init__(
        self,
        user_repository: UserRepository,
        logger: Logger,
    ) -> None:
        super().__init__(
            repository=user_repository,
            logger=logger,
        )

    @with_session()
    def get_users(
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
        is_active: Optional[bool] = None,
        zip_codes_any: Optional[List[str]] = None,
        load_addresses: bool = True,
        load_city: bool = True,
        additional_zip_code_sup: Optional[int] = None,
        order_by: Optional[List[str]] = None,
        direction: Optional[List[str]] = None,
        session: Optional[Session] = None,
    ) -> List[UserRead]:
        users = self._repository.get_all(
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
            is_active_equal=is_active,
            zip_codes_any=zip_codes_any,
            load_addresses=load_addresses,
            load_city=load_city,
            additional_zip_code_sup=additional_zip_code_sup,
            order_by=order_by,
            direction=direction,
            session=session,
        )

        return [UserRead.model_validate(item) for item in users]

    @with_session()
    def get_users_paginate(
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
        is_active: Optional[bool] = None,
        zip_codes_any: Optional[List[str]] = None,
        load_addresses: bool = True,
        load_city: bool = True,
        additional_zip_code_sup: Optional[int] = None,
        order_by: Optional[List[str]] = None,
        direction: Optional[List[str]] = None,
        session: Optional[Session] = None,
    ) -> Tuple[List[UserRead], str]:
        users, pagination = self._repository.get_paginate(
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
            is_active_equal=is_active,
            zip_codes_any=zip_codes_any,
            load_addresses=load_addresses,
            load_city=load_city,
            additional_zip_code_sup=additional_zip_code_sup,
            order_by=order_by,
            direction=direction,
            session=session,
        )

        users = [UserRead.model_validate(item) for item in users]

        return users, pagination

    @with_session()
    def get_user_by_id(
        self,
        id: int,
        session: Optional[Session] = None,
    ) -> UserRead:
        user = self._repository.get_by_id(
            id=id,
            session=session,
        )

        if user is None:
            return None

        return UserRead.model_validate(user)