# MODULES
from typing import Any, Callable, Dict, List, Optional, Tuple

# SQLALCHEMY
from sqlalchemy import Column
from sqlalchemy.orm import Session

# SESSION_REPOSITORY
from session_repository import Operators, SessionRepository

# CONTEXTLIB
from contextlib import AbstractContextManager
from session_repository.enum import LoadingTechnique
from session_repository.utils import RelationshipOption, SpecificJoin

# MODEL
from tests.models.database.database import Address, User


class UserRepository(SessionRepository):
    def __init__(
        self,
        session_factory: Callable[..., AbstractContextManager[Session]],
    ) -> None:
        super().__init__(session_factory)

    @classmethod
    def __get_filters(
        cls,
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
        is_active_equal: Optional[bool] = None,
        zip_codes_any: Optional[List[str]] = None,
    ) -> Dict[Column, Any]:
        return {
            User.id: {
                Operators.IN: ids_in,
                Operators.NOT_IN: ids_not_in,
            },
            User.email: {
                Operators.IIN: emails_iin,
                Operators.IN: emails_in,
                Operators.NOT_IIN: emails_not_iin,
                Operators.NOT_IN: emails_not_in,
                Operators.ILIKE: email_ilike,
                Operators.LIKE: email_like,
                Operators.NOT_ILIKE: email_not_ilike,
                Operators.NOT_LIKE: email_not_like,
                Operators.EQUAL: email_equal,
                Operators.IEQUAL: email_iequal,
                Operators.DIFFERENT: email_different,
                Operators.IDIFFERENT: email_idifferent,
            },
            User.is_active: {
                Operators.EQUAL: is_active_equal,
            },
            User.addresses: {
                Operators.ANY: {
                    Address.zip_code: {
                        Operators.IN: zip_codes_any,
                    },
                }
            },
        }

    @classmethod
    def __get_relationship_options(
        cls,
        load_addresses: bool = False,
        load_city: bool = False,
        additional_zip_code_sup: Optional[int] = None,
    ):
        return {
            User.addresses: RelationshipOption(
                lazy=LoadingTechnique.JOINED
                if load_addresses
                else LoadingTechnique.NOLOAD,
                specific_join=SpecificJoin(
                    extra_conditions=(Address.zip_code > additional_zip_code_sup),
                )
                if additional_zip_code_sup
                else None,
                children={
                    Address.city: RelationshipOption(
                        lazy=LoadingTechnique.JOINED
                        if load_city
                        else LoadingTechnique.NOLOAD,
                    )
                },
            ),
        }

    def get_all(
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
        is_active_equal: Optional[bool] = None,
        zip_codes_any: Optional[List[int]] = None,
        load_addresses: bool = True,
        load_city: bool = True,
        additional_zip_code_sup: Optional[int] = None,
        order_by: Optional[List[str]] = None,
        direction: Optional[List[str]] = None,
        session: Optional[Session] = None,
    ) -> List[User]:
        users = self._select_all(
            session=session,
            model=User,
            optional_filters=self.__get_filters(
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
                is_active_equal=is_active_equal,
                zip_codes_any=zip_codes_any,
            ),
            relationship_options=self.__get_relationship_options(
                load_addresses=load_addresses,
                load_city=load_city,
                additional_zip_code_sup=additional_zip_code_sup,
            ),
            order_by=order_by,
            direction=direction,
        )

        return users

    def get_paginate(
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
        is_active_equal: Optional[bool] = None,
        zip_codes_any: Optional[List[int]] = None,
        load_addresses: bool = True,
        load_city: bool = True,
        additional_zip_code_sup: Optional[int] = None,
        order_by: Optional[List[str]] = None,
        direction: Optional[List[str]] = None,
        session: Optional[Session] = None,
    ) -> Tuple[List[User], str]:
        users, pagination = self._select_paginate(
            session=session,
            model=User,
            optional_filters=self.__get_filters(
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
                is_active_equal=is_active_equal,
                zip_codes_any=zip_codes_any,
            ),
            relationship_options=self.__get_relationship_options(
                load_addresses=load_addresses,
                load_city=load_city,
                additional_zip_code_sup=additional_zip_code_sup,
            ),
            order_by=order_by,
            direction=direction,
            page=page,
            per_page=per_page,
        )

        return users, pagination

    def get_by_id(
        self,
        id: int,
        session: Optional[Session] = None,
    ) -> Optional[User]:
        user = self._select(
            session=session,
            model=User,
            filters={
                User.id: {
                    Operators.EQUAL: id,
                },
            },
        )

        return user
