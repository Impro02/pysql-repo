# MODULES
from typing import Any, Dict, List, Optional, cast

# SQLALCHEMY
from sqlalchemy import BinaryExpression
from sqlalchemy.orm import InstrumentedAttribute

# PYSQL_REPO
from pysql_repo import Operators, LoadingTechnique, RelationshipOption
from pysql_repo._utils import FilterType

# MODELS
from tests.models.database.database import Address, User


class UserRepositoryBase:
    @classmethod
    def get_filters(
        cls,
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
    ) -> FilterType:
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
                        Operators.IN: zip_codes_in,
                        Operators.NOT_IN: zip_codes_not_in,
                    },
                }
            },
        }

    @classmethod
    def get_relationship_options(
        cls,
        load_addresses: bool = False,
        load_city: bool = False,
        zip_codes_not_in: Optional[List[int]] = None,
        zip_codes_in: Optional[List[int]] = None,
    ) -> Dict[InstrumentedAttribute[Any], RelationshipOption]:
        extra_join_addresses = []
        if zip_codes_not_in:
            extra_join_addresses.append(Address.zip_code.not_in(zip_codes_not_in))
        if zip_codes_in:
            extra_join_addresses.append(Address.zip_code.in_(zip_codes_in))

        return {
            User.addresses: RelationshipOption(
                lazy=(
                    LoadingTechnique.JOINED
                    if load_addresses
                    else LoadingTechnique.NOLOAD
                ),
                added_criteria=(
                    cast(BinaryExpression[Any], extra_join_addresses)
                    if len(extra_join_addresses) > 0
                    else None
                ),
                children={
                    Address.city: RelationshipOption(
                        lazy=(
                            LoadingTechnique.JOINED
                            if load_city
                            else LoadingTechnique.NOLOAD
                        ),
                    )
                },
            ),
        }
