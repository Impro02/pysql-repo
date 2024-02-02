# MODULES
from ast import Delete
from dataclasses import dataclass, field
import json
from typing import (
    Any,
    Dict,
    Iterable,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
)

# SQLALCHEMY
from sqlalchemy import (
    ColumnExpressionArgument,
    Select,
    Update,
    and_,
    asc,
    delete,
    desc,
    insert,
    select,
    distinct,
    tuple_,
    func,
    update,
)
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import (
    Session,
    noload,
    lazyload,
    joinedload,
    subqueryload,
    selectinload,
    raiseload,
    contains_eager,
)
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy.sql.dml import ReturningDelete, ReturningInsert, ReturningUpdate
from sqlalchemy.sql.elements import Null, BinaryExpression

# Enum
from pysql_repo.constants.enum import LoadingTechnique, Operators

_FilterType = Dict[Union[InstrumentedAttribute, Tuple[InstrumentedAttribute]], Any]

_T = TypeVar("_T", bound=declarative_base())


@dataclass
class RelationshipOption:
    lazy: LoadingTechnique
    added_criteria: Optional[BinaryExpression] = field(default=None)
    children: Dict[InstrumentedAttribute, "RelationshipOption"] = field(default=None)


def build_select_stmt(
    stmt: Select[Tuple[_T]],
    model: Optional[Type[_T]] = None,
    filters: Optional[_FilterType] = None,
    optional_filters: Optional[_FilterType] = None,
    relationship_options: Optional[
        Dict[InstrumentedAttribute, RelationshipOption]
    ] = None,
    group_by: Optional[ColumnExpressionArgument] = None,
    order_by: Optional[Union[List[str], str]] = None,
    direction: Optional[Union[List[str], str]] = None,
    limit: int = None,
) -> Select[Tuple[_T]]:
    stmt = apply_relationship_options(
        stmt=stmt,
        relationship_options=relationship_options,
    )

    stmt = apply_filters(
        stmt=stmt,
        filter_dict=filters,
    )
    stmt = apply_filters(
        stmt=stmt,
        filter_dict=optional_filters,
        with_optional=True,
    )

    stmt = apply_group_by(
        stmt=stmt,
        group_by=group_by,
    )

    stmt = apply_order_by(
        stmt=stmt,
        model=model,
        order_by=order_by,
        direction=direction,
    )

    return apply_limit(
        stmt=stmt,
        limit=limit,
    )


def build_update_stmt(
    model: Type[_T],
    values: Dict,
    filters: Optional[_FilterType] = None,
) -> ReturningUpdate[Tuple[_T]]:
    return (
        apply_filters(
            stmt=update(model),
            filter_dict=filters,
        )
        .values(values)
        .returning(model)
    )


def build_insert_stmt(
    model: Type[_T],
) -> ReturningInsert[Tuple[_T]]:
    return insert(model).returning(model)


def build_delete_stmt(
    model: Type[_T],
    filters: _FilterType,
) -> ReturningDelete[Tuple[_T]]:
    return apply_filters(
        stmt=delete(model),
        filter_dict=filters,
    ).returning(model)


def select_distinct(
    model: Type[_T],
    expr: ColumnExpressionArgument,
) -> Select[Tuple[_T]]:
    return select(distinct(expr)) if expr is not None else select(model)


def apply_group_by(
    stmt: Select[Tuple[_T]],
    group_by: ColumnExpressionArgument,
) -> Select[Tuple[_T]]:
    return stmt.group_by(group_by) if group_by is not None else stmt


def apply_relationship_options(
    stmt: Union[Select[Tuple[_T]], Update],
    relationship_options: Dict[InstrumentedAttribute, RelationshipOption],
    parents: List[InstrumentedAttribute] = None,
) -> Union[Select[Tuple[_T]], Update]:
    def get_load(
        loading_technique: LoadingTechnique,
        items: List[InstrumentedAttribute],
        extra_conditions: Optional[BinaryExpression] = None,
    ):
        items_post = []
        for item in items:
            if extra_conditions is not None:
                items_post.append(item.and_(*extra_conditions))
            else:
                items_post.append(item)

        if loading_technique == LoadingTechnique.CONTAINS_EAGER:
            return contains_eager(*items_post)
        elif loading_technique == LoadingTechnique.LAZY:
            return lazyload(*items_post)
        elif loading_technique == LoadingTechnique.JOINED:
            return joinedload(*items_post)
        elif loading_technique == LoadingTechnique.SUBQUERY:
            return subqueryload(*items_post)
        elif loading_technique == LoadingTechnique.SELECTIN:
            return selectinload(*items_post)
        elif loading_technique == LoadingTechnique.RAISE:
            return raiseload(*items_post)
        elif loading_technique == LoadingTechnique.NOLOAD:
            return noload(*items_post)

        return None

    if relationship_options is None:
        return stmt

    for relationship, sub_relationships in relationship_options.items():
        if any(
            [
                relationship is None,
                not isinstance(relationship, InstrumentedAttribute),
                sub_relationships is None,
                not isinstance(sub_relationships, RelationshipOption),
            ]
        ):
            continue

        sub_items = [relationship] if parents is None else [*parents, relationship]

        load = get_load(
            loading_technique=sub_relationships.lazy,
            items=sub_items,
            extra_conditions=sub_relationships.added_criteria,
        )

        if load is not None:
            stmt = stmt.options(load)

        if (children := sub_relationships.children) is not None:
            stmt = apply_relationship_options(
                stmt,
                relationship_options=children,
                parents=sub_items,
            )

    return stmt


def apply_filters(
    stmt: Union[Select[Tuple[_T]], Update],
    filter_dict: _FilterType,
    with_optional: bool = False,
) -> Union[Select[Tuple[_T]], Update, Delete]:
    filters = get_filters(
        filters=filter_dict,
        with_optional=with_optional,
    )

    return stmt if len(filters) == 0 else stmt.filter(and_(*filters))


def apply_order_by(
    stmt: Select[Tuple[_T]],
    model: Type[_T],
    order_by: Union[List[str], str],
    direction: Union[List[str], str],
) -> Select[Tuple[_T]]:
    if order_by is None or direction is None:
        return stmt

    if isinstance(order_by, str):
        order_by = [order_by]

    if isinstance(direction, str):
        direction = [direction]

    if len(order_by) != len(direction):
        raise ValueError("order_by length must be equals to direction length")

    order_by_list = []
    for column, dir in zip(order_by, direction):
        if dir == "desc":
            order_by_list.append(desc(getattr(model, column)))
        elif dir == "asc":
            order_by_list.append(asc(getattr(model, column)))

    return stmt.order_by(*order_by_list)


def apply_pagination(
    session: Session,
    stmt: Select[Tuple[_T]],
    page: int,
    per_page: int,
) -> Tuple[Select[Tuple[_T]], str]:
    if page is None or per_page is None:
        return stmt, None

    total_results = session.scalar(select(func.count()).select_from(stmt))
    total_pages = (total_results + per_page - 1) // per_page

    pagination = {
        "total": total_results,
        "page": page,
        "per_page": per_page,
        "total_pages": total_pages,
    }

    pagination = json.dumps(pagination)

    stmt = stmt.offset((page - 1) * per_page).limit(per_page)

    return stmt, pagination


async def async_apply_pagination(
    session: AsyncSession,
    stmt: Select[Tuple[_T]],
    page: int,
    per_page: int,
) -> Tuple[Select[Tuple[_T]], str]:
    if page is None or per_page is None:
        return stmt, None

    total_results = await session.scalar(select(func.count()).select_from(stmt))
    total_pages = (total_results + per_page - 1) // per_page

    pagination = {
        "total": total_results,
        "page": page,
        "per_page": per_page,
        "total_pages": total_pages,
    }

    pagination = json.dumps(pagination)

    stmt = stmt.offset((page - 1) * per_page).limit(per_page)

    return stmt, pagination


def apply_limit(
    stmt: Select[Tuple[_T]],
    limit: int,
) -> Select[Tuple[_T]]:
    return stmt.limit(limit) if limit is not None else stmt


def get_conditions_from_dict(
    values: _FilterType,
    with_optional: bool = False,
) -> List[ColumnExpressionArgument]:
    conditions = []
    for key, value in values.items():
        if type(value) is set:
            value = list(value)
        elif type(value) is dict:
            for k, v in value.items():
                if with_optional and v is None:
                    continue

                match k:
                    case Operators.EQUAL:
                        conditions.append(key == v)
                    case Operators.IEQUAL:
                        if not isinstance(v, Null):
                            conditions.append(func.lower(key) == func.lower(v))
                        else:
                            conditions.append(key == v)
                    case Operators.DIFFERENT:
                        conditions.append(key != v)
                    case Operators.IDIFFERENT:
                        if not isinstance(v, Null):
                            conditions.append(func.lower(key) != func.lower(v))
                        else:
                            conditions.append(key != v)
                    case Operators.LIKE:
                        if not isinstance(v, Null):
                            conditions.append(key.like(v))
                        else:
                            conditions.append(key == v)
                    case Operators.NOT_LIKE:
                        if not isinstance(v, Null):
                            conditions.append(~key.like(v))
                        else:
                            conditions.append(key != v)
                    case Operators.ILIKE:
                        if not isinstance(v, Null):
                            conditions.append(key.ilike(v))
                        else:
                            conditions.append(key == v)
                    case Operators.NOT_ILIKE:
                        if not isinstance(v, Null):
                            conditions.append(~key.ilike(v))
                        else:
                            conditions.append(key != v)
                    case Operators.BETWEEN:
                        if len(v) != 2:
                            continue
                        if v[0] is not None:
                            conditions.append(key > v[0])
                        if v[1] is not None:
                            conditions.append(key < v[1])
                    case Operators.BETWEEN_OR_EQUAL:
                        if len(v) != 2:
                            continue
                        if v[0] is not None:
                            conditions.append(key >= v[0])
                        if v[1] is not None:
                            conditions.append(key <= v[1])
                    case Operators.SUPERIOR:
                        conditions.append(key > v)
                    case Operators.INFERIOR:
                        conditions.append(key < v)
                    case Operators.SUPERIOR_OR_EQUAL:
                        conditions.append(key >= v)
                    case Operators.INFERIOR_OR_EQUAL:
                        conditions.append(key <= v)
                    case Operators.IN:
                        v = v if isinstance(v, Iterable) else [v]
                        if isinstance(key, tuple):
                            conditions.append(tuple_(*key).in_(v))
                        else:
                            conditions.append(key.in_(v))
                    case Operators.IIN:
                        v = v if isinstance(v, Iterable) else [v]
                        if isinstance(key, tuple):
                            conditions.append(
                                tuple_([func.lower(key_) for key_ in key]).in_(
                                    [
                                        (
                                            func.lower(v_)
                                            if not isinstance(v_, Null)
                                            else v_
                                        )
                                        for v_ in v
                                    ]
                                )
                            )
                        else:
                            conditions.append(
                                func.lower(key).in_(
                                    [
                                        (
                                            func.lower(v_)
                                            if not isinstance(v_, Null)
                                            else v_
                                        )
                                        for v_ in v
                                    ]
                                )
                            )
                    case Operators.NOT_IN:
                        v = v if isinstance(v, Iterable) else [v]
                        if isinstance(key, tuple):
                            conditions.append(tuple_(*key).notin_(v))
                        else:
                            conditions.append(key.notin_(v))

                    case Operators.NOT_IIN:
                        v = v if isinstance(v, Iterable) else [v]
                        if isinstance(key, tuple):
                            conditions.append(
                                tuple_([func.lower(key_) for key_ in key]).notin_(
                                    [
                                        (
                                            func.lower(v_)
                                            if not isinstance(v_, Null)
                                            else v_
                                        )
                                        for v_ in v
                                    ]
                                )
                            )
                        else:
                            conditions.append(
                                func.lower(key).notin_(
                                    [
                                        (
                                            func.lower(v_)
                                            if not isinstance(v_, Null)
                                            else v_
                                        )
                                        for v_ in v
                                    ]
                                )
                            )
                    case Operators.HAS:
                        v = get_filters(
                            v,
                            with_optional=with_optional,
                        )
                        for condition in v:
                            conditions.append(key.has(condition))
                    case Operators.ANY:
                        v = get_filters(
                            v,
                            with_optional=with_optional,
                        )

                        if len(v) == 0:
                            continue

                        conditions.append(key.any(and_(*v)))

    return conditions


def get_filters(
    filters: _FilterType,
    with_optional: bool = False,
) -> List[ColumnExpressionArgument]:
    if filters is None:
        return []
    if not isinstance(filters, dict):
        raise TypeError("<filters> must be type of <dict>")

    filters = [{x: y} for x, y in filters.items()]

    conditions = []
    for filter_c in filters:
        if type(filter_c) is not dict:
            continue

        conditions_from_dict = get_conditions_from_dict(
            filter_c,
            with_optional=with_optional,
        )
        conditions.extend(conditions_from_dict)

    return conditions
