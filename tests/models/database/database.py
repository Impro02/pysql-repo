from typing import List
from sqlalchemy import Boolean, ForeignKey, Integer, String
from sqlalchemy.orm import relationship, Mapped, mapped_column
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class City(Base):
    __tablename__ = "CITY"

    id: Mapped[int] = mapped_column(
        "ID",
        Integer,
        primary_key=True,
        index=True,
    )
    name: Mapped[str] = mapped_column(
        "NAME",
        String,
        index=True,
    )
    state: Mapped[str] = mapped_column(
        "STATE",
        String,
        index=True,
    )

    addresses: Mapped[List["Address"]] = relationship(
        "Address",
        back_populates="city",
        lazy="joined",
    )


class Address(Base):
    __tablename__ = "ADDRESS"

    id: Mapped[int] = mapped_column(
        "ID",
        Integer,
        primary_key=True,
        index=True,
    )
    street: Mapped[str] = mapped_column(
        "STREET",
        String,
        index=True,
    )
    zip_code: Mapped[str] = mapped_column(
        "ZIP_CODE",
        Integer,
        index=True,
    )
    user_id: Mapped[int] = mapped_column(
        "USER_ID",
        Integer,
        ForeignKey("USER.ID"),
    )
    city_id: Mapped[int] = mapped_column(
        "CITY_ID",
        Integer,
        ForeignKey("CITY.ID"),
    )

    user: Mapped["User"] = relationship(
        "User",
        back_populates="addresses",
        lazy="joined",
    )
    city: Mapped["City"] = relationship(
        "City",
        back_populates="addresses",
        lazy="joined",
    )


class User(Base):
    __tablename__ = "USER"

    id: Mapped[int] = mapped_column(
        "ID",
        Integer,
        primary_key=True,
        index=True,
    )
    email: Mapped[str] = mapped_column(
        "EMAIL",
        String,
        unique=True,
        index=True,
    )
    hashed_password: Mapped[str] = mapped_column(
        "HASHED_PASSWORD",
        String,
    )
    full_name: Mapped[str] = mapped_column(
        "FULL_NAME",
        String,
        index=True,
    )
    is_active: Mapped[bool] = mapped_column(
        "IS_ACTIVE",
        Boolean,
        default=True,
    )

    addresses: Mapped[List["Address"]] = relationship(
        "Address",
        back_populates="user",
        lazy="joined",
    )
