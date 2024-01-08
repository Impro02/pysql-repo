from sqlalchemy import Boolean, Column, ForeignKey, Integer, String
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class City(Base):
    __tablename__ = "CITY"

    id = Column(
        "ID",
        Integer,
        primary_key=True,
        index=True,
    )
    name = Column(
        "NAME",
        String,
        index=True,
    )
    state = Column(
        "STATE",
        String,
        index=True,
    )

    addresses = relationship(
        "Address",
        back_populates="city",
    )


class Address(Base):
    __tablename__ = "ADDRESS"

    id = Column(
        "ID",
        Integer,
        primary_key=True,
        index=True,
    )
    street = Column(
        "STREET",
        String,
        index=True,
    )
    zip_code = Column(
        "ZIP_CODE",
        Integer,
        index=True,
    )
    user_id = Column(
        "USER_ID",
        Integer,
        ForeignKey("USER.ID"),
    )
    city_id = Column(
        "CITY_ID",
        Integer,
        ForeignKey("CITY.ID"),
    )

    user = relationship(
        "User",
        back_populates="addresses",
    )
    city = relationship(
        "City",
        back_populates="addresses",
    )


class User(Base):
    __tablename__ = "USER"

    id = Column(
        "ID",
        Integer,
        primary_key=True,
        index=True,
    )
    email = Column(
        "EMAIL",
        String,
        unique=True,
        index=True,
    )
    hashed_password = Column(
        "HASHED_PASSWORD",
        String,
    )
    full_name = Column(
        "FULL_NAME",
        String,
        index=True,
    )
    is_active = Column(
        "IS_ACTIVE",
        Boolean,
        default=True,
    )

    addresses = relationship(
        "Address",
        back_populates="user",
    )
