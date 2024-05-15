# MODULES
import json
import os
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, ParamSpec, Union, cast
from unittest import TestCase as _TestCase

# DATABASE
from pysql_repo._database import DataBase
from pysql_repo._database_base import DataBaseConfigTypedDict


# TESTS
from tests.models.database.database import Address, Base, City, User
from tests.repositories.user.user_repository import UserRepository
from tests.services.user.user_service import UserService
from tests.utils import LOGGER_TESTS

_P = ParamSpec("_P")


class TestCase(_TestCase):

    _database_path = (
        Path(os.path.expanduser("~")) / "fastapi" / "pysql-repo" / "back" / "src"
    )

    databases_config: DataBaseConfigTypedDict = {
        "connection_string": f"sqlite:///{_database_path}/tests.db",
        "ini": True,
        "init_database_dir_json": os.path.join(
            os.path.abspath(os.getcwd()),
            "tests",
            "databases",
            "ini",
        ),
    }

    _database = DataBase(
        databases_config=databases_config,
        base=Base,
    )

    _user_repository = UserRepository(
        session_factory=_database.session_factory,
    )
    _user_service = UserService(
        user_repository=_user_repository,
        logger=LOGGER_TESTS,
    )

    @classmethod
    def setUpClass(cls) -> None:
        cls._database_path.mkdir(parents=True, exist_ok=True)

        cls._database.create_database()

    @classmethod
    def setUp(cls) -> None:
        if cls._database.init_database_dir_json is not None:
            cls._database.init_tables_from_json_files(
                directory=Path(cls._database.init_database_dir_json),
                table_names=[
                    User.__tablename__,
                    Address.__tablename__,
                    City.__tablename__,
                ],
            )


def load_expected_data(
    saved_dir_path: Optional[Path] = None,
    saved_file_path: Optional[str] = None,
    format: str = "json",
    encoding: str = "utf-8",
) -> Callable[[Callable[_P, Any]], Callable[_P, Any]]:

    def decorator(func: Callable[_P, Any]) -> Callable[_P, Any]:

        def wrapper(*args: _P.args, **kwargs: _P.kwargs) -> Any:
            qualname = func.__qualname__
            if "." not in qualname:
                raise ValueError("The function must be a method of a class")

            if saved_dir_path is None:
                return

            file_path = (
                saved_dir_path / f"{qualname.replace('.', '__')}.{format}"
                if saved_file_path is None
                else saved_dir_path / saved_file_path
            )

            expected_data: Union[List[Dict[str, Any]], Dict[str, Any], bytes] = {}
            if os.path.exists(file_path):
                with open(file_path, encoding=encoding) as file:
                    match format:
                        case "json":
                            expected_data = cast(
                                Union[List[Dict[str, Any]], Dict[str, Any]],
                                json.load(file),
                            )
                        case "txt":
                            expected_data_str = file.read()
                            expected_data = expected_data_str.encode(encoding)

            kwargs["expected_data"] = expected_data
            kwargs["saved_path"] = file_path

            data = func(*args, **kwargs)

            return data

        return wrapper

    return decorator
