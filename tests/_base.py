# MODULES
import json
import os
from pathlib import Path
from unittest import TestCase as _TestCase

# DATABASE
from pysql_repo._database import DataBase

# TESTS
from tests.models.database.database import Address, Base, City, User
from tests.repositories.user.user_repository import UserRepository
from tests.services.user.user_service import UserService
from tests.utils import LOGGER_DB, LOGGER_TESTS


class TestCase(_TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._database_path = (
            Path(os.path.expanduser("~")) / "fastapi" / "pysql-repo" / "back" / "src"
        )

        cls._database_path.mkdir(parents=True, exist_ok=True)

        cls._database = DataBase(
            databases_config={
                "connection_string": f"sqlite:///{cls._database_path}/tests.db",
                "ini": True,
                "init_database_dir_json": os.path.join(
                    os.path.abspath(os.getcwd()),
                    "tests",
                    "databases",
                    "ini",
                ),
            },
            base=Base,
        )

        cls._database.create_database()

        cls._user_repository = UserRepository(
            session_factory=cls._database.session_factory,
        )
        cls._user_service = UserService(
            user_repository=cls._user_repository,
            logger=LOGGER_TESTS,
        )

    @classmethod
    def setUp(cls):
        cls._database.init_tables_from_json_files(
            directory=Path(cls._database.init_database_dir_json),
            table_names=[
                User.__tablename__,
                Address.__tablename__,
                City.__tablename__,
            ],
        )


def load_expected_data(
    saved_dir_path: Path = None,
    saved_file_path: str = None,
    format: str = "json",
    encoding: str = "utf-8",
):
    def decorator(func):
        def wrapper(self, *args, **kwargs):
            class_name = self.__class__.__name__
            function_name = func.__name__

            if not isinstance(self, TestCase):
                raise TypeError(
                    f"{self.__class__.__name__} must be instance of {TestCase.__name__}"
                )

            if saved_dir_path is None:
                return

            file_path = (
                saved_dir_path / f"{class_name}__{function_name}.{format}"
                if saved_file_path is None
                else saved_dir_path / saved_file_path
            )

            expected_data = {}
            if os.path.exists(file_path):
                with open(file_path, encoding=encoding) as file:
                    match format:
                        case "json":
                            expected_data = json.load(file)
                        case "txt":
                            expected_data = file.read()
                            expected_data = expected_data.encode(encoding)

            data = func(self, expected_data, file_path, *args, **kwargs)

            return data

        return wrapper

    return decorator
