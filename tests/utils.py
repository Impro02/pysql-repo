# MODULES
import json
import logging
import os
from pathlib import Path
import sys
from unittest import IsolatedAsyncioTestCase, TestCase

# DATABASE
from pysql_repo.database import AsyncDatabase, DataBase

# TESTS
from tests.models.database.database import Address, Base, City, User
from tests.repositories.user_repository import AsyncUserRepository, UserRepository
from tests.services.user_service import AsyncUserService, UserService


def create_logger(
    name: str,
    level: int,
    formatter: logging.Formatter,
    stream_output: bool = False,
):
    logger = logging.getLogger(name=name)

    if logger.hasHandlers():
        return logger

    logger.setLevel(level)

    if stream_output:
        # Add a stream handler to log messages to stdout
        stream_handler = logging.StreamHandler(stream=sys.stdout)
        stream_handler.setLevel(level)
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

    return logger


FORMATTER = logging.Formatter(
    "%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
)

LOGGER_DB = create_logger(
    "sqlalchemy.engine",
    level=logging.INFO,
    formatter=FORMATTER,
    stream_output=True,
)

LOGGER_TESTS = create_logger(
    "tests",
    level=logging.DEBUG,
    formatter=FORMATTER,
    stream_output=True,
)


class SavedPath:
    PATH_ASSET = Path("tests") / "assets"
    PATH_ASSET_SAVED = PATH_ASSET / "saved"

    PATH_ASSET_USERS = PATH_ASSET_SAVED / "users"


class TestCustom(TestCase):
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
            logger=LOGGER_DB,
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


class AsyncTestCustom(IsolatedAsyncioTestCase):
    _database_path = (
        Path(os.path.expanduser("~")) / "fastapi" / "pysql-repo" / "back" / "src"
    )

    _database = AsyncDatabase(
        databases_config={
            "connection_string": f"sqlite+aiosqlite:///{_database_path}/tests.db",
            "ini": True,
            "init_database_dir_json": os.path.join(
                os.path.abspath(os.getcwd()),
                "tests",
                "databases",
                "ini",
            ),
            "create_on_start": True,
        },
        base=Base,
        logger=LOGGER_DB,
    )

    def __init__(self, methodName: str = "runTest") -> None:
        super().__init__(methodName)

        self._database_path.mkdir(parents=True, exist_ok=True)

        self._user_repository = AsyncUserRepository(
            session_factory=self._database.session_factory,
        )

        self._user_service = AsyncUserService(
            user_repository=self._user_repository,
            logger=LOGGER_TESTS,
        )

    @classmethod
    async def asyncSetUp(cls):
        await cls._database.init_tables_from_json_files(
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


def async_load_expected_data(
    saved_dir_path: Path = None,
    saved_file_path: str = None,
    format: str = "json",
    encoding: str = "utf-8",
):
    def decorator(func):
        async def wrapper(self, *args, **kwargs):
            class_name = self.__class__.__name__
            function_name = func.__name__

            if not isinstance(self, IsolatedAsyncioTestCase):
                raise TypeError(
                    f"{self.__class__.__name__} must be instance of {IsolatedAsyncioTestCase.__name__}"
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

            data = await func(self, expected_data, file_path, *args, **kwargs)

            return data

        return wrapper

    return decorator
