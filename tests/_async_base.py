# MODULES
import json
import os
from pathlib import Path
from unittest import IsolatedAsyncioTestCase as _IsolatedAsyncioTestCase

# DATABASE
from pysql_repo.asyncio.async_database import AsyncDatabase

# TESTS
from tests.models.database.database import Address, Base, City, User
from tests.repositories.user.async_user_repository import AsyncUserRepository
from tests.services.user.async_user_service import AsyncUserService
from tests.utils import LOGGER_DB, LOGGER_TESTS


class IsolatedAsyncioTestCase(_IsolatedAsyncioTestCase):
    _created: bool = False

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
        if not cls._created:
            cls._created = True
            await cls._database.create_database()

        await cls._database.init_tables_from_json_files(
            directory=Path(cls._database.init_database_dir_json),
            table_names=[
                User.__tablename__,
                Address.__tablename__,
                City.__tablename__,
            ],
        )


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
