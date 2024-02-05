# MODULES
from typing import Any, Generator, List, Optional
from pathlib import Path
from logging import Logger

# SQLALCHEMY
from sqlalchemy import text, MetaData, Connection, Table
from sqlalchemy.orm import DeclarativeMeta
from sqlalchemy.inspection import inspect
from sqlalchemy.ext.asyncio import (
    create_async_engine,
    async_sessionmaker,
    AsyncSession,
)

# CONTEXTLIB
from contextlib import asynccontextmanager

# UTILS
from pysql_repo._database_base import (
    Database as _Database,
    DataBaseConfigTypedDict as _DataBaseConfigTypedDict,
)


class AsyncDatabase(_Database):
    """
    Represents an asynchronous database.

    Attributes:
        _database_config: The configuration for the databases.
        _engine: The asynchronous engine used for database operations.
        _logger: The logger object used for logging.
        _base: The base class for declarative models.
        _metadata_views: Optional list of metadata views.
        _session_factory: The factory for creating asynchronous sessions.
        _views: The list of metadata views.

    Methods:
        views: Get the list of views in the database.
        ini: Get the 'ini' property from the database configuration.
        init_database_dir_json: Get the 'init_database_dir_json' property from the database configuration.
        _pre_process_data_for_initialization: Pre-processes the data for initialization.
        _get_pre_process_data_for_initialization: Gets the pre-processed data for initialization.
        _get_ordered_tables: Gets the ordered tables based on the given table names.
        create_database: Creates the database by dropping existing views and creating tables.
        session_factory: Context manager for creating an async session.
        init_tables_from_json_files: Initializes tables from JSON files.
    """

    def __init__(
        self,
        databases_config: _DataBaseConfigTypedDict,
        logger: Logger,
        base: DeclarativeMeta,
        metadata_views: Optional[List[MetaData]] = None,
    ) -> None:
        """
        Initializes an instance of AsyncDatabase.

        Args:
            databases_config: A dictionary containing the configuration for the databases.
            logger: The logger object to be used for logging.
            base: The base class for declarative models.
            metadata_views: Optional list of metadata views.

        Returns:
            None
        """
        self._database_config = databases_config
        self._engine = create_async_engine(
            self._database_config.get("connection_string"),
            echo=False,
            connect_args=self._database_config.get("connect_args") or {},
        )
        self._logger = logger
        self._base = base
        self._metadata_views = metadata_views

        self._session_factory = async_sessionmaker(
            bind=self._engine,
            autoflush=False,
            expire_on_commit=False,
        )

        self._views = [
            table
            for metadata in self._metadata_views or []
            for table in metadata.sorted_tables
        ]

    async def create_database(self) -> None:
        """
        Creates the database by dropping existing views and creating tables.

        Returns:
            None
        """

        def inspect_view_names(conn: Connection):
            inspector = inspect(conn)

            return [item.lower() for item in inspector.get_view_names()]

        async with self._engine.connect() as conn:
            current_view_names = await conn.run_sync(inspect_view_names)

        async with self.session_factory() as session:
            for view in self.views:
                if view.key.lower() in current_view_names:
                    await session.execute(text(f"DROP VIEW {view}"))

        async with self._engine.begin() as conn:
            await conn.run_sync(self._base.metadata.create_all)

    @asynccontextmanager
    async def session_factory(self) -> Generator[AsyncSession, Any, None]:
        """
        Context manager for creating an async session.

        Yields:
            AsyncSession: The async session object.

        Raises:
            Exception: If an exception occurs during the session, it is raised.

        Returns:
            None
        """
        async with self._session_factory() as session:
            try:
                yield session
            except Exception as ex:
                self._logger.error("Session rollback because of exception", exc_info=ex)
                await session.rollback()
                raise ex
            finally:
                await session.close()

    async def init_tables_from_json_files(
        self,
        directory: Path,
        table_names: List[str],
        timezone: str = "CET",
    ) -> List[Table]:
        """
        Initializes tables from JSON files.

        Args:
            directory: The directory containing the JSON files.
            table_names: The names of the tables to be initialized.
            timezone: The timezone to be used for initialization.

        Returns:
            List[Table]: The ordered list of tables that were initialized.
        """
        ordered_tables = self._get_ordered_tables(table_names=table_names)

        async with self.session_factory() as session:
            for table in ordered_tables:
                path = directory / f"{(table_name := table.name.upper())}.json"

                raw_data = self._get_pre_process_data_for_initialization(
                    path=path,
                    timezone=timezone,
                )

                if raw_data is None:
                    continue

                await session.execute(table.delete())
                await session.execute(table.insert().values(raw_data))

                self._logger.info(
                    f"Successfully initialized {table_name=} from the file at {str(path)}."
                )

            await session.commit()

        return ordered_tables
