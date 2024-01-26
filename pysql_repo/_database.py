# MODULES
import pytz
import re
from typing import Any, Dict, List, Optional, TypedDict
from pathlib import Path
from datetime import datetime
from logging import Logger

# SQLALCHEMY
from sqlalchemy import Table, MetaData
from sqlalchemy.orm import DeclarativeMeta
from sqlalchemy.schema import sort_tables


# LIBS
from pysql_repo.libs.file_lib import open_json_file


class DataBaseConfigTypedDict(TypedDict):
    connection_string: str
    ini: bool
    init_database_dir_json: Optional[str]
    create_on_start: bool
    connect_args: Optional[Dict]


class Database:
    def __init__(
        self,
        databases_config: DataBaseConfigTypedDict,
        logger: Logger,
        base: DeclarativeMeta,
        metadata_views: Optional[List[MetaData]] = None,
    ) -> None:
        self._database_config = databases_config
        self._logger = logger
        self._base = base
        self._metadata_views = metadata_views

        self._views = [
            table
            for metadata in self._metadata_views or []
            for table in metadata.sorted_tables
        ]

    @property
    def views(self) -> List[Table]:
        return self._views

    @property
    def ini(self):
        return self._database_config.get("ini")

    @property
    def init_database_dir_json(self):
        return self._database_config.get("init_database_dir_json")

    @classmethod
    def _pre_process_data_for_initialization(cls, data: Dict[str, Any], timezone: str):
        for key, value in data.items():
            if isinstance(value, str) and re.match(
                r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{3})?(Z|[+-]\d{2}:?\d{2})?",
                value,
            ):
                if value.endswith("Z"):
                    utc_dt = datetime.fromisoformat(value[:-1])
                    local_tz = pytz.timezone(timezone)
                    local_dt = utc_dt.replace(tzinfo=pytz.utc).astimezone(local_tz)
                    data[key] = local_dt
                else:
                    data[key] = datetime.fromisoformat(value)

        return data

    def _get_pre_process_data_for_initialization(
        self,
        path: Path,
        timezone: str,
    ) -> Optional[List[Dict[str, Any]]]:
        try:
            raw_data = open_json_file(path=path)
        except FileNotFoundError:
            self._logger.warning(
                f"Failed to initialize table due to the absence of the file at [{path}]."
            )

            return

        return [
            self._pre_process_data_for_initialization(
                data,
                timezone=timezone,
            )
            for data in raw_data
        ]

    def _get_ordered_tables(self, table_names: List[str]) -> List[Table]:
        if not (init := self.ini):
            raise ValueError(
                f"Unable to init database tables because {init=} in config"
            )

        table_names = table_names or set()
        tables = {
            k: v for k, v in self._base.metadata.tables.items() if k in table_names
        }

        return sort_tables(tables.values())