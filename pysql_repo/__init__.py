# MODULES
import logging
import time

# SQLALCHEMY
from sqlalchemy import Engine, event


logging.basicConfig()
_logger = logging.getLogger("pysql_repo.cursor")
_logger.setLevel(logging.INFO)


@event.listens_for(Engine, "before_cursor_execute")
def before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
    conn.info.setdefault("query_start_time", []).append(time.perf_counter())
    _logger.info("Start Query: %s, {%s}", statement, parameters)


@event.listens_for(Engine, "after_cursor_execute")
def after_cursor_execute(conn, cursor, statement, parameters, context, executemany):
    total = time.perf_counter() - conn.info["query_start_time"].pop(-1)
    _logger.info("Query completed in %fs", total)
