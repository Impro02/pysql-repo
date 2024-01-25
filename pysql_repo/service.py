# MODULES
from typing import TypeVar, Generic
from logging import Logger

# MODELS
from pysql_repo.repository import (
    SessionRepository,
    AsyncSessionRepository,
)


_T = TypeVar("_T", bound=SessionRepository)
_T_ASYNC = TypeVar("_T_ASYNC", bound=AsyncSessionRepository)


class SessionService(Generic[_T]):
    def __init__(
        self,
        repository: _T,
        logger: Logger,
    ) -> None:
        self._repository = repository
        self._logger = logger

    def session_manager(self):
        return self._repository.session_manager()


class AsyncSessionService(Generic[_T_ASYNC]):
    def __init__(
        self,
        repository: _T_ASYNC,
        logger: Logger,
    ) -> None:
        self._repository = repository
        self._logger = logger

    def session_manager(self):
        return self._repository.session_manager()
