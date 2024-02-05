# MODULES
from typing import TypeVar, Generic
from logging import Logger

# MODELS
from pysql_repo._repository import Repository


_T = TypeVar("_T", bound=Repository)


class Service(Generic[_T]):
    """
    A generic service class.

    Attributes:
        _repository: The repository object.
        _logger: The logger object.

    Methods:
        session_manager(): Returns the session factory.
    """

    def __init__(
        self,
        repository: _T,
        logger: Logger,
    ) -> None:
        self._repository = repository
        self._logger = logger

    def session_manager(self):
        """
        Returns the session manager object.
        """
        return self._repository.session_manager()
