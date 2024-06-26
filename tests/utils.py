# MODULES
import logging
from pathlib import Path
import sys


def create_logger(
    name: str,
    level: int,
    formatter: logging.Formatter,
    stream_output: bool = False,
) -> logging.Logger:
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
