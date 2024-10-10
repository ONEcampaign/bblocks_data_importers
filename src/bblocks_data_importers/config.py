"""Configuration for the data importers.

Including:
- Path configuration
- Logger configuration
- Custom exceptions

TODO: cache settings

"""

import logging
from pathlib import Path

# Configure Logging
logger = logging.getLogger(__name__)
shell_handler = logging.StreamHandler()  # Create terminal handler
logger.setLevel(logging.INFO)  # Set levels for the logger, shell and file
shell_handler.setLevel(logging.INFO)  # Set levels for the logger, shell and file

# Format the outputs   "%(levelname)s (%(asctime)s): %(message)s"
fmt_file = "%(levelname)s: %(message)s"

# "%(levelname)s %(asctime)s [%(filename)s:%(funcName)s:%(lineno)d] %(message)s"
fmt_shell = "%(levelname)s: %(message)s"

shell_formatter = logging.Formatter(fmt_shell)  # Create formatters
shell_handler.setFormatter(shell_formatter)  # Add formatters to handlers
logger.addHandler(shell_handler)  # Add handlers to the logger


class Paths:
    """Configuration for paths"""

    project = Path(__file__).resolve().parent.parent
    data = project / "bblocks_data_importers" / ".data"


class DataExtractionError(Exception):
    """Raised when data extraction fails."""


class DataFormattingError(Exception):
    """Raised when data formatting fails."""


def set_data_path(path):
    """Set the path to the folder containing the raw data or where raw data will be stored.

    Args:
        path: Path to the raw data folder
    """

    Paths.data = Path(path).resolve()
