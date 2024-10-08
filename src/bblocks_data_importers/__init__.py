# read version from installed package
from importlib.metadata import version

from bblocks_data_importers.who.ghed import GHED

__version__ = version("bblocks_data_importers")
