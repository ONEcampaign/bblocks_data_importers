# read version from installed package
from importlib.metadata import version

from bblocks_data_importers.who.ghed import GHED
from bblocks_data_importers.imf.weo import WEO

__version__ = version("bblocks_data_importers")
