# read version from installed package
from importlib.metadata import version

from bblocks_data_importers.who.ghed import GHED
from bblocks_data_importers.imf.weo import WEO
from bblocks_data_importers.wfp.wfp import WFPFoodSecurity, WFPInflation
from bblocks_data_importers.world_bank.wb_api import WorldBank
from bblocks_data_importers.world_bank.ids import InternationalDebtStatistics

__version__ = version("bblocks_data_importers")
