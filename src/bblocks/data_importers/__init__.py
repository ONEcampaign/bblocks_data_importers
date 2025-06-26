from importlib.metadata import version

from bblocks.data_importers.who.ghed import GHED
from bblocks.data_importers.imf.weo import WEO
from bblocks.data_importers.wfp.wfp import WFPFoodSecurity, WFPInflation
from bblocks.data_importers.world_bank.wb_api import WorldBank
from bblocks.data_importers.world_bank.ids import InternationalDebtStatistics
from bblocks.data_importers.undp.hdi import HumanDevelopmentIndex
from bblocks.data_importers.unaids.unaids import UNAIDS

__version__ = version("bblocks-data-importers")
