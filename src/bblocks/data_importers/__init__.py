"""Data Importers for various data sources.

Available data importers:
- `WorldBank`: World Bank data
- `WEO`: IMF World Economic Outlook data
- `GHED`: WHO Global Health Expenditure Database
- `WFPFoodSecurity`: WFP Food Security data
- `WFPInflation`: WFP Inflation data
- `InternationalDebtStatistics`: World Bank International Debt Statistics
- `HumanDevelopmentIndex`: UNDP Human Development


Usage:

Import the package
```python
import bblocks.data_importers as bbdata
```

Then, you can use the data importers as follows:
Instantiate the data importer eg for IMF World Economic Outlook
```python
weo = bbdata.WEO()
```

Get the data by calling the `get_data` method
```python
data = weo.get_data()
```

Each data importer has its own set of parameters and methods specific to the data source. Read the documentation
for each data importer for more details on how to use them. At a minimum, each data importer has a `get_data` method
that returns the data to the user
"""

from importlib.metadata import version

from bblocks.data_importers.who.ghed import GHED
from bblocks.data_importers.imf.weo import WEO
from bblocks.data_importers.wfp.wfp import WFPFoodSecurity, WFPInflation
from bblocks.data_importers.world_bank.wb_api import WorldBank
from bblocks.data_importers.world_bank.ids import InternationalDebtStatistics
from bblocks.data_importers.undp.hdi import HumanDevelopmentIndex
from bblocks.data_importers.unaids.unaids import UNAIDS

__version__ = version("bblocks-data-importers")
