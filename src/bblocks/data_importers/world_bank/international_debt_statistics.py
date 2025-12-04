""" """

import pandas as pd

from bblocks.data_importers.world_bank.world_bank import WorldBank, get_wb_databases


class InternationalDebtStatistics(WorldBank):
    """World Bank Data Importer.

    This class provides a simplified interface for fetching and managing data from the
    International Debt Statistics database.

    It leverages the `wbgapi` package to interact with the World Bank API, making it easy to retrieve
    and manipulate debt-related data for various countries and indicators.
    """

    def __init__(self):
        super().__init__(db=6) # IDS database ID is 6

    def __repr__(self):
        return f"InternationalDebtStatistics(db={self._db})"

    @property
    def last_updated(self):
        """Last updated date of the IDS database."""

        return get_wb_databases().loc[lambda d: d.id==self._db, "last_updated"].values[0]

    @property
    def debt_stock_indicators(self) -> pd.DataFrame:
        """Get the metadata for PPG debt stock indicators.
        """

        inds = ["DT.DOD.BLAT.CD", "DT.DOD.MLAT.CD", "DT.DOD.PBND.CD", "DT.DOD.PCBK.CD","DT.DOD.PROP.CD"]

        return self.get_indicator_metadata(inds)

    @property
    def debt_service_indicators(self) -> pd.DataFrame:
        """Get the metadata for PPG debt service indicators.
        """

        inds = ["DT.AMT.BLAT.CD", "DT.AMT.MLAT.CD",
  "DT.AMT.PBND.CD",
  "DT.AMT.PCBK.CD",
  "DT.AMT.PROP.CD",
  "DT.INT.BLAT.CD",
  "DT.INT.MLAT.CD",
  "DT.INT.PBND.CD",
  "DT.INT.PCBK.CD",
  "DT.INT.PROP.CD"]

        return self.get_indicator_metadata(inds)



