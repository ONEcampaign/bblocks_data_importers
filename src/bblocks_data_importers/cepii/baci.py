import io
import os
import pandas as pd
from bs4 import BeautifulSoup
import requests
import re
import zipfile
from typing import Literal
import tempfile
import numpy as np
from requests.exceptions import RequestException
from pathlib import Path

from bblocks_data_importers.config import (
    logger,
    DataExtractionError,
    DataFormattingError,
    Fields,
)
from bblocks_data_importers.protocols import DataImporter
from bblocks_data_importers.utilities import convert_dtypes
from bblocks_data_importers.data_validators import DataFrameValidator


class BACI(DataImporter):

    def __init__(
        self,
        data_file: str | None = None,
        baci_version: str = "latest",
        hs_version: str = "17",
        country_format: Literal["code", "name", "iso2", "iso3"] = "name",
        product_name: bool = True,
    ):
        self._baci_version: str | None = None
        self._hs_version: str = hs_version
        self._country_format: str = country_format
        self._product_name: bool = product_name
        self._data_file: io.BytesIO | None = None
        self._data: pd.DataFrame | None = None
        self._metadata: pd.DataFrame | None = None

        self._data_file = Path(data_file) if data_file else None

        # if the data file is passed and the filed does not exist, raise an error
        if self._data_file and not self._data_file.exists():
            raise FileNotFoundError(
                f"The file path `{self._data_file}` does not exist. Please provide a valid file "
                f"path."
            )

        if baci_version == "latest":
            self._baci_version = self._get_latest_version()
        else:
            self._baci_version = baci_version

    def _get_latest_version(self):
        url = "https://www.cepii.fr/CEPII/en/bdd_modele/bdd_modele_item.asp?id=37"

        response = requests.get(url)
        if response.status_code != 200:
            raise RuntimeError(f"Error {response.status_code}")

        soup = BeautifulSoup(response.text, "html.parser")
        div = soup.find("div", id="telechargement")

        if not div:
            raise RuntimeError(
                "Latest version not found. HTML object not present in the webpage."
            )

        match = re.search(
            r"This is the\s+(\d+)\s+version", div.get_text(), re.IGNORECASE
        )
        if match:
            return match.group(1)

        else:
            raise RuntimeError("Latest version not found.")

    def _get_zip_data(self):
        url = f"https://www.cepii.fr/DATA_DOWNLOAD/baci/data/BACI_HS{self._hs_version}_V{self._baci_version}.zip"
        logger.info("Downloading BACI data. This may take a while...")

        response = requests.get(url)
        if response.status_code != 200:
            raise RuntimeError(f"Error {response.status_code}")

        zip_data = io.BytesIO(response.content)

        if self._data_file:
            with zipfile.ZipFile(zip_data) as zip_ref:
                zip_ref.extractall(self._data_file)
            logger.info(
                f"BACI data saved to {self._data_file}/BACI_HS{self._hs_version}_V{self._baci_version}"
            )
            return None  # disk-based
        else:
            self._zip_obj = zipfile.ZipFile(zip_data)
            return self._zip_obj  # in-memory

    def _load_csv(self, filename):
        if self._data_file:
            path = self._data_file / filename
            return pd.read_csv(path, dtype={"k": str, "code": str})
        elif hasattr(self, "_zip_obj"):
            with self._zip_obj.open(filename) as f:
                return pd.read_csv(f, dtype={"k": str, "code": str})
        else:
            raise RuntimeError("No data source found.")

    def _format_data(self, raw_df, country_map, product_map):

        df = raw_df.rename(
            columns={
                "t": Fields.year,
                "i": Fields.exporter,
                "j": Fields.importer,
                "k": Fields.product_code,
                "v": Fields.value,
                "q": Fields.quantity,
            }
        )

        if self._country_format != "code":
            df[Fields.exporter] = (
                df[Fields.exporter].map(country_map)
            )
            df[Fields.importer] = (
                df[Fields.importer].map(country_map)
            )

        if self._product_name:
            df[Fields.product_name] = (
                df[Fields.product_code].map(product_map)
            )

        df = convert_dtypes(df, "pyarrow")

        return df

    def _combine_data(self) -> pd.DataFrame:
        folder = f"BACI_HS{self._hs_version}_V{self._baci_version}"

        product_codes_df = self._load_csv(
            f"product_codes_HS{self._hs_version}_V{self._baci_version}.csv"
        )
        product_codes_dict = dict(
            zip(product_codes_df["code"], product_codes_df["description"])
        )

        # print(product_codes_dict)

        country_codes_df = self._load_csv(f"country_codes_V{self._baci_version}.csv")
        country_codes_dict = dict(
            zip(
                country_codes_df["country_code"],
                country_codes_df[f"country_{self._country_format}"],
            )
        )

        # print(country_codes_dict)

        # Get all CSVs starting with "BACI"
        if self._data_file:
            files = os.listdir(self._data_file)
        else:
            files = [
                f
                for f in self._zip_obj.namelist()
                if f.startswith(folder) and f.endswith(".csv")
            ]
            files = [f.split("/")[-1] for f in files]  # remove prefix

        dfs = [
            self._format_data(
                self._load_csv(f),
                country_codes_dict,
                product_codes_dict,
            )
            for f in files
            if f.startswith("BACI") and f.endswith(".csv")
        ]

        return pd.concat(dfs, ignore_index=True)

    def _load_data(self) -> None:

        if not self._data_file:
            logger.info("Importing data from BACI database")
            self._get_zip_data()
        else:
            logger.info(f"Importing data from local file: {self._data_file}")

        df = self._combine_data()

        required_cols = [
            Fields.year,
            Fields.exporter,
            Fields.importer,
            Fields.product_code,
            Fields.value,
            Fields.quantity,
        ]

        if self._product_name:
            required_cols.append(Fields.product_name)

        DataFrameValidator().validate(
            df,
            required_cols=required_cols,
        )

        self._data = df
        logger.info("Data imported successfully")

    def get_data(
        self,
    ) -> pd.DataFrame:

        if self._data is None:
            self._load_data()

        return self._data
