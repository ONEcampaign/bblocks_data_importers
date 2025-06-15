"""Extract BACI data"""

from pathlib import Path
import os
import io
import zipfile
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
from pyarrow import csv as pv
import requests

from bblocks.data_importers.config import logger, Fields, DataExtractionError


BASE_URL = "https://www.cepii.fr"


def rename_data_columns(table: pa.Table) -> pa.Table:
    """Rename BACI raw data PyArrow columns to standardized field names.
    """

    column_map = {
        "t": Fields.year,
        "i": Fields.exporter_code,
        "j": Fields.importer_code,
        "k": Fields.product_code,
        "v": Fields.value,
        "q": Fields.quantity,
    }

    # Get the current column names
    current_names = table.schema.names

    # Build the new names list
    new_names = [column_map.get(name, name) for name in current_names]

    return table.rename_columns(new_names)


def rename_country_columns(country_df: pd.DataFrame) -> pd.DataFrame:
    """Remane columns in the country codes DataFrame to standardized field names."""


    column_map = {
        "country_code": Fields.country_code,
        "country_name": Fields.country_name,
        "country_iso3": Fields.iso3_code,
        "country_iso2": Fields.iso2_code,
    }

    # Rename columns using the mapping
    return country_df.rename(columns=column_map)

def rename_product_columns(product_df: pd.DataFrame) -> pd.DataFrame:
    """Rename columns in the product codes DataFrame to standardized field names."""

    column_map = {
        "code": Fields.product_code,
        "description": Fields.product_description,
    }

    # Rename columns using the mapping
    return product_df.rename(columns=column_map)

def parse_readme(readme_content: str) -> dict:
    """
    """

    # normalize all line breaks to "\n" for consistent processing
    readme_content = readme_content.replace("\r\n", "\n").replace("\r", "\n")

    blocks = [block.strip() for block in readme_content.split("\n\n") if block.strip()]
    metadata = {}

    for block in blocks:
        if block.startswith("List of Variables:"):
            continue
        lines = block.splitlines()
        if ":" in lines[0]:
            key, first_value_line = lines[0].split(":", 1)
            key = key.strip()
            value_lines = [first_value_line.strip()] + [
                line.strip() for line in lines[1:]
            ]
            metadata[key] = " ".join(value_lines)

    return metadata

class BaciDataManager:
    """Manager class for handling BACI data extraction and processing."""

    def __init__(self, version: str, hs_version: str):
        self.version = version
        self.hs_version = hs_version

        self.download_url =  f"{BASE_URL}/DATA_DOWNLOAD/baci/data/BACI_{hs_version}_V{version}.zip"
        self.zip_file: None | zipfile.ZipFile = None

        self.data: None | pa.lib.Table = None
        self.country_codes: None | pd.DataFrame = None
        self.product_codes: None | pd.DataFrame = None
        self.metadata: None | dict = None

    def extract_zipfile_from_disk(self, zip_path: Path) -> None:
        """Extract the BACI ZIP file from a local path."""

        logger.info(f"Extracting BACI data from local path: {zip_path}")

        try:
            self.zip_file = zipfile.ZipFile(zip_path)
        except zipfile.BadZipFile as e:
            raise DataExtractionError(f"Failed to open local ZIP file: {e}")

    def extract_zipfile_from_web(self) -> None:
        """Extract the BACI ZIP file from the download URL."""

        try:
            logger.info(f"Downloading BACI data for version {self.version} and HS version {self.hs_version}")
            response = requests.get(self.download_url)
            response.raise_for_status()
        except requests.RequestException as e:
            raise DataExtractionError(f"Failed to download BACI data: {e}")

        # Create a BytesIO object to hold the downloaded ZIP file in memory
        zip_data = io.BytesIO(response.content)
        try:
            self.zip_file = zipfile.ZipFile(zip_data)
        except zipfile.BadZipFile as e:
            raise DataExtractionError(f"Failed to extract data from ZIP file: {e}")

    def extract_zip_file(self, zip_path: Path | None = None) -> None:
        """Extract the BACI ZIP file from either a local path or a web URL."""

        # if the path exists, extract from disk otherwise download from the web
        if zip_path and zip_path.exists():
            self.extract_zipfile_from_disk(zip_path)
        else:
            self.extract_zipfile_from_web()


    def save_zip_file(self, directory: str | os.PathLike, override: bool=False) -> None:
        """Save the zip file to a local path.

        Args:
            directory: The directory where the zip file should be saved.
            override: If True, will overwrite the existing file if it exists. Defaults to False.
        """

        # if the zipfile has not been extracted, raise an error
        if not self.zip_file:
            raise ValueError("BACI data has not bee extracted yet. No data available to save.")

        file_name = self.zip_file.filename

        # check if the path already exists, if it doesn't raise an error
        if not os.path.exists(directory):
            raise FileNotFoundError(f"Directory {directory} does not exist.")

        # check if the file already exists, if it does and override is False, raise an error
        file_path = Path(directory) / file_name
        if file_path.exists() and not override:
            raise FileExistsError(f"File '{file_path}' already exists. Use `override=True` to overwrite it.")

        # save the zip file to the specified path
        with open(file_path, "wb") as f:
            f.write(self.zip_file.fp.read())

    def _list_data_files(self) -> list[str]:
        """List all relevant BACI data files in the ZIP archive."""

        files = self.zip_file.namelist()

        # Filter for CSV files that start with "BACI" and hs version such a "BACI_HS22....csv"
        data_files = [f for f in files
                          if f.startswith(f"BACI_{self.hs_version}")
                          and f.endswith(".csv")
                          ]

        if not data_files:
            raise FileNotFoundError(
                f"No BACI data files found for HS version {self.hs_version}"
            )

        return data_files


    def read_data_files(self):
        """Read the data files from the ZIP
         file and combine them into a single PyArrow Table."""

        tables = [] # List to hold individual tables

        # From the list of data files, read each CSV into a PyArrow Table
        for file in self._list_data_files():
            with self.zip_file.open(file) as f:
                table = pv.read_csv(f)
                tables.append(table)

        # If no tables were read, raise an error
        if not tables:
            raise FileNotFoundError("No BACI data files found in the ZIP archive.")

        # Concatenate all tables into a single PyArrow Table
        combined = pa.concat_tables(tables)

        # format data
        combined = rename_data_columns(combined)

        self.data = combined

    def read_product_codes(self):
        """Read product codes from the ZIP file."""

        # Find the product codes file in the ZIP archive
        product_code_file = next((f for f in self.zip_file.namelist()
                                  if f.startswith("product_codes")), None)

        if not product_code_file:
            raise FileNotFoundError("No product codes file found in the ZIP file.")

        # Read the product codes CSV file into a DataFrame
        products = pd.read_csv(self.zip_file.open(product_code_file))
        products = rename_product_columns(products)
        self.product_codes = products

    def read_country_codes(self):
        """Read country codes from the ZIP file."""

        # Find the country codes file in the ZIP archive
        country_codes_file = next((f for f in self.zip_file.namelist()
                                   if f.startswith("country_codes")), None)

        if not country_codes_file:
            raise FileNotFoundError("No country codes file found in the ZIP file.")

        # Read the country codes CSV file into a DataFrame
        country_codes = pd.read_csv(self.zip_file.open(country_codes_file))
        country_codes = rename_country_columns(country_codes)
        self.country_codes = country_codes

    def read_metadata(self) -> None:
        """Read metadata from the Readme.txt file in the ZIP archive.
        """

        # Find the Readme.txt file in the ZIP archive
        readme_file = next((f for f in self.zip_file.namelist()
                            if f.startswith("Readme.txt")
                            and f.endswith(".txt"))
                           , None)

        if not readme_file:
            raise FileNotFoundError("No Readme.txt file found in the ZIP file.")

        with self.zip_file.open(readme_file) as f:
            readme_content = f.read().decode('utf-8')

        # Parse the Readme content to extract metadata
        metadata = parse_readme(readme_content)
        if not metadata:
            raise DataExtractionError("No metadata found in Readme.txt")

        self.metadata = metadata

    def load_data(self, zip_path: Path | None = None):
        """Extract, read, and process all BACI data files."""

        # Extract the ZIP file from the URL
        self.extract_zip_file(zip_path)

        # Read the main data files
        self.read_data_files()
        # Read country codes
        self.read_country_codes()
        # Read product codes
        self.read_product_codes()
        # Read metadata
        self.read_metadata()

        # TODO: Validation
