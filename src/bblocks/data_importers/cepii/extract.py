"""Extract BACI data

This module contains the functionality to extract, format, cache and query BACI data.
The `BaciDataManager` class is the main object for working with BACI data.
This object contains the functionality to extract, format, cache and query the data for a specific
BACI version and HS version. It is injected into the main BACI class, which is used to access the data.
Data is cached in a temporary directory as Parquet files for efficient querying, and PyArrow is used for data processing
of the main data files.
"""

from pathlib import Path
import os
import io
import zipfile
import pandas as pd
import tempfile
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from pyarrow import csv as pv
import requests

from bblocks.data_importers.config import logger, Fields, DataExtractionError


BASE_URL = "https://www.cepii.fr"


def filter_years(years: int | list[int] | range | tuple[int, int]) -> ds.Expression:
    """Create a PyArrow Expression to filter the dataset by year(s).
    Years can be specified as:
        - A single year as an int,
        - A list or range of years,
        - A 2-tuple (start_year, end_year) to filter a range of years.
    """

    if isinstance(years, int):
        expr = ds.field(Fields.year) == years
    elif isinstance(years, (list, range)):
        expr = ds.field(Fields.year).isin(list(years))
    elif isinstance(years, tuple) and len(years) == 2:
        start_year, end_year = years
        expr = (ds.field(Fields.year) >= start_year) & (
            ds.field(Fields.year) <= end_year
        )
    else:
        raise ValueError(f"Invalid type for years filter: {type(years)}")

    return expr


def filter_products(
    products: int | list[int] | range | tuple[int, int]
) -> ds.Expression:
    """Create a PyArrow Expression to filter the dataset by the specified product code(s).
    Products can be specified as:
        - A single product code as an int,
        - A list or range of product codes,
        - A 2-tuple (start_product_code, end_product_code) to filter a range of product codes.
    """

    if isinstance(products, int):
        expr = ds.field(Fields.product_code) == products
    elif isinstance(products, (list, range)):
        expr = ds.field(Fields.product_code).isin(list(products))
    elif isinstance(products, tuple) and len(products) == 2:
        start_prod, end_prod = products
        expr = (ds.field(Fields.product_code) >= start_prod) & (
            ds.field(Fields.product_code) <= end_prod
        )
    else:
        raise ValueError(f"Invalid type for products filter: {type(products)}")

    return expr


def rename_data_columns(table: pa.Table) -> pa.Table:
    """Rename BACI raw data PyArrow columns to standardized field names."""

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
    """Rename columns in the country codes DataFrame to standardized field names."""

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
    """Parse the Readme.txt content to extract metadata."""

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
            # Strip each line individually and join with space
            value_lines = [first_value_line] + lines[1:]
            value = " ".join(line.strip() for line in value_lines).strip()
            metadata[key] = value

    return metadata


def add_country_labels(data: pd.DataFrame, country_codes: pd.DataFrame) -> pd.DataFrame:
    """Adds country names and ISO3 codes to the BACI data DataFrame."""

    names_map = country_codes.set_index(Fields.country_code)[
        Fields.country_name
    ].to_dict()
    iso3_map = country_codes.set_index(Fields.country_code)[Fields.iso3_code].to_dict()

    return data.assign(
        **{
            Fields.exporter_name: lambda df: df[Fields.exporter_code].map(names_map),
            Fields.importer_name: lambda df: df[Fields.importer_code].map(names_map),
            Fields.exporter_iso3_code: lambda df: df[Fields.exporter_code].map(
                iso3_map
            ),
            Fields.importer_iso3_code: lambda df: df[Fields.importer_code].map(
                iso3_map
            ),
        }
    )


def add_product_descriptions(
    data: pd.DataFrame, product_codes: pd.DataFrame
) -> pd.DataFrame:
    """Adds product descriptions to the BACI data DataFrame."""

    descriptions_map = product_codes.set_index(Fields.product_code)[
        Fields.product_description
    ].to_dict()

    return data.assign(
        product_description=lambda df: df[Fields.product_code].map(descriptions_map)
    )


class BaciDataManager:
    """Manager class for handling BACI data extraction and processing.

    This class is responsible for downloading the BACI data ZIP file, extracting its contents,
    reading the main data files, and providing methods to query the data as a Pandas DataFrame.
    It also handles reading country and product codes, metadata, and available years.
    The data is cached in a temporary directory as Parquet files for efficient querying.
    Attributes:
        version: The BACI data version (e.g., "2023").
        hs_version: The HS version (e.g., "HS22").

        download_url: The URL to download the BACI data ZIP file.
            This is generated based on the version and HS version. It is not scraped from the website.
        zip_file: The ZIP file object containing the BACI data.
        arrow_temp_dir: Temporary directory for storing Parquet files.
        dataset: PyArrow dataset for efficient querying of the main data files.
        country_codes: DataFrame containing country codes and names.
        product_codes: DataFrame containing product codes and descriptions.
        metadata: Dictionary containing metadata extracted from the Readme.txt file.
        available_years: List of years for which data is available in the dataset.
    """

    def __init__(self, version: str, hs_version: str):
        self.version = version
        self.hs_version = hs_version

        self.download_url = (
            f"{BASE_URL}/DATA_DOWNLOAD/baci/data/BACI_{hs_version}_V{version}.zip"
        )
        self.zip_file: None | zipfile.ZipFile = None

        # Pyarrow dataset and temporary directory for Arrow files
        self.arrow_temp_dir: tempfile.TemporaryDirectory | None = None
        self.dataset: ds.Dataset | None = None

        # other data attributes
        self.country_codes: None | pd.DataFrame = None
        self.product_codes: None | pd.DataFrame = None
        self.metadata: None | dict = None
        self.available_years: list[int] | None = None

    def extract_zipfile_from_web(self) -> None:
        """Extract the BACI ZIP file from the download URL."""

        try:
            logger.info(
                f"Downloading BACI data for version {self.version} and HS version {self.hs_version}"
            )
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

    def save_zip_file(self, path: str | os.PathLike, override: bool = False) -> None:
        """Save the zip file to a local path.

        Args:
            path: The path where the ZIP file should be saved.
            override: If True, will overwrite the existing file if it exists. Defaults to False.
        """

        target = Path(path)
        directory = target.parent

        # Verify path ends with .zip
        if target.suffix.lower() != ".zip":
            raise ValueError(
                f"The path must include a file name with a .zip extension: {target.name}"
            )

        # Verify directory exists
        if not directory.exists():
            raise FileNotFoundError(f"Directory {directory} does not exist.")

        # Check for existing file
        if target.exists() and not override:
            raise FileExistsError(
                f"File '{target}' already exists. Use `override=True` to overwrite it."
            )

        # Write ZIP contents to disk
        with open(target, "wb") as f:
            self.zip_file.fp.seek(
                0
            )  # Ensure we read from the start of the BytesIO object
            f.write(self.zip_file.fp.read())

        logger.info(
            f"Data for BACI version {self.version} and HS version {self.hs_version} saved to {target}"
        )

    def _list_data_files(self) -> list[str]:
        """List all relevant BACI data files in the ZIP archive."""

        files = self.zip_file.namelist()

        # Filter for CSV files that start with "BACI" and hs version such a "BACI_HS22....csv"
        data_files = [
            f
            for f in files
            if f.startswith(f"BACI_{self.hs_version}") and f.endswith(".csv")
        ]

        if not data_files:
            raise FileNotFoundError(
                f"No BACI data files found for HS version {self.hs_version}"
            )

        return data_files

    def read_data_files(self):
        """Stream and write BACI data to disk in Parquet format."""

        logger.info(
            f"Streaming BACI data files to Parquet in temporary cache directory"
        )

        self.arrow_temp_dir = tempfile.TemporaryDirectory()
        parquet_dir = Path(self.arrow_temp_dir.name)

        for file in self._list_data_files():
            with self.zip_file.open(file) as f:
                # Read CSV file into a PyArrow table and handle empty data files
                try:
                    table = pv.read_csv(f)
                except pa.ArrowInvalid:
                    logger.warning(f"Skipping empty data file: {file}")
                    continue

                table = rename_data_columns(table)

                output_file = parquet_dir / f"{Path(file).stem}.parquet"
                pq.write_table(table, output_file)

        # Load entire directory as a dataset for efficient filtering
        self.dataset = ds.dataset(str(parquet_dir), format="parquet")

    def read_product_codes(self):
        """Read product codes from the ZIP file."""
        product_code_file = next(
            (f for f in self.zip_file.namelist() if f.startswith("product_codes")), None
        )

        if not product_code_file:
            raise FileNotFoundError("No product codes file found in the ZIP file.")

        # Read the product codes CSV file into a DataFrame and handle empty data
        try:
            products = pd.read_csv(self.zip_file.open(product_code_file))
        except pd.errors.EmptyDataError:
            products = pd.DataFrame()

        products = rename_product_columns(products)
        self.product_codes = products

    def read_country_codes(self):
        """Read country codes from the ZIP file."""

        # Find the country codes file in the ZIP archive
        country_codes_file = next(
            (f for f in self.zip_file.namelist() if f.startswith("country_codes")), None
        )

        if not country_codes_file:
            raise FileNotFoundError("No country codes file found in the ZIP file.")

        # Read the country codes CSV file into a DataFrame and handle empty data
        try:
            country_codes = pd.read_csv(self.zip_file.open(country_codes_file))
        except pd.errors.EmptyDataError:
            country_codes = pd.DataFrame()

        country_codes = rename_country_columns(country_codes)
        self.country_codes = country_codes

    def read_metadata(self) -> None:
        """Read metadata from the Readme.txt file in the ZIP archive."""

        # Find the Readme.txt file in the ZIP archive
        readme_file = next(
            (
                f
                for f in self.zip_file.namelist()
                if f.startswith("Readme.txt") and f.endswith(".txt")
            ),
            None,
        )

        if not readme_file:
            raise FileNotFoundError("No Readme.txt file found in the ZIP file.")

        with self.zip_file.open(readme_file) as f:
            readme_content = f.read().decode("utf-8")

        # Parse the Readme content to extract metadata
        metadata = parse_readme(readme_content)
        if not metadata:
            raise DataExtractionError("No metadata found in Readme.txt")

        self.metadata = metadata

    def set_available_years(self) -> None:
        """Inspect the loaded dataset and populate the available_years attribute."""

        # Scan only the year column
        scanner = self.dataset.scanner(columns=[Fields.year])
        table = scanner.to_table()
        # Convert to pandas to extract unique years
        df_years = table.to_pandas(types_mapper=pd.ArrowDtype)
        self.available_years = sorted(df_years[Fields.year].unique().tolist())

    def load_data(self):
        """Extract, read, and process all BACI data files."""

        # Extract the ZIP file from the URL
        self.extract_zipfile_from_web()

        logger.info(f"Extracting data")
        # Read the main data files
        self.read_data_files()
        # Read country codes
        self.read_country_codes()
        # Read product codes
        self.read_product_codes()
        # Read metadata
        self.read_metadata()

        # Set available years based on the dataset
        self.set_available_years()

    def get_data_frame(
        self,
        years: int | list[int] | range | tuple[int, int] | None,
        products: int | None,
        incl_country_labels: bool,
        incl_product_labels: bool,
    ) -> pd.DataFrame:
        """Get the BACI data as a Pandas DataFrame.

        years: Years to filter the data. Default is None. Options include:
            - A single year as an int,
            - Any of a list/range of ints,
            - Or falls between start/end in a 2-tuple.

        Returns:
            A Pandas DataFrame containing the (filtered) BACI data.
        """

        filters = []  # List to hold filter expressions

        # Filter year
        if years is not None:
            filters.append(filter_years(years))

        # TODO: exporter filtering
        # TODO: importer filtering
        # Filter products
        if products is not None:
            filters.append(filter_products(products))

        # Combine all filters
        combined_filter = None
        for expr in filters:
            combined_filter = (
                expr if combined_filter is None else combined_filter & expr
            )

        scanner = (
            self.dataset.scanner(filter=combined_filter)
            if combined_filter is not None
            else self.dataset.scanner()
        )

        df = scanner.to_table().to_pandas(types_mapper=pd.ArrowDtype)

        # Add labels if requested
        if incl_country_labels:
            df = add_country_labels(df, self.country_codes)

        if incl_product_labels:
            df = add_product_descriptions(df, self.product_codes)

        return df

    def __del__(self):
        """Cleanup resources when the object is deleted."""
        if self.arrow_temp_dir:
            self.arrow_temp_dir.cleanup()
