"""
Helper functions for importing and processing the CEPII BACI trade database.

This module includes:
- Web scraping to identify available BACI versions and supported HS classifications.
- Utilities to download, extract, and process BACI datasets.
- Conversion of raw CSV data into a standardized, Parquet-based format for efficient disk use.
"""

import io

import bs4
import pandas as pd
import zipfile

import requests
from bs4 import BeautifulSoup
import re

from pyarrow import dataset as ds
from pathlib import Path
import pyarrow as pa
import pyarrow.csv as pv

from bblocks.data_importers.config import logger, Fields


# ---------------------------
# Web scraping & version logic
# ---------------------------

BASE_CEPII_URL = "https://www.cepii.fr"
BACI_URL = BASE_CEPII_URL + "/CEPII/en/bdd_modele/bdd_modele_item.asp?id=37"


def _get_soup() -> BeautifulSoup:
    """Request the CEPII BACI page and return a BeautifulSoup object."""

    try:
        logger.debug(f"Fetching soup for BACI page")
        response = requests.get(BACI_URL)
        response.raise_for_status()
    except requests.RequestException as e:
        raise RuntimeError(f"Failed to fetch BACI page: {e}")

    return BeautifulSoup(response.content, "html.parser")


def _extract_section_div(soup: BeautifulSoup, section_title: str) -> bs4.Tag:
    """Extract the contents of a specific section <div> from the BACI page.

    """
    # get a list of all section divs
    section_divs = soup.find_all("div", {"class": "titre-rubrique"})

    # loop through the divs to find the one with the matching title
    for div in section_divs:
        if div.get_text() == section_title:
            section_div = div

            # return the parent div that contains the section content
            return section_div.parent
    else:
        raise ValueError(f"Section '{section_title}' not found in BACI page.")


def _parse_latest_version(latest_div: bs4.Tag) -> dict:
    """Get the latest BACI version"""

    # Extract the div containing the latest version and its HS versions
    # div = _extract_section_div(soup, "Download")

    # Get the latest version from the div text
    match = re.search(r'This is the\s+([A-Za-z0-9]+)\s+version', latest_div.text, re.IGNORECASE)
    if match:
        version = match.group(1)
    else:
        raise ValueError("Latest version could not be found")

    # Extract all HS versions from the div text
    hs_versions = re.findall(r'\bHS\d{2}\b', latest_div.text)

    if not hs_versions:
        raise ValueError("No HS versions found in the latest version section.")

    return {version: {"hs_versions": hs_versions, "latest": True}}


def _parse_archive_versions(archive_div: bs4.Tag) -> dict[int, list[str]]:
    """ """

    # Step 1: Split by version headers
    blocks = re.split(r'\n+(?=\d{6}[a-z]?\s+version:)', archive_div.text)

    # Step 2: Parse each block
    version_dict = {}

    for block in blocks:
        header_match = re.match(r'(\d{6}[a-z]?)\s+version:', block)
        if header_match:
            version = header_match.group(1)
            hs_versions = re.findall(r'\bHS\d{2}\b', block)
            version_dict[version] = {"hs_versions": hs_versions}

    if not version_dict:
        raise ValueError("No archive BACI versions found")

    return version_dict


def parse_baci_and_hs_versions() -> dict:
    """Parse version declarations and associated HS versions.
    """

    soup = _get_soup()

    # get the latest version and hs versions
    latest_div = _extract_section_div(soup, "Download")
    latest_version = _parse_latest_version(latest_div)

    # get the archive versions and hs versions
    archive_div = _extract_section_div(soup, "Archives")
    archive_versions = _parse_archive_versions(archive_div)

    return {**latest_version, **archive_versions}




#
#
#
#
# def get_available_versions(
#     url: str = BACI_URL,
# ) -> dict[str, dict[str, list[int] or bool]]:
#     """Orchestrate the process of parsing BACI page, extracting the divs with the BACI and HS versions and populate a
#     dictionary with the information for easy access.
#
#     Args:
#         url(str): URL of the CEPII BACI dataset page.
#
#     Returns:
#         dict: Dictionary mapping BACI to HS versions and latest flag.
#     """
#     html = _get_soup(url)
#     text_latest = _extract_section_div(html, div_id="telechargement")
#     text_other = _extract_section_div(html, div_id="version")
#
#     return _parse_baci_and_hs_versions(text_latest + text_other)
#
#
# # ---------------------------
# # File extraction and cleanup
# # ---------------------------
#
#
# def extract_zip(zip_data: io.BytesIO, extract_path: Path):
#     """Extract ZIP archive contents to target folder.
#
#     Args:
#         zip_data (io.BytesIO): ZIP file in memory.
#         extract_path (Path): Destination path to extract files into
#     """
#     with zipfile.ZipFile(zip_data) as zip_ref:
#         zip_ref.extractall(path=extract_path)
#
#
# def cleanup_csvs(path: Path):
#     """Delete all BACI CSV files in a directory.
#
#     Args:
#         path (Path): Directory containing extracted CSV files.
#     """
#     for f in path.glob("BACI*.csv"):
#         f.unlink()
#
#
# # ---------------------------
# # Data transformation
# # ---------------------------
#
#
# def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
#     """Rename BACI raw columns to standardized field names.
#
#     Args:
#         df (pd.DataFrame): Raw BACI DataFrame.
#
#     Returns:
#         DataFrame with renamed columns.
#     """
#     return df.rename(
#         columns={
#             "t": Fields.year,
#             "i": Fields.exporter_code,
#             "j": Fields.importer_code,
#             "k": Fields.product_code,
#             "v": Fields.value,
#             "q": Fields.quantity,
#         }
#     )
#
#
# def map_country_codes(
#     df: pd.DataFrame, country_codes_df: pd.DataFrame, include_names: bool = False
# ) -> pd.DataFrame:
#     """Map exporter/importer codes to ISO3 and optionally country names.
#
#     Args:
#         df (pd.DataFrame): Input BACI DataFrame.
#         country_codes_df (pd.DataFrame): Mapping of country_code -> ISO3 / name.
#         include_names (bool): If True, include country name columns.
#
#     Returns:
#         DataFrame with mapped ISO3 codes and names.
#     """
#     iso3_map = dict(
#         zip(country_codes_df["country_code"], country_codes_df["country_iso3"])
#     )
#     df[Fields.exporter_iso3_code] = df[Fields.exporter_code].map(iso3_map)
#     df[Fields.importer_iso3_code] = df[Fields.importer_code].map(iso3_map)
#
#     if include_names:
#         name_map = dict(
#             zip(country_codes_df["country_code"], country_codes_df["country_name"])
#         )
#         df[Fields.exporter_name] = df[Fields.exporter_code].map(name_map)
#         df[Fields.importer_name] = df[Fields.importer_code].map(name_map)
#
#     return df
#
#
# def organise_columns(df: pd.DataFrame) -> pd.DataFrame:
#     """Reorder and drop unnecessary columns in the final output.
#
#     Args:
#         df (pd.DataFrame): DataFrame to format
#
#     Returns
#         DataFrame with organized columns.
#     """
#     ordered_columns = [
#         Fields.year,
#         Fields.exporter_iso3_code,
#         Fields.exporter_name,
#         Fields.importer_iso3_code,
#         Fields.importer_name,
#         Fields.product_code,
#         Fields.value,
#         Fields.quantity,
#     ]
#
#     existing_cols = [col for col in ordered_columns if col in df.columns]
#
#     df = df.drop(columns=[Fields.exporter_code, Fields.importer_code])[existing_cols]
#
#     return df
#
#
# # ---------------------------
# # Data I/O
# # ---------------------------
#
#
# def combine_data(path):
#     """
#     Combine BACI CSV files for multiple years into a single PyArrow Table.
#
#     Args:
#         path (Path): Directory containing raw BACI CSVs.
#
#     Returns:
#         pyarrow.Table: Consolidated BACI data.
#     """
#     logger.info("Building consolidated dataset")
#     tables = []
#
#     column_types = {
#         "t": pa.int16(),
#         "i": pa.int32(),
#         "j": pa.int32(),
#         "k": pa.string(),
#         "v": pa.float32(),
#         "q": pa.float32(),
#     }
#
#     for csv_path in path.glob("BACI*.csv"):
#         table = pv.read_csv(
#             csv_path,
#             read_options=pv.ReadOptions(autogenerate_column_names=False),
#             convert_options=pv.ConvertOptions(column_types=column_types),
#         )
#         tables.append(table)
#
#     if not tables:
#         raise FileNotFoundError("No BACI CSV files found in data directory.")
#
#     return pa.concat_tables(tables)
#
#
# def save_parquet(table: pa.Table, path: Path):
#     """
#     Save the provided PyArrow Table to partitioned Parquet format by year.
#
#     Args:
#         table (pa.Table): Consolidated trade data.
#         path (Path): Destination directory for Parquet files.
#     """
#     logger.info(f"Saving consolidated BACI dataset to {path}")
#
#     ds.write_dataset(
#         data=table,
#         base_dir=str(path),
#         format="parquet",
#         partitioning=["t"],
#         existing_data_behavior="overwrite_or_ignore",
#     )
#
#
# def load_parquet(
#     parquet_dir: Path, filter_years: set[int] | None = None
# ) -> pd.DataFrame:
#     """
#     Load partitioned Parquet data into a pandas DataFrame, optionally filtered by year.
#
#     Args:
#         parquet_dir (Path): Directory containing Parquet partitions.
#         filter_years (set[int] | None): Years to filter.
#
#     Returns:
#         pd.DataFrame
#     """
#     dataset = ds.dataset(str(parquet_dir), format="parquet", partitioning=["t"])
#
#     if filter_years:
#         logger.info(f"Filtering for years: {filter_years}")
#         filter_expr = ds.field("t").isin(filter_years)
#         table = dataset.to_table(filter=filter_expr)
#     else:
#         table = dataset.to_table()
#
#     return table.to_pandas(types_mapper=pd.ArrowDtype)
#
#
# # ---------------------------
# # Metadata
# # ---------------------------
#
#
# def generate_metadata(path: Path) -> dict:
#     """
#     Extract metadata blocks from the BACI Readme.txt file.
#
#     Args:
#         path (Path): Path to Readme.txt.
#
#     Returns:
#         dict: Metadata key-value pairs.
#     """
#     with open(path, encoding="utf-8") as file:
#         content = file.read()
#
#     blocks = [block.strip() for block in content.split("\n\n") if block.strip()]
#     metadata = {}
#
#     for block in blocks:
#         if block.startswith("List of Variables:"):
#             continue
#         lines = block.splitlines()
#         if ":" in lines[0]:
#             key, first_value_line = lines[0].split(":", 1)
#             key = key.strip()
#             value_lines = [first_value_line.strip()] + [
#                 line.strip() for line in lines[1:]
#             ]
#             metadata[key] = " ".join(value_lines)
#
#     return metadata
#
#
# # ---------------------------
# # Validation
# # ---------------------------
#
#
# def validate_years(years: int | list[int] | set[int] | range | None) -> set[int] | None:
#     """Validate and normalize the years passed onto `get_data()`.
#
#     Accepts a single integer, list, set, range, or None, and returns a set of unique integer years.
#     Raises a TypeError if the input type is unsupported, or a ValueError if any element is not an integer.
#
#     Args:
#         years (int | list[int] | set[int] | range | None): Year(s) to validate and normalize.
#
#     Returns:
#         set[int] | None: A set of valid integer years, or None if no filtering is requested.
#     """
#     if years is None:
#         return None
#
#     # Normalize to a set
#     if isinstance(years, int):
#         years = {years}
#     elif isinstance(years, (list, range, set)):
#         years = set(years)
#     else:
#         raise TypeError(
#             f"`years` must be int, list[int], set[int], range, or None — not {type(years)}"
#         )
#
#     # Validate contents
#     if not all(isinstance(y, int) for y in years):
#         raise ValueError("All `years` entries must be integers.")
#
#     return years
#
#
# def verify_years(parquet_dir: Path, filter_years: set[int] | None) -> set[int] | None:
#     """Verify that the requested years are available in the partitioned data.
#
#     Args:
#         parquet_dir (Path): Path to directory containing year-partitioned data.
#         filter_years (set[int] | None): Requested year filters.
#
#     Returns:
#         set[int] | None: Cleaned set of valid years, or None to disable filtering.
#     """
#     available_years = {
#         int(p.name) for p in parquet_dir.iterdir() if p.is_dir() and p.name.isdigit()
#     }
#
#     if filter_years is not None and not set(filter_years).issubset(available_years):
#         logger.warning(
#             f"Provided years %s are out of range. Will return all available years.",
#             filter_years,
#         )
#         return None
#
#     return set(filter_years) if filter_years is not None else None
