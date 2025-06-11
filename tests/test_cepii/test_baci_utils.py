"""Test baci_utils module for BACI utilities."""

import pytest
from unittest.mock import patch, Mock
import requests
import pandas as pd
import pyarrow as pa
from pathlib import Path
import shutil
import io
import zipfile
import tempfile

from bblocks.data_importers.config import Fields
from bblocks.data_importers.cepii.baci_utils import (
    fetch_baci_page,
    extract_div,
    parse_baci_and_hs_versions,
    BACI_URL,
    get_available_versions,
    extract_zip,
    rename_columns,
    map_country_codes,
    organise_columns,
    combine_data,
    save_parquet,
    load_parquet,
    cleanup_csvs,
    generate_metadata,
    validate_years,
    verify_years,
)

# ---------------------------
# Web scraping & version logic
# ---------------------------

# Mock HTML with valid information
HTML_WITH_DIVS = """
<html>
  <body>
    <div id="telechargement">
      This is the 202501 version.
        <li>HS22</li> 
        <li>HS17</li> 
        <li>HS12</li>
    </div>
    <div id="version">
      202401b version: 
      <ul>
        <li>HS17</li> 
        <li>HS12</li>
      </ul>
    </div>
  </body>
</html>
"""

# Mock HTML with missing information
HTML_MISSING_DIV = "<html><body><div id='other'>No BACI info here</div></body></html>"


@pytest.fixture
def html_text():
    """Fixture to return valid HTML."""
    return HTML_WITH_DIVS


@patch("bblocks.data_importers.cepii.baci.requests.get")
def test_fetch_baci_page_success(mock_get):
    """Test BACI page is fetched correctly."""
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.text = "<html>test</html>"
    mock_get.return_value = mock_response

    content = fetch_baci_page(BACI_URL)
    assert "<html>" in content


@patch("bblocks.data_importers.cepii.baci_utils.requests.get")
def test_fetch_baci_page_failure(mock_get):
    """Test HTTPError is raised when BACI page fetch fails."""
    mock_response = Mock()
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
        "404 Client Error"
    )
    mock_get.return_value = mock_response

    with pytest.raises(requests.exceptions.HTTPError, match="404 Client Error"):
        fetch_baci_page(BACI_URL)


def test_extract_div_success(html_text):
    """Test divs are extracted correctly."""
    result = extract_div(html_text, div_id="telechargement")
    assert "This is the 202501 version" in result


def test_extract_div_missing():
    """Test error is raised if version is missing."""
    with pytest.raises(RuntimeError, match="Latest BACI version not found"):
        extract_div(HTML_MISSING_DIV, div_id="telechargement")


def test_parse_baci_and_hs_versions(html_text):
    """Test that HTML is parsed correctly and versions are outputted as dict."""
    parsed = parse_baci_and_hs_versions(html_text)
    assert "202501" in parsed
    assert parsed["202501"]["latest"] is True
    assert set(parsed["202501"]["hs"]) == {"22", "17", "12"}

    assert "202401b" in parsed
    assert parsed["202401b"]["latest"] is False
    assert set(parsed["202401b"]["hs"]) == {"17", "12"}


@patch("bblocks.data_importers.cepii.baci_utils.fetch_baci_page")
def test_get_available_versions(mock_fetch):
    """Test that versions are returned correctly."""
    mock_fetch.return_value = HTML_WITH_DIVS
    versions = get_available_versions()

    assert "202501" in versions
    assert "202401b" in versions
    assert versions["202501"]["latest"] is True
    assert "22" in versions["202501"]["hs"]


# ---------------------------
# File extraction and cleanup
# ---------------------------


def create_in_memory_zip(filename: str, content: str) -> io.BytesIO:
    """Helper to create a ZIP archive in memory with one file."""
    mem_zip = io.BytesIO()
    with zipfile.ZipFile(mem_zip, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(filename, content)
    mem_zip.seek(0)
    return mem_zip


def test_extract_zip_extracts_file_correctly():
    """Test that zip file is extracted correctly."""
    zip_data = create_in_memory_zip("test.txt", "hello world")

    with tempfile.TemporaryDirectory() as tmpdir:
        extract_path = Path(tmpdir)
        extract_zip(zip_data, extract_path)

        extracted_file = extract_path / "test.txt"
        assert extracted_file.exists()
        assert extracted_file.read_text() == "hello world"


def test_cleanup_csvs_removes_only_matching(tmp_path):
    """Test that `cleanup_csvs()` removes only CSV files starting with BACI."""
    csv1 = tmp_path / "BACI_2022.csv"
    csv2 = tmp_path / "BACI_2023.csv"
    other = tmp_path / "unrelated.csv"

    for file in [csv1, csv2, other]:
        file.write_text("dummy")

    cleanup_csvs(tmp_path)

    assert not csv1.exists()
    assert not csv2.exists()
    assert other.exists()


# ---------------------------
# Data transformation
# ---------------------------


@pytest.fixture
def raw_baci_df():
    """Fixture to return a raw BACI dataframe."""
    return pd.DataFrame(
        {"t": [2022], "i": [250], "j": [840], "k": [123456], "v": [1000], "q": [50]}
    )


@pytest.fixture
def country_codes_df():
    """Fixture to return a dataframe of country codes."""
    return pd.DataFrame(
        {
            "country_code": [250, 840],
            "country_iso3": ["FRA", "USA"],
            "country_name": ["France", "United States"],
        }
    )


def test_rename_columns(raw_baci_df):
    """Test that columns are renamed correctly."""
    df = rename_columns(raw_baci_df)
    expected_cols = {
        Fields.year,
        Fields.exporter_code,
        Fields.importer_code,
        Fields.product_code,
        Fields.value,
        Fields.quantity,
    }
    assert set(df.columns) == expected_cols


def test_map_country_codes_without_names(raw_baci_df, country_codes_df):
    """Test that country codes are mapped correctly to ISO-3."""
    df = rename_columns(raw_baci_df)
    df = map_country_codes(df, country_codes_df, include_names=False)

    assert Fields.exporter_iso3_code in df.columns
    assert Fields.importer_iso3_code in df.columns
    assert df.loc[0, Fields.exporter_iso3_code] == "FRA"
    assert df.loc[0, Fields.importer_iso3_code] == "USA"
    assert Fields.exporter_name not in df.columns
    assert Fields.importer_name not in df.columns


def test_map_country_codes_with_names(raw_baci_df, country_codes_df):
    """Test that country codes are mapped correctly to names."""
    df = rename_columns(raw_baci_df)
    df = map_country_codes(df, country_codes_df, include_names=True)

    assert Fields.exporter_name in df.columns
    assert Fields.importer_name in df.columns
    assert df.loc[0, Fields.exporter_name] == "France"
    assert df.loc[0, Fields.importer_name] == "United States"


def test_organise_columns(raw_baci_df, country_codes_df):
    """Test that columns are rearranged correctly."""
    df = rename_columns(raw_baci_df)
    df = map_country_codes(df, country_codes_df, include_names=True)
    df = organise_columns(df)

    expected_order = [
        Fields.year,
        Fields.exporter_iso3_code,
        Fields.exporter_name,
        Fields.importer_iso3_code,
        Fields.importer_name,
        Fields.product_code,
        Fields.value,
        Fields.quantity,
    ]
    assert df.columns.tolist() == expected_order
    assert Fields.exporter_code not in df.columns
    assert Fields.importer_code not in df.columns


# ---------------------------
# Data I/O
# ---------------------------


@pytest.fixture
def output_dir(tmp_path) -> Path:
    """Fixture to return a temporary output directory."""
    path = tmp_path / "output"
    path.mkdir()
    yield path
    shutil.rmtree(path, ignore_errors=True)


@pytest.fixture
def mock_baci_csvs(tmp_path: Path):
    """Fixture to return mock CSV files."""
    # Create 2 synthetic CSVs for different years
    df_2020 = pd.DataFrame(
        {"t": [2020], "i": [250], "j": [840], "k": ["0101"], "v": [1000.0], "q": [50.0]}
    )
    df_2021 = pd.DataFrame(
        {"t": [2021], "i": [250], "j": [840], "k": ["0102"], "v": [2000.0], "q": [75.0]}
    )

    for df, year in [(df_2020, 2020), (df_2021, 2021)]:
        file_path = tmp_path / f"BACI_{year}.csv"
        df.to_csv(file_path, index=False)

    return tmp_path


@pytest.fixture
def baci_table() -> pa.Table:
    """Fixture to create a reusable PyArrow Table for testing."""
    return pa.table(
        {
            "t": pa.array(
                [2022, 2023, 2023], type=pa.int16()
            ),  # use consistent types for partitioning
            "i": [1, 2, 3],
            "j": [4, 5, 6],
            "k": ["010101", "020202", "030303"],
            "v": [1.0, 2.0, 3.0],
            "q": [0.5, 0.7, 0.9],
        }
    )


def test_combine_data(mock_baci_csvs):
    """Test that CSVs are combined correctly."""
    table = combine_data(mock_baci_csvs)
    assert isinstance(table, pa.Table)
    assert table.num_rows == 2
    assert set(table.column_names) == {"t", "i", "j", "k", "v", "q"}


def test_combine_data_no_files(tmp_path):
    """Test that error is raised if no CSVs are found in directory"""
    with pytest.raises(FileNotFoundError, match="No BACI CSV files found"):
        combine_data(tmp_path)


def test_save_parquet_creates_partitioned_files(baci_table, output_dir):
    """Test that save_parquet creates partitioned parquet files."""
    save_parquet(baci_table, output_dir)

    # Check that partitioned folders exist
    assert (output_dir / "2022").exists()
    assert (output_dir / "2023").exists()
    assert any(output_dir.rglob("*.parquet"))


@pytest.mark.parametrize(
    "filter_years,expected_years,expected_len",
    [
        (None, [2022, 2023], 3),
        ([2023], [2023], 2),
    ],
)
def test_load_parquet_filters_correctly(
    baci_table, output_dir, filter_years, expected_years, expected_len
):
    """Test load_parquet behavior with and without year filtering."""
    save_parquet(baci_table, output_dir)
    df = load_parquet(output_dir, filter_years)
    assert df["t"].unique().tolist() == expected_years
    assert len(df) == expected_len


# ---------------------------
# Metadata
# ---------------------------


@pytest.fixture
def sample_readme(tmp_path: Path) -> Path:
    """Fixture to return a sample readme file."""
    content = """
        Version: 202401\n
        Release Date: Jan 2024\n
        List of Variables:\n
        t: year\n
        i: exporter\n
        j: importer\n
        Source: CEPII\n
        """

    readme_path = tmp_path / "Readme.txt"
    readme_path.write_text(content, encoding="utf-8")
    return readme_path


def test_generate_metadata_parses_key_value_blocks(sample_readme):
    """Test that metadata dictionary is created correctly."""
    metadata = generate_metadata(sample_readme)

    assert isinstance(metadata, dict)
    assert "Version" in metadata
    assert "Release Date" in metadata
    assert "Source" in metadata
    assert "List of Variables" not in metadata  # Explicitly skipped

    assert metadata["Version"] == "202401"
    assert metadata["Release Date"] == "Jan 2024"
    assert metadata["Source"] == "CEPII"


# ---------------------------
# Validation
# ---------------------------


@pytest.mark.parametrize(
    "input_years,expected",
    [
        (2022, {2022}),
        ([2022, 2023], {2022, 2023}),
        ({2021, 2022}, {2021, 2022}),
        (range(2020, 2023), {2020, 2021, 2022}),
        ([2022, 2022, 2023], {2022, 2023}),
        (None, None),
    ],
)
def test_validate_years_valid_inputs(input_years, expected):
    assert validate_years(input_years) == expected


@pytest.mark.parametrize(
    "bad_input",
    [
        "2022",  # string
        2022.0,  # float
        ["2022", 2023],  # list with string
        [2022, 2023.0],  # list with float
        object(),  # completely invalid type
    ],
)
def test_validate_years_invalid_types(bad_input):
    with pytest.raises((TypeError, ValueError)):
        validate_years(bad_input)


@pytest.fixture
def mock_parquet_dir(tmp_path: Path) -> Path:
    """Fixture to return a mock partitioned parquet directory by year."""
    for year in ["2019", "2020", "2021"]:
        (tmp_path / year).mkdir()

    # Include some invalid items (non-year dirs, files)
    (tmp_path / "README.md").write_text("ignore me")
    (tmp_path / "notayear").mkdir()

    return tmp_path


@pytest.mark.parametrize(
    "requested,expected",
    [
        ({2019}, {2019}),
        ({2019, 2021}, {2019, 2021}),
        (None, None),
    ],
)
def test_verify_years_accepts_valid_years(mock_parquet_dir, requested, expected):
    """Test that `verify_years()` accepts valid requested year sets."""
    assert verify_years(mock_parquet_dir, requested) == expected


def test_verify_years_returns_none_for_invalid_years(mock_parquet_dir, caplog):
    """Test that `verify_years()` ignores filter if years are our of range."""
    result = verify_years(mock_parquet_dir, {2018, 2022})
    assert result is None
    assert any(
        "Will return all available years." in message for message in caplog.messages
    )
