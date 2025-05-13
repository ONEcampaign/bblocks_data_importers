"""Test baci module"""

import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch
import zipfile
import io
import pandas as pd
import pyarrow as pa
import shutil
import re

from bblocks_data_importers.cepii.baci import BACI


# ---------- FIXTURES ---------- #

@pytest.fixture
def baci(tmp_path) -> BACI:
    """Fixture to initialize a BACI importer with a temporary path."""
    return BACI(data_path=tmp_path, baci_version="202401")


@pytest.fixture
def data_dir(tmp_path) -> Path:
    """Fixture to create a BACI data directory under tmp_path."""
    path = tmp_path / "BACI_HS22_V202401"
    path.mkdir(parents=True)
    yield path
    shutil.rmtree(path, ignore_errors=True)


@pytest.fixture
def raw_df() -> pd.DataFrame:
    """Fixture to generate a raw BACI DataFrame for testing."""
    return pd.DataFrame(
        {
            "t": [2022],
            "i": [250],
            "j": [276],
            "k": ["010101"],
            "v": [100.0],
            "q": [1.5],
        }
    )


# ---------- INIT ---------- #

def test_init_raises_if_path_does_not_exist(tmp_path):
    """Test that FileNotFoundError is raised when given a non-existent path."""
    with pytest.raises(FileNotFoundError):
        BACI(data_path=tmp_path / "not_here")


# ---------- VERSION ---------- #

@patch("bblocks_data_importers.cepii.baci.requests.get")
def test_get_latest_version_success(mock_get, tmp_path):
    """Test successful retrieval of latest BACI version from CEPII."""
    mock_get.return_value.status_code = 200
    mock_get.return_value.text = (
        "<div id='telechargement'><p>This is the 202401 version</p></div>"
    )
    baci = BACI(data_path=tmp_path, baci_version="latest")
    assert baci._baci_version == "202401"


@patch("bblocks_data_importers.cepii.baci.requests.get")
def test_get_latest_version_div_missing(mock_get, tmp_path):
    """Test that RuntimeError is raised if version div is missing."""
    mock_get.return_value.status_code = 200
    mock_get.return_value.text = "<div id='not_telechargement'></div>"
    with pytest.raises(RuntimeError, match="HTML object not present"):
        BACI(data_path=tmp_path, baci_version="latest")


@patch("bblocks_data_importers.cepii.baci.requests.get")
def test_get_latest_version_http_error(mock_get, tmp_path):
    """Test that RuntimeError is raised on HTTP error."""
    mock_get.return_value.status_code = 503
    with pytest.raises(RuntimeError, match="Error 503"):
        BACI(data_path=tmp_path, baci_version="latest")


@patch("bblocks_data_importers.cepii.baci.requests.get")
def test_get_latest_version_unparsable(mock_get, tmp_path):
    """Test that RuntimeError is raised on unparsable HTML version info."""
    mock_get.return_value.status_code = 200
    mock_get.return_value.text = "<div id='telechargement'><p>No version here</p></div>"
    with pytest.raises(RuntimeError, match="Latest BACI version not found"):
        BACI(data_path=tmp_path, baci_version="latest")


# ---------- DOWNLOAD ---------- #

@patch("bblocks_data_importers.cepii.baci.requests.get")
def test_download_and_extract_creates_file(mock_get, baci, data_dir):
    """Test that a CSV file is extracted after downloading the ZIP archive."""
    zip_buf = io.BytesIO()
    with zipfile.ZipFile(zip_buf, "w") as z:
        z.writestr("file.csv", "col1,col2\n1,2")
    zip_buf.seek(0)

    mock_get.return_value.status_code = 200
    mock_get.return_value.content = zip_buf.read()

    baci._data_directory = data_dir.name
    baci._download_and_extract()
    assert (data_dir / "file.csv").exists()


@patch("bblocks_data_importers.cepii.baci.requests.get")
def test_download_and_extract_http_error(mock_get, baci):
    """Test that RuntimeError is raised on HTTP error during download."""
    mock_get.return_value.status_code = 404
    baci._data_directory = "BACI_HS22_V202401"
    with pytest.raises(RuntimeError, match="Error 404"):
        baci._download_and_extract()


# ---------- LOAD DATA ---------- #

@patch("bblocks_data_importers.cepii.baci.load_parquet")
@patch("bblocks_data_importers.cepii.baci.DataFrameValidator")
def test_load_data_from_parquet(mock_validator, mock_loader, baci, tmp_path):
    """Test _load_data reads from existing parquet and formats correctly."""
    path = tmp_path / "BACI_HS22_V202401" / "parquet"
    path.mkdir(parents=True)
    (path / "mock.parquet").touch()

    baci._data_directory = Path("BACI_HS22_V202401")
    baci._data_path = tmp_path
    baci._include_country_names = True

    mock_loader.return_value = pd.DataFrame(
        {"t": [2022], "i": [1], "j": [2], "k": ["x"], "v": [1.0], "q": [1.0]}
    )
    baci._format_data = MagicMock(return_value=pd.DataFrame(columns=[
        "year",
        "exporter_iso3",
        "importer_iso3",
        "product_code",
        "value",
        "quantity",
        "exporter_name",
        "importer_name"
    ]))

    baci._load_data([2022])

    assert baci._loaded_years == [2022]
    baci._format_data.assert_called_once()
    args, kwargs = mock_validator().validate.call_args
    required = kwargs.get("required_cols", args[1] if len(args) > 1 else [])
    assert "exporter_name" in required
    assert "importer_name" in required

# ---------- FORMAT DATA ---------- #

def test_format_data_maps_iso_and_names(baci, tmp_path, raw_df):
    """Test that _format_data maps country codes to ISO3 and names if enabled."""
    baci._include_country_names = True
    baci._baci_version = "202401"
    baci._hs_version = "22"

    (tmp_path / "country_codes_V202401.csv").write_text(
        "country_code,country_iso3,country_name\n250,FRA,France\n276,DEU,Germany"
    )

    df = baci._format_data(raw_df, tmp_path)

    assert df.loc[0, "exporter_iso3"] == "FRA"
    assert df.loc[0, "importer_iso3"] == "DEU"
    assert df.loc[0, "exporter_name"] == "France"
    assert df.loc[0, "importer_name"] == "Germany"


def test_format_data_maps_only_iso(baci, tmp_path, raw_df):
    """Test that _format_data only maps to ISO3 when include_country_names is False."""
    baci._include_country_names = False
    baci._baci_version = "202401"
    baci._hs_version = "22"

    (tmp_path / "country_codes_V202401.csv").write_text(
        "country_code,country_iso3,country_name\n250,FRA,France\n276,DEU,Germany"
    )

    df = baci._format_data(raw_df, tmp_path)

    assert df.loc[0, "exporter_iso3"] == "FRA"
    assert df.loc[0, "importer_iso3"] == "DEU"
    assert "exporter_name" not in df.columns
    assert "importer_name" not in df.columns

@patch("bblocks_data_importers.cepii.baci.DataFrameValidator")
def test_load_data_triggers_download_if_missing(mock_validator, baci, tmp_path):
    """Test _load_data downloads data and formats it when parquet is missing."""
    extract_path = tmp_path / "BACI_HS22_V202401"
    extract_path.mkdir(parents=True)

    baci._data_directory = Path("BACI_HS22_V202401")
    baci._data_path = tmp_path
    baci._include_country_names = False

    baci._download_and_extract = MagicMock()
    baci._combine_data = MagicMock(return_value=pa.table({
        "t": [2022], "i": [1], "j": [2], "k": ["x"], "v": [1.0], "q": [1.0]
    }))
    baci._format_data = MagicMock(return_value="formatted")

    baci._load_data([2022])

    baci._download_and_extract.assert_called_once()
    baci._combine_data.assert_called_once()
    baci._format_data.assert_called_once()
    assert baci._data == "formatted"
    assert baci._loaded_years == [2022]

# ---------- COMBINE ---------- #

def test_combine_data_returns_table(baci, data_dir):
    """Test that _combine_data returns a valid PyArrow Table."""
    baci._data_directory = data_dir.name
    pd.DataFrame(
        {"t": [2022], "i": [1], "j": [2], "k": ["010101"], "v": [1.0], "q": [1.0]}
    ).to_csv(data_dir / "BACI.csv", index=False)

    table = baci._combine_data()
    assert isinstance(table, pa.Table)
    assert table.num_rows == 1
    assert table.num_columns == 6


def test_combine_data_raises_if_none_found(baci, data_dir):
    """Test that FileNotFoundError is raised if no CSV files are found."""
    baci._data_directory = data_dir.name
    with pytest.raises(FileNotFoundError, match="No BACI CSV files found"):
        baci._combine_data()

# ---------- GET DATA ---------- #

def test_get_data_triggers_load_and_sets_config(baci):
    """Test get_data initializes settings and loads if not cached."""
    baci._data = None
    baci._load_data = MagicMock()
    baci._data = "mocked_df"

    df = baci.get_data(hs_version="22", include_country_names=True, years=[2022])

    assert df == "mocked_df"
    assert baci._hs_version == "22"
    assert baci._include_country_names is True
    assert baci._data_directory == Path("BACI_HS22_V202401")
    baci._load_data.assert_called_once_with(filter_years=[2022])


def test_get_data_uses_cache_if_config_unchanged(baci):
    """Test get_data does not reload if data and config are cached."""
    baci._data = "cached"
    baci._hs_version = "22"
    baci._include_country_names = True
    baci._loaded_years = [2022]
    baci._load_data = MagicMock()

    df = baci.get_data(hs_version="22", include_country_names=True, years=[2022])
    assert df == "cached"
    baci._load_data.assert_not_called()


# ---------- METADATA ---------- #

def test_extract_metadata_warns_on_missing_readme(baci, tmp_path):
    """Test _extract_metadata sets empty metadata and warns if Readme.txt is missing."""
    extract_path = tmp_path / "BACI_HS22_V202401"
    parquet_dir = extract_path / "parquet"
    parquet_dir.mkdir(parents=True)
    (parquet_dir / "dummy.parquet").touch()

    baci._data_path = tmp_path
    baci._data_directory = Path("BACI_HS22_V202401")

    with patch("bblocks_data_importers.cepii.baci.logger") as mock_logger:
        baci._extract_metadata()
        assert baci._metadata == {}
        mock_logger.warning.assert_called_once()

def test_extract_metadata_raises_if_not_loaded(baci):
    """Test that RuntimeError is raised if data directory is not set."""
    baci._data_directory = None
    with pytest.raises(RuntimeError, match=re.escape("Run `get_data()` first")):
        baci._extract_metadata()

def test_extract_metadata_raises_if_completely_missing(baci, tmp_path):
    """Test _extract_metadata raises FileNotFoundError if no Readme or data found."""
    path = tmp_path / "BACI_HS22_V202401"
    path.mkdir()

    baci._data_path = tmp_path
    baci._data_directory = Path("BACI_HS22_V202401")

    with pytest.raises(FileNotFoundError):
        baci._extract_metadata()


def test_extract_metadata_parses_block(tmp_path):
    """Test _extract_metadata parses valid metadata blocks."""
    readme = tmp_path / "BACI_HS22_V202401" / "Readme.txt"
    readme.parent.mkdir(parents=True)
    readme.write_text(
        """
        Version: 202401\n
        Release Date: Jan 2024\n
        List of Variables:\n
        t: year\n
        i: exporter\n
        j: importer\n
        Source: CEPII\n
        """
    )

    baci = BACI(data_path=tmp_path, baci_version="202401")
    baci._data_directory = Path("BACI_HS22_V202401")
    baci._extract_metadata()

    assert baci._metadata["Version"] == "202401"
    assert baci._metadata["Release Date"] == "Jan 2024"
    assert baci._metadata["Source"] == "CEPII"


def test_get_metadata_triggers_extraction(tmp_path):
    """Test get_metadata triggers _extract_metadata when metadata is None."""
    readme = tmp_path / "BACI_HS22_V202401" / "Readme.txt"
    readme.parent.mkdir(parents=True)
    readme.write_text("Version: 202401")

    baci = BACI(data_path=tmp_path, baci_version="202401")
    baci._data_directory = Path("BACI_HS22_V202401")

    assert baci.get_metadata()["Version"] == "202401"
    assert isinstance(baci._metadata, dict)


# ---------- CACHE ---------- #

def test_clear_cache_resets_state(baci):
    """Test that clear_cache resets internal data and metadata."""
    baci._data = "dummy"
    baci._metadata = {"foo": "bar"}
    baci.clear_cache()
    assert baci._data is None
    assert baci._metadata is None


# ---------- PRODUCT DICT ---------- #

def test_get_product_dict_raises_if_not_loaded(baci):
    """Test that RuntimeError is raised if data directory is not set."""
    baci._data_directory = None
    with pytest.raises(RuntimeError, match=re.escape("Run `get_data()` first")):
        baci.get_product_dict()


def test_get_product_dict_returns_dict(baci, tmp_path):
    """Test that get_product_dict returns correct mapping of HS codes."""
    path = tmp_path / "BACI_HS22_V202401"
    path.mkdir()
    baci._data_directory = path.name
    baci._hs_version = "22"
    baci._baci_version = "202401"
    df = pd.DataFrame({"code": ["010101"], "description": ["test"]})
    df.to_csv(path / "product_codes_HS22_V202401.csv", index=False)

    baci._data_path = tmp_path
    result = baci.get_product_dict()
    assert result["010101"] == "test"


# ---------- DELETE LOCAL FILES ---------- #

def test_delete_local_files_removes_directory(baci, tmp_path):
    """Test that delete_local_files removes the expected directory."""
    path = tmp_path / "BACI_HS22_V202401"
    path.mkdir()
    baci._data_path = tmp_path
    baci._data_directory = path.name
    baci.delete_local_files()
    assert not path.exists()

def test_delete_local_files_raises_if_not_loaded(baci):
    """Test that RuntimeError is raised if data directory is not set."""
    baci._data_directory = None
    with pytest.raises(RuntimeError, match=re.escape("Run `get_data()` first")):
        baci.delete_local_files()

def test_delete_local_files_warns_if_missing(baci, tmp_path):
    """Test that delete_local_files logs info if directory does not exist."""
    baci._data_path = tmp_path
    baci._data_directory = "BACI_HS22_V202401"
    baci.delete_local_files()
