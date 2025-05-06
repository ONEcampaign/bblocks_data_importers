"""Test baci module"""

import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch
import re
import zipfile
import io
import pandas as pd
import pyarrow as pa

from bblocks_data_importers.cepii.baci import BACI


# ---------- FIXTURES ---------- #

@pytest.fixture
def baci(tmp_path) -> BACI:
    return BACI(data_path=tmp_path, baci_version="202401")

@pytest.fixture
def data_dir(tmp_path) -> Path:
    path = tmp_path / "BACI_HS22_V202401"
    path.mkdir(parents=True)
    return path

@pytest.fixture
def raw_df() -> pd.DataFrame:
    return pd.DataFrame({
        "t": [2022],
        "i": [250],
        "j": [276],
        "k": ["010101"],
        "v": [100.0],
        "q": [1.5],
    })


# ---------- INIT ---------- #

def test_init_raises_if_path_does_not_exist(tmp_path):
    with pytest.raises(FileNotFoundError):
        BACI(data_path=tmp_path / "not_here")


# ---------- VERSION ---------- #

@patch("bblocks_data_importers.cepii.baci.requests.get")
def test_get_latest_version_success(mock_get, tmp_path):
    mock_get.return_value.status_code = 200
    mock_get.return_value.text = "<div id='telechargement'><p>This is the 202401 version</p></div>"
    baci = BACI(data_path=tmp_path, baci_version="latest")
    assert baci._baci_version == "202401"

@patch("bblocks_data_importers.cepii.baci.requests.get")
def test_get_latest_version_div_missing(mock_get, tmp_path):
    mock_get.return_value.status_code = 200
    mock_get.return_value.text = "<div id='not_telechargement'></div>"
    with pytest.raises(RuntimeError, match="HTML object not present"):
        BACI(data_path=tmp_path, baci_version="latest")

@patch("bblocks_data_importers.cepii.baci.requests.get")
def test_get_latest_version_http_error(mock_get, tmp_path):
    mock_get.return_value.status_code = 503
    with pytest.raises(RuntimeError, match="Error 503"):
        BACI(data_path=tmp_path, baci_version="latest")

@patch("bblocks_data_importers.cepii.baci.requests.get")
def test_get_latest_version_unparsable(mock_get, tmp_path):
    mock_get.return_value.status_code = 200
    mock_get.return_value.text = "<div id='telechargement'><p>No version here</p></div>"
    with pytest.raises(RuntimeError, match="Latest BACI version not found"):
        BACI(data_path=tmp_path, baci_version="latest")


# ---------- DOWNLOAD ---------- #

@patch("bblocks_data_importers.cepii.baci.requests.get")
def test_download_and_extract_creates_file(mock_get, baci, data_dir):
    zip_buf = io.BytesIO()
    with zipfile.ZipFile(zip_buf, "w") as z:
        z.writestr("file.csv", "col1,col2\n1,2")
    zip_buf.seek(0)

    mock_get.return_value.status_code = 200
    mock_get.return_value.content = zip_buf.read()

    baci._download_and_extract()
    assert (data_dir / "file.csv").exists()

@patch("bblocks_data_importers.cepii.baci.requests.get")
def test_download_and_extract_http_error(mock_get, baci):
    mock_get.return_value.status_code = 404
    with pytest.raises(RuntimeError, match="Error 404"):
        baci._download_and_extract()


# ---------- FORMAT ---------- #

def test_format_data_maps_fields(baci, tmp_path, raw_df):
    baci._country_format = "name"
    baci._product_description = True

    (tmp_path / "product_codes_HS22_V202401.csv").write_text("code,description\n010101,Foo product")
    (tmp_path / "country_codes_V202401.csv").write_text("country_code,country_name\n250,France\n276,Germany")

    df = baci._format_data(raw_df, tmp_path)

    assert df.iloc[0]["exporter"] == "France"
    assert df.iloc[0]["importer"] == "Germany"
    assert df.iloc[0]["product_description"] == "Foo product"


# ---------- COMBINE ---------- #

def test_combine_data_returns_table(baci, data_dir):
    pd.DataFrame({
        "t": [2022], "i": [1], "j": [2], "k": ["010101"], "v": [1.0], "q": [1.0]
    }).to_csv(data_dir / "BACI.csv", index=False)

    table = baci._combine_data()
    assert isinstance(table, pa.Table)
    assert table.num_rows == 1
    assert table.num_columns == 6

def test_combine_data_raises_if_none_found(baci, data_dir):
    with pytest.raises(FileNotFoundError, match="No BACI CSV files found"):
        baci._combine_data()


# ---------- CLEANUP ---------- #

def test_cleanup_csvs_removes_baci_only(tmp_path):
    (tmp_path / "BACI_foo.csv").write_text("x")
    (tmp_path / "BACI_bar.csv").write_text("x")
    (tmp_path / "unrelated.csv").write_text("x")

    BACI._cleanup_csvs(tmp_path)
    assert not (tmp_path / "BACI_foo.csv").exists()
    assert (tmp_path / "unrelated.csv").exists()


# ---------- LOAD PARQUET ---------- #

@patch("bblocks_data_importers.cepii.baci.ds.dataset")
def test_load_parquet_dataset_applies_filter(mock_dataset, baci, tmp_path):
    mock_ds = MagicMock()
    mock_ds.to_table.return_value.to_pandas.return_value = "filtered_df"
    mock_dataset.return_value = mock_ds

    result = baci._load_parquet_dataset(tmp_path, filter_years=[2022])
    assert result == "filtered_df"
    mock_ds.to_table.assert_called_once()

@patch("bblocks_data_importers.cepii.baci.ds.dataset")
def test_load_parquet_dataset_no_filter(mock_dataset, baci, tmp_path):
    mock_ds = MagicMock()
    mock_ds.to_table.return_value.to_pandas.return_value = "full_df"
    mock_dataset.return_value = mock_ds

    result = baci._load_parquet_dataset(tmp_path, filter_years=None)
    assert result == "full_df"
    mock_ds.to_table.assert_called_once_with()

# ---------- LOAD DATA ---------- #

@patch("bblocks_data_importers.cepii.baci.DataFrameValidator")
def test_load_data_uses_existing_parquet(mock_val, baci, tmp_path):
    path = tmp_path / "BACI_HS22_V202401/parquet"
    path.mkdir(parents=True)
    (path / "file.parquet").touch()

    baci._load_parquet_dataset = MagicMock(return_value=pd.DataFrame({
        "t": [2022], "i": [1], "j": [2], "k": ["x"], "v": [1.0], "q": [1.0]
    }))
    baci._format_data = MagicMock(return_value="formatted")
    baci._product_description = True

    baci._load_data([2022])
    baci._load_parquet_dataset.assert_called_once()
    baci._format_data.assert_called_once()

@patch("bblocks_data_importers.cepii.baci.DataFrameValidator")
def test_load_data_triggers_download(mock_val, baci, tmp_path):
    (tmp_path / "BACI_HS22_V202401").mkdir()
    baci._download_and_extract = MagicMock()
    baci._combine_data = MagicMock(return_value=pa.table({
        "t": [2022], "i": [1], "j": [2], "k": ["x"], "v": [1.0], "q": [1.0]
    }))
    baci._save_parquet = MagicMock()
    baci._cleanup_csvs = MagicMock()
    baci._format_data = MagicMock(return_value="formatted")
    baci._product_description = False

    baci._load_data([2022])
    baci._download_and_extract.assert_called_once()


# ---------- GET DATA ---------- #

def test_get_data_loads_on_first_call(baci):
    baci._data = None
    baci._load_data = MagicMock()
    baci._data = "mocked"
    assert baci.get_data(country_format="iso3", product_description=False, years=[2022]) == "mocked"

def test_get_data_uses_cached_data(baci):
    baci._data = "cached"
    baci._country_format = "iso3"
    baci._product_description = False
    baci._loaded_years = [2022]
    baci._load_data = MagicMock()

    assert baci.get_data(country_format="iso3", product_description=False, years=[2022]) == "cached"
    baci._load_data.assert_not_called()


# ---------- METADATA ---------- #

def test_extract_metadata_warns_if_readme_missing(baci, tmp_path):
    path = tmp_path / "BACI_HS22_V202401/parquet"
    path.mkdir(parents=True)
    (path / "dummy.parquet").touch()

    with patch("bblocks_data_importers.cepii.baci.logger") as mock_logger:
        baci._extract_metadata()
        assert baci._metadata == {}
        mock_logger.warning.assert_called_once()

def test_extract_metadata_raises_if_no_files(baci, tmp_path):
    (tmp_path / "BACI_HS22_V202401").mkdir()
    with pytest.raises(FileNotFoundError):
        baci._extract_metadata()

def test_extract_metadata_parses_correctly(tmp_path):
    readme = tmp_path / "BACI_HS22_V202401/Readme.txt"
    readme.parent.mkdir(parents=True)
    readme.write_text(
        """
        Version: 202401\n
        Release Date: Jan 2024\n
        Source: CEPII\n
        """
    )

    baci = BACI(data_path=tmp_path, baci_version="202401")
    baci._extract_metadata()

    assert baci._metadata.get("Version") == "202401"
    assert baci._metadata.get("Release Date") == "Jan 2024"

# ---------- CACHE ---------- #

def test_clear_cache_resets_state(baci):
    baci._data = "anything"
    baci._metadata = {"foo": "bar"}
    baci.clear_cache()
    assert baci._data is None
    assert baci._metadata is None
