"""Test suite for the BACI importer class."""

import io
import pytest
import pandas as pd
import pyarrow as pa
import requests
from unittest.mock import patch, MagicMock

from bblocks.data_importers.cepii.baci import BACI, VERSIONS_DICT
from bblocks.data_importers.protocols import DataImporter


# ------------------------- Fixtures ------------------------- #

@pytest.fixture
def tmp_baci_dir(tmp_path):
    """Temporary base directory for BACI data."""
    path = tmp_path / "baci"
    path.mkdir()
    return path


@pytest.fixture
def baci_instance(tmp_path):
    """Factory for creating a BACI instance."""
    def _create(data_path=None, baci_version="202501", hs_version="22"):
        path = data_path or (tmp_path / "baci")
        path.mkdir(parents=True, exist_ok=True)
        return BACI(data_path=path, baci_version=baci_version, hs_version=hs_version)
    return _create


@pytest.fixture
def extract_path(tmp_path):
    """Path to a mocked extraction directory."""
    path = tmp_path / "BACI_HS22_V202501"
    path.mkdir(parents=True)
    return path


@pytest.fixture
def mock_df():
    """Raw BACI DataFrame with original column names and types."""
    return pd.DataFrame({
        "t": [2022], "i": [250], "j": [840], "k": ["0101"], "v": [1000.0], "q": [50.0]
    })


@pytest.fixture
def processed_baci_df():
    """Pre-processed BACI DataFrame with pyarrow-backed dtypes."""
    df = pd.DataFrame({
        "year": [2022],
        "exporter_iso3_code": ["FRA"],
        "exporter_name": ["France"],
        "importer_iso3_code": ["USA"],
        "importer_name": ["United States"],
        "product_code": ["0101"],
        "value": [1000],
        "quantity": [50],
    })
    return df.convert_dtypes(dtype_backend="pyarrow")


# ------------------------- Init / Protocol ------------------------- #

def test_protocol(baci_instance):
    """Ensure BACI implements required DataImporter protocol."""
    obj = baci_instance()
    assert isinstance(obj, DataImporter)
    assert hasattr(obj, "get_data")
    assert hasattr(obj, "clear_cache")


@patch("bblocks.data_importers.cepii.baci_utils.get_available_versions", return_value=VERSIONS_DICT)
def test_init_valid(mock_versions, baci_instance):
    """BACI initializes with correct versions."""
    b = baci_instance()
    assert b._baci_version == "202501"
    assert b._hs_version == "22"


def test_baci_invalid_path_raises():
    """Should raise FileNotFoundError if path does not exist."""
    with pytest.raises(FileNotFoundError):
        BACI(data_path="bad_path", baci_version="202501", hs_version="22")


@patch("bblocks.data_importers.cepii.baci_utils.get_available_versions", return_value=VERSIONS_DICT)
def test_init_invalid_version_raises(mock_versions, baci_instance):
    """Should raise if BACI version is invalid."""
    with pytest.raises(ValueError):
        baci_instance(baci_version="999999")


@patch("bblocks.data_importers.cepii.baci_utils.get_available_versions", return_value=VERSIONS_DICT)
def test_init_invalid_hs_version_raises(mock_versions, baci_instance):
    """Should raise if HS version is invalid."""
    with pytest.raises(ValueError):
        baci_instance(hs_version="88")


@patch("bblocks.data_importers.cepii.baci_utils.get_available_versions", return_value=VERSIONS_DICT)
def test_init_no_path_raises(mock_versions):
    """Should raise ValueError if path is missing."""
    with pytest.raises(ValueError):
        BACI(data_path=None)


def test_init_latest_version(baci_instance):
    """Should resolve 'latest' BACI version from dictionary."""
    b = baci_instance(baci_version="latest")
    assert b._baci_version == next(v for v, d in VERSIONS_DICT.items() if d["latest"])


# ------------------------- Download & Load ------------------------- #

def test_download_zip_success(monkeypatch, baci_instance):
    """Test successful zip file download returns BytesIO object."""
    class MockResponse:
        status_code = 200
        content = b"zipdata"
        def raise_for_status(self): pass

    monkeypatch.setattr(
        "bblocks.data_importers.cepii.baci.requests.get", lambda _: MockResponse()
    )
    result = baci_instance()._download_zip()
    assert isinstance(result, io.BytesIO)
    assert result.getvalue() == b"zipdata"


def test_download_zip_failure(monkeypatch, baci_instance):
    """Test download failure raises an HTTPError."""
    mock_resp = MagicMock()
    mock_resp.raise_for_status.side_effect = requests.exceptions.HTTPError("404")
    monkeypatch.setattr(
        "bblocks.data_importers.cepii.baci.requests.get", lambda _: mock_resp
    )
    with pytest.raises(requests.exceptions.HTTPError):
        baci_instance()._download_zip()


def test_load_country_codes(tmp_path, baci_instance):
    """Ensure country codes CSV is loaded into DataFrame correctly."""
    expected_df = pd.DataFrame({
        "country_code": [250],
        "country_iso3": ["FRA"],
        "country_name": ["France"]
    })
    path = tmp_path / "BACI_HS22_V202501"
    path.mkdir()
    expected_df.to_csv(path / "country_codes_V202501.csv", index=False)

    result = baci_instance(data_path=tmp_path)._load_country_codes()
    pd.testing.assert_frame_equal(result, expected_df)

# ------------------------- Internal Logic ------------------------- #

def test_format_data(monkeypatch, baci_instance, mock_df):
    """Test that formatting raw BACI data returns expected structure."""
    baci = baci_instance()
    monkeypatch.setattr(baci, "_load_country_codes", lambda: pd.DataFrame({
        "country_code": [250, 840],
        "country_iso3": ["FRA", "USA"],
        "country_name": ["France", "United States"],
    }))
    df = baci._format_data(mock_df)

    expected_columns = {
        "year", "exporter_iso3_code", "exporter_name",
        "importer_iso3_code", "importer_name",
        "product_code", "value", "quantity",
    }
    assert set(df.columns) == expected_columns
    assert df.loc[0, "exporter_iso3_code"] == "FRA"


@patch("bblocks.data_importers.cepii.baci.cleanup_csvs")
@patch("bblocks.data_importers.cepii.baci.save_parquet")
@patch("bblocks.data_importers.cepii.baci.combine_data")
@patch("bblocks.data_importers.cepii.baci.extract_zip")
@patch.object(BACI, "_download_zip")
def test_ensure_parquet_dir_is_returned(
        mock_download_zip,
        mock_extract_zip,
        mock_combine_data,
        mock_save_parquet,
        mock_cleanup_csvs,
        tmp_path,
):
    """Test _ensure_parquet_data_exists returns directory when .parquet exists."""
    extract_path = tmp_path / "BACI_HS22_V202501"
    parquet_path = extract_path / "parquet" / "2022"
    parquet_path.mkdir(parents=True)
    (parquet_path / "part-000.parquet").touch()

    baci = BACI(data_path=tmp_path, baci_version="202501", hs_version="22")
    result = baci._ensure_parquet_data_exists()

    assert result == extract_path / "parquet"
    assert not mock_download_zip.called

# ------------------------- get_data() Behavior ------------------------- #

@patch("bblocks.data_importers.cepii.baci.cleanup_csvs")
@patch("bblocks.data_importers.cepii.baci.save_parquet")
@patch("bblocks.data_importers.cepii.baci.combine_data")
@patch("bblocks.data_importers.cepii.baci.extract_zip")
@patch("bblocks.data_importers.cepii.baci.load_parquet")
@patch("bblocks.data_importers.cepii.baci.requests.get")
def test_get_data_success(
        mock_requests_get,
        mock_load_parquet,
        mock_extract_zip,
        mock_combine_data,
        mock_save_parquet,
        mock_cleanup_csvs,
        tmp_path,
        mock_df,
        processed_baci_df,
):
    """Test full get_data workflow returns formatted DataFrame."""
    mock_requests_get.return_value.status_code = 200
    mock_requests_get.return_value.content = b"zip"
    mock_load_parquet.return_value = mock_df

    extract_path = tmp_path / "BACI_HS22_V202501"
    parquet_dir = extract_path / "parquet"
    parquet_dir.mkdir(parents=True)

    (extract_path / "country_codes_V202501.csv").write_text(
        "country_code,country_iso3,country_name\n250,FRA,France\n840,USA,United States"
    )

    baci = BACI(data_path=tmp_path, baci_version="202501", hs_version="22")
    result = baci.get_data()

    assert isinstance(result, pd.DataFrame)
    pd.testing.assert_frame_equal(
        result.sort_index(axis=1),
        processed_baci_df.sort_index(axis=1),
        check_dtype=False
    )


@pytest.mark.parametrize(
    "input_years, expected",
    [
        (2022, {2022}),
        ([2021, 2022], {2021, 2022}),
        (range(2019, 2021), {2019, 2020}),
        ({2020, 2023}, {2020, 2023}),
        (None, None),
    ],
)
def test_get_data_normalizes_years(monkeypatch, baci_instance, input_years, expected):
    """Test get_data normalizes all year input types to set[int]."""
    baci = baci_instance()
    captured = {}

    def mock_load_data(filter_years, force_reload=False):
        captured["filter_years"] = filter_years
        baci._data = pd.DataFrame()

    monkeypatch.setattr(baci, "_load_data", mock_load_data)
    monkeypatch.setattr(baci, "_data", None)

    baci.get_data(years=input_years)
    assert captured["filter_years"] == expected

# ------------------------- Caching ------------------------- #

DUMMY_DATA = pd.DataFrame({"year": [2022], "value": [1000], "quantity": [10]})
DUMMY_METADATA = {"version": "202501"}


def test_clear_cache_only_resets_memory(baci_instance, tmp_baci_dir):
    """Test clear_cache with clear_disk=False only resets memory."""
    baci = baci_instance(data_path=tmp_baci_dir)
    baci._data = DUMMY_DATA
    baci._metadata = DUMMY_METADATA
    baci._extract_path.mkdir(parents=True, exist_ok=True)
    (baci._extract_path / "dummy.txt").write_text("cache")

    baci.clear_cache(clear_disk=False)

    assert baci._data is None
    assert baci._metadata is None
    assert baci._extract_path.exists()


def test_clear_cache_removes_disk_and_memory(baci_instance, tmp_baci_dir):
    """Test clear_cache with clear_disk=True removes memory and local files."""
    baci = baci_instance(data_path=tmp_baci_dir)
    baci._data = DUMMY_DATA
    baci._metadata = DUMMY_METADATA
    baci._extract_path.mkdir(parents=True)
    (baci._extract_path / "dummy.txt").write_text("cache")

    baci.clear_cache(clear_disk=True)

    assert baci._data is None
    assert baci._metadata is None
    assert not baci._extract_path.exists()

# ------------------------- Disk vs Memory Behavior ------------------------- #

MOCK_ARROW_TABLE = pa.table({
    "t": pa.array([2022], type=pa.int16()),
    "i": pa.array([250], type=pa.int32()),
    "j": pa.array([840], type=pa.int32()),
    "k": pa.array(["0101"]),
    "v": pa.array([1000.0]),
    "q": pa.array([50.0]),
})


@patch("bblocks.data_importers.cepii.baci.save_parquet")
@patch("bblocks.data_importers.cepii.baci.combine_data", return_value=MOCK_ARROW_TABLE)
@patch("bblocks.data_importers.cepii.baci.extract_zip")
@patch("bblocks.data_importers.cepii.baci.load_parquet")
@patch.object(BACI, "_format_data")
def test_download_triggered_after_disk_clear(
        mock_format_data,
        mock_load_parquet,
        mock_extract_zip,
        mock_combine_data,
        mock_save_parquet,
        baci_instance,
        processed_baci_df,
):
    """Ensure get_data triggers a re-download if disk cache is deleted."""
    baci = baci_instance()
    cached_dir = baci._extract_path / "parquet" / "2022"
    cached_dir.mkdir(parents=True)
    (cached_dir / "dummy.parquet").touch()

    df = processed_baci_df
    mock_load_parquet.return_value = df
    mock_format_data.return_value = df

    # First run: from disk
    baci.get_data()
    assert not getattr(baci, "_download_called", False)

    # Remove disk and simulate re-download via patch
    baci.clear_cache(clear_disk=True)

    def fake_download():
        baci._download_called = True
        raise RuntimeError("Short-circuiting download")

    with patch.object(BACI, "_download_zip", side_effect=fake_download):
        with pytest.raises(RuntimeError, match="Short-circuiting download"):
            baci.get_data()

    assert hasattr(baci, "_download_called")


@patch("bblocks.data_importers.cepii.baci.combine_data")
@patch("bblocks.data_importers.cepii.baci.extract_zip")
@patch("bblocks.data_importers.cepii.baci.save_parquet")
@patch("bblocks.data_importers.cepii.baci.load_parquet")
@patch.object(BACI, "_format_data")
@patch.object(BACI, "_download_zip", return_value=io.BytesIO(b"PK\x03\x04"))
def test_get_data_reads_from_disk_after_memory_clear(
        mock_download_zip,
        mock_format_data,
        mock_load_parquet,
        mock_save_parquet,
        mock_extract_zip,
        mock_combine_data,
        baci_instance,
        tmp_baci_dir,
        processed_baci_df,
):
    """Test that get_data reads from disk again after clearing only memory."""
    baci = baci_instance(data_path=tmp_baci_dir)
    disk_path = baci._extract_path / "parquet" / "2022"
    disk_path.mkdir(parents=True)
    (disk_path / "part-000.parquet").touch()

    df = processed_baci_df
    mock_load_parquet.return_value = df
    mock_format_data.return_value = df

    first_df = baci.get_data()
    assert first_df.equals(df)
    assert not mock_download_zip.called

    baci.clear_cache(clear_disk=False)
    assert baci._data is None
    assert baci._extract_path.exists()

    second_df = baci.get_data()
    assert not mock_download_zip.called
    assert mock_load_parquet.call_count == 2
    assert second_df.equals(df)

# ------------------------- Metadata & HS Map ------------------------- #

def test_extract_hs_map_missing_file_with_parquet_raises(baci_instance, extract_path):
    """Raise error when HS map file is missing but parquet exists."""
    (extract_path / "parquet").mkdir()
    baci = baci_instance(data_path=extract_path.parent)
    with pytest.raises(FileNotFoundError, match="HS map file .* not found"):
        baci._extract_hs_map()


def test_extract_metadata_missing_file_with_parquet_raises(baci_instance, extract_path):
    """Raise error when Readme.txt is missing but parquet exists."""
    (extract_path / "parquet").mkdir()
    baci = baci_instance(data_path=extract_path.parent)
    with pytest.raises(FileNotFoundError, match="Metadata file 'Readme.txt' not found"):
        baci._extract_metadata()


# ------------------------- Simple Accessors ------------------------- #

@patch("bblocks.data_importers.cepii.baci.BACI.get_data")
def test_get_metadata_returns_dict(mock_get_data, tmp_baci_dir):
    """Test get_metadata parses values from Readme.txt."""
    b = BACI(data_path=tmp_baci_dir, baci_version="202501", hs_version="22")
    (b._extract_path).mkdir()
    (b._extract_path / "Readme.txt").write_text(
        """
        Version: 202501\n
        Source: CEPII
        """
    )

    result = b.get_metadata()
    assert result["Version"] == "202501"
    assert result["Source"] == "CEPII"


@patch("bblocks.data_importers.cepii.baci.BACI.get_data")
def test_get_hs_map_returns_dict(mock_get_data, tmp_baci_dir):
    """Test get_hs_map parses product codes file into dictionary."""
    b = BACI(data_path=tmp_baci_dir, baci_version="202501", hs_version="22")
    (b._extract_path).mkdir()
    (b._extract_path / "product_codes_HS22_V202501.csv").write_text(
        """code,description\n0101,Horses\n0102,Cattle"""
    )

    hs_map = b.get_hs_map()
    assert hs_map == {"0101": "Horses", "0102": "Cattle"}


@patch("bblocks.data_importers.cepii.baci_utils.get_available_versions")
def test_get_versions(mock_get_versions):
    """Test get_baci_versions returns expected version keys."""
    mock_get_versions.return_value = {"202501": {"hs": ["22"], "latest": True}}
    from bblocks.data_importers.cepii.baci import get_baci_versions
    result = get_baci_versions()
    assert "202501" in result
