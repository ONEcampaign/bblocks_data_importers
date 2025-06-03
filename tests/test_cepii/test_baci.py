"""Test suite for the BACI importer class."""

import io
import pytest
import pandas as pd
import requests
from unittest.mock import patch, MagicMock

from bblocks.data_importers.cepii.baci import BACI, VERSIONS_DICT


# ------------------------- Fixtures ------------------------- #


@pytest.fixture
def tmp_baci_dir(tmp_path):
    """Create a temporary BACI data directory."""
    path = tmp_path / "baci"
    path.mkdir()
    return path


@pytest.fixture
def baci_instance(tmp_path):
    """Factory for creating a BACI instance with optional custom paths or versions."""

    def _create(data_path=None, baci_version="202501", hs_version="22"):
        path = data_path or (tmp_path / "baci")
        path.mkdir(parents=True, exist_ok=True)
        return BACI(data_path=path, baci_version=baci_version, hs_version=hs_version)

    return _create


@pytest.fixture
def extract_path(tmp_path):
    """Fixture for a standard extract path directory layout."""
    path = tmp_path / "BACI_HS22_V202501"
    path.mkdir(parents=True)
    return path


@pytest.fixture
def mock_df():
    """Mock raw BACI DataFrame simulating unformatted trade data."""
    return pd.DataFrame(
        {"t": [2022], "i": [250], "j": [840], "k": ["0101"], "v": [1000.0], "q": [50.0]}
    )


@pytest.fixture
def processed_baci_df():
    """Mock final formatted BACI DataFrame matching expected output schema."""
    return pd.DataFrame(
        {
            "year": [2022],
            "exporter_iso3": ["FRA"],
            "exporter_name": ["France"],
            "importer_iso3": ["USA"],
            "importer_name": ["United States"],
            "product_code": ["0101"],
            "value": [1000],
            "quantity": [50],
        }
    )


# ------------------------- Init Tests ------------------------- #


@patch(
    "bblocks.data_importers.cepii.static_methods.get_available_versions",
    return_value=VERSIONS_DICT,
)
def test_init_valid(mock_versions, baci_instance):
    """Check that a BACI instance initializes with valid inputs."""
    b = baci_instance()
    assert b._baci_version == "202501"
    assert b._hs_version == "22"


def test_baci_invalid_path_raises():
    """Ensure FileNotFoundError is raised for invalid path."""
    with pytest.raises(FileNotFoundError):
        BACI(data_path="bad_path", baci_version="202501", hs_version="22")


@patch(
    "bblocks.data_importers.cepii.static_methods.get_available_versions",
    return_value=VERSIONS_DICT,
)
def test_init_invalid_version_raises(mock_versions, baci_instance):
    """Ensure ValueError is raised for an unsupported BACI version."""
    with pytest.raises(ValueError):
        baci_instance(baci_version="999999")


@patch(
    "bblocks.data_importers.cepii.static_methods.get_available_versions",
    return_value=VERSIONS_DICT,
)
def test_init_invalid_hs_version_raises(mock_versions, baci_instance):
    """Ensure ValueError is raised for an unsupported HS version."""
    with pytest.raises(ValueError):
        baci_instance(hs_version="88")


@patch(
    "bblocks.data_importers.cepii.static_methods.get_available_versions",
    return_value=VERSIONS_DICT,
)
def test_init_no_path_raises(mock_versions):
    """Ensure ValueError is raised when no path is provided."""
    with pytest.raises(ValueError):
        BACI(data_path=None)


def test_init_latest_version(baci_instance):
    """Ensure 'latest' version resolves to the latest version key in VERSIONS_DICT."""
    b = baci_instance(baci_version="latest")
    assert b._baci_version == next(v for v, d in VERSIONS_DICT.items() if d["latest"])


# ------------------------- Download & Load ------------------------- #


def test_download_zip_success(monkeypatch, baci_instance):
    """Test successful zip file download from mocked HTTP response."""

    class MockResponse:
        status_code = 200
        content = b"zipdata"

        def raise_for_status(self):
            pass

    monkeypatch.setattr(
        "bblocks.data_importers.cepii.baci.requests.get", lambda _: MockResponse()
    )
    buf = baci_instance()._download_zip()
    assert isinstance(buf, io.BytesIO)
    assert buf.getvalue() == b"zipdata"


def test_download_zip_failure(monkeypatch, baci_instance):
    """Test that download failure raises HTTPError."""
    mock_resp = MagicMock()
    mock_resp.raise_for_status.side_effect = requests.exceptions.HTTPError(
        "404 Client Error"
    )
    monkeypatch.setattr(
        "bblocks.data_importers.cepii.baci.requests.get", lambda _: mock_resp
    )

    with pytest.raises(requests.exceptions.HTTPError):
        baci_instance()._download_zip()


def test_load_country_codes(tmp_path, baci_instance):
    """Ensure country codes CSV is loaded correctly."""
    csv_df = pd.DataFrame(
        {"country_code": [250], "country_iso3": ["FRA"], "country_name": ["France"]}
    )
    extract_path = tmp_path / "BACI_HS22_V202501"
    extract_path.mkdir(parents=True)
    csv_df.to_csv(extract_path / "country_codes_V202501.csv", index=False)

    df = baci_instance(data_path=tmp_path)._load_country_codes()
    pd.testing.assert_frame_equal(df, csv_df)


# ------------------------- Internal Logic ------------------------- #


def test_format_data(monkeypatch, baci_instance, mock_df):
    """Test full formatting of raw data into final expected DataFrame structure."""
    baci = baci_instance()
    monkeypatch.setattr(
        baci,
        "_load_country_codes",
        lambda: pd.DataFrame(
            {
                "country_code": [250, 840],
                "country_iso3": ["FRA", "USA"],
                "country_name": ["France", "United States"],
            }
        ),
    )
    df = baci._format_data(mock_df)

    expected_cols = {
        "year",
        "exporter_iso3",
        "exporter_name",
        "importer_iso3",
        "importer_name",
        "product_code",
        "value",
        "quantity",
    }
    assert set(df.columns) == expected_cols
    assert df.loc[0, "exporter_iso3"] == "FRA"


def test_ensure_parquet_dir_is_returned(monkeypatch, tmp_path):
    """Test that a path is returned in _ensure_parquet_data_exists"""
    extract_path = tmp_path / "BACI_HS22_V202501"
    parquet_path = extract_path / "parquet"
    parquet_path.mkdir(parents=True, exist_ok=True)
    (parquet_path / "part-000.parquet").touch()

    baci = BACI(data_path=tmp_path, baci_version="202501", hs_version="22")
    result = baci._ensure_parquet_data_exists()
    assert result == parquet_path


# ------------------------- get_data() & Caching ------------------------- #


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
    """Test that get_data returns expected DataFrame, mocking parquet load and ZIP I/O."""
    # Mock HTTP ZIP download
    mock_requests_get.return_value.status_code = 200
    mock_requests_get.return_value.content = b"fakezipcontent"

    # Mock Parquet load result
    mock_load_parquet.return_value = mock_df

    # Create expected directory structure
    extract_path = tmp_path / "BACI_HS22_V202501"
    parquet_dir = extract_path / "parquet"
    parquet_dir.mkdir(parents=True, exist_ok=True)

    # Create required country_codes CSV
    country_codes_path = extract_path / "country_codes_V202501.csv"
    country_codes_path.write_text(
        "country_code,country_iso3,country_name\n250,FRA,France\n840,USA,United States"
    )

    # Run test
    baci = BACI(data_path=tmp_path, baci_version="202501", hs_version="22")
    df = baci.get_data()

    print(df)

    print(processed_baci_df)
    # Validate output
    assert isinstance(df, pd.DataFrame)
    pd.testing.assert_frame_equal(
        df.sort_index(axis=1), processed_baci_df.sort_index(axis=1), check_dtype=False
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
    """Test that various formats of input years are normalized to a set of integers."""
    baci = baci_instance()
    captured = {}

    def mock_load_data(filter_years):
        captured["filter_years"] = filter_years
        baci._data = pd.DataFrame()  # Prevent load fallback

    monkeypatch.setattr(baci, "_load_data", mock_load_data)
    monkeypatch.setattr(baci, "_data", None)

    baci.get_data(years=input_years)

    assert "filter_years" in captured
    assert captured["filter_years"] == expected


# ------------------------- Metadata & HS Map ------------------------- #


def test_extract_hs_map_missing_file_with_parquet_raises(baci_instance, extract_path):
    """Raise FileNotFoundError when HS map file is missing and parquet dir exists."""
    (extract_path / "parquet").mkdir()
    baci = baci_instance(data_path=extract_path.parent)
    with pytest.raises(FileNotFoundError, match="HS map file .* not found"):
        baci._extract_hs_map()


def test_extract_metadata_missing_file_with_parquet_raises(baci_instance, extract_path):
    """Raise FileNotFoundError when Readme.txt is missing and parquet dir exists."""
    (extract_path / "parquet").mkdir()
    baci = baci_instance(data_path=extract_path.parent)
    with pytest.raises(FileNotFoundError, match="Metadata file 'Readme.txt' not found"):
        baci._extract_metadata()


# ------------------------- Simple Accessors ------------------------- #


@patch("bblocks.data_importers.cepii.baci.BACI.get_data")
def test_get_metadata_returns_dict(mock_get_data, tmp_baci_dir):
    """Test get_metadata returns parsed dictionary from Readme.txt."""
    b = BACI(data_path=tmp_baci_dir, baci_version="202501", hs_version="22")
    path = b._extract_path
    path.mkdir(parents=True)
    (path / "Readme.txt").write_text(
        """
        Version: 202501\n
        Source: CEPII
        """
    )

    metadata = b.get_metadata()
    assert "Version" in metadata
    assert "Source" in metadata


@patch("bblocks.data_importers.cepii.baci.BACI.get_data")
def test_get_hs_map_returns_dict(mock_get_data, tmp_baci_dir):
    """Test get_hs_map returns dictionary from product code CSV."""
    b = BACI(data_path=tmp_baci_dir, baci_version="202501", hs_version="22")
    path = b._extract_path
    path.mkdir(parents=True)
    (path / "product_codes_HS22_V202501.csv").write_text(
        "code,description\n0101,Horses\n0102,Cattle"
    )

    hs_map = b.get_hs_map()
    assert hs_map["0101"] == "Horses"


def test_clear_cache_deletes_disk(tmp_baci_dir):
    """Test that clear_cache() removes the extracted path when clear_disk=True."""
    b = BACI(data_path=tmp_baci_dir, baci_version="202501", hs_version="22")
    b._extract_path.mkdir(parents=True)
    (b._extract_path / "dummy.txt").write_text("x")
    assert b._extract_path.exists()
    b.clear_cache(clear_disk=True)
    assert not b._extract_path.exists()


@patch("bblocks.data_importers.cepii.static_methods.get_available_versions")
def test_get_versions(mock_get_versions):
    """Tests get_available_versions()` returns a versions dictionary"""
    mock_get_versions.return_value = {"202501": {"hs": ["22"], "latest": True}}
    from bblocks.data_importers.cepii.baci import get_baci_versions

    result = get_baci_versions()
    assert "202501" in result
