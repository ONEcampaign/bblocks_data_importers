"""Test suite for the BACI importer class."""

import io
import zipfile
import pytest
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from unittest.mock import patch, MagicMock

from bblocks_data_importers.cepii.baci import BACI, VERSIONS_DICT


# ------------------------- Fixtures ------------------------- #


@pytest.fixture
def tmp_baci_dir(tmp_path):
    path = tmp_path / "baci"
    path.mkdir()
    return path


@pytest.fixture
def baci_instance(tmp_path):
    def _create(data_path=None, baci_version="202501", hs_version="22"):
        path = data_path or (tmp_path / "baci")
        path.mkdir(parents=True, exist_ok=True)
        return BACI(data_path=path, baci_version=baci_version, hs_version=hs_version)

    return _create


@pytest.fixture
def extract_path(tmp_path):
    path = tmp_path / "BACI_HS22_V202501"
    path.mkdir(parents=True)
    return path


@pytest.fixture
def mock_df():
    return pd.DataFrame(
        {"t": [2022], "i": [250], "j": [840], "k": ["0101"], "v": [1000.0], "q": [50.0]}
    )


@pytest.fixture
def mock_table_dataframe(mock_df):
    mock_table = MagicMock()
    mock_table.to_pandas.return_value = mock_df
    return mock_table


@pytest.fixture
def mock_parquet_dir(tmp_baci_dir, mock_df):
    parquet_dir = tmp_baci_dir / "BACI_HS22_V202501" / "parquet"
    parquet_dir.mkdir(parents=True, exist_ok=True)

    schema = pa.schema(
        [
            ("t", pa.int16()),
            ("i", pa.int32()),
            ("j", pa.int32()),
            ("k", pa.string()),
            ("v", pa.float32()),
            ("q", pa.float32()),
        ]
    )
    table = pa.Table.from_pandas(mock_df, schema=schema, preserve_index=False)
    pq.write_table(table, parquet_dir / "part-000.parquet")
    return parquet_dir


@pytest.fixture
def processed_baci_df():
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


@pytest.fixture
def fake_zip_file():
    mem_zip = io.BytesIO()
    with zipfile.ZipFile(mem_zip, mode="w") as zf:
        zf.writestr("BACI_2022.csv", "t,i,j,k,v,q\n2022,1,2,0101,1000.0,50.0")
        zf.writestr(
            "country_codes_V202501.csv",
            "country_code,country_iso3,country_name\n1,FRA,France\n2,USA,United States",
        )
        zf.writestr(
            "product_codes_HS22_V202501.csv",
            "code,description\n0101,Horses\n0102,Cattle",
        )
        zf.writestr("Readme.txt", "Version: 202501\nSource: CEPII")
    mem_zip.seek(0)
    return mem_zip


# ------------------------- Init Tests ------------------------- #


@patch(
    "bblocks_data_importers.cepii.baci.get_available_versions",
    return_value=VERSIONS_DICT,
)
def test_init_valid(mock_versions, baci_instance):
    b = baci_instance()
    assert b._baci_version == "202501"
    assert b._hs_version == "22"


def test_baci_invalid_path_raises():
    with pytest.raises(FileNotFoundError):
        BACI(data_path="bad_path", baci_version="202501", hs_version="22")


@patch(
    "bblocks_data_importers.cepii.baci.get_available_versions",
    return_value=VERSIONS_DICT,
)
def test_init_invalid_version_raises(mock_versions, baci_instance):
    with pytest.raises(ValueError):
        baci_instance(baci_version="999999")


@patch(
    "bblocks_data_importers.cepii.baci.get_available_versions",
    return_value=VERSIONS_DICT,
)
def test_init_invalid_hs_version_raises(mock_versions, baci_instance):
    with pytest.raises(ValueError):
        baci_instance(hs_version="88")


@patch(
    "bblocks_data_importers.cepii.baci.get_available_versions",
    return_value=VERSIONS_DICT,
)
def test_init_no_path_raises(mock_versions):
    with pytest.raises(ValueError):
        BACI(data_path=None)


def test_init_latest_version(baci_instance):
    b = baci_instance(baci_version="latest")
    assert b._baci_version == next(v for v, d in VERSIONS_DICT.items() if d["latest"])


# ------------------------- Download & Load ------------------------- #


def test_download_zip_success(monkeypatch, baci_instance):
    class MockResponse:
        status_code = 200
        content = b"zipdata"

        def raise_for_status(self):
            pass

    monkeypatch.setattr(
        "bblocks_data_importers.cepii.baci.requests.get", lambda _: MockResponse()
    )
    buf = baci_instance()._download_zip()
    assert isinstance(buf, io.BytesIO)
    assert buf.getvalue() == b"zipdata"


def test_download_zip_failure(monkeypatch, baci_instance):
    mock_resp = MagicMock()
    mock_resp.raise_for_status.side_effect = requests.exceptions.HTTPError(
        "404 Client Error"
    )
    monkeypatch.setattr(
        "bblocks_data_importers.cepii.baci.requests.get", lambda _: mock_resp
    )

    with pytest.raises(requests.exceptions.HTTPError):
        baci_instance()._download_zip()


def test_load_country_codes(tmp_path, baci_instance):
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


# ------------------------- get_data() & Caching ------------------------- #


@patch("bblocks_data_importers.cepii.baci.cleanup_csvs")
@patch("bblocks_data_importers.cepii.baci.save_parquet")
@patch("bblocks_data_importers.cepii.baci.combine_data")
@patch("bblocks_data_importers.cepii.baci.extract_zip")
@patch("bblocks_data_importers.cepii.baci.load_parquet")
@patch("bblocks_data_importers.cepii.baci.requests.get")
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
    (extract_path / "parquet").mkdir()
    baci = baci_instance(data_path=extract_path.parent)
    with pytest.raises(FileNotFoundError, match="HS map file .* not found"):
        baci._extract_hs_map()


def test_extract_metadata_missing_file_with_parquet_raises(baci_instance, extract_path):
    (extract_path / "parquet").mkdir()
    baci = baci_instance(data_path=extract_path.parent)
    with pytest.raises(FileNotFoundError, match="Metadata file 'Readme.txt' not found"):
        baci._extract_metadata()


# ------------------------- Simple Accessors ------------------------- #


@patch("bblocks_data_importers.cepii.baci.BACI.get_data")
def test_get_metadata_returns_dict(mock_get_data, tmp_baci_dir):
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


@patch("bblocks_data_importers.cepii.baci.BACI.get_data")
def test_get_hs_map_returns_dict(mock_get_data, tmp_baci_dir):
    b = BACI(data_path=tmp_baci_dir, baci_version="202501", hs_version="22")
    path = b._extract_path
    path.mkdir(parents=True)
    (path / "product_codes_HS22_V202501.csv").write_text(
        "code,description\n0101,Horses\n0102,Cattle"
    )

    hs_map = b.get_hs_map()
    assert hs_map["0101"] == "Horses"


def test_clear_cache_deletes_disk(tmp_baci_dir):
    b = BACI(data_path=tmp_baci_dir, baci_version="202501", hs_version="22")
    b._extract_path.mkdir(parents=True)
    (b._extract_path / "dummy.txt").write_text("x")
    assert b._extract_path.exists()
    b.clear_cache(clear_disk=True)
    assert not b._extract_path.exists()
