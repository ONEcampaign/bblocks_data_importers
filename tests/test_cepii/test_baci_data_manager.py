"""Tests for BaciDataManager class"""

import os
import io
import zipfile
import pytest
import requests
import pyarrow.dataset as ds
import pyarrow as pa
import pandas as pd
from unittest.mock import patch, MagicMock

from bblocks.data_importers.cepii.extract import BaciDataManager
from bblocks.data_importers.config import DataExtractionError

# TESTS


def test_baci_data_manager_init():
    """
    Test that initializing `BaciDataManager` sets all attributes correctly,
    including the download URL and placeholders for zip file, dataset, and metadata.
    """
    version = "2023"
    hs_version = "HS22"
    manager = BaciDataManager(version=version, hs_version=hs_version)

    expected_url = (
        f"https://www.cepii.fr/DATA_DOWNLOAD/baci/data/BACI_{hs_version}_V{version}.zip"
    )

    # Check basic attributes
    assert manager.version == version
    assert manager.hs_version == hs_version
    assert manager.download_url == expected_url

    # Attributes that should be None or empty at init
    assert manager.zip_file is None
    assert manager.arrow_temp_dir is None
    assert manager.dataset is None
    assert manager.country_codes is None
    assert manager.product_codes is None
    assert manager.metadata is None
    assert manager.available_years is None


def test_extract_zipfile_success():
    """
    Test that `extract_zipfile_from_web` correctly downloads and loads a valid ZIP file.
    """
    fake_zip_bytes = io.BytesIO()
    with zipfile.ZipFile(fake_zip_bytes, "w") as zf:
        zf.writestr("dummy.txt", "content")
    fake_zip_bytes.seek(0)

    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = fake_zip_bytes.getvalue()
        mock_get.return_value = mock_response

        manager = BaciDataManager(version="202501", hs_version="HS22")
        manager.extract_zipfile_from_web()

        assert isinstance(manager.zip_file, zipfile.ZipFile)
        assert "dummy.txt" in manager.zip_file.namelist()


def test_extract_zipfile_http_error():
    """
    Test that `extract_zipfile_from_web` raises `DataExtractionError` on HTTP failure.
    """
    with patch("requests.get") as mock_get:
        mock_get.side_effect = requests.exceptions.HTTPError("404 Not Found")

        manager = BaciDataManager(version="202501", hs_version="HS22")
        with pytest.raises(DataExtractionError, match="Failed to download BACI data"):
            manager.extract_zipfile_from_web()


def test_extract_zipfile_bad_zip():
    """
    Test that `extract_zipfile_from_web` raises `DataExtractionError` if ZIP file is corrupted or invalid.
    """
    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"This is not a zip file"
        mock_get.return_value = mock_response

        manager = BaciDataManager(version="202501", hs_version="HS22")
        with pytest.raises(
            DataExtractionError, match="Failed to extract data from ZIP"
        ):
            manager.extract_zipfile_from_web()


def test_save_zip_file_success(tmp_path):
    """
    Test that `save_zip_file` writes the contents of the in-memory zip file to disk.
    """
    # Setup: create a dummy zip file in memory
    zip_bytes = io.BytesIO()
    with zipfile.ZipFile(zip_bytes, "w") as zf:
        zf.writestr("test.csv", "dummy data")
    zip_bytes.seek(0)

    # Setup manager and inject zip_file
    manager = BaciDataManager("202501", "HS22")
    manager.zip_file = zipfile.ZipFile(zip_bytes)
    manager.zip_file.fp.seek = MagicMock()
    manager.zip_file.fp.read = MagicMock(return_value=zip_bytes.getvalue())

    # Use tmp_path for safe testing
    zip_path = tmp_path / "myfile.zip"
    manager.save_zip_file(zip_path)

    # Assert file was created and is valid
    assert zip_path.exists()
    assert zipfile.is_zipfile(zip_path)


def test_save_zip_file_invalid_suffix(tmp_path):
    """
    Test that `save_zip_file` raises ValueError if the file does not end with `.zip`.
    """
    manager = BaciDataManager("202501", "HS22")
    manager.zip_file = MagicMock()

    bad_path = tmp_path / "myfile.txt"
    with pytest.raises(
        ValueError, match="must include a file name with a .zip extension"
    ):
        manager.save_zip_file(bad_path)


def test_save_zip_file_missing_directory(tmp_path):
    """
    Test that `save_zip_file` raises FileNotFoundError if the directory doesn't exist.
    """
    manager = BaciDataManager("202501", "HS22")
    manager.zip_file = MagicMock()

    bad_dir = tmp_path / "nonexistent"
    bad_path = bad_dir / "myfile.zip"

    with pytest.raises(FileNotFoundError, match="does not exist"):
        manager.save_zip_file(bad_path)


def test_save_zip_file_file_exists(tmp_path):
    """
    Test that `save_zip_file` raises FileExistsError if file exists and override is False.
    """
    zip_file = tmp_path / "existing.zip"
    zip_file.write_text("already here")

    manager = BaciDataManager("202501", "HS22")
    manager.zip_file = MagicMock()

    with pytest.raises(FileExistsError, match="already exists"):
        manager.save_zip_file(zip_file)


def test_save_zip_file_overrides_existing(tmp_path):
    """
    Test that `save_zip_file` overwrites an existing file when override=True is set.
    """
    path = tmp_path / "output.zip"
    path.write_text("original content")

    # Create new zip content
    zip_bytes = io.BytesIO()
    with zipfile.ZipFile(zip_bytes, "w") as zf:
        zf.writestr("newfile.csv", "new data")
    zip_bytes.seek(0)

    manager = BaciDataManager("202501", "HS22")
    manager.zip_file = zipfile.ZipFile(zip_bytes)
    manager.zip_file.fp.seek = MagicMock()
    manager.zip_file.fp.read = MagicMock(return_value=zip_bytes.getvalue())

    manager.save_zip_file(path, override=True)

    assert zipfile.is_zipfile(path)
    with zipfile.ZipFile(path, "r") as zf:
        assert "newfile.csv" in zf.namelist()


def test_list_data_files_success():
    """
    Test that `_list_data_files` returns only CSV files matching the expected BACI prefix.
    """
    zip_bytes = io.BytesIO()
    with zipfile.ZipFile(zip_bytes, "w") as zf:
        zf.writestr("BACI_HS22_2021.csv", "...")
        zf.writestr("BACI_HS22_2022.csv", "...")
        zf.writestr("not_data.csv", "...")  # Should be ignored
    zip_bytes.seek(0)

    manager = BaciDataManager("202501", "HS22")
    manager.zip_file = zipfile.ZipFile(zip_bytes)

    files = manager._list_data_files()
    assert "BACI_HS22_2021.csv" in files
    assert "BACI_HS22_2022.csv" in files
    assert "not_data.csv" not in files


def test_list_data_files_no_matching():
    """
    Test that `_list_data_files` raises FileNotFoundError when ZIP has no matching BACI files.
    """
    zip_bytes = io.BytesIO()
    with zipfile.ZipFile(zip_bytes, "w") as zf:
        zf.writestr("metadata.txt", "hello")
        zf.writestr("country_codes.csv", "...")
    zip_bytes.seek(0)

    manager = BaciDataManager("202501", "HS22")
    manager.zip_file = zipfile.ZipFile(zip_bytes)

    with pytest.raises(FileNotFoundError, match="No BACI data files found"):
        manager._list_data_files()


def test_list_data_files_empty_zip():
    """
    Test that `_list_data_files` raises FileNotFoundError if the ZIP is empty.
    """
    zip_bytes = io.BytesIO()
    with zipfile.ZipFile(zip_bytes, "w"):
        pass  # no files added
    zip_bytes.seek(0)

    manager = BaciDataManager("202501", "HS22")
    manager.zip_file = zipfile.ZipFile(zip_bytes)

    with pytest.raises(FileNotFoundError, match="No BACI data files found"):
        manager._list_data_files()


def test_read_data_files_success():
    """
    Test that `read_data_files` reads valid BACI CSVs from the ZIP, renames columns, writes Parquet,
    and loads the dataset into memory.
    """
    # Create an in-memory CSV
    csv_content = "t,i,j,k,v,q\n2021,1,2,100,500,10\n"

    zip_bytes = io.BytesIO()
    with zipfile.ZipFile(zip_bytes, "w") as zf:
        zf.writestr("BACI_HS22_2021.csv", csv_content)
    zip_bytes.seek(0)

    manager = BaciDataManager("202501", "HS22")
    manager.zip_file = zipfile.ZipFile(zip_bytes)

    manager.read_data_files()

    # Confirm Parquet files were written
    assert manager.arrow_temp_dir is not None
    parquet_dir = manager.arrow_temp_dir.name
    assert any(f.endswith(".parquet") for f in os.listdir(parquet_dir))

    # Confirm dataset is loaded and matches expected structure
    assert isinstance(manager.dataset, ds.Dataset)
    table = manager.dataset.to_table()
    df = table.to_pandas()
    assert "year" in df.columns
    assert df.iloc[0]["year"] == 2021


def test_read_data_files_skips_empty_csv(caplog):
    """
    Test that `read_data_files` skips over an empty BACI CSV file without crashing.
    It should not include that file in the dataset, and it should log a warning.
    """
    caplog.set_level("WARNING")

    zip_bytes = io.BytesIO()
    with zipfile.ZipFile(zip_bytes, "w") as zf:
        zf.writestr("BACI_HS22_2021.csv", "")  # empty file
    zip_bytes.seek(0)

    manager = BaciDataManager("202501", "HS22")
    manager.zip_file = zipfile.ZipFile(zip_bytes)

    manager.read_data_files()

    # Check that dataset is created but contains no data
    assert manager.arrow_temp_dir is not None
    assert isinstance(manager.dataset, ds.Dataset)
    assert manager.dataset.count_rows() == 0

    # Optional: verify log message
    assert "Skipping empty data file" in caplog.text


def test_read_data_files_no_matching_files():
    """
    Test that `read_data_files` raises FileNotFoundError when the ZIP has no BACI CSVs.
    Also confirms that no dataset is set.
    """
    zip_bytes = io.BytesIO()
    with zipfile.ZipFile(zip_bytes, "w") as zf:
        zf.writestr("not_baci.csv", "something,else\n1,2\n")
    zip_bytes.seek(0)

    manager = BaciDataManager("202501", "HS22")
    manager.zip_file = zipfile.ZipFile(zip_bytes)

    with pytest.raises(FileNotFoundError, match="No BACI data files found"):
        manager.read_data_files()

    # Should remain unset after failure
    assert manager.dataset is None


def test_read_product_codes_success():
    """
    Test that `read_product_codes` correctly finds, loads, and renames product codes.
    """
    csv_content = "code,description\n100,Cotton\n101,Coffee\n"
    zip_bytes = io.BytesIO()
    with zipfile.ZipFile(zip_bytes, "w") as zf:
        zf.writestr("product_codes.csv", csv_content)
    zip_bytes.seek(0)

    manager = BaciDataManager("202501", "HS22")
    manager.zip_file = zipfile.ZipFile(zip_bytes)

    manager.read_product_codes()

    df = manager.product_codes
    assert isinstance(df, pd.DataFrame)
    assert "product_code" in df.columns
    assert "product_description" in df.columns
    assert df.loc[0, "product_description"] == "Cotton"


def test_read_product_codes_empty_file():
    """
    Test that `read_product_codes` handles an empty product_codes.csv file gracefully.
    The resulting DataFrame should be empty but not raise an exception.
    """
    zip_bytes = io.BytesIO()
    with zipfile.ZipFile(zip_bytes, "w") as zf:
        zf.writestr("product_codes.csv", "")  # empty file
    zip_bytes.seek(0)

    manager = BaciDataManager("202501", "HS22")
    manager.zip_file = zipfile.ZipFile(zip_bytes)

    manager.read_product_codes()

    assert isinstance(manager.product_codes, pd.DataFrame)
    assert manager.product_codes.empty


def test_read_product_codes_missing_file():
    """
    Test that `read_product_codes` raises FileNotFoundError when no product_codes file exists.
    """
    zip_bytes = io.BytesIO()
    with zipfile.ZipFile(zip_bytes, "w") as zf:
        zf.writestr("something_else.csv", "code,description\n100,Cotton\n")
    zip_bytes.seek(0)

    manager = BaciDataManager("202501", "HS22")
    manager.zip_file = zipfile.ZipFile(zip_bytes)

    with pytest.raises(FileNotFoundError, match="No product codes file found"):
        manager.read_product_codes()


def test_read_country_codes_success():
    """
    Test that `read_country_codes` loads and renames columns correctly
    when a valid country_codes.csv file is present in the ZIP.
    """
    csv_content = (
        "country_code,country_name,country_iso3,country_iso2\n1,Nigeria,NGA,NG\n"
    )
    zip_bytes = io.BytesIO()
    with zipfile.ZipFile(zip_bytes, "w") as zf:
        zf.writestr("country_codes.csv", csv_content)
    zip_bytes.seek(0)

    manager = BaciDataManager("202501", "HS22")
    manager.zip_file = zipfile.ZipFile(zip_bytes)

    manager.read_country_codes()

    df = manager.country_codes
    assert isinstance(df, pd.DataFrame)
    assert "country_code" in df.columns
    assert "country_name" in df.columns
    assert "iso3_code" in df.columns
    assert "iso2_code" in df.columns
    assert df.iloc[0]["country_name"] == "Nigeria"


def test_read_country_codes_empty_file():
    """
    Test that `read_country_codes` handles an empty country_codes.csv file gracefully.
    The resulting DataFrame should be empty but not raise an exception.
    """
    zip_bytes = io.BytesIO()
    with zipfile.ZipFile(zip_bytes, "w") as zf:
        zf.writestr("country_codes.csv", "")  # empty file
    zip_bytes.seek(0)

    manager = BaciDataManager("202501", "HS22")
    manager.zip_file = zipfile.ZipFile(zip_bytes)

    manager.read_country_codes()

    assert isinstance(manager.country_codes, pd.DataFrame)
    assert manager.country_codes.empty


def test_read_country_codes_missing_file():
    """
    Test that `read_country_codes` raises FileNotFoundError when no matching file is found.
    """
    zip_bytes = io.BytesIO()
    with zipfile.ZipFile(zip_bytes, "w") as zf:
        zf.writestr("unrelated.csv", "data\n1\n")
    zip_bytes.seek(0)

    manager = BaciDataManager("202501", "HS22")
    manager.zip_file = zipfile.ZipFile(zip_bytes)

    with pytest.raises(FileNotFoundError, match="No country codes file found"):
        manager.read_country_codes()


def test_read_metadata_success():
    """
    Test that `read_metadata` successfully extracts metadata from a well-formed Readme.txt file.
    """
    readme = (
        "Version: 202501\n\n"
        "Release Date: 2025 01 30\n\n"
        "Content:\nTrade data\n\n"
        "List of Variables:\nt: year\n"
    )

    zip_bytes = io.BytesIO()
    with zipfile.ZipFile(zip_bytes, "w") as zf:
        zf.writestr("Readme.txt", readme)
    zip_bytes.seek(0)

    manager = BaciDataManager("202501", "HS22")
    manager.zip_file = zipfile.ZipFile(zip_bytes)

    manager.read_metadata()

    assert isinstance(manager.metadata, dict)
    assert manager.metadata.get("Version") == "202501"
    assert manager.metadata.get("Content") == "Trade data"
    assert "List of Variables" not in manager.metadata


def test_read_metadata_missing_file():
    """
    Test that `read_metadata` raises FileNotFoundError if Readme.txt is not found in the ZIP.
    """
    zip_bytes = io.BytesIO()
    with zipfile.ZipFile(zip_bytes, "w") as zf:
        zf.writestr("not_readme.txt", "irrelevant content")
    zip_bytes.seek(0)

    manager = BaciDataManager("202501", "HS22")
    manager.zip_file = zipfile.ZipFile(zip_bytes)

    with pytest.raises(FileNotFoundError, match="No Readme.txt file found"):
        manager.read_metadata()


def test_read_metadata_empty_file():
    """
    Test that `read_metadata` raises DataExtractionError if Readme.txt is empty.
    """
    zip_bytes = io.BytesIO()
    with zipfile.ZipFile(zip_bytes, "w") as zf:
        zf.writestr("Readme.txt", "")
    zip_bytes.seek(0)

    manager = BaciDataManager("202501", "HS22")
    manager.zip_file = zipfile.ZipFile(zip_bytes)

    with pytest.raises(DataExtractionError, match="No metadata found"):
        manager.read_metadata()


def test_read_metadata_unstructured_file():
    """
    Test that `read_metadata` raises DataExtractionError if Readme.txt is present
    but contains no key-value metadata.
    """
    readme = "This is a Readme file\nbut it has no metadata:\njust free text\n"

    zip_bytes = io.BytesIO()
    with zipfile.ZipFile(zip_bytes, "w") as zf:
        zf.writestr("Readme.txt", readme)
    zip_bytes.seek(0)

    manager = BaciDataManager("202501", "HS22")
    manager.zip_file = zipfile.ZipFile(zip_bytes)

    with pytest.raises(DataExtractionError, match="No metadata found"):
        manager.read_metadata()


def test_set_available_years_success():
    """
    Test that `set_available_years` extracts and assigns sorted unique years from the dataset.
    """
    table = pa.table({"year": pa.array([2020, 2019, 2021, 2020])})
    dataset = ds.dataset(table)

    manager = BaciDataManager("202501", "HS22")
    manager.dataset = dataset

    manager.set_available_years()

    assert manager.available_years == [2019, 2020, 2021]


def test_set_available_years_without_dataset():
    """
    Test that `set_available_years` raises an AttributeError when no dataset is loaded.
    """
    manager = BaciDataManager("202501", "HS22")
    manager.dataset = None

    with pytest.raises(AttributeError):
        manager.set_available_years()


def test_set_available_years_single_year():
    """
    Test that `set_available_years` works correctly when the dataset contains only one year.
    """
    table = pa.table({"year": pa.array([2022, 2022, 2022])})
    dataset = ds.dataset(table)

    manager = BaciDataManager("202501", "HS22")
    manager.dataset = dataset

    manager.set_available_years()
    assert manager.available_years == [2022]


def test_load_data_calls_all_methods():
    """
    Test that `load_data` calls the expected internal methods in order:
    - extract_zipfile_from_web
    - read_data_files
    - read_country_codes
    - read_product_codes
    - read_metadata
    - set_available_years
    """

    manager = BaciDataManager("202501", "HS22")

    with (
        patch.object(manager, "extract_zipfile_from_web") as mock_extract,
        patch.object(manager, "read_data_files") as mock_read_data,
        patch.object(manager, "read_country_codes") as mock_read_countries,
        patch.object(manager, "read_product_codes") as mock_read_products,
        patch.object(manager, "read_metadata") as mock_read_metadata,
        patch.object(manager, "set_available_years") as mock_set_years,
    ):

        manager.load_data()

        mock_extract.assert_called_once()
        mock_read_data.assert_called_once()
        mock_read_countries.assert_called_once()
        mock_read_products.assert_called_once()
        mock_read_metadata.assert_called_once()
        mock_set_years.assert_called_once()


def test_load_data_populates_all_fields():
    """
    End-to-end test of `load_data` using a well-formed ZIP file.
    Validates that the dataset, codes, metadata, and years are all populated.
    """
    # Craft a minimal valid ZIP archive
    zip_bytes = io.BytesIO()
    with zipfile.ZipFile(zip_bytes, "w") as zf:
        zf.writestr("BACI_HS22_2021.csv", "t,i,j,k,v,q\n2021,1,2,100,1000,50\n")
        zf.writestr(
            "country_codes.csv",
            "country_code,country_name,country_iso3,country_iso2\n1,Nigeria,NGA,NG\n2,France,FRA,FR\n",
        )
        zf.writestr("product_codes.csv", "code,description\n100,Test Product\n")
        zf.writestr("Readme.txt", "Version: 202501\nContent: Test data\n")

    zip_bytes.seek(0)

    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = zip_bytes.getvalue()
        mock_get.return_value = mock_response

        manager = BaciDataManager("202501", "HS22")
        manager.load_data()

        # Assertions
        assert isinstance(manager.dataset, ds.Dataset)
        assert isinstance(manager.country_codes, pd.DataFrame)
        assert isinstance(manager.product_codes, pd.DataFrame)
        assert isinstance(manager.metadata, dict)
        assert "Version" in manager.metadata
        assert manager.available_years == [2021]


def test_get_data_frame_no_filters():
    """
    Tests that get_data_frame returns a DataFrame with the expected columns and values when no filters are passed.
    """
    table = pa.table(
        {
            "year": pa.array([2021, 2022]),
            "product_code": pa.array([100, 200]),
            "exporter_code": pa.array([1, 2]),
            "importer_code": pa.array([3, 4]),
            "value": pa.array([5000, 6000]),
            "quantity": pa.array([100, 120]),
        }
    )
    manager = BaciDataManager("202501", "HS22")
    manager.dataset = ds.dataset(table)

    df = manager.get_data_frame(
        None, None, incl_country_labels=False, incl_product_labels=False
    )

    assert len(df) == 2
    assert "year" in df.columns
    assert df.iloc[0]["year"] == 2021


@pytest.mark.parametrize(
    "year_filter,expected",
    [
        (2021, [2021]),
        ([2021, 2022], [2021, 2022]),
        (range(2020, 2022), [2020, 2021]),
        ((2021, 2022), [2021, 2022]),
    ],
)
def test_get_data_frame_filters_years(year_filter, expected):
    """
    Tests that get_data_frame returns the expected DataFrame under different year filters.
    """
    table = pa.table(
        {
            "year": pa.array([2020, 2021, 2022]),
            "product_code": pa.array([1, 2, 3]),
            "exporter_code": pa.array([1, 1, 1]),
            "importer_code": pa.array([2, 2, 2]),
            "value": pa.array([100, 200, 300]),
            "quantity": pa.array([10, 20, 30]),
        }
    )

    manager = BaciDataManager("202501", "HS22")
    manager.dataset = ds.dataset(table)

    df = manager.get_data_frame(year_filter, None, False, False)
    assert set(df["year"].unique()) == set(expected)


def test_get_data_frame_filters_products():
    """
    Tests that get_data_frame returns the expected DataFrame with product filters.
    """
    table = pa.table(
        {
            "year": pa.array([2021, 2021]),
            "product_code": pa.array([100, 200]),
            "exporter_code": pa.array([1, 2]),
            "importer_code": pa.array([3, 4]),
            "value": pa.array([500, 600]),
            "quantity": pa.array([50, 60]),
        }
    )

    manager = BaciDataManager("202501", "HS22")
    manager.dataset = ds.dataset(table)

    df = manager.get_data_frame(2021, 100, False, False)
    assert len(df) == 1
    assert df.iloc[0]["product_code"] == 100


def test_get_data_frame_with_country_and_product_labels():
    """
    Test get_data_frame with country and product labels.
    """
    table = pa.table(
        {
            "year": pa.array([2021]),
            "product_code": pa.array([100]),
            "exporter_code": pa.array([1]),
            "importer_code": pa.array([2]),
            "value": pa.array([1000]),
            "quantity": pa.array([10]),
        }
    )
    manager = BaciDataManager("202501", "HS22")
    manager.dataset = ds.dataset(table)

    # Add mapping DataFrames
    manager.country_codes = pd.DataFrame(
        {
            "country_code": [1, 2],
            "country_name": ["Nigeria", "France"],
            "iso3_code": ["NGA", "FRA"],
            "iso2_code": ["NG", "FR"],
        }
    )
    manager.product_codes = pd.DataFrame(
        {
            "product_code": [100],
            "product_description": ["Cotton"],
        }
    )

    df = manager.get_data_frame(2021, 100, True, True)

    assert "exporter_name" in df.columns
    assert df.loc[0, "exporter_name"] == "Nigeria"
    assert "product_description" in df.columns
    assert df.loc[0, "product_description"] == "Cotton"


def test_get_data_frame_filters_to_empty():
    """
    Test that get_data_frame returns an empty DataFrame when no rows match
    """
    table = pa.table(
        {
            "year": pa.array([2020]),
            "product_code": pa.array([999]),
            "exporter_code": pa.array([99]),
            "importer_code": pa.array([88]),
            "value": pa.array([123]),
            "quantity": pa.array([1]),
        }
    )
    manager = BaciDataManager("202501", "HS22")
    manager.dataset = ds.dataset(table)

    df = manager.get_data_frame(2022, 100, False, False)
    assert df.empty


def test_get_data_frame_raises_without_dataset():
    """
    Test that get_data_frame raises an AttributeError when no dataset is loaded.
    """
    manager = BaciDataManager("202501", "HS22")
    manager.dataset = None

    with pytest.raises(AttributeError):
        manager.get_data_frame(2021, 100, False, False)


def test_load_and_get_data_frame_with_labels():
    zip_bytes = io.BytesIO()
    with zipfile.ZipFile(zip_bytes, "w") as zf:
        zf.writestr("BACI_HS22_2021.csv", "t,i,j,k,v,q\n2021,1,2,100,1000,50\n")
        zf.writestr(
            "country_codes.csv",
            "country_code,country_name,country_iso3,country_iso2\n1,Nigeria,NGA,NG\n2,France,FRA,FR\n",
        )
        zf.writestr("product_codes.csv", "code,description\n100,Test Product\n")
        zf.writestr("Readme.txt", "Version: 202501\nContent: Test")

    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = zip_bytes.getvalue()
        mock_get.return_value = mock_response

        manager = BaciDataManager("202501", "HS22")
        manager.load_data()

        df = manager.get_data_frame(2021, 100, True, True)

        assert "exporter_name" in df.columns
        assert "product_description" in df.columns
        assert df.loc[0, "exporter_name"] == "Nigeria"
        assert df.loc[0, "product_description"] == "Test Product"


def test_del_calls_cleanup():
    manager = BaciDataManager("202501", "HS22")
    manager.arrow_temp_dir = MagicMock()
    manager.__del__()
    manager.arrow_temp_dir.cleanup.assert_called_once()
