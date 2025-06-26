"""Tests for BaciDataManager class."""

import os
import io
import zipfile
import pytest
import requests
import pyarrow.csv as pv
import pyarrow.parquet as pq
import pyarrow.dataset as ds
import pyarrow as pa
import pandas as pd
from unittest.mock import patch, MagicMock

from bblocks.data_importers.cepii.extract import BaciDataManager
from bblocks.data_importers.config import DataExtractionError


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


def test_read_data_files_no_matching_files():
    """
    Test that `read_data_files` raises FileNotFoundError when the ZIP has no BACI CSVs.
    """
    zip_bytes = io.BytesIO()
    with zipfile.ZipFile(zip_bytes, "w") as zf:
        zf.writestr("not_baci.csv", "something,else\n1,2\n")
    zip_bytes.seek(0)

    manager = BaciDataManager("202501", "HS22")
    manager.zip_file = zipfile.ZipFile(zip_bytes)

    with pytest.raises(FileNotFoundError, match="No BACI data files found"):
        manager.read_data_files()


import pyarrow.lib


def test_read_data_files_malformed_csv():
    """
    Test that `read_data_files` raises a PyArrow error when a BACI CSV is malformed.
    """
    # Malformed CSV: missing values
    csv_content = "t,i,j\n2021,1\n"  # too few columns

    zip_bytes = io.BytesIO()
    with zipfile.ZipFile(zip_bytes, "w") as zf:
        zf.writestr("BACI_HS22_bad.csv", csv_content)
    zip_bytes.seek(0)

    manager = BaciDataManager("202501", "HS22")
    manager.zip_file = zipfile.ZipFile(zip_bytes)

    with pytest.raises(pa.lib.ArrowInvalid):
        manager.read_data_files()


def test_read_data_files_unmapped_columns():
    """
    Test that `read_data_files` succeeds even if the CSV has no BACI-mappable columns.
    """
    csv_content = "foo,bar\n1,2\n"

    zip_bytes = io.BytesIO()
    with zipfile.ZipFile(zip_bytes, "w") as zf:
        zf.writestr("BACI_HS22_weird.csv", csv_content)
    zip_bytes.seek(0)

    manager = BaciDataManager("202501", "HS22")
    manager.zip_file = zipfile.ZipFile(zip_bytes)

    manager.read_data_files()

    table = manager.dataset.to_table()
    df = table.to_pandas()
    assert "foo" in df.columns
    assert df.iloc[0]["foo"] == 1

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


def test_read_product_codes_unmapped_columns():
    """
    Test that `read_product_codes` still works if columns don't match expected renaming keys.
    """
    csv_content = "id,label\n100,Test\n"
    zip_bytes = io.BytesIO()
    with zipfile.ZipFile(zip_bytes, "w") as zf:
        zf.writestr("product_codes_extra.csv", csv_content)
    zip_bytes.seek(0)

    manager = BaciDataManager("202501", "HS22")
    manager.zip_file = zipfile.ZipFile(zip_bytes)

    manager.read_product_codes()

    df = manager.product_codes
    assert "id" in df.columns
    assert "label" in df.columns
    assert "product_code" not in df.columns


def test_read_product_codes_empty():
    """
    Test that `read_product_codes` loads an empty DataFrame if the product_codes file is empty.
    """
    csv_content = ""
    zip_bytes = io.BytesIO()
    with zipfile.ZipFile(zip_bytes, "w") as zf:
        zf.writestr("product_codes.csv", csv_content)
    zip_bytes.seek(0)

    manager = BaciDataManager("202501", "HS22")
    manager.zip_file = zipfile.ZipFile(zip_bytes)

    manager.read_product_codes()

    df = manager.product_codes
    assert isinstance(df, pd.DataFrame)
    assert df.empty
