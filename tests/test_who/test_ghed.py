"""Tests for GHED module"""

from bblocks_data_importers.who.ghed import GHED, URL
from bblocks_data_importers.config import DataExtractionError, DataFormattingError
from bblocks_data_importers.config import Paths

import pytest
from unittest import mock
from pathlib import Path
import io
import requests
import pandas as pd
import numpy as np
import os


TEST_FILE_PATH = "tests/test_data/test_ghed.XLSX"
FORMATTED_METADATA = pd.read_feather("tests/test_data/formatted_metadata_ghed.feather")


@pytest.fixture
def mock_raw_data():
    """Fixture for simulating raw data as BytesIO from the test file"""
    with open(TEST_FILE_PATH, "rb") as file:
        raw_data_content = file.read()
    return io.BytesIO(raw_data_content)


# Fixture for mocking the response
@pytest.fixture
def mock_successful_response():
    """Fixture for a successful response mock"""
    mock_response = mock.Mock()
    mock_response.status_code = 200
    mock_response.content = b'some binary content'
    return mock_response


@pytest.fixture
def mock_http_error_response():
    """Fixture for an HTTP error response (non-200 status code)"""
    mock_response = mock.Mock()
    mock_response.status_code = 404
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("404 Client Error")
    return mock_response






def test_init_without_data_file():
    """Test initialization without a data file
    No file path passed, so it should not raise an exception
    """
    ghed = GHED()
    assert ghed._data_file is None
    assert ghed._raw_data is None
    assert ghed._data is None
    assert ghed._metadata is None


@mock.patch("bblocks_data_importers.who.ghed.Path.exists", return_value=True)
def test_init_with_valid_data_file(mock_exists):
    """Test initialization with a valid data file
     Passing a valid file path, so it should not raise an exception
    """
    ghed = GHED(data_file="some_valid_path.xlsx")
    assert isinstance(ghed._data_file, Path)
    mock_exists.assert_called_once()

    assert ghed._raw_data is None
    assert ghed._data is None
    assert ghed._metadata is None


@mock.patch("bblocks_data_importers.who.ghed.Path.exists", return_value=False)
def test_init_with_invalid_data_file(mock_exists):
    """Test initialization with an invalid data file
    Should raise a FileNotFoundError
    """
    with pytest.raises(FileNotFoundError):
        GHED(data_file="invalid_path.xlsx")
    mock_exists.assert_called_once()



# Test case 1: Test successful data extraction
@mock.patch("bblocks_data_importers.who.ghed.requests.get")
def test_extract_raw_data_success(mock_get, mock_successful_response):
    """Test successful data extraction
    Should return a BytesIO object with the correct content
    """
    # Use the fixture to set the mock response
    mock_get.return_value = mock_successful_response

    # Call the method
    result = GHED._extract_raw_data()

    # Check that the result is a BytesIO object with the correct content
    assert isinstance(result, io.BytesIO)
    assert result.getvalue() == b'some binary content'
    mock_get.assert_called_once_with(URL)


# Test case 2: Simulate network failure (RequestException)
@mock.patch("bblocks_data_importers.who.ghed.requests.get")
def test_extract_raw_data_request_exception(mock_get):
    """Test network failure during data extraction"""
    # Set the mock to raise a RequestException
    mock_get.side_effect = requests.exceptions.RequestException("Network error")

    # Assert that a DataExtractionError is raised when calling the method
    with pytest.raises(DataExtractionError):
        GHED._extract_raw_data()

    # Ensure that the GET request was attempted
    mock_get.assert_called_once_with(URL)


# Test case 3: Simulate non-200 status code (e.g., 404)
@mock.patch("bblocks_data_importers.who.ghed.requests.get")
def test_extract_raw_data_http_error(mock_get, mock_http_error_response):
    """Test HTTP error during data extraction"""
    # Use the fixture to set the mock response
    mock_get.return_value = mock_http_error_response

    # Assert that a DataExtractionError is raised when calling the method
    with pytest.raises(DataExtractionError):
        GHED._extract_raw_data()

    # Ensure that the GET request was made to the correct URL
    mock_get.assert_called_once_with(URL)


def test_format_metadata_success(mock_raw_data):
    """Test the _format_metadata method for successful formatting"""

    result = GHED._format_metadata(mock_raw_data)
    expected_metadata = pd.read_feather("tests/test_data/formatted_metadata_ghed.feather").replace({None: np.nan})
    pd.testing.assert_frame_equal(result, expected_metadata)


def test_format_metadata_malformed_data():
    """Test the _format_metadata method raises a DataFormattingError when the data is malformed"""
    # Create some malformed data (e.g., random binary data that cannot be read as Excel)
    malformed_data = b'invalid raw content'

    # Call the method and ensure it raises a DataFormattingError
    with pytest.raises(DataFormattingError):
        GHED._format_metadata(io.BytesIO(malformed_data))


def test_format_metadata_missing_columns(mock_raw_data):
    """Test the _format_metadata method raises a DataFormattingError when required columns are missing"""

    # format the raw data to remove a required column
    raw_df = pd.read_excel(mock_raw_data, sheet_name="Metadata")
    raw_df.drop(columns=["variable code"], inplace=True)
    modified_raw_data = io.BytesIO()
    with pd.ExcelWriter(modified_raw_data, engine='xlsxwriter') as writer:
        raw_df.to_excel(writer, sheet_name="Metadata")
    modified_raw_data.seek(0)  # Reset the pointer to the start of the BytesIO object

    # test
    with pytest.raises(DataFormattingError):
        GHED._format_metadata(modified_raw_data)


def test_format_data_success(mock_raw_data):
    """Test the _format_data method for successful formatting"""

    result = GHED._format_data(mock_raw_data)
    expected_data = pd.read_feather("tests/test_data/formatted_data_ghed.feather").replace({None: np.nan})
    result = result.replace({None: np.nan})

    pd.testing.assert_frame_equal(result, expected_data)


def test_format_data_malformed_data():
    """Test the _format_data method raises a DataFormattingError when the data is malformed"""
    malformed_data = b'invalid raw content'

    with pytest.raises(DataFormattingError):
        GHED._format_data(io.BytesIO(malformed_data))


def test_format_data_missing_columns(mock_raw_data):
    """Test the _format_data method raises a DataFormattingError when required columns are missing"""

    # format the raw data to remove a required column
    raw_df = pd.read_excel(mock_raw_data, sheet_name="Data")
    raw_df.drop(columns=["country"], inplace=True)
    modified_raw_data = io.BytesIO()
    with pd.ExcelWriter(modified_raw_data, engine='xlsxwriter') as writer:
        raw_df.to_excel(writer, sheet_name="Data")
    modified_raw_data.seek(0)  # Reset the pointer to the start of the BytesIO object

    # test
    with pytest.raises(DataFormattingError):
        GHED._format_data(modified_raw_data)


def test_read_local_data_success(mock_raw_data):
    """Test the _read_local_data method for successful reading"""
    # Create a mock Path object for the test
    mock_path = Path(TEST_FILE_PATH)

    # Call the method with the mock path
    result = GHED._read_local_data(mock_path)

    # Check that the result is a BytesIO object containing the correct content
    assert isinstance(result, io.BytesIO)
    assert result.getvalue() == mock_raw_data.getvalue()


@mock.patch("bblocks_data_importers.who.ghed.Path.open", side_effect=IOError)
def test_read_local_data_read_error(mock_open):
    """Test the _read_local_data method raises a DataExtractionError when the file cannot be read"""
    # Create a mock path for a corrupted or inaccessible file
    mock_path = Path("corrupted_file.xlsx")

    # Call the method and ensure it raises a DataExtractionError
    with pytest.raises(DataExtractionError):
        GHED._read_local_data(mock_path)

    # Check that the file open was attempted
    mock_open.assert_called_once_with("rb")


@mock.patch("bblocks_data_importers.who.ghed.GHED._extract_raw_data")
@mock.patch("bblocks_data_importers.who.ghed.GHED._format_data")
@mock.patch("bblocks_data_importers.who.ghed.GHED._format_metadata")
def test_load_data_no_local_file(mock_format_metadata, mock_format_data, mock_extract_raw_data):
    """Test that _extract_raw_data is called when no local file is provided"""
    # Mock the return value for _extract_raw_data
    mock_extract_raw_data.return_value = io.BytesIO(b'some raw data')

    # Initialize GHED without a local file
    ghed = GHED()

    # Call the method to load data
    ghed._load_data()

    # Ensure that _extract_raw_data is called
    mock_extract_raw_data.assert_called_once()

    # Ensure that the formatting methods are called
    mock_format_data.assert_called_once()
    mock_format_metadata.assert_called_once()


@mock.patch("bblocks_data_importers.who.ghed.GHED._read_local_data")
@mock.patch("bblocks_data_importers.who.ghed.GHED._format_data")
@mock.patch("bblocks_data_importers.who.ghed.GHED._format_metadata")
def test_load_data_with_local_file(mock_format_metadata, mock_format_data, mock_read_local_data):
    """Test that _read_local_data is called when a local file is provided"""
    # Mock the return value for _read_local_data
    mock_read_local_data.return_value = io.BytesIO(b'some raw data')

    # Initialize GHED with a local file
    ghed = GHED(data_file=TEST_FILE_PATH)

    # Call the method to load data
    ghed._load_data()

    # Ensure that _read_local_data is called
    mock_read_local_data.assert_called_once()

    # Ensure that the formatting methods are called
    mock_format_data.assert_called_once()
    mock_format_metadata.assert_called_once()


def test_clear_cache():
    """Test that clear_cache sets _raw_data, _data, and _metadata to None"""
    # Initialize GHED and set mock values for the cache attributes
    ghed = GHED()
    ghed._raw_data = io.BytesIO(b'some raw data')
    ghed._data = pd.DataFrame({"column": [1, 2, 3]})
    ghed._metadata = pd.DataFrame({"column": ["meta1", "meta2", "meta3"]})

    # Ensure that the cache is populated before calling clear_cache
    assert ghed._raw_data is not None
    assert ghed._data is not None
    assert ghed._metadata is not None

    # Call clear_cache
    ghed.clear_cache()

    # Ensure that all cached data is cleared (set to None)
    assert ghed._raw_data is None
    assert ghed._data is None
    assert ghed._metadata is None


@mock.patch("bblocks_data_importers.who.ghed.Path.exists", return_value=True)
@mock.patch("bblocks_data_importers.who.ghed.open", new_callable=mock.mock_open)
def test_download_raw_data_success(mock_file_open, mock_path_exists):
    """Test that download_raw_data successfully saves the raw data to disk"""
    # Initialize GHED and set mock raw data
    ghed = GHED()
    ghed._raw_data = io.BytesIO(b'some raw data')

    # Call download_raw_data with a valid path and file name
    ghed.download_raw_data(path="some_valid_directory", file_name="ghed_test", overwrite=True)

    # Ensure that the file is opened for writing in binary mode
    mock_file_open.assert_called_once_with(Path("some_valid_directory") / "ghed_test.xlsx", "wb")

    # Ensure that data was written to the file
    mock_file_open().write.assert_called_once_with(b'some raw data')


@mock.patch("bblocks_data_importers.who.ghed.Path.exists", return_value=True)
@mock.patch("bblocks_data_importers.who.ghed.open", new_callable=mock.mock_open)
def test_download_raw_data_file_exists_no_overwrite(mock_file_open, mock_path_exists):
    """Test that download_raw_data raises a FileExistsError if the file exists and overwrite is False"""
    # Initialize GHED and set mock raw data
    ghed = GHED()
    ghed._raw_data = io.BytesIO(b'some raw data')

    # Simulate the file already existing (mock_path_exists is set to True)
    mock_path_exists.return_value = True

    # Test that a FileExistsError is raised when overwrite is False
    with pytest.raises(FileExistsError):
        ghed.download_raw_data(path="some_valid_directory", file_name="ghed_test", overwrite=False)

    # Ensure that the file was not opened for writing
    mock_file_open.assert_not_called()


@mock.patch("bblocks_data_importers.who.ghed.Path.exists", return_value=False)
def test_download_raw_data_directory_not_found(mock_path_exists):
    """Test that download_raw_data raises a FileNotFoundError if the directory does not exist"""
    # Initialize GHED and set mock raw data
    ghed = GHED()
    ghed._raw_data = io.BytesIO(b'some raw data')

    # Simulate the directory not existing (mock_path_exists is set to False)
    mock_path_exists.return_value = False

    # Test that a FileNotFoundError is raised
    with pytest.raises(FileNotFoundError):
        ghed.download_raw_data(path="non_existent_directory", file_name="ghed_test")

    # Ensure that the file was not opened for writing
    mock_path_exists.assert_called_once_with()


@mock.patch("bblocks_data_importers.who.ghed.GHED._load_data")
@mock.patch("bblocks_data_importers.who.ghed.Path.exists", return_value=True)
@mock.patch("bblocks_data_importers.who.ghed.open", new_callable=mock.mock_open)
def test_download_raw_data_calls_load_data_if_raw_data_none(mock_file_open, mock_path_exists, mock_load_data, mock_raw_data):
    """Test that _load_data is called if _raw_data is None in download_raw_data"""
    # Initialize GHED with no raw data
    ghed = GHED()

    # Simulate that _load_data sets _raw_data to mock_raw_data after being called
    mock_load_data.side_effect = lambda: setattr(ghed, '_raw_data', mock_raw_data)

    # Call download_raw_data while _raw_data is initially None
    ghed.download_raw_data(path="some_valid_directory", file_name="ghed_test", overwrite=True)

    # Ensure that _load_data is called because _raw_data was None
    mock_load_data.assert_called_once()

    # Ensure that the file is opened for writing
    mock_file_open.assert_called_once_with(Path("some_valid_directory") / "ghed_test.xlsx", "wb")

    # Ensure that the raw data is written to the file
    mock_file_open().write.assert_called_once_with(mock_raw_data.getvalue())


@mock.patch("bblocks_data_importers.who.ghed.GHED._load_data")
def test_get_data_calls_load_data_if_data_none(mock_load_data):
    """Test that get_data calls _load_data if _data is None and returns a DataFrame"""
    # Initialize GHED (default _data is None)
    ghed = GHED()

    # Simulate _load_data setting _data after being called
    mock_load_data.side_effect = lambda: setattr(ghed, '_data', pd.DataFrame({"column": [1, 2, 3]}))

    # Call get_data (since _data is None, it should call _load_data)
    result = ghed.get_data()

    # Ensure that _load_data is called because _data was None
    mock_load_data.assert_called_once()

    # Ensure that the result is a DataFrame
    assert isinstance(result, pd.DataFrame)

@mock.patch("bblocks_data_importers.who.ghed.GHED._load_data")
def test_get_data_does_not_call_load_data_if_data_exists(mock_load_data):
    """Test that get_data does not call _load_data if _data is already populated"""
    # Initialize GHED with pre-populated data
    ghed = GHED()
    ghed._data = pd.DataFrame({"column": [1, 2, 3]})  # Simulate that data is already loaded

    # Call get_data
    result = ghed.get_data()

    # Ensure that _load_data is not called because _data is already populated
    mock_load_data.assert_not_called()

    # Ensure that the result is the pre-loaded DataFrame
    assert isinstance(result, pd.DataFrame)
    assert result.equals(ghed._data)  # Ensure it's the same data


@mock.patch("bblocks_data_importers.who.ghed.GHED._load_data")
def test_get_metadata_calls_load_data_if_metadata_none(mock_load_data):
    """Test that get_metadata calls _load_data if _metadata is None and returns a DataFrame"""
    # Initialize GHED (default _metadata is None)
    ghed = GHED()

    # Simulate _load_data setting _metadata after being called
    mock_load_data.side_effect = lambda: setattr(ghed, '_metadata', pd.DataFrame({"column": ["meta1", "meta2"]}))

    # Call get_metadata (since _metadata is None, it should call _load_data)
    result = ghed.get_metadata()

    # Ensure that _load_data is called because _metadata was None
    mock_load_data.assert_called_once()

    # Ensure that the result is a DataFrame
    assert isinstance(result, pd.DataFrame)


@mock.patch("bblocks_data_importers.who.ghed.GHED._load_data")
def test_get_metadata_does_not_call_load_data_if_metadata_exists(mock_load_data):
    """Test that get_metadata does not call _load_data if _metadata is already populated"""
    # Initialize GHED with pre-populated metadata
    ghed = GHED()
    ghed._metadata = pd.DataFrame({"column": ["meta1", "meta2"]})  # Simulate that metadata is already loaded

    # Call get_metadata
    result = ghed.get_metadata()

    # Ensure that _load_data is not called because _metadata is already populated
    mock_load_data.assert_not_called()

    # Ensure that the result is the pre-loaded DataFrame
    assert isinstance(result, pd.DataFrame)
    assert result.equals(ghed._metadata)  # Ensure it's the same metadata

