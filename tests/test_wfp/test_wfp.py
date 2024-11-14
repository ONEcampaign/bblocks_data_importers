"""Test wfp module"""

import io
import pandas as pd
import pytest
import requests
from unittest import mock

import bblocks_data_importers.wfp.wfp as WFP 
from bblocks_data_importers.wfp.wfp import (
    WFPInflation,  
    WFPFoodSecurity
)
from bblocks_data_importers.config import (
    DataExtractionError,
    DataFormattingError
)

# Fixtures
@pytest.fixture(autouse=True)
def reset_cached_countries():
    """Reset the global _cached_countries variable before each test"""

    WFP._cached_countries = None


@pytest.fixture
def mock_request_countries():
    """Fixture to simulate successful response for WFP available countries"""

    mock_request = mock.Mock()
    mock_request.json.return_value = {
        "body": {
            "features": [
                {
                    "properties": {
                        "iso3": "VEN",
                        "adm0_id": 1,
                        "dataType": None
                    }
                },
                {
                    "properties": {
                        "iso3": "LBN",
                        "adm0_id": 2,
                        "dataType": "ACTUAL DATA"
                    }
                }
            ]
        }
    }
    mock_request.raise_for_status = mock.Mock()
    
    with mock.patch(
        "requests.get", return_value=mock_request
    ): yield mock_request


@pytest.fixture
def mock_cached_countries():
    """Fixture to simulate cached countries"""
    
    mock_cache = {
        'VNM': {
            'entity_code': 1, 
            'data_type': 'PREDICTION', 
            'country_name': 'Vietnam'
        }
    }
    WFP._cached_countries = mock_cache


@pytest.fixture
def mock_response_inflation():
    """Fixture to simulate inflation data request"""

    mock_response = mock.Mock()
    mock_response.status_code = 200
    mock_response.content = b"mocked response data"

    with mock.patch(
        "requests.post", return_value=mock_response
    ) as mock_post: 
        yield mock_post

@pytest.fixture
def wfp_inflation():
    """
    Fixture to instantiate the WFPInflation class with mocked attributes
    """
    
    instance = WFPInflation()
    instance._countries = {
        "USA": {"entity_code": 1},
        "CAN": {"entity_code": 2},
    }
    return instance

# TESTS

def test_wfp_inflation_init():
    """
    Test WFPInflation class initialization
    """

    # Instantiate class
    wfp_inflation_importer = WFPInflation()

    # Assertions
    assert wfp_inflation_importer._timeout == 20
    assert wfp_inflation_importer._indicators == {
            "Headline inflation (YoY)": 116,
            "Headline inflation (MoM)": 117,
            "Food inflation": 71,
        }
    assert wfp_inflation_importer._countries == None
    assert wfp_inflation_importer._data == {
            "Headline inflation (YoY)": {},
            "Headline inflation (MoM)": {},
            "Food inflation": {},
        }
    

def test_wfp_inflation_load_available_countries_no_cached(mock_request_countries):
    """
    Test `load_available_countries` method of WFPInflation class handling of no chached countries
    """

    # Instantiate class and call method
    wfp_inflation_importer = WFPInflation()
    wfp_inflation_importer.load_available_countries()

    # Assertions
    assert len(wfp_inflation_importer._countries) == 2     
    assert wfp_inflation_importer._countries["VEN"] == {
            'entity_code': 1, 
            'data_type': None, 
            'country_name': 'Venezuela'
        }
    assert wfp_inflation_importer._countries["LBN"] == {
            'entity_code': 2, 
            'data_type': 'ACTUAL DATA', 
            'country_name': 'Lebanon'
        }


def test_wfp_inflation_load_available_countries_timeout(mock_request_countries):
    """
    Test `load_available_countries` method of WFPInflation class handling of `requests.post` timeout
    """

    # Instantiate class
    wfp_inflation_importer = WFPInflation() 
    
    # Mock request with timeout
    with mock.patch(
        "requests.get", side_effect=requests.exceptions.Timeout
    ):  
        # Assert that DataExtrationError is raised when method is called
        with pytest.raises(
            DataExtractionError, 
            match="Request timed out while getting country IDs after 3 attempts"
        ): 
            wfp_inflation_importer.load_available_countries()


def test_wfp_inflation_load_available_countries_exception(mock_request_countries):
    """
    Test `load_available_countries` method of WFPInflation class handling of `requests.post` exception errors
    """

    # Instantiate class
    wfp_inflation_importer = WFPInflation()

    # Mock request with network error
    with mock.patch(
        "requests.get", side_effect=requests.exceptions.RequestException("Network error")
    ): 
        # Assert that DataExtrationError is raised when method is called
        with pytest.raises(
            DataExtractionError, 
            match="Error getting country IDs after 3 attempts: Network error"
        ): 
            wfp_inflation_importer.load_available_countries()


def test_wfp_inflation_load_available_countries_chached(mock_cached_countries):
    """
    Test `load_available_countries` method of WFPInflation class handling of cached countries
    """
    
    # Mock request
    with mock.patch("requests.get") as mock_get:
        # Instantiate class and call method
        wfp_inflation_importer = WFPInflation()
        wfp_inflation_importer.load_available_countries()

        # Assertions
        assert len(wfp_inflation_importer._countries) == 1
        assert wfp_inflation_importer._countries["VNM"]["country_name"] == "Vietnam"
        mock_get.assert_not_called()


# Parameterization for single and multiple indicators
@pytest.mark.parametrize(
    "indicator_code, expected_json",
    [
        (100, {"adm0Code": 1, "economicIndicatorIds": [100]}), # Single 
        ([100, 200], {"adm0Code": 1, "economicIndicatorIds": [100, 200]}), # Multiple
    ],
)
def test_wfp_inflation_extract_data(mock_response_inflation, indicator_code, expected_json):
    """
    Test `extract_data` method of WFPInflation class handling of one and multiple indicators
    """
    
    # Instantiate class
    wfp_inflation_importer = WFPInflation()
    # Call method
    country_code = 1
    result = wfp_inflation_importer.extract_data(country_code, indicator_code)
    
    # Assertions
    assert isinstance(result, io.BytesIO)
    assert result.getvalue() == b"mocked response data"
    mock_response_inflation.assert_called_once_with(
        f"{WFP.VAM_API}/economicExplorer/TradingEconomics/InflationExport",
        json=expected_json,
        headers=WFP.VAM_HEADERS,
        timeout=wfp_inflation_importer._timeout,
    )

def test_wfp_inflation_extract_data_timeout(mock_response_inflation):
    """
    Test `extract_data` method of WFPInflation class handling of `requests.post` timeout
    """

    # Instantiate class
    wfp_inflation_importer = WFPInflation()
    
    country_code = 1
    indicator_code = 100
    
    # Mock response with timeout
    mock_response_inflation.side_effect = requests.exceptions.Timeout
    
    # Assert that DataExtrationError is raised when method is called
    with pytest.raises(DataExtractionError, match="Request timed out while getting inflation data"):
        wfp_inflation_importer.extract_data(country_code, indicator_code)


def test_wfp_inflation_extract_data_request_exception(mock_response_inflation):
    """
    Test `extract_data` method of WFPInflation class handling of `requests.post` exception
    """

    # Instantiate class
    wfp_inflation_importer = WFPInflation()
    
    country_code = 1
    indicator_code = 100

    # Mock response with error
    mock_response_inflation.side_effect = requests.exceptions.RequestException("Mocked request error")
    
    # Assert that DataExtrationError is raised when method is called
    with pytest.raises(DataExtractionError, match="Error getting inflation data: Mocked request error"):
        wfp_inflation_importer.extract_data(country_code, indicator_code)


def test_wfp_inflation_format_data_successful():
    """
    Test `format_data` method of WFPInflation class handling of valid data
    """

    # Mock data
    test_csv = """IndicatorName,CountryName,Date,Value,SourceOfTheData
    Headline inflation (YoY),Ecuador,31/10/2024,5.1,MockSource"""
    data = io.BytesIO(test_csv.encode("utf-8"))
    indicator_name = "Headline inflation (YoY)"
    iso3_code = "ECU"

    # Create expected DataFrame
    expected_df = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-10-31"]),
            "value": [5.1],
            "source": ["MockSource"],
            "indicator_name": ["Headline inflation (YoY)"],
            "iso3_code": ["ECU"],
            "country_name": ["Ecuador"],
            "unit": ["percent"],
        }
    )

    # Instantiate class
    wfp_inflation_importer = WFPInflation()

    # Call method
    result_df = wfp_inflation_importer.format_data(data, indicator_name, iso3_code)

    # Assert that resulted and expected DataFrames are equal
    pd.testing.assert_frame_equal(result_df, expected_df, check_dtype=False)


def test_wfp_inflation_format_data_exception():
    """
    Test `format_data` method of WFPInflation class handling of invalid data
    """
    
    # Mock data
    invalid_data = io.BytesIO(b"Invalid,CSV,Data")
    indicator_name = "Inflation"
    iso3_code = "TST"

    # Initiate class
    wfp_inflation_importer = WFPInflation()
    
    # Assert that DataFormattingError is raised when method is called
    with pytest.raises(DataFormattingError, match="Error formatting data for country - TST:"):
        wfp_inflation_importer.format_data(invalid_data, indicator_name, iso3_code)


def test_wfp_inflation_load_data_all_countries_loaded(wfp_inflation):
    """
    Test `load_data` method of WFPInflation class handling of all countries being loaded
    """
    
    # Add data to class
    wfp_inflation._data["Headline inflation (YoY)"] = {"USA": mock.Mock(), "CAN": mock.Mock()}

    # Mock logs
    with mock.patch("bblocks_data_importers.config.logger.info") as mock_logger:
        # Call method
        wfp_inflation.load_data("Headline inflation (YoY)", ["USA", "CAN"])
        # Assert that no messages are logged
        mock_logger.assert_not_called()


def test_wfp_inflation_load_data_country_not_available(wfp_inflation):
    """
    Test `load_data` method of WFPInflation class handling of a country not being available
    """

    # Mock logs
    with mock.patch("bblocks_data_importers.config.logger.warning") as mock_logger:
        # Call method
        wfp_inflation.load_data("Headline inflation (YoY)", ["USA", "MEX"])
        # Assertions
        mock_logger.assert_called_once_with("Data not found for country - MEX")
        assert wfp_inflation._data["Headline inflation (YoY)"]["MEX"] is None


def test_wfp_inflation_load_data_successful(wfp_inflation):
    """
    Test `load_data` method of WFPInflation class handling of no loaded countries
    """

    # Mock data
    mock_extracted_data = "mocked data"
    mock_formatted_data = pd.DataFrame({"value": [1]}) 

    # Mock logs, `extract_data` and `format_data` methods
    with mock.patch("bblocks_data_importers.config.logger.info") as mock_logger, \
         mock.patch.object(wfp_inflation, "extract_data", return_value=mock_extracted_data) as mock_extract, \
         mock.patch.object(wfp_inflation, "format_data", return_value=mock_formatted_data) as mock_format:
        
        # Call method
        wfp_inflation.load_data("Headline inflation (YoY)", ["USA"])

        # Assertions
        mock_extract.assert_called_once_with(1, 116)
        mock_format.assert_called_once_with(mock_extracted_data, "Headline inflation (YoY)", "USA")
        assert wfp_inflation._data["Headline inflation (YoY)"]["USA"] is not None
        mock_logger.assert_any_call("Data imported successfully for indicator: Headline inflation (YoY)")

def test_wfp_inflation_load_data_empty_data(wfp_inflation):
    """
    Test `load_data` method of WFPInflation class handling of empty formatted data
    """

    # Mock data
    mock_extracted_data = "mocked data"
    mock_empty_df = pd.DataFrame()

    # Mock logs, `extract_data` and `format_data` methods
    with mock.patch("bblocks_data_importers.config.logger.warning") as mock_warning, \
         mock.patch.object(wfp_inflation, "extract_data", return_value=mock_extracted_data) as mock_extract, \
         mock.patch.object(wfp_inflation, "format_data", return_value=mock_empty_df) as mock_format:

        # Call method
        wfp_inflation.load_data("Headline inflation (YoY)", ["USA"])

        # Assertions
        mock_extract.assert_called_once_with(1, 116)
        mock_format.assert_called_once_with(mock_extracted_data, "Headline inflation (YoY)", "USA")
        assert wfp_inflation._data["Headline inflation (YoY)"]["USA"] is None
        mock_warning.assert_any_call("No Headline inflation (YoY) data found for country - USA")