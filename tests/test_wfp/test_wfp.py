"""Test WFPInflation and WFPFoodSecurity modules."""

import io
import pandas as pd
import pytest
import requests
from unittest import mock

import bblocks_data_importers.wfp.wfp as WFP
from bblocks_data_importers.wfp.wfp import WFPInflation, WFPFoodSecurity, Fields
from bblocks_data_importers.config import DataExtractionError, DataFormattingError
from bblocks_data_importers.utilities import convert_dtypes
from bblocks_data_importers.data_validators import DataFrameValidator


# Fixtures
@pytest.fixture(autouse=True)
def reset_cached_countries():
    """
    Reset the global _cached_countries variable before each test.
    """
    WFP._cached_countries = None


@pytest.fixture
def mock_request_countries():
    """
    Fixture to simulate successful response for available WFP countries.
    """
    mock_request = mock.Mock()
    mock_request.json.return_value = {
        "body": {
            "features": [
                {"properties": {"iso3": "VEN", "adm0_id": 1, "dataType": None}},
                {
                    "properties": {
                        "iso3": "LBN",
                        "adm0_id": 2,
                        "dataType": "ACTUAL DATA",
                    }
                },
            ]
        }
    }
    mock_request.raise_for_status = mock.Mock()

    with mock.patch("requests.get", return_value=mock_request):
        yield mock_request


@pytest.fixture
def mock_cached_countries():
    """
    Fixture to simulate cached countries.
    """
    mock_cache = {
        "VNM": {"entity_code": 1, "data_type": "PREDICTION", "country_name": "Vietnam"}
    }
    WFP._cached_countries = mock_cache


@pytest.fixture
def mock_response_inflation():
    """
    Fixture to simulate request for WFPInflation data.
    """
    mock_response = mock.Mock()
    mock_response.status_code = 200
    mock_response.content = b"mocked response data"

    with mock.patch("requests.post", return_value=mock_response) as mock_post:
        yield mock_post


@pytest.fixture
def wfp_inflation():
    """
    Fixture to provide an instance of WFPInflation with default settings.
    """
    instance = WFPInflation()
    return instance


@pytest.fixture
def wfp_inflation_pre_load():
    """
    Fixture to provide instance of the WFPInflation class before calling the `load_data` method.
    """
    instance = WFPInflation()
    instance._countries = {
        "USA": {"entity_code": 1},
        "CAN": {"entity_code": 2},
    }
    return instance


@pytest.fixture
def wfp_inflation_post_load():
    """
    Fixture to provide an instance of the WFPInflation class after calling the `load_data` method.
    """
    instance = WFPInflation()
    instance._data = {
        "Headline inflation (YoY)": {
            "USA": pd.DataFrame({"value": [1]}),
            "CAN": pd.DataFrame({"value": [2]}),
        },
    }
    instance._countries = {
        "USA": {"entity_code": 1, "name": "United States"},
        "CAN": {"entity_code": 2, "name": "Canada"},
    }
    instance._indicators = {"Headline inflation (YoY)": 100}
    return instance


@pytest.fixture
def wfp_foodsecurity():
    """
    Fixture to provide an instance of WFPFoodSecurity with default settings.
    """
    instance = WFPFoodSecurity()
    return instance


@pytest.fixture
def wfp_foodsecurity_pre_load():
    """
    Fixture to provide an instance of WFPFoodSecurity before calling the `load_data` method.
    """
    instance = WFPFoodSecurity()
    instance._data = {"national": {}, "subnational": {}}
    instance._countries = {
        "USA": {Fields.entity_code: 1},
        "CAN": {Fields.entity_code: 2},
    }
    return instance


@pytest.fixture
def wfp_foodsecurity_post_load():
    """
    Fixture to provide an instance of WFPFoodSecurity before calling the `load_data` method.
    """
    instance = WFPFoodSecurity()
    instance._data = {"national": {}, "subnational": {}}
    instance._countries = {
        "USA": {
            "entity_code": 1,
            "data_type": "National",
            "country_name": "United States",
        },
        "CAN": {"entity_code": 2, "data_type": "Subnational", "country_name": "Canada"},
    }
    return instance


# TESTS

# WFPInflation


def test_wfp_inflation_init(wfp_inflation):
    """
    Test that the WFPInflation class initializes with the correct default attributes.
    """
    # Assertions
    assert wfp_inflation._timeout == 20
    assert wfp_inflation._indicators == {
        "Headline inflation (YoY)": 116,
        "Headline inflation (MoM)": 117,
        "Food inflation": 71,
    }
    assert wfp_inflation._countries == None
    assert wfp_inflation._data == {
        "Headline inflation (YoY)": {},
        "Headline inflation (MoM)": {},
        "Food inflation": {},
    }


def test_wfp_inflation_load_available_countries_no_cached(
    mock_request_countries, wfp_inflation
):
    """
    Test that WFPInflation's `load_available_countries` method successfully loads countries when no cached data is available.
    """
    # Call `load_available_countries` method
    wfp_inflation.load_available_countries()

    # Assertions
    assert len(wfp_inflation._countries) == 2
    assert wfp_inflation._countries["VEN"] == {
        "entity_code": 1,
        "data_type": None,
        "country_name": "Venezuela",
    }
    assert wfp_inflation._countries["LBN"] == {
        "entity_code": 2,
        "data_type": "ACTUAL DATA",
        "country_name": "Lebanon",
    }


def test_wfp_inflation_load_available_countries_timeout(
    mock_request_countries, wfp_inflation
):
    """
    Test that WFPInflation's `load_available_countries` method raises DataExtractionError for `requests.get` timeouts.
    """
    # Mock request with timeout
    with mock.patch("requests.get", side_effect=requests.exceptions.Timeout):
        # Assert that DataExtrationError is raised when `load_available_countries` method is called
        with pytest.raises(
            DataExtractionError,
            match="Request timed out while getting country IDs after 3 attempts",
        ):
            wfp_inflation.load_available_countries()


def test_wfp_inflation_load_available_countries_exception(
    mock_request_countries, wfp_inflation
):
    """
    Test that WFPInflation's `load_available_countries` method raises DataExtractionError for `requests.get` exceptions.
    """
    # Mock request with network error
    with mock.patch(
        "requests.get",
        side_effect=requests.exceptions.RequestException("Network error"),
    ):
        # Assert that DataExtrationError is raised when `load_available_countries` method is called
        with pytest.raises(
            DataExtractionError,
            match="Error getting country IDs after 3 attempts: Network error",
        ):
            wfp_inflation.load_available_countries()


def test_wfp_inflation_load_available_countries_chached(
    mock_cached_countries, wfp_inflation
):
    """
    Test that WFPInflation's `load_available_countries` method uses cached countries without making a request.
    """
    # Mock request
    with mock.patch("requests.get") as mock_get:
        # Call `load_available_countries` method
        wfp_inflation.load_available_countries()

        # Assertions
        assert len(wfp_inflation._countries) == 1
        assert wfp_inflation._countries["VNM"]["country_name"] == "Vietnam"
        mock_get.assert_not_called()


# Parameterization for single and multiple indicators
@pytest.mark.parametrize(
    "indicator_code, expected_json",
    [
        (100, {"adm0Code": 1, "economicIndicatorIds": [100]}),  # Single
        ([100, 200], {"adm0Code": 1, "economicIndicatorIds": [100, 200]}),  # Multiple
    ],
)
def test_wfp_inflation_extract_data(
    mock_response_inflation, wfp_inflation, indicator_code, expected_json
):
    """
    Test that WFPInflation's `extract_data` method processes single and multiple indicators correctly.
    """
    # Call `extract_data` method
    country_code = 1
    result = wfp_inflation.extract_data(country_code, indicator_code)

    # Assertions
    assert isinstance(result, io.BytesIO)
    assert result.getvalue() == b"mocked response data"
    mock_response_inflation.assert_called_once_with(
        f"{WFP.VAM_API}/economicExplorer/TradingEconomics/InflationExport",
        json=expected_json,
        headers=WFP.VAM_HEADERS,
        timeout=wfp_inflation._timeout,
    )


def test_wfp_inflation_extract_data_timeout(mock_response_inflation, wfp_inflation):
    """
    Test that WFPInflation's `extract_data` method raises DataExtractionError for `requests.post` timeouts.
    """
    country_code = 1
    indicator_code = 100

    # Mock response with timeout
    mock_response_inflation.side_effect = requests.exceptions.Timeout

    # Assert that DataExtrationError is raised when `extract_data` method is called
    with pytest.raises(
        DataExtractionError, match="Request timed out while getting inflation data"
    ):
        wfp_inflation.extract_data(country_code, indicator_code)


def test_wfp_inflation_extract_data_request_exception(
    mock_response_inflation, wfp_inflation
):
    """
    Test that WFPInflation's `extract_data` method raises DataExtractionError for `requests.post` exceptions.
    """
    country_code = 1
    indicator_code = 100

    # Mock response with error
    mock_response_inflation.side_effect = requests.exceptions.RequestException(
        "Mocked request error"
    )

    # Assert that DataExtrationError is raised when `extract_data` method is called
    with pytest.raises(
        DataExtractionError, match="Error getting inflation data: Mocked request error"
    ):
        wfp_inflation.extract_data(country_code, indicator_code)


def test_wfp_inflation_format_data_successful(wfp_inflation):
    """
    Test that WFPInflation's `format_data` method processes valid data into the expected DataFrame.
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
    ).pipe(convert_dtypes)

    # Call `format_data` method
    result_df = wfp_inflation.format_data(data, indicator_name, iso3_code)

    # Assert that resulted and expected DataFrames are equal
    pd.testing.assert_frame_equal(result_df, expected_df)


def test_wfp_inflation_format_data_exception(wfp_inflation):
    """
    Test that WFPInflation's `format_data` method raises DataFormattingError for invalid data.
    """
    # Mock data
    invalid_data = io.BytesIO(b"Invalid,CSV,Data")
    indicator_name = "Inflation"
    iso3_code = "TST"

    # Assert that DataFormattingError is raised when `format_data` method is called
    with pytest.raises(
        DataFormattingError, match="Error formatting data for country - TST:"
    ):
        wfp_inflation.format_data(invalid_data, indicator_name, iso3_code)


def test_wfp_inflation_load_data_all_countries_loaded(wfp_inflation_pre_load):
    """
    Test that WFPInflation's `load_data` method does nothing and logs nothing when all countries are already loaded.
    """
    # Add data to class
    wfp_inflation_pre_load._data["Headline inflation (YoY)"] = {
        "USA": mock.Mock(),
        "CAN": mock.Mock(),
    }

    # Mock logs
    with mock.patch("bblocks_data_importers.config.logger.info") as mock_logger:
        # Call `load_data` method
        wfp_inflation_pre_load.load_data("Headline inflation (YoY)", ["USA", "CAN"])
        # Assert that no messages are logged
        mock_logger.assert_not_called()


def test_wfp_inflation_load_data_country_not_available(wfp_inflation_pre_load):
    """
    Test that WFPInflation's `load_data` method logs a warning and sets data to None for unavailable countries.
    """
    # Mock logs
    with mock.patch("bblocks_data_importers.config.logger.warning") as mock_logger:
        # Call `load_data` method
        wfp_inflation_pre_load.load_data("Headline inflation (YoY)", ["USA", "MEX"])
        # Assertions
        mock_logger.assert_called_once_with("Data not found for country - MEX")
        assert wfp_inflation_pre_load._data["Headline inflation (YoY)"]["MEX"] is None


def test_wfp_inflation_load_data_successful(wfp_inflation_pre_load):
    """
    Test that WFPInflation's `load_data` method successfully processes and loads data for a given indicator and country.
    """
    # Mock data
    mock_extracted_data = "mocked data"
    mock_formatted_data = pd.DataFrame({"value": [1]})

    # Mock logs, `extract_data` and `format_data` methods
    with mock.patch(
        "bblocks_data_importers.config.logger.info"
    ) as mock_logger, mock.patch.object(
        wfp_inflation_pre_load, "extract_data", return_value=mock_extracted_data
    ) as mock_extract, mock.patch.object(
        wfp_inflation_pre_load, "format_data", return_value=mock_formatted_data
    ) as mock_format:

        # Call `load_data` method
        wfp_inflation_pre_load.load_data("Headline inflation (YoY)", ["USA"])

        # Assertions
        mock_extract.assert_called_once_with(1, 116)
        mock_format.assert_called_once_with(
            mock_extracted_data, "Headline inflation (YoY)", "USA"
        )
        assert (
            wfp_inflation_pre_load._data["Headline inflation (YoY)"]["USA"] is not None
        )
        mock_logger.assert_any_call(
            "Data imported successfully for indicator: Headline inflation (YoY)"
        )


def test_wfp_inflation_load_data_empty_data(wfp_inflation_pre_load):
    """
    Test that WFPInflation's `load_data` method handles empty formatted data by logging a warning and setting the data to None.
    """
    # Mock data
    mock_extracted_data = "mocked data"
    mock_empty_df = pd.DataFrame()

    # Mock logs, `extract_data` and `format_data` methods
    with mock.patch(
        "bblocks_data_importers.config.logger.warning"
    ) as mock_warning, mock.patch.object(
        wfp_inflation_pre_load, "extract_data", return_value=mock_extracted_data
    ) as mock_extract, mock.patch.object(
        wfp_inflation_pre_load, "format_data", return_value=mock_empty_df
    ) as mock_format:

        # Call `load_data` method
        wfp_inflation_pre_load.load_data("Headline inflation (YoY)", ["USA"])

        # Assertions
        mock_extract.assert_called_once_with(1, 116)
        mock_format.assert_called_once_with(
            mock_extracted_data, "Headline inflation (YoY)", "USA"
        )
        assert wfp_inflation_pre_load._data["Headline inflation (YoY)"]["USA"] is None
        mock_warning.assert_any_call(
            "No Headline inflation (YoY) data found for country - USA"
        )


def test_wfp_inflation_available_indicators_with_data(wfp_inflation_post_load):
    """
    Test that WFPInflation's `available_indicators` property returns a list of indicators when `_indicators` is populated.
    """
    # Mock `_indicators`
    wfp_inflation_post_load._indicators = {
        "Indicator A": 100,
        "Indicator B": 200,
    }

    # Call the property
    result = wfp_inflation_post_load.available_indicators

    # Assertions
    assert result == ["Indicator A", "Indicator B"]


def test_wfp_inflation_available_indicators_empty(wfp_inflation_post_load):
    """
    Test that WFPInflation's `available_indicators` property returns an empty list when `_indicators` is empty.
    """
    # Mock `_indicators` as empty
    wfp_inflation_post_load._indicators = {}

    # Call the property
    result = wfp_inflation_post_load.available_indicators

    # Assertions
    assert result == []


def test_wfp_inflation_available_indicators_no_indicators(wfp_inflation_post_load):
    """
    Test that WFPInflation's `available_indicators` property raises an AttributeError if `_indicators` is not initialized.
    """
    # Remove `_indicators` attribute
    del wfp_inflation_post_load._indicators

    # Assert that accessing the property raises an AttributeError
    with pytest.raises(AttributeError):
        _ = wfp_inflation_post_load.available_indicators


def test_wfp_inflation_get_data_all_indicators_and_countries(wfp_inflation_post_load):
    """
    Test that WFPInflation's `get_data` method returns all available data when no indicators or countries are specified.
    """
    with mock.patch.object(
        wfp_inflation_post_load, "load_available_countries"
    ) as mock_load:
        # Call `get_data` method
        result = wfp_inflation_post_load.get_data()

        # Assert that all data is returned
        expected = pd.concat(
            [
                wfp_inflation_post_load._data["Headline inflation (YoY)"]["USA"],
                wfp_inflation_post_load._data["Headline inflation (YoY)"]["CAN"],
            ],
            ignore_index=True,
        )
        pd.testing.assert_frame_equal(result, expected)

        # Ensure `load_available_countries` was not called since countries are already loaded
        mock_load.assert_not_called()


def test_wfp_inflation_get_data_specific_indicator_and_countries(
    wfp_inflation_post_load,
):
    """
    Test that WFPInflation's `get_data` emthod returns data for a specific indicator and specified countries.
    """
    # Mock `load_data`
    with mock.patch.object(wfp_inflation_post_load, "load_data") as mock_load_data:
        # Call `load_data` method
        result = wfp_inflation_post_load.get_data(
            indicators="Headline inflation (YoY)", countries=["USA"]
        )

        # Assert that only data for the specified indicator and country is returned
        expected = wfp_inflation_post_load._data["Headline inflation (YoY)"]["USA"]
        pd.testing.assert_frame_equal(result, expected)

        # Ensure `load_data` was called with the correct arguments
        mock_load_data.assert_called_once_with(
            indicator_name="Headline inflation (YoY)", iso3_codes=["USA"]
        )


def test_wfp_inflation_get_data_invalid_indicator(wfp_inflation_post_load):
    """
    Test that WFPInflation's get_data` method raises a ValueError when an invalid indicator is provided.
    """
    with pytest.raises(ValueError, match="Invalid indicator - InvalidIndicator"):
        wfp_inflation_post_load.get_data(indicators="InvalidIndicator")


def test_wfp_inflation_get_data_no_valid_countries(wfp_inflation_post_load):
    """
    Test that WFPInflation's `get_data` method raises a ValueError when no valid countries are found.
    """
    # Mock the `convert_countries_to_unique_list` function
    with mock.patch(
        "bblocks_data_importers.utilities.convert_countries_to_unique_list",
        return_value=[],
    ) as mock_convert:
        # Assertions
        with pytest.raises(ValueError, match="No valid countries found"):
            wfp_inflation_post_load.get_data(countries=["InvalidCountry"])

        # mock_convert.assert_called_once_with(["InvalidCountry"], to="ISO3")


def test_wfp_inflation_get_data_no_data_found(wfp_inflation_post_load):
    """
    Test that WFPInflation's `get_data` method returns an empty DataFrame and logs a warning when no data is found.
    """
    # Clear existing data
    wfp_inflation_post_load._data = {"Headline inflation (YoY)": {}}

    # Mock logger warning
    with mock.patch("bblocks_data_importers.config.logger.warning") as mock_warning:
        # Call method
        result = wfp_inflation_post_load.get_data(
            indicators="Headline inflation (YoY)", countries=["USA"]
        )

        # Assertions
        pd.testing.assert_frame_equal(result, pd.DataFrame())
        mock_warning.assert_any_call("No data found for the requested countries")
        # mock_warning.assert_called_once_with("No data found for the requested countries")


def test_wfp_inflation_get_data_load_countries_when_none(wfp_inflation_post_load):
    """
    Test that `get_data` calls `load_available_countries` when `_countries` is None.
    """
    # Set `_countries` to None
    wfp_inflation_post_load._countries = None

    # Mock `load_available_countries` to populate `_countries`
    with mock.patch.object(
        wfp_inflation_post_load,
        "load_available_countries",
        side_effect=lambda: setattr(
            wfp_inflation_post_load,
            "_countries",
            {
                "USA": {"entity_code": 1, "name": "United States"},
                "CAN": {"entity_code": 2, "name": "Canada"},
            },
        ),
    ) as mock_load:
        # Call the method
        result = wfp_inflation_post_load.get_data()

        # Assert that `load_available_countries` was called
        mock_load.assert_called_once()

        # Assert that the result contains the combined data
        expected = pd.concat(
            [
                wfp_inflation_post_load._data["Headline inflation (YoY)"]["USA"],
                wfp_inflation_post_load._data["Headline inflation (YoY)"]["CAN"],
            ],
            ignore_index=True,
        )
        pd.testing.assert_frame_equal(result, expected)


def test_wfp_inflation_get_data_countries_as_string(wfp_inflation_post_load):
    """
    Test that WFPInflation's `get_data` method converts a single country string into a list.
    """
    # Mock `load_data`
    with mock.patch.object(wfp_inflation_post_load, "load_data") as mock_load_data:
        # Call method with `countries` as a string
        wfp_inflation_post_load.get_data(countries="USA")

        # Ensure `load_data` is called with the country wrapped in a list
        mock_load_data.assert_called_once_with(
            indicator_name="Headline inflation (YoY)", iso3_codes=["USA"]
        )


def test_wfp_inflation_clear_cache(wfp_inflation_post_load):
    """
    Test that WFPInflation's `clear_cache` method clears the cached data.
    """
    # Mock logger
    with mock.patch("bblocks_data_importers.config.logger.info") as mock_logger:
        # Call method
        wfp_inflation_post_load.clear_cache()

        # Assertions
        assert wfp_inflation_post_load._data == {
            "Headline inflation (YoY)": {},
            "Headline inflation (MoM)": {},
            "Food inflation": {},
        }
        assert wfp_inflation_post_load._countries == None
        assert WFP._cached_countries == None

        mock_logger.assert_called_once_with("Cache cleared")


# WFPFoodSecurity


def test_wfp_foodsecurity_init():
    """
    Test that the WFPFoodSecurity class initializes with the correct default attributes.
    """
    # Instantiate class
    wfp_foodsecurity_importer = WFPFoodSecurity()

    # Assertions
    assert wfp_foodsecurity_importer._timeout == 20
    assert wfp_foodsecurity_importer._retries == 2
    assert wfp_foodsecurity_importer._countries == None
    assert wfp_foodsecurity_importer._data == {"national": {}, "subnational": {}}


def test_wfp_foodsecurity_load_available_countries_no_cached(
    mock_request_countries, wfp_foodsecurity
):
    """
    Test that WFPFoodSecurity's `load_available_countries` method successfully loads countries when no cached data is available.
    """
    # Call `load_available_countries` method
    wfp_foodsecurity._load_available_countries()

    # Assertions
    assert wfp_foodsecurity._countries == {
        "LBN": {"entity_code": 2, "data_type": "ACTUAL DATA", "country_name": "Lebanon"}
    }


def test_wfp_foodsecurity_load_available_countries_timeout(
    mock_request_countries, wfp_foodsecurity
):
    """
    Test that WFPFoodSecurity's `load_available_countries` method raises DataExtractionError for `requests.get` timeouts.
    """
    # Mock request with timeout
    with mock.patch("requests.get", side_effect=requests.exceptions.Timeout):
        # Assert that DataExtrationError is raised when `load_available_countries` method is called
        with pytest.raises(
            DataExtractionError,
            match="Request timed out while getting country IDs after 3 attempts",
        ):
            wfp_foodsecurity._load_available_countries()


def test_wfp_foodsecurity_load_available_countries_exception(
    mock_request_countries, wfp_foodsecurity
):
    """
    Test that WFPFoodSecurity's `load_available_countries` method raises DataExtractionError for `requests.get` exceptions.
    """
    # Mock request with network error
    with mock.patch(
        "requests.get",
        side_effect=requests.exceptions.RequestException("Network error"),
    ):
        # Assert that DataExtrationError is raised when `load_available_countries` method is called
        with pytest.raises(
            DataExtractionError,
            match="Error getting country IDs after 3 attempts: Network error",
        ):
            wfp_foodsecurity._load_available_countries()


def test_wfp_foodsecurity_load_available_countries_chached(
    mock_cached_countries, wfp_foodsecurity
):
    """
    Test that WFPFoodSecurity's `load_available_countries` method uses cached countries without making a request.
    """
    # Mock request
    with mock.patch("requests.get") as mock_get:
        # Call `load_available_countries` method
        wfp_foodsecurity._load_available_countries()

        # Assertions
        assert len(wfp_foodsecurity._countries) == 1
        assert wfp_foodsecurity._countries["VNM"]["country_name"] == "Vietnam"
        mock_get.assert_not_called()


def test_wfp_foodsecurity_extract_data_national_success(wfp_foodsecurity):
    """
    Test that WFPFoodSecurity's `_extract_data` mehtod successfully retrieves data for the national level.
    """
    # Mock response
    mock_response = mock.Mock()
    mock_response.json.return_value = {"key": "value"}
    mock_response.raise_for_status = mock.Mock()

    with mock.patch("requests.get", return_value=mock_response) as mock_get:
        result = wfp_foodsecurity._extract_data(entity_code=1, level="national")

        # Assertions
        assert result == {"key": "value"}
        mock_get.assert_called_once_with(
            "https://api.hungermapdata.org/v2/adm0/1/countryData.json",
            headers=WFP.HUNGERMAP_HEADERS,
            timeout=wfp_foodsecurity._timeout,
        )


def test_wfp_foodsecurity_extract_data_subnational_success(wfp_foodsecurity):
    """
    Test that WFPFoodSecurity's `_extract_data` method successfully retrieves data for the subnational level.
    """
    # Mock response
    mock_response = mock.Mock()
    mock_response.json.return_value = {"key": "value"}
    mock_response.raise_for_status = mock.Mock()

    with mock.patch("requests.get", return_value=mock_response) as mock_get:
        result = wfp_foodsecurity._extract_data(entity_code=1, level="subnational")

        # Assertions
        assert result == {"key": "value"}
        mock_get.assert_called_once_with(
            "https://api.hungermapdata.org/v2/adm0/1/adm1data.json",
            headers=WFP.HUNGERMAP_HEADERS,
            timeout=wfp_foodsecurity._timeout,
        )


def test_wfp_foodsecurity_extract_data_invalid_level(wfp_foodsecurity):
    """
    Test that WFPFoodSecurity's `_extract_data` method raises a ValueError for an invalid level.
    """
    with pytest.raises(ValueError, match="level must be 'national' or 'subnational'"):
        wfp_foodsecurity._extract_data(entity_code=1, level="invalid")


def test_wfp_foodsecurity_extract_data_timeout(wfp_foodsecurity):
    """
    Test that WFPFoodSecurity's `_extract_data` method raises DataExtractionError after repeated timeouts.
    """
    with mock.patch("requests.get", side_effect=requests.exceptions.Timeout):
        with pytest.raises(
            DataExtractionError,
            match="Request timed out for adm0 code - 1 after 3 attempts",
        ):
            wfp_foodsecurity._extract_data(entity_code=1, level="national")


def test_wfp_foodsecurity_extract_data_request_exception(wfp_foodsecurity):
    """
    Test that WFPFoodSecurity's `_extract_data` method raises DataExtractionError for request exceptions.
    """
    with mock.patch(
        "requests.get", side_effect=requests.exceptions.RequestException("Mocked error")
    ):
        with pytest.raises(
            DataExtractionError,
            match="Error extracting data for country adm0_code - 1 after 3 attempts: Mocked error",
        ):
            wfp_foodsecurity._extract_data(entity_code=1, level="national")


def test_wfp_foodsecurity_parse_national_data_success():
    """
    Test that WFPFoodSecurity's `_parse_national_data` method successfully parses valid national data into a DataFrame.
    """
    # Mock input data
    input_data = {
        "fcsGraph": [
            {"x": "2023-10-01", "fcs": 20, "fcsHigh": 25, "fcsLow": 15},
            {"x": "2023-10-02", "fcs": 30, "fcsHigh": 35, "fcsLow": 25},
        ]
    }
    iso_code = "USA"

    # Expected DataFrame
    expected_df = pd.DataFrame(
        {
            Fields.date: pd.to_datetime(["2023-10-01", "2023-10-02"]),
            Fields.value: [20, 30],
            Fields.value_upper: [25, 35],
            Fields.value_lower: [15, 25],
            Fields.iso3_code: ["USA", "USA"],
            Fields.country_name: ["United States", "United States"],
            Fields.indicator_name: [
                "people with insufficient food consumption",
                "people with insufficient food consumption",
            ],
            Fields.source: ["World Food Programme", "World Food Programme"],
        }
    ).pipe(convert_dtypes)

    result_df = WFPFoodSecurity._parse_national_data(input_data, iso_code)

    # Assert that the resulting DataFrame matches the expected DataFrame
    pd.testing.assert_frame_equal(result_df, expected_df)


def test_wfp_foodsecurity_parse_national_data_missing_field():
    """
    Test that WFPFoodSecurity's `_parse_national_data` method raises DataFormattingError for missing `fcsGraph` in the input data.
    """
    input_data = {}  # Missing "fcsGraph" field
    iso_code = "USA"

    with pytest.raises(
        DataFormattingError,
        match="Error parsing national data for country - USA: 'fcsGraph'",
    ):
        WFPFoodSecurity._parse_national_data(input_data, iso_code)


def test_wfp_foodsecurity_parse_national_data_invalid_data():
    """
    Test that WFPFoodSecurity's `_parse_national_data` method raises DataFormattingError for invalid data in `fcsGraph`.
    """
    # Input data with incorrect field types
    input_data = {"fcsGraph": [{"x": "not-a-date", "fcs": "invalid"}]}
    iso_code = "USA"

    with pytest.raises(
        DataFormattingError,
        match="Error parsing national data for country - USA:",
    ):
        WFPFoodSecurity._parse_national_data(input_data, iso_code)


def test_wfp_foodsecurity_parse_subnational_data_success():
    """
    Test that WFPFoodSecurity's `_parse_subnational_data` method successfully parses valid subnational data into a DataFrame.
    """
    # Mock input data
    input_data = {
        "features": [
            {
                "properties": {
                    "Name": "Region A",
                    "fcsGraph": [
                        {"x": "2023-10-01", "fcs": 20, "fcsHigh": 25, "fcsLow": 15},
                        {"x": "2023-10-02", "fcs": 30, "fcsHigh": 35, "fcsLow": 25},
                    ],
                }
            },
            {
                "properties": {
                    "Name": "Region B",
                    "fcsGraph": [
                        {"x": "2023-10-01", "fcs": 40, "fcsHigh": 45, "fcsLow": 35},
                        {"x": "2023-10-02", "fcs": 50, "fcsHigh": 55, "fcsLow": 45},
                    ],
                }
            },
        ]
    }
    iso_code = "USA"

    # Expected DataFrame
    expected_df = pd.DataFrame(
        {
            Fields.date: pd.to_datetime(
                ["2023-10-01", "2023-10-02", "2023-10-01", "2023-10-02"]
            ),
            Fields.value: [20, 30, 40, 50],
            Fields.value_upper: [25, 35, 45, 55],
            Fields.value_lower: [15, 25, 35, 45],
            "region_name": ["Region A", "Region A", "Region B", "Region B"],
            Fields.iso3_code: ["USA", "USA", "USA", "USA"],
            Fields.country_name: [
                "United States",
                "United States",
                "United States",
                "United States",
            ],
            Fields.indicator_name: [
                "people with insufficient food consumption",
                "people with insufficient food consumption",
                "people with insufficient food consumption",
                "people with insufficient food consumption",
            ],
            Fields.source: [
                "World Food Programme",
                "World Food Programme",
                "World Food Programme",
                "World Food Programme",
            ],
        }
    )

    result_df = WFPFoodSecurity._parse_subnational_data(input_data, iso_code)

    # Assert that the resulting DataFrame matches the expected DataFrame
    pd.testing.assert_frame_equal(result_df, expected_df, check_dtype=False)


def test_wfp_foodsecurity_parse_subnational_data_missing_features():
    """
    Test that WFPFoodSecurity's `_parse_subnational_data` method raises DataFormattingError for missing `features` in the input data.
    """
    input_data = {}  # Missing "features" field
    iso_code = "USA"

    with pytest.raises(
        DataFormattingError,
        match="Error parsing subnational data for country - USA: 'features'",
    ):
        WFPFoodSecurity._parse_subnational_data(input_data, iso_code)


def test_wfp_foodsecurity_parse_subnational_data_invalid_data():
    """
    Test that WFPFoodSecurity's `_parse_subnational_data` method raises DataFormattingError for invalid data in the `features` field.
    """
    # Input data with incorrect structure
    input_data = {
        "features": [
            {
                "properties": {
                    "Name": "Region A",
                    "fcsGraph": [{"x": "not-a-date", "fcs": "invalid"}],
                }
            }
        ]
    }
    iso_code = "USA"

    with pytest.raises(
        DataFormattingError,
        match="Error parsing subnational data for country - USA:",
    ):
        WFPFoodSecurity._parse_subnational_data(input_data, iso_code)


def test_wfp_foodsecurity_load_data_all_countries_loaded(wfp_foodsecurity_pre_load):
    """
    Test that WFPFoodSecurity's `_load_data` method does nothing if all specified countries are already loaded.
    """
    wfp_foodsecurity_pre_load._data["national"] = {
        "USA": mock.Mock(),
        "CAN": mock.Mock(),
    }

    with mock.patch("bblocks_data_importers.config.logger.info") as mock_logger:
        wfp_foodsecurity_pre_load._load_data(["USA", "CAN"], level="national")
        mock_logger.assert_not_called()


def test_wfp_foodsecurity_load_data_country_not_available(wfp_foodsecurity_pre_load):
    """
    Test that WFPFoodSecurity's `_load_data` method logs a warning if a country is not available in `_countries`.
    """
    with mock.patch("bblocks_data_importers.config.logger.info") as mock_logger:
        wfp_foodsecurity_pre_load._load_data(["USA", "MEX"], level="national")
        mock_logger.assert_any_call("No data found for country - MEX")
        assert "MEX" not in wfp_foodsecurity_pre_load._data["national"]


def test_wfp_foodsecurity_load_data_success_national(wfp_foodsecurity_pre_load):
    """
    Test that WFPFoodSecurity's `_load_data` method successfully processes and loads national data for a country.
    """
    mock_response = {"fcsGraph": [{"x": "2023-10-01", "fcs": 20}]}
    mock_parsed_df = pd.DataFrame({"value": [20]})

    with mock.patch.object(
        wfp_foodsecurity_pre_load, "_extract_data", return_value=mock_response
    ) as mock_extract, mock.patch.object(
        wfp_foodsecurity_pre_load, "_parse_national_data", return_value=mock_parsed_df
    ) as mock_parse, mock.patch(
        "bblocks_data_importers.config.logger.info"
    ) as mock_logger, mock.patch.object(
        DataFrameValidator, "validate"
    ) as mock_validate:

        wfp_foodsecurity_pre_load._load_data(["USA"], level="national")

        mock_extract.assert_called_once_with(1, level="national")
        mock_parse.assert_called_once_with(mock_response, "USA")
        mock_validate.assert_called_once_with(
            mock_parsed_df,
            required_cols=[
                Fields.iso3_code,
                Fields.date,
                Fields.value,
                Fields.indicator_name,
            ],
        )
        assert "USA" in wfp_foodsecurity_pre_load._data["national"]
        mock_logger.assert_any_call("National data imported successfully")


def test_wfp_foodsecurity_load_data_success_subnational(wfp_foodsecurity_pre_load):
    """
    Test that WFPFoodSecurity's `_load_data` method successfully processes and loads subnational data for a country.
    """
    mock_response = {
        "features": [
            {
                "properties": {
                    "Name": "Region A",
                    "fcsGraph": [{"x": "2023-10-01", "fcs": 20}],
                }
            }
        ],
    }
    mock_parsed_df = pd.DataFrame({"value": [20]})

    with mock.patch.object(
        wfp_foodsecurity_pre_load, "_extract_data", return_value=mock_response
    ) as mock_extract, mock.patch.object(
        wfp_foodsecurity_pre_load,
        "_parse_subnational_data",
        return_value=mock_parsed_df,
    ) as mock_parse, mock.patch(
        "bblocks_data_importers.config.logger.info"
    ) as mock_logger, mock.patch.object(
        DataFrameValidator, "validate"
    ) as mock_validate:

        wfp_foodsecurity_pre_load._load_data(["USA"], level="subnational")

        mock_extract.assert_called_once_with(1, level="subnational")
        mock_parse.assert_called_once_with(mock_response, "USA")
        mock_validate.assert_called_once_with(
            mock_parsed_df,
            required_cols=[
                Fields.iso3_code,
                Fields.date,
                Fields.value,
                Fields.indicator_name,
                Fields.region_name,
            ],
        )
        assert "USA" in wfp_foodsecurity_pre_load._data["subnational"]
        mock_logger.assert_any_call("Subnational data imported successfully")


def test_wfp_foodsecurity_load_data_validation_error(wfp_foodsecurity_pre_load):
    """
    Test that WFPFoodSecurity's `_load_data` method raises an error if the DataFrame validation fails.
    """
    mock_response = {"fcsGraph": [{"x": "2023-10-01", "fcs": 20}]}
    mock_parsed_df = pd.DataFrame({"value": [20]})

    with mock.patch.object(
        wfp_foodsecurity_pre_load, "_extract_data", return_value=mock_response
    ), mock.patch.object(
        wfp_foodsecurity_pre_load, "_parse_national_data", return_value=mock_parsed_df
    ), mock.patch(
        "bblocks_data_importers.config.logger.info"
    ), mock.patch.object(
        DataFrameValidator, "validate", side_effect=ValueError("Validation error")
    ):

        with pytest.raises(ValueError, match="Validation error"):
            wfp_foodsecurity_pre_load._load_data(["USA"], level="national")


def test_wfp_foodsecurity_available_countries_with_loaded_countries(
    wfp_foodsecurity_post_load,
):
    """
    Test that WFPFoodSecurity's `available_countries` method correctly returns a DataFrame when `_countries` is already loaded.
    """
    # Expected DataFrame
    expected_df = pd.DataFrame(
        {
            Fields.iso3_code: ["USA", "CAN"],
            "entity_code": [1, 2],
            "data_type": ["National", "Subnational"],
            "country_name": ["United States", "Canada"],
        }
    )

    # Call the property
    result_df = wfp_foodsecurity_post_load.available_countries

    # Assertions
    pd.testing.assert_frame_equal(result_df, expected_df, check_dtype=False)


def test_wfp_foodsecurity_available_countries_with_unloaded_countries(
    wfp_foodsecurity_post_load,
):
    """
    Test that WFPFoodSecurity's `available_countries` method calls `_load_available_countries` if `_countries` is None.
    """
    # Remove loaded countries
    wfp_foodsecurity_post_load._countries = None

    # Mock `_load_available_countries` to populate `_countries`
    with mock.patch.object(
        wfp_foodsecurity_post_load,
        "_load_available_countries",
        side_effect=lambda: setattr(
            wfp_foodsecurity_post_load,
            "_countries",
            {
                "USA": {
                    "entity_code": 1,
                    "data_type": "National",
                    "country_name": "United States",
                },
                "CAN": {
                    "entity_code": 2,
                    "data_type": "Subnational",
                    "country_name": "Canada",
                },
            },
        ),
    ) as mock_load:

        # Call the property
        result_df = wfp_foodsecurity_post_load.available_countries

        # Expected DataFrame
        expected_df = pd.DataFrame(
            {
                Fields.iso3_code: ["USA", "CAN"],
                "entity_code": [1, 2],
                "data_type": ["National", "Subnational"],
                "country_name": ["United States", "Canada"],
            }
        ).pipe(convert_dtypes)

        # Assertions
        mock_load.assert_called_once()
        pd.testing.assert_frame_equal(result_df, expected_df)


def test_wfp_foodsecurity_get_data_all_countries_national(wfp_foodsecurity_post_load):
    """
    Test that WFPFoodSecurity's `get_data` method returns data for all countries at the national level.
    """
    # Mock data
    wfp_foodsecurity_post_load._data["national"] = {
        "USA": pd.DataFrame({"value": [20]}),
        "CAN": pd.DataFrame({"value": [30]}),
    }

    # Call the method
    result = wfp_foodsecurity_post_load.get_data(level="national")

    # Expected DataFrame
    expected_df = pd.concat(
        [pd.DataFrame({"value": [20]}), pd.DataFrame({"value": [30]})],
        ignore_index=True,
    )

    # Assertions
    pd.testing.assert_frame_equal(result, expected_df)


def test_wfp_foodsecurity_get_data_specific_countries_national(
    wfp_foodsecurity_post_load,
):
    """
    Test that WFPFoodSecurity's `get_data` method returns data for specific countries at the national level.
    """
    # Mock data
    wfp_foodsecurity_post_load._data["national"] = {
        "USA": pd.DataFrame({"value": [20]}),
        "CAN": pd.DataFrame({"value": [30]}),
    }

    # Call the method
    result = wfp_foodsecurity_post_load.get_data(countries=["USA"], level="national")

    # Expected DataFrame
    expected_df = pd.DataFrame({"value": [20]})

    # Assertions
    pd.testing.assert_frame_equal(result, expected_df)


def test_wfp_foodsecurity_get_data_invalid_countries(wfp_foodsecurity_post_load):
    """
    Test that WFPFoodSecurity's `get_data` method raises a ValueError for invalid countries.
    """
    with mock.patch(
        "bblocks_data_importers.utilities.convert_countries_to_unique_list",
        return_value=[],
    ):
        with pytest.raises(ValueError, match="No valid countries found"):
            wfp_foodsecurity_post_load.get_data(
                countries=["InvalidCountry"], level="national"
            )


def test_wfp_foodsecurity_get_data_no_data_found(wfp_foodsecurity_post_load):
    """
    Test that WFPFoodSecurity's `get_data` method returns an empty DataFrame and logs a warning when no data is found.
    """
    wfp_foodsecurity_post_load._data["national"] = {}

    # Mock `_load_data` to simulate loading with no results
    with mock.patch.object(
        wfp_foodsecurity_post_load, "_load_data"
    ) as mock_load, mock.patch(
        "bblocks_data_importers.config.logger.warning"
    ) as mock_warning:

        # Call the method
        result = wfp_foodsecurity_post_load.get_data(
            countries=["USA"], level="national"
        )

        # Assertions
        # Assert an empty DataFrame is returned
        pd.testing.assert_frame_equal(result, pd.DataFrame())

        # Assert `_load_data` was called
        mock_load.assert_called_once_with(["USA"], "national")

        # Assert warning log is called
        mock_warning.assert_called_once_with(
            "No data found for the requested countries"
        )


def test_wfp_foodsecurity_get_data_subnational_level(wfp_foodsecurity_post_load):
    """
    Test that WFPFoodSecurity's `get_data` method returns data for the subnational level.
    """
    # Mock data
    wfp_foodsecurity_post_load._data["subnational"] = {
        "USA": pd.DataFrame({"value": [40]}),
        "CAN": pd.DataFrame({"value": [50]}),
    }

    # Call the method
    result = wfp_foodsecurity_post_load.get_data(countries=["USA"], level="subnational")

    # Expected DataFrame
    expected_df = pd.DataFrame({"value": [40]})

    # Assertions
    pd.testing.assert_frame_equal(result, expected_df)


def test_wfp_foodsecurity_get_data_unloaded_countries(wfp_foodsecurity_post_load):
    """
    Test that WFPFoodSecurity's `get_data` method triggers `_load_data` when data for countries is not preloaded.
    """
    # Mock `_load_data`
    with mock.patch.object(wfp_foodsecurity_post_load, "_load_data") as mock_load:
        wfp_foodsecurity_post_load.get_data(countries=["USA"], level="national")

        # Assert `_load_data` is called with correct arguments
        mock_load.assert_called_once_with(["USA"], "national")


def test_wfp_foodsecurity_get_data_load_countries_when_none(wfp_foodsecurity_post_load):
    """
    Test that WFPFoodSecurity's `get_data` method calls `_load_available_countries` when `_countries` is None.
    """
    # Set `_countries` to None
    wfp_foodsecurity_post_load._countries = None

    # Mock `_load_available_countries`
    with mock.patch.object(
        wfp_foodsecurity_post_load,
        "_load_available_countries",
        side_effect=lambda: setattr(
            wfp_foodsecurity_post_load,
            "_countries",
            {
                "USA": {"entity_code": 1, "name": "United States"},
                "CAN": {"entity_code": 2, "name": "Canada"},
            },
        ),
    ) as mock_load:
        # Call the method
        result = wfp_foodsecurity_post_load.get_data()

        # Ensure `_load_available_countries` is called
        mock_load.assert_called_once()

        # Assert result contains combined data for all countries
        expected = pd.concat(
            [
                wfp_foodsecurity_post_load._data["national"]["USA"],
                wfp_foodsecurity_post_load._data["national"]["CAN"],
            ],
            ignore_index=True,
        )
        pd.testing.assert_frame_equal(result, expected)


def test_wfp_foodsecurity_get_data_load_countries_when_none(wfp_foodsecurity_post_load):
    """
    Test that WFPFoodSecurity's `get_data` method calls `_load_available_countries` when `_countries` is None.
    """
    # Set `_countries` to None
    wfp_foodsecurity_post_load._countries = None

    # Mock `_load_available_countries` to populate `_countries`
    with mock.patch.object(
        wfp_foodsecurity_post_load,
        "_load_available_countries",
        side_effect=lambda: setattr(
            wfp_foodsecurity_post_load,
            "_countries",
            {
                "USA": {"entity_code": 1, "name": "United States"},
                "CAN": {"entity_code": 2, "name": "Canada"},
            },
        ),
    ) as mock_load_countries, mock.patch.object(
        wfp_foodsecurity_post_load,
        "_load_data",
        side_effect=lambda countries, level: wfp_foodsecurity_post_load._data[
            level
        ].update(
            {
                "USA": pd.DataFrame(
                    {
                        "date": ["2023-10-01", "2023-10-02"],
                        "value": [20, 30],
                        "iso3_code": ["USA", "USA"],
                    }
                ),
                "CAN": pd.DataFrame(
                    {
                        "date": ["2023-10-01", "2023-10-02"],
                        "value": [40, 50],
                        "iso3_code": ["CAN", "CAN"],
                    }
                ),
            }
        ),
    ) as mock_load_data:
        # Call the method
        result = wfp_foodsecurity_post_load.get_data()

        # Ensure `_load_available_countries` was called
        mock_load_countries.assert_called_once()

        # Ensure `_load_data` was called
        mock_load_data.assert_called_once_with(["USA", "CAN"], "national")

        # Expected DataFrame
        expected = pd.DataFrame(
            {
                "date": ["2023-10-01", "2023-10-02", "2023-10-01", "2023-10-02"],
                "value": [20, 30, 40, 50],
                "iso3_code": ["USA", "USA", "CAN", "CAN"],
            }
        )

        # Assert the result matches the expected DataFrame
        pd.testing.assert_frame_equal(result, expected)


def test_wfp_foodsecurity_get_data_single_country_string(wfp_foodsecurity_post_load):
    """
    Test that WFPFoodSecurity's `get_data` method converts a single country string into a list.
    """
    # Mock `load_data`
    with mock.patch.object(wfp_foodsecurity_post_load, "_load_data") as mock_load_data:
        # Call the method with `countries` as a string
        wfp_foodsecurity_post_load.get_data(countries="USA")

        # Ensure `_load_data` is called with the country wrapped in a list
        mock_load_data.assert_called_once_with(["USA"], "national")


def test_wfp_foodsecurity_clear_cache(wfp_foodsecurity_post_load):
    """
    Test that WFPFoodSecurity's `clear_cache` method clears the cached data.
    """
    # Mock logger
    with mock.patch("bblocks_data_importers.config.logger.info") as mock_logger:
        # Call method
        wfp_foodsecurity_post_load.clear_cache()

        # Assertions
        assert wfp_foodsecurity_post_load._data == {"national": {}, "subnational": {}}
        assert wfp_foodsecurity_post_load._countries == None
        assert WFP._cached_countries == None

        mock_logger.assert_called_once_with("Cache cleared")