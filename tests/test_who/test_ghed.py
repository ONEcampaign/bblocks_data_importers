"""Tests for GHED module"""

from bblocks_data_importers.who import ghed

import pytest
import pandas as pd
import numpy as np
import io
from unittest.mock import patch
from requests.exceptions import RequestException


# Mock response content
mock_excel_data = b"mocked_excel_file_content"


@patch("your_module_name.requests.get")  # Mock the requests.get call
def test_extract_data_success(mock_get):
    """Test successful extraction of data from the GHED database."""
    # Arrange
    mock_get.return_value.status_code = 200  # Simulate a successful status code
    mock_get.return_value.content = mock_excel_data  # Mock content as bytes

    # Act
    result = ghed.extract_data()

    # Assert
    assert isinstance(result, io.BytesIO)  # Check the returned type is io.BytesIO
    assert result.getvalue() == mock_excel_data  # Ensure content matches the mock data


@patch("your_module_name.requests.get")  # Mock the requests.get call
def test_extract_data_failure(mock_get):
    """Test handling of connection failure when extracting data."""
    # Arrange
    mock_get.side_effect = RequestException(
        "Connection error"
    )  # Simulate a connection error

    # Act & Assert
    with pytest.raises(
        ConnectionError, match="Error connecting to GHED database: Connection error"
    ):
        ghed.extract_data()


def mock_ghed_data():
    # Mock "Data" sheet
    data = {
        "country": ["Country A", "Country B"],
        "code": ["A", "B"],
        "region (WHO)": ["Region 1", "Region 2"],
        "income group": ["High", "Low"],
        "year": [2020, 2020],
        "indicator_1": [100, 200],
        "indicator_2": [300, np.nan],
    }
    data_df = pd.DataFrame(data)

    # Mock "Codebook" sheet
    codebook = {
        "variable code": ["indicator_1", "indicator_2"],
        "Indicator short code": ["IND1", "IND2"],
        "variable name": ["Indicator 1", "Indicator 2"],
        "Indicator name": ["Indicator 1", "Indicator 2"],
        "Category 1": ["Category A", "Category B"],
        "Category 2": ["Subcategory A", "Subcategory B"],
        "Indicator units": ["USD", "Percentage"],
        "Indicator currency": ["USD", None],
    }
    codebook_df = pd.DataFrame(codebook)

    # Mock "Metadata" sheet
    metadata = {
        "country": ["Country A", "Country B"],
        "long code (GHED data explorer)": ["A1", "B1"],
        "variable name": ["Indicator 1", "Indicator 2"],
        "code": ["A", "B"],
        "variable code": ["indicator_1", "indicator_2"],
    }
    metadata_df = pd.DataFrame(metadata)

    return {"Data": data_df, "Codebook": codebook_df, "Metadata": metadata_df}
