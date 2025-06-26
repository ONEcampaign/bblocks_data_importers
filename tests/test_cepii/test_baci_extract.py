"""Tests for BACI extract module."""

import pytest
import pandas as pd
import numpy as np
import pyarrow.dataset as ds
import pyarrow as pa

from bblocks.data_importers.cepii.extract import (
    filter_years,
    filter_products,
    rename_data_columns,
    rename_product_columns,
    rename_country_columns,
    parse_readme,
    add_country_labels,
    add_product_descriptions,
)
from bblocks.data_importers.config import Fields

# FIXTURES


@pytest.fixture
def valid_readme_content():
    return (
        "BACI\n\n"
        "Version: 202501\n\n"
        "Release Date: 2025 01 30\n\n"
        "Content:\n"
        "Trade flows at the year - exporter - importer - product level.\n\n"
        "List of Variables:\n"
        "t: year\n"
        "i: exporter\n"
        "j: importer\n\n"
        "Reference:\n"
        "Gaulier, G. and Zignago, S. (2010)\n"
    )


# TESTS


@pytest.mark.parametrize(
    "years, expected",
    [
        (2021, ds.field(Fields.year) == 2021),
        ([2020, 2021], ds.field(Fields.year).isin([2020, 2021])),
        (range(2019, 2022), ds.field(Fields.year).isin([2019, 2020, 2021])),
        (
            (2018, 2020),
            (ds.field(Fields.year) >= 2018) & (ds.field(Fields.year) <= 2020),
        ),
    ],
)
def test_filter_years(years, expected):
    """
    Test `filter_years` function with different valid inputs.
    """
    result = filter_years(years)
    assert result.equals(expected)


@pytest.mark.parametrize(
    "bad_input",
    [
        "2020",
        3.14,
        (2020,),  # Invalid tuple
        None,
        {"start": 2020, "end": 2022},
    ],
)
def test_filter_years_invalid_input(bad_input):
    """
    Test `filter_years` function with different invalid inputs.
    """
    with pytest.raises(ValueError):
        filter_years(bad_input)


@pytest.mark.parametrize(
    "products, expected",
    [
        (1234, ds.field(Fields.product_code) == 1234),
        ([1234, 5678], ds.field(Fields.product_code).isin([1234, 5678])),
        (range(1000, 1003), ds.field(Fields.product_code).isin([1000, 1001, 1002])),
        (
            (2000, 3000),
            (ds.field(Fields.product_code) >= 2000)
            & (ds.field(Fields.product_code) <= 3000),
        ),
    ],
)
def test_filter_products(products, expected):
    """
    Test `filter_products` function under different scenarios with different inputs and expected results.
    """
    result = filter_products(products)
    assert result.equals(expected)


@pytest.mark.parametrize(
    "bad_input",
    [
        "1234",
        99.9,
        (1234,),  # Invalid tuple
        None,
        {"start": 1000, "end": 2000},
    ],
)
def test_filter_products_invalid_input(bad_input):
    """
    Test `filter_products` function with different invalid inputs.
    """
    with pytest.raises(ValueError):
        filter_products(bad_input)


@pytest.mark.parametrize(
    "input_cols,expected_cols",
    [
        (
            ["t", "i", "j", "k", "v", "q"],
            [
                Fields.year,
                Fields.exporter_code,
                Fields.importer_code,
                Fields.product_code,
                Fields.value,
                Fields.quantity,
            ],
        ),
        (["t", "i", "j"], [Fields.year, Fields.exporter_code, Fields.importer_code]),
        (["t", "foo", "v"], [Fields.year, "foo", Fields.value]),
        (["foo", "bar"], ["foo", "bar"]),  # No renaming expected
    ],
)
def test_rename_data_columns(input_cols, expected_cols):
    """
    Test that `rename_data_columns` function renames columns correctly under different scenarios.
    """
    arrays = [pa.array([1]) for _ in input_cols]
    batch = pa.record_batch(arrays, names=input_cols)
    table = pa.Table.from_batches([batch])

    renamed = rename_data_columns(table)
    assert renamed.schema.names == expected_cols


def test_rename_data_columns_empty_table():
    """
    Test that `rename_data_columns` function returns an empty table if the input table is empty.
    """
    table = pa.table({})
    renamed = rename_data_columns(table)
    assert renamed.num_columns == 0


@pytest.mark.parametrize(
    "input_columns,expected_columns",
    [
        (["code", "description"], [Fields.product_code, Fields.product_description]),
        (["code"], [Fields.product_code]),
        (["description"], [Fields.product_description]),
        (["code", "foo"], [Fields.product_code, "foo"]),
        (["foo", "bar"], ["foo", "bar"]),  # No renaming expected
    ],
)
def test_rename_product_columns(input_columns, expected_columns):
    """
    Test that `rename_product_columns` correctly renames known product code columns
    to standardized field names. Columns not in the mapping should remain unchanged.
    """
    df = pd.DataFrame([[1] * len(input_columns)], columns=input_columns)
    renamed = rename_product_columns(df)
    assert list(renamed.columns) == expected_columns


@pytest.mark.parametrize(
    "input_columns,expected_columns",
    [
        (
            ["country_code", "country_name", "country_iso3", "country_iso2"],
            [
                Fields.country_code,
                Fields.country_name,
                Fields.iso3_code,
                Fields.iso2_code,
            ],
        ),
        (
            ["country_code", "country_iso3"],
            [Fields.country_code, Fields.iso3_code],
        ),
        (
            ["country_code", "foo"],
            [Fields.country_code, "foo"],
        ),
        (
            ["foo", "bar"],
            ["foo", "bar"],  # No known matches, should remain the same
        ),
    ],
)
def test_rename_country_columns(input_columns, expected_columns):
    """
    Test that `rename_country_columns` correctly renames known country-related columns
    to standardized field names from the `Fields` enum. Unmapped columns are left unchanged.
    """
    df = pd.DataFrame([[1] * len(input_columns)], columns=input_columns)
    renamed = rename_country_columns(df)
    assert list(renamed.columns) == expected_columns


# def test_parse_readme_valid(valid_readme_content):
#     """
#     Test that `parse_readme` extracts only top-level metadata fields and
#     ignores the 'List of Variables' block, which includes 't', 'i', and 'j'.
#     """
#     metadata = parse_readme(valid_readme_content)
#
#     print(metadata)
#
#     # Check expected top-level keys
#     assert metadata["Version"] == "202501"
#     assert metadata["Release Date"] == "2025 01 30"
#     assert metadata["Content"] ==   "Trade flows at the year - exporter - importer - product level."
#     assert metadata["Reference"] == "Gaulier, G. and Zignago, S. (2010)"
#
#     # Ensure 'List of Variables' and its entries are not included
#     assert "List of Variables" not in metadata
#     assert "t" not in metadata
#     assert "i" not in metadata
#     assert "j" not in metadata


def test_parse_readme_invalid():
    """
    Test that `parse_readme` returns an empty dictionary when input has no colon-separated blocks.
    """
    input_text = "This readme is broken\nIt has no keys\nJust plain text\n"
    result = parse_readme(input_text)
    assert result == {}


@pytest.mark.parametrize(
    "data_df,country_df,expected_exporter_names",
    [
        # Matching codes
        (
            pd.DataFrame({Fields.exporter_code: [1], Fields.importer_code: [2]}),
            pd.DataFrame(
                {
                    Fields.country_code: [1, 2],
                    Fields.country_name: ["Nigeria", "Kenya"],
                    Fields.iso3_code: ["NGA", "KEN"],
                    Fields.iso2_code: ["NG", "KE"],
                }
            ),
            ["Nigeria"],
        ),
        # Missing code in country_codes
        (
            pd.DataFrame({Fields.exporter_code: [99], Fields.importer_code: [2]}),
            pd.DataFrame(
                {
                    Fields.country_code: [2],
                    Fields.country_name: ["Kenya"],
                    Fields.iso3_code: ["KEN"],
                    Fields.iso2_code: ["KE"],
                }
            ),
            [np.nan],
        ),
        # Empty data input
        (
            pd.DataFrame(columns=[Fields.exporter_code, Fields.importer_code]),
            pd.DataFrame(
                {
                    Fields.country_code: [1],
                    Fields.country_name: ["Test"],
                    Fields.iso3_code: ["TST"],
                    Fields.iso2_code: ["TT"],
                }
            ),
            [],
        ),
    ],
)
def test_add_country_labels(data_df, country_df, expected_exporter_names):
    """
    Test `add_country_labels` to ensure it maps exporter/importer codes to country names and ISO3 codes,
    and handles missing or unmatched codes gracefully.
    """
    result = add_country_labels(data_df, country_df)
    assert list(result.get(Fields.exporter_name, [])) == expected_exporter_names


@pytest.mark.parametrize(
    "data_df,product_df,expected_descriptions",
    [
        # Matching product code
        (
            pd.DataFrame({Fields.product_code: [100]}),
            pd.DataFrame(
                {
                    Fields.product_code: [100],
                    Fields.product_description: ["Cotton"],
                }
            ),
            ["Cotton"],
        ),
        # Missing product code
        (
            pd.DataFrame({Fields.product_code: [999]}),
            pd.DataFrame(
                {
                    Fields.product_code: [100],
                    Fields.product_description: ["Cotton"],
                }
            ),
            [np.nan],
        ),
        # Empty input
        (
            pd.DataFrame(columns=[Fields.product_code]),
            pd.DataFrame(
                {
                    Fields.product_code: [100],
                    Fields.product_description: ["Cotton"],
                }
            ),
            [],
        ),
    ],
)
def test_add_product_descriptions(data_df, product_df, expected_descriptions):
    """
    Test `add_product_descriptions` to ensure it maps product codes to their descriptions correctly,
    and handles unmatched codes or empty input gracefully.
    """
    result = add_product_descriptions(data_df, product_df)
    assert list(result.get("product_description", [])) == expected_descriptions
