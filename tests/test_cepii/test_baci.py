"""Tests for BACI class."""

import pytest
from unittest import mock
import pandas as pd

from bblocks.data_importers import BACI
from bblocks.data_importers.protocols import DataImporter

# FIXTURES


@pytest.fixture
def mock_versions_dict():
    """Fixture for simulating BACI and HS versions dict"""
    return {
        "202501": {"hs_versions": ["HS17", "HS22"], "latest": True},
        "202401b": {"hs_versions": ["HS12", "HS17"]},
    }


@pytest.fixture
def mock_lastest_version(mock_versions_dict):
    """Fixture for simulating last version"""
    return next((k for k, v in mock_versions_dict.items() if v.get("latest")), None)


@pytest.fixture
def mock_baci_df():
    import pandas as pd

    return pd.DataFrame(
        {
            "year": [2022, 2023],
            "exporter_code": [4, 6],
            "importer_code": [20, 30],
            "product_code": [210610, 80211],
            "value": [100, 200],
        }
    )


@pytest.fixture
def dummy_data_manager(mock_baci_df):
    dm = mock.Mock()
    dm.get_data_frame.return_value = mock_baci_df
    dm.available_years = [2022]
    dm.country_codes = mock.Mock()
    dm.metadata = {}
    dm.product_codes = mock.Mock()
    return dm


# TESTS
def test_protocol():
    """Test that importer class implements the DataImporter protocol"""

    importer_obj = BACI()

    assert isinstance(
        importer_obj, DataImporter
    ), "BACI does not implement DataImporter protocol"
    assert hasattr(importer_obj, "get_data"), "BACI does not have get_data method"
    assert hasattr(importer_obj, "clear_cache"), "BACI does not have clear_cache method"


def test_baci_init():
    """Test BACI class initialization"""
    baci_importer = BACI()

    assert baci_importer._data == {}
    assert baci_importer._versions is None
    assert baci_importer._latest_version is None


def test_load_versions_sets_internal_state(mock_versions_dict, mock_lastest_version):
    """Test that _load_versions sets _versions and _latest_version correctly"""
    with mock.patch(
        "bblocks.data_importers.cepii.baci.parse_baci_and_hs_versions",
        return_value=mock_versions_dict,
    ):
        baci = BACI()
        baci._load_versions()

        assert baci._versions == mock_versions_dict
        assert baci._latest_version == mock_lastest_version


def test_get_data_triggers_version_loading(
    mock_versions_dict, mock_lastest_version, dummy_data_manager
):
    """Test that get_data triggers version loading if not previously called"""
    with (
        mock.patch(
            "bblocks.data_importers.cepii.baci.parse_baci_and_hs_versions",
            return_value=mock_versions_dict,
        ),
        mock.patch(
            "bblocks.data_importers.cepii.baci.BaciDataManager",
            return_value=dummy_data_manager,
        ),
    ):

        baci = BACI()

        # Confirm versions are not loaded initially
        assert baci._versions is None

        df = baci.get_data(hs_version="HS22")

        # Confirm versions were loaded
        assert baci._versions == mock_versions_dict
        assert baci._latest_version == mock_lastest_version
        assert not df.empty


def test_get_data_defaults_to_latest_version(
    mock_versions_dict, mock_lastest_version, dummy_data_manager
):
    """Test that get_data defaults to the latest version if no hs_version is provided"""
    with (
        mock.patch(
            "bblocks.data_importers.cepii.baci.parse_baci_and_hs_versions",
            return_value=mock_versions_dict,
        ),
        mock.patch(
            "bblocks.data_importers.cepii.baci.BaciDataManager",
            return_value=dummy_data_manager,
        ),
    ):

        baci = BACI()
        df = baci.get_data(hs_version="HS22")

        assert baci._latest_version == mock_lastest_version
        assert mock_lastest_version in baci._data
        assert "HS22" in baci._data[mock_lastest_version]


@pytest.mark.parametrize(
    "baci_version, hs_version, should_raise",
    [
        ("202501", "HS22", False),
        ("202401b", "HS12", False),
        ("badversion", "HS22", True),
        ("202501", "HS99", True),
    ],
)
def test_invalid_version_and_hs(
    mock_versions_dict, dummy_data_manager, baci_version, hs_version, should_raise
):
    """Test that get_data raises ValueError if invalid baci_version or hs_version is provided"""
    with (
        mock.patch(
            "bblocks.data_importers.cepii.baci.parse_baci_and_hs_versions",
            return_value=mock_versions_dict,
        ),
        mock.patch(
            "bblocks.data_importers.cepii.baci.BaciDataManager",
            return_value=dummy_data_manager,
        ),
    ):

        baci = BACI()

        if should_raise:
            with pytest.raises(ValueError):
                baci.get_data(hs_version=hs_version, baci_version=baci_version)
        else:
            df = baci.get_data(hs_version=hs_version, baci_version=baci_version)
            assert not df.empty


def test_get_data_does_not_reload_if_data_cached(
    mock_versions_dict, mock_lastest_version, dummy_data_manager, mock_baci_df
):
    """Test that get_data does not reload data if it is already cached"""
    with (
        mock.patch(
            "bblocks.data_importers.cepii.baci.parse_baci_and_hs_versions",
            return_value=mock_versions_dict,
        ),
        mock.patch(
            "bblocks.data_importers.cepii.baci.BaciDataManager",
            return_value=dummy_data_manager,
        ) as mock_mgr,
    ):

        hs_version = "HS22"
        baci = BACI()

        # Confirm data is not loaded
        assert baci._data == {}

        # First call should trigger loading
        df1 = baci.get_data(hs_version=hs_version)
        pd.testing.assert_frame_equal(df1, mock_baci_df)

        # Second call should use cache
        df2 = baci.get_data(hs_version=hs_version)
        pd.testing.assert_frame_equal(df2, mock_baci_df)

        # Confirm BaciDataManager was called only once
        assert mock_mgr.call_count == 1


def test_get_data_passes_filter_args(mock_versions_dict, dummy_data_manager):
    """Test that get_data passes filter arguments to BaciDataManager"""
    with (
        mock.patch(
            "bblocks.data_importers.cepii.baci.parse_baci_and_hs_versions",
            return_value=mock_versions_dict,
        ),
        mock.patch(
            "bblocks.data_importers.cepii.baci.BaciDataManager",
            return_value=dummy_data_manager,
        ),
    ):

        baci = BACI()
        baci.get_data(
            hs_version="HS22", years=[2020], products=[10001], incl_country_labels=True
        )

        dummy_data_manager.get_data_frame.assert_called_once_with(
            years=[2020],
            products=[10001],
            incl_country_labels=True,
            incl_product_labels=False,
        )


def test_get_data_caches_multiple_hs_versions(
    mock_versions_dict, dummy_data_manager, mock_baci_df
):
    """Ensure get_data caches different hs_versions independently under the same baci_version"""

    with (
        mock.patch(
            "bblocks.data_importers.cepii.baci.parse_baci_and_hs_versions",
            return_value=mock_versions_dict,
        ),
        mock.patch(
            "bblocks.data_importers.cepii.baci.BaciDataManager",
            return_value=dummy_data_manager,
        ) as mock_mgr,
    ):

        baci = BACI()
        baci_version = "202501"
        hs_version_1 = "HS17"
        hs_version_2 = "HS22"

        # First call loads HS17
        df1 = baci.get_data(hs_version=hs_version_1, baci_version=baci_version)
        pd.testing.assert_frame_equal(df1, mock_baci_df)

        # Second call loads HS22 under same BACI version
        df2 = baci.get_data(hs_version=hs_version_2, baci_version=baci_version)
        pd.testing.assert_frame_equal(df2, mock_baci_df)

        # Confirm both HS versions are cached under the same BACI version
        assert hs_version_1 in baci._data[baci_version]
        assert hs_version_2 in baci._data[baci_version]
        assert mock_mgr.call_count == 2


def test_get_data_does_not_override_cache_with_different_filters(
    mock_versions_dict, mock_lastest_version, dummy_data_manager, mock_baci_df
):
    """Ensure that calling get_data with different filters doesn't reload or override cached data"""

    with (
        mock.patch(
            "bblocks.data_importers.cepii.baci.parse_baci_and_hs_versions",
            return_value=mock_versions_dict,
        ),
        mock.patch(
            "bblocks.data_importers.cepii.baci.BaciDataManager",
            return_value=dummy_data_manager,
        ) as mock_mgr,
    ):

        baci = BACI()
        hs_version = "HS22"

        # First call with one set of filters
        df1 = baci.get_data(hs_version=hs_version, years=2022)
        pd.testing.assert_frame_equal(df1, mock_baci_df)

        # Second call with different filters (products param)
        df2 = baci.get_data(hs_version=hs_version, products=[10101])
        pd.testing.assert_frame_equal(df2, mock_baci_df)

        # Data should still be loaded only once (same BaciDataManager instance reused)
        assert mock_mgr.call_count == 1

        # Ensure cached instance is reused
        cached_instance = baci._data[mock_lastest_version][hs_version]
        cached_instance.get_data_frame.assert_any_call(
            years=2022,
            products=None,
            incl_country_labels=False,
            incl_product_labels=False,
        )
        cached_instance.get_data_frame.assert_any_call(
            years=None,
            products=[10101],
            incl_country_labels=False,
            incl_product_labels=False,
        )


def test_clear_cache(mock_versions_dict, mock_lastest_version, mock_baci_df):
    """Tests that clear cache empties importer object data"""

    baci = BACI()

    baci._data = {mock_lastest_version: {"HS22": mock_baci_df}}
    baci._versions = mock_versions_dict
    baci._latest_version = mock_lastest_version

    baci.clear_cache()

    assert baci._data == {}
    assert baci._versions is None
    assert baci._latest_version is None
