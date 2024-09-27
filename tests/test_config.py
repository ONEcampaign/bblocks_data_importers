"""Tests for the config module."""

from bblocks_data_importers.config import Paths, set_raw_data_path


def test_paths():
    """Test the Paths class."""
    assert Paths.raw_data.name == ".raw_data"


def test_set_raw_data_path():
    """Test the set_raw_data_path function."""
    set_raw_data_path("new_path")
    assert Paths.raw_data.name == "new_path"
