"""Test file_handling module for BACI utilities"""

import pytest
import pandas as pd
import pyarrow as pa
from pathlib import Path
import shutil

from bblocks_data_importers.cepii.file_handling import save_parquet, load_parquet, cleanup_csvs


@pytest.fixture
def output_dir(tmp_path) -> Path:
    path = tmp_path / "output"
    path.mkdir()
    yield path
    shutil.rmtree(path, ignore_errors=True)

@pytest.fixture
def baci_table() -> pa.Table:
    """Fixture to create a reusable PyArrow Table for testing."""
    return pa.table({
        "t": pa.array([2022, 2023, 2023], type=pa.int16()),  # use consistent types for partitioning
        "i": [1, 2, 3],
        "j": [4, 5, 6],
        "k": ["010101", "020202", "030303"],
        "v": [1.0, 2.0, 3.0],
        "q": [0.5, 0.7, 0.9],
    })


def test_save_parquet_creates_partitioned_files(baci_table, output_dir):
    """Test that save_parquet creates partitioned parquet files."""
    save_parquet(baci_table, output_dir)

    # Check that partitioned folders exist
    assert (output_dir / "2022").exists()
    assert (output_dir / "2023").exists()
    assert any(output_dir.rglob("*.parquet"))


def test_load_parquet_returns_no_filtered_dataframe(baci_table, output_dir):
    """Test that load_parquet returns DataFrame without filtering."""
    save_parquet(baci_table, output_dir)

    df = load_parquet(output_dir)
    assert isinstance(df, pd.DataFrame)
    assert df["t"].unique().tolist() == [2022, 2023]
    assert len(df) == 3

def test_load_parquet_returns_filtered_dataframe(baci_table, output_dir):
    """Test that load_parquet filters correctly by year."""
    save_parquet(baci_table, output_dir)

    df = load_parquet(output_dir, filter_years=[2023])
    assert isinstance(df, pd.DataFrame)
    assert df["t"].unique().tolist() == [2023]
    assert len(df) == 2


def test_cleanup_csvs_removes_only_matching(tmp_path):
    """Test that cleanup_csvs removes only CSV files starting with BACI."""
    csv1 = tmp_path / "BACI_2022.csv"
    csv2 = tmp_path / "BACI_2023.csv"
    other = tmp_path / "unrelated.csv"

    for file in [csv1, csv2, other]:
        file.write_text("dummy")

    cleanup_csvs(tmp_path)

    assert not csv1.exists()
    assert not csv2.exists()
    assert other.exists()