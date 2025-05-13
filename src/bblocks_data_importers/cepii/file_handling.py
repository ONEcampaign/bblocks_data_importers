import pandas as pd
from pyarrow import dataset as ds
from pathlib import Path
import pyarrow as pa
from bblocks_data_importers.config import logger


def save_parquet(table: pa.Table, path: Path):
    """
    Save the provided PyArrow Table to partitioned Parquet format.

    Args:
        table (pa.Table): Consolidated trade data.
        path (Path): Destination directory for Parquet files.
    """
    logger.info(f"Saving consolidated BACI dataset to {path}")

    ds.write_dataset(
        data=table,
        base_dir=str(path),
        format="parquet",
        partitioning=["t"],
        existing_data_behavior="overwrite_or_ignore",
    )


def load_parquet(parquet_dir: Path, filter_years: list[int] | None = None):
    dataset = ds.dataset(str(parquet_dir), format="parquet", partitioning=["t"])

    if filter_years:
        logger.info(f"Filtering for years: {filter_years}")
        filter_expr = ds.field("t").isin(filter_years)
        table = dataset.to_table(filter=filter_expr)
    else:
        table = dataset.to_table()

    return table.to_pandas(types_mapper=pd.ArrowDtype)


def cleanup_csvs(path: Path):
    for f in path.glob("BACI*.csv"):
        f.unlink()
