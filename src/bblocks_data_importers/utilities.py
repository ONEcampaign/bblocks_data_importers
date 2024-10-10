import pandas as pd


def ensure_pyarrow_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """Ensures that a DataFrame uses pyarrow dtypes."""
    return df.convert_dtypes(dtype_backend="pyarrow")
