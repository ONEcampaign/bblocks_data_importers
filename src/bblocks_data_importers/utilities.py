import pandas as pd
from typing import Literal


def convert_dtypes(df: pd.DataFrame, backend: Literal["pyarrow", "numpy_nullable"] = "pyarrow") -> pd.DataFrame:
    """Converts the DataFrame to the specified backend dtypes

    Args:
        df: The DataFrame to convert
        backend: The backend to use for the conversion. Default is "pyarrow"

    Returns:
        A DataFrame with the pyarrow dtypes
    """
    try:
        return df.convert_dtypes(dtype_backend=backend)
    except TypeError as e:
        raise TypeError(f"Data conversion error: {e}")
    except ValueError as e:
        raise ValueError(f"Data conversion error: {e}")


