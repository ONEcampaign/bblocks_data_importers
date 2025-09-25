import re
from io import BytesIO
from typing import Final

import camelot
import httpx
import pandas as pd
from bblocks.places import resolve_places

URL: Final[str] = "https://www.imf.org/external/Pubs/ft/dsa/DSAlist.pdf"
_FOOTNOTE_TRAILER = re.compile(r"\s*\d+/\s*$")


def _strip_footnote_trailer(x: str | None) -> str | None:
    if not isinstance(x, str):
        return x
    return _FOOTNOTE_TRAILER.sub("", x).strip()


def _download_pdf(url: str) -> bytes:
    """Download PDF"""
    headers = {
        "User-Agent": "bblocks data importers @ https://data.one.org",
        "Accept": "application/pdf",
    }
    with httpx.Client(follow_redirects=True, timeout=httpx.Timeout(30.0)) as client:
        r = client.get(url, headers=headers)
        r.raise_for_status()
        return r.content


def _pdf_to_df(src: bytes) -> pd.DataFrame:
    """Extract the single table from the one-page PDF"""

    file = BytesIO(src)

    try:
        tables = camelot.read_pdf(file, flavor="stream")
        if len(tables) != 1:
            raise ValueError("Invalid PDF format. Check PDF")

        return tables[0].df

    except ValueError:
        raise ValueError("Could not read PDF to a dataframe")


def __clean_headers(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and standardize the DSA table headers."""
    # strip whitespace and footnote trailers in all string cells
    columns = {
        1: "country",
        2: "latest_publication",
        3: "risk_of_debt_distress",
        5: "debt_sustainability",
        6: "joint_with_wb",
    }
    return df.filter(columns.keys()).rename(columns=columns)


def __normalise_booleans(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """Normalize boolean columns to True/False."""
    if column in df.columns:
        df[column] = df[column].str.lower().eq("yes")
    return df


def __normalise_debt_distress(df: pd.DataFrame) -> pd.DataFrame:
    df["risk_of_debt_distress"] = (
        df["risk_of_debt_distress"]
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
        .str.replace("^in debt distress$", "In debt distress", case=False, regex=True)
        .str.replace("^low$", "Low", case=False, regex=True)
        .str.replace("^moderate$", "Moderate", case=False, regex=True)
        .str.replace("^high$", "High", case=False, regex=True)
    )
    return df


def __normalise_debt_sustainability(df: pd.DataFrame) -> pd.DataFrame:
    df["debt_sustainability"] = (
        df["debt_sustainability"]
        .apply(_strip_footnote_trailer)
        .str.strip()
        .str.replace("^sustainable$", "Sustainable", case=False, regex=True)
        .str.replace("^unsustainable$", "Unsustainable", case=False, regex=True)
    )

    return df


def __normalise_date(df: pd.DataFrame, column: str) -> pd.DataFrame:
    if column in df.columns:
        df[column] = pd.to_datetime(df[column], errors="coerce", format=None, utc=False)
    return df


def __normalise_country_names(df: pd.DataFrame, column: str) -> pd.DataFrame:
    if column in df.columns:
        df[column] = df[column].apply(_strip_footnote_trailer)
        df[column] = resolve_places(
            df[column], to_type="name_short", not_found="ignore"
        )
    return df


def __insert_iso3_codes(df: pd.DataFrame) -> pd.DataFrame:
    iso3_codes = resolve_places(df["country"], to_type="iso3_code", not_found="ignore")
    df.insert(df.columns.get_loc("country"), "iso3_code", iso3_codes)
    return df


def get_dsa() -> pd.DataFrame:
    """Get the latest IMF LIC DSA list as a tidy DataFrame.

    Returns:
        pd.DataFrame: A DataFrame containing the latest LIC DSA list with the following columns
            - country: Country name
            - iso3_code: ISO3 country code
            - latest_publication: Date of the latest DSA publication
            - risk_of_debt_distress: Risk of debt distress classification
            - debt_sustainability: Debt sustainability classification
            - joint_with_wb: Boolean indicating if the DSA was done jointly with the World Bank

    """

    content = _download_pdf(url=URL)
    data = _pdf_to_df(content)
    return (
        data.pipe(__clean_headers)
        .pipe(__normalise_booleans, "joint_with_wb")
        .pipe(__normalise_debt_distress)
        .pipe(__normalise_debt_sustainability)
        .pipe(__normalise_date, "latest_publication")
        .pipe(__normalise_country_names, "country")
        .pipe(__insert_iso3_codes)
        .dropna(subset=["country"])
        .reset_index(drop=True)
    )


if __name__ == "__main__":
    df = get_dsa()
