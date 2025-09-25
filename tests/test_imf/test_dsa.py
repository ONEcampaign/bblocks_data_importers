"""Tests for the IMF DSA importer."""

from io import BytesIO
from types import SimpleNamespace
from unittest import mock

import pandas as pd
import pandas.testing as pdt
import pytest
import httpx


from bblocks.data_importers.imf import dsa


@pytest.fixture
def sample_raw_table():
    """Mimic the table returned from camelot before cleaning."""

    return pd.DataFrame(
        {
            0: ["unused", "unused"],
            1: ["Country One 1/ ", "Country Two"],
            2: ["2024-01-15", "2024-04-01"],
            3: [" in debt distress ", "low"],
            4: ["ignored", "ignored"],
            5: ["SUSTAINABLE", "unsustainable 2/"],
            6: ["Yes", "No"],
        }
    )


def test_strip_footnote_trailer_removes_trailing_marker():
    assert dsa._strip_footnote_trailer("Example 3/  ") == "Example"


def test_strip_footnote_trailer_passthrough_for_non_strings():
    assert dsa._strip_footnote_trailer(None) is None
    assert dsa._strip_footnote_trailer(123) == 123


def test_download_pdf_uses_httpx_client():
    fake_content = b"%PDF"
    mock_client = mock.MagicMock()
    mock_response = mock.MagicMock()
    mock_response.content = fake_content
    mock_client.get.return_value = mock_response
    mock_client.__enter__.return_value = mock_client
    mock_client.__exit__.return_value = False

    with mock.patch("httpx.Client", return_value=mock_client) as client_cls:
        result = dsa._download_pdf("https://example.com/dsa.pdf")

    assert result == fake_content
    client_kwargs = client_cls.call_args.kwargs
    assert client_kwargs["follow_redirects"] is True
    assert client_kwargs["timeout"] == httpx.Timeout(30.0)
    mock_client.get.assert_called_once_with(
        "https://example.com/dsa.pdf",
        headers={
            "User-Agent": "bblocks data importers @ https://data.one.org",
            "Accept": "application/pdf",
        },
    )
    mock_response.raise_for_status.assert_called_once()


def test_pdf_to_df_returns_single_table():
    fake_df = pd.DataFrame({"a": [1, 2]})
    table = SimpleNamespace(df=fake_df)

    with mock.patch("camelot.read_pdf", return_value=[table]) as read_pdf:
        result = dsa._pdf_to_df(b"pdf-bytes")

    assert result.equals(fake_df)
    read_args, read_kwargs = read_pdf.call_args
    assert isinstance(read_args[0], BytesIO)
    assert read_kwargs == {"flavor": "stream"}


def test_pdf_to_df_raises_when_pdf_invalid():
    with mock.patch("camelot.read_pdf", return_value=[]):
        with pytest.raises(ValueError, match="Could not read PDF to a dataframe"):
            dsa._pdf_to_df(b"broken")


def test_clean_headers_selects_expected_columns(sample_raw_table):
    cleaned = dsa.__clean_headers(sample_raw_table)

    assert list(cleaned.columns) == [
        "country",
        "latest_publication",
        "risk_of_debt_distress",
        "debt_sustainability",
        "joint_with_wb",
    ]


def test_normalise_booleans(sample_raw_table):
    df = dsa.__clean_headers(sample_raw_table)
    result = dsa.__normalise_booleans(df, "joint_with_wb")
    assert result["joint_with_wb"].tolist() == [True, False]


def test_normalise_debt_distress_standardises_labels(sample_raw_table):
    df = dsa.__clean_headers(sample_raw_table)
    result = dsa.__normalise_debt_distress(df)
    assert result["risk_of_debt_distress"].tolist() == [
        "In debt distress",
        "Low",
    ]


def test_normalise_debt_sustainability_standardises_labels(sample_raw_table):
    df = dsa.__clean_headers(sample_raw_table)
    result = dsa.__normalise_debt_sustainability(df)
    assert result["debt_sustainability"].tolist() == [
        "Sustainable",
        "Unsustainable",
    ]


def test_normalise_date_parses_column(sample_raw_table):
    df = dsa.__clean_headers(sample_raw_table)
    result = dsa.__normalise_date(df, "latest_publication")
    assert pd.api.types.is_datetime64_any_dtype(result["latest_publication"])
    assert result.loc[0, "latest_publication"].date().isoformat() == "2024-01-15"


def test_normalise_country_names_resolves(sample_raw_table):
    df = dsa.__clean_headers(sample_raw_table)

    with mock.patch("bblocks.data_importers.imf.dsa.resolve_places") as resolver:
        resolver.return_value = pd.Series(["Country One", "Country Two"])
        result = dsa.__normalise_country_names(df, "country")

    assert result["country"].tolist() == ["Country One", "Country Two"]
    resolver.assert_called_once_with(mock.ANY, to_type="name_short", not_found="ignore")


def test_insert_iso3_codes_adds_column(sample_raw_table):
    with mock.patch("bblocks.data_importers.imf.dsa.resolve_places") as resolver:
        resolver.side_effect = [
            pd.Series(["Country One", "Country Two"]),
            pd.Series(["C1", "C2"]),
        ]
        df = dsa.__clean_headers(sample_raw_table)
        df = dsa.__normalise_country_names(df, "country")
        result = dsa.__insert_iso3_codes(df)

    assert list(result.columns)[0] == "iso3_code"
    assert result["iso3_code"].tolist() == ["C1", "C2"]
    iso_call = resolver.mock_calls[1]
    _, args, kwargs = iso_call
    assert args[0].equals(result["country"])
    assert kwargs == {"to_type": "iso3_code", "not_found": "ignore"}


def test_get_dsa_returns_expected_dataframe(sample_raw_table):
    expected = pd.DataFrame(
        {
            "iso3_code": ["C1", "C2"],
            "country": ["Country One", "Country Two"],
            "latest_publication": pd.to_datetime(["2024-01-15", "2024-04-01"]),
            "risk_of_debt_distress": ["In debt distress", "Low"],
            "debt_sustainability": ["Sustainable", "Unsustainable"],
            "joint_with_wb": [True, False],
        }
    )

    def fake_resolve_places(values, to_type, not_found):
        if to_type == "name_short":
            return pd.Series(["Country One", "Country Two"], index=values.index)
        if to_type == "iso3_code":
            return pd.Series(["C1", "C2"], index=values.index)
        raise AssertionError("Unexpected to_type")

    with (
        mock.patch("bblocks.data_importers.imf.dsa._download_pdf", return_value=b"pdf"),
        mock.patch(
            "bblocks.data_importers.imf.dsa._pdf_to_df", return_value=sample_raw_table
        ),
        mock.patch(
            "bblocks.data_importers.imf.dsa.resolve_places",
            side_effect=fake_resolve_places,
        ),
    ):
        result = dsa.get_dsa()

    pdt.assert_frame_equal(result, expected)
