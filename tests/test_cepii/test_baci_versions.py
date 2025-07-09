import pytest
from bs4 import BeautifulSoup
import requests
from bblocks.data_importers.cepii.baci_versions import (
    _get_soup,
    _extract_section_div,
    _parse_latest_version,
    _parse_archive_versions,
    parse_baci_and_hs_versions,
)
from unittest.mock import patch, MagicMock


# FIXTURES


@pytest.fixture
def soup_with_sections():
    """
    Returns a BeautifulSoup object representing a mock BACI HTML page
    with two labeled sections: 'Download' and 'Archives'.
    """
    html = """
    <html>
    <body>
        <div class="content_box">
            <div class="titre-rubrique">Download</div>
            <div>
                This is the 202501 version.<br>
                <ul>
                    <li>HS22</li> 
                    <li>HS17</li> 
                    <li>HS12</li>
                </ul>
            </div>
        </div>
        <div class="content_box">
            <div class="titre-rubrique">Archives</div>
            <div>
                <a>202401b version:</a>
                <ul>
                    <li>HS02</li> 
                    <li>HS96</li>
                </ul>
                <a>202401 version:</a>
                <ul>
                    <li>HS96</li> 
                    <li>HS92</li>
                </ul>
            </div>
        </div>
    </body>
    </html>
    """
    return BeautifulSoup(html, "html.parser")


@pytest.fixture
def latest_div(soup_with_sections):
    """
    Extracts the 'Download' section div from the mock soup.
    Used to test parsing of the latest BACI version.
    """
    return _extract_section_div(soup_with_sections, "Download")


@pytest.fixture
def archive_div(soup_with_sections):
    """
    Extracts the 'Archives' section div from the mock soup.
    Used to test parsing of archived BACI versions.
    """
    return _extract_section_div(soup_with_sections, "Archives")


# TESTS


def test_get_soup_returns_soup():
    """
    Test that _get_soup returns a BeautifulSoup object
    when the request is successful and returns valid HTML.
    """
    fake_html = "<html><body><p>Hello</p></body></html>"
    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.status_code = 200


def test_get_soup_raises_on_request_failure():
    with patch("requests.get") as mock_get:
        mock_get.side_effect = requests.RequestException("timeout")
        with pytest.raises(RuntimeError, match="Failed to fetch BACI page"):
            _get_soup()


def test_extract_section_div_success(soup_with_sections):
    """
    Test that _extract_section_div returns the correct parent div
    for a known section title.
    """
    div = _extract_section_div(soup_with_sections, "Download")
    assert "202501 version" in div.text


def test_extract_section_div_is_case_sensitive(soup_with_sections):
    with pytest.raises(ValueError):
        _extract_section_div(soup_with_sections, "download")  # lowercased


def test_parse_versions_fails_if_latest_is_missing(monkeypatch, soup_with_sections):
    def broken_latest(*args):
        raise ValueError("latest failed")

    monkeypatch.setattr(
        "bblocks.data_importers.cepii.baci_versions._get_soup",
        lambda: soup_with_sections,
    )
    monkeypatch.setattr(
        "bblocks.data_importers.cepii.baci_versions._parse_latest_version",
        broken_latest,
    )

    with pytest.raises(ValueError, match="latest failed"):
        parse_baci_and_hs_versions()


def test_extract_section_div_not_found(soup_with_sections):
    """
    Test that _extract_section_div raises ValueError
    if the requested section title does not exist.
    """
    with pytest.raises(ValueError, match="not found"):
        _extract_section_div(soup_with_sections, "Nonexistent Section")


def test_parse_latest_version(latest_div):
    """
    Test that _parse_latest_version correctly extracts the version number,
    sets `latest` to True, and returns a list of HS versions.
    """
    result = _parse_latest_version(latest_div)
    assert "202501" in result
    assert result["202501"]["latest"] is True
    assert result["202501"]["hs_versions"] == ["HS22", "HS17", "HS12"]


def test_parse_latest_version_raises_if_version_missing():
    html = BeautifulSoup(
        "<div>This is the version available for HS96</div>", "html.parser"
    )
    with pytest.raises(ValueError, match="could not be found"):
        _parse_latest_version(html)


def test_parse_latest_version_raises_if_hs_versions_missing():
    html = BeautifulSoup(
        "<div>This is the 202501 version with no HS codes</div>", "html.parser"
    )
    with pytest.raises(ValueError, match="No HS versions"):
        _parse_latest_version(html)


def test_parse_archive_versions(archive_div):
    """
    Test that _parse_archive_versions extracts each version and
    its associated list of HS versions correctly from the text block.
    """
    result = _parse_archive_versions(archive_div)

    print(result)

    assert "202401b" in result
    assert "202401" in result
    assert result["202401b"]["hs_versions"] == ["HS02", "HS96"]
    assert result["202401"]["hs_versions"] == ["HS96", "HS92"]


def test_parse_archive_versions_raises_on_empty_block():
    html = BeautifulSoup(
        "<div>Nothing here matches version format</div>", "html.parser"
    )
    with pytest.raises(ValueError, match="No archive BACI versions"):
        _parse_archive_versions(html)


def test_parse_archive_versions_empty_hs_list_ok():
    html = BeautifulSoup("<div>202101 version:</div>", "html.parser")
    result = _parse_archive_versions(html)
    assert result == {"202101": {"hs_versions": []}}
