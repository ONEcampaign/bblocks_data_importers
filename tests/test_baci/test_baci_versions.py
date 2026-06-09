"""Tests for baci_versions module."""

from unittest import mock

import pytest
import requests
from bs4 import BeautifulSoup

from bblocks.data_importers.baci import baci_versions

# Minimal HTML matching the new baci_webpage.html structure
VALID_HTML = """
<html>
  <body>
    <section id="download-links">
      <p>This is the 202601 version.</p>
      <ul>
        <li><a href="https://example.com/BACI_HS92_V202601.zip">HS92</a></li>
        <li><a href="https://example.com/BACI_HS22_V202601.zip">HS22</a></li>
        <li><a href="https://example.com/release_notes.pdf">release notes</a></li>
      </ul>
    </section>
    <section id="archives">
      <div class="panel-tabset">
        <ul class="nav nav-tabs">
          <li><a class="nav-link active">202501</a></li>
          <li><a class="nav-link">202401b</a></li>
        </ul>
        <div class="tab-content">
          <div class="tab-pane active">
            <ul>
              <li><a href="https://example.com/BACI_HS92_V202501.zip">HS92</a></li>
              <li><a href="https://example.com/BACI_HS22_V202501.zip">HS22</a></li>
            </ul>
          </div>
          <div class="tab-pane">
            <ul>
              <li><a href="https://example.com/BACI_HS92_V202401b.zip">HS92</a></li>
            </ul>
          </div>
        </div>
      </div>
    </section>
  </body>
</html>
"""

# Archive panel that also has product code links (older versions like 202102)
ARCHIVE_WITH_PRODUCT_CODES_HTML = """
<html>
  <body>
    <section id="download-links">
      <p>This is the 202601 version.</p>
      <ul>
        <li><a href="https://example.com/BACI_HS22_V202601.zip">HS22</a></li>
      </ul>
    </section>
    <section id="archives">
      <div class="panel-tabset">
        <ul class="nav nav-tabs">
          <li><a class="nav-link active">202102</a></li>
        </ul>
        <div class="tab-content">
          <div class="tab-pane active">
            <p><strong>Trade flows</strong></p>
            <ul>
              <li><a href="https://example.com/BACI_HS92_V202102.zip">HS92</a></li>
              <li><a href="https://example.com/BACI_HS17_V202102.zip">HS17</a></li>
            </ul>
            <p><strong>Product codes</strong></p>
            <ul>
              <li><a href="https://example.com/product_codes_HS92_V202102.csv">HS92</a></li>
              <li><a href="https://example.com/product_codes_HS17_V202102.csv">HS17</a></li>
            </ul>
          </div>
        </div>
      </div>
    </section>
  </body>
</html>
"""


def _make_mock_response(html: str):
    mock_response = mock.Mock()
    mock_response.content = html.encode()
    mock_response.raise_for_status = mock.Mock()
    return mock_response


def test_get_soup_success():
    with mock.patch(
        "requests.get", return_value=_make_mock_response("<html></html>")
    ) as mock_get:
        soup = baci_versions._get_soup()

    mock_get.assert_called_once_with(baci_versions.BACI_URL)
    assert isinstance(soup, BeautifulSoup)


def test_get_soup_failure():
    with mock.patch(
        "requests.get", side_effect=requests.RequestException("boom")
    ):
        with pytest.raises(RuntimeError, match="Failed to fetch BACI page"):
            baci_versions._get_soup()


def test_parse_latest_version_success():
    soup = BeautifulSoup(VALID_HTML, "html.parser")
    section = soup.find("section", {"id": "download-links"})

    result = baci_versions._parse_latest_version(section)

    assert "202601" in result
    assert result["202601"]["latest"] is True
    assert result["202601"]["hs_versions"] == ["HS92", "HS22"]


def test_parse_latest_version_ignores_non_data_links():
    """Links without BACI_HSxx_V in href (e.g. release notes) are not included."""
    soup = BeautifulSoup(VALID_HTML, "html.parser")
    section = soup.find("section", {"id": "download-links"})

    result = baci_versions._parse_latest_version(section)

    hs = result["202601"]["hs_versions"]
    assert "release notes" not in hs
    assert len(hs) == 2


def test_parse_latest_version_missing_version_string():
    html = '<section id="download-links"><a href="BACI_HS22_V202601.zip">HS22</a></section>'
    soup = BeautifulSoup(html, "html.parser")
    section = soup.find("section", {"id": "download-links"})

    with pytest.raises(ValueError, match="Latest version could not be found"):
        baci_versions._parse_latest_version(section)


def test_parse_latest_version_missing_hs_links():
    html = '<section id="download-links"><p>This is the 202601 version.</p></section>'
    soup = BeautifulSoup(html, "html.parser")
    section = soup.find("section", {"id": "download-links"})

    with pytest.raises(ValueError, match="No HS versions found"):
        baci_versions._parse_latest_version(section)


def test_parse_archive_versions_success():
    soup = BeautifulSoup(VALID_HTML, "html.parser")
    section = soup.find("section", {"id": "archives"})

    result = baci_versions._parse_archive_versions(section)

    assert "202501" in result
    assert result["202501"]["hs_versions"] == ["HS92", "HS22"]
    assert "latest" not in result["202501"]

    assert "202401b" in result
    assert result["202401b"]["hs_versions"] == ["HS92"]


def test_parse_archive_versions_no_duplicates_from_product_codes():
    """HS links in product code sub-sections must not be counted as trade flow versions."""
    soup = BeautifulSoup(ARCHIVE_WITH_PRODUCT_CODES_HTML, "html.parser")
    section = soup.find("section", {"id": "archives"})

    result = baci_versions._parse_archive_versions(section)

    assert result["202102"]["hs_versions"] == ["HS92", "HS17"]


def test_parse_archive_versions_empty():
    html = '<section id="archives"><div class="panel-tabset"></div></section>'
    soup = BeautifulSoup(html, "html.parser")
    section = soup.find("section", {"id": "archives"})

    with pytest.raises(ValueError, match="No archive BACI versions found"):
        baci_versions._parse_archive_versions(section)


def test_parse_baci_and_hs_versions_success():
    with mock.patch(
        "bblocks.data_importers.cepii.baci_versions._get_soup",
        return_value=BeautifulSoup(VALID_HTML, "html.parser"),
    ):
        result = baci_versions.parse_baci_and_hs_versions()

    assert "202601" in result
    assert result["202601"]["latest"] is True
    assert "202501" in result
    assert "202401b" in result
    assert "latest" not in result["202501"]


def test_parse_baci_and_hs_versions_missing_download_section():
    html = '<html><body><section id="archives"></section></body></html>'
    with mock.patch(
        "bblocks.data_importers.cepii.baci_versions._get_soup",
        return_value=BeautifulSoup(html, "html.parser"),
    ):
        with pytest.raises(ValueError, match="Download links section not found"):
            baci_versions.parse_baci_and_hs_versions()


def test_parse_baci_and_hs_versions_missing_archive_section():
    html = f"""
    <html><body>
      <section id="download-links">
        <p>This is the 202601 version.</p>
        <a href="BACI_HS22_V202601.zip">HS22</a>
      </section>
    </body></html>
    """
    with mock.patch(
        "bblocks.data_importers.cepii.baci_versions._get_soup",
        return_value=BeautifulSoup(html, "html.parser"),
    ):
        with pytest.raises(ValueError, match="Archives section not found"):
            baci_versions.parse_baci_and_hs_versions()
