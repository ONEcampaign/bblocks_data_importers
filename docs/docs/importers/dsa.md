# Debt Sustainability Analysis (DSA) Importer

The `get_dsa` helper fetches and tidies the IMF's Low-Income Country (LIC) debt sustainability assessments. It
downloads the latest PDF released by the IMF and returns a pandas DataFrame with cleaned, structured columns ready for
analysis.

## About the LIC DSA list

The IMF publishes a regularly updated list of debt sustainability assessments for low-income countries in PDF format.
Each release summarises the publication date of the latest assessment, the assessed risk of debt distress, and whether
the work was carried out jointly with the World Bank.

The source document can be accessed on the IMF website: [Latest LIC DSA list](https://www.imf.org/external/Pubs/ft/dsa/DSAlist.pdf)

## Basic usage

Call `get_dsa()` to receive the cleaned table. No additional parameters are required.

```python
from bblocks.data_importers.imf.dsa import get_dsa

df = get_dsa()
```

The importer normalises the raw PDF output so you can start analysing immediately.

## Column reference

- `iso3_code` – ISO-3 country code derived via `bblocks.places.resolve_places`.
- `country` – Short country name with IMF footnote markers removed and names harmonised through `resolve_places`.
- `latest_publication` – Parsed as `datetime64[ns]` (local time). Values that cannot be parsed are set to `NaT`.
- `risk_of_debt_distress` – Title-case labels with leading/trailing whitespace collapsed (e.g. `Low`, `High`, `In debt distress`).
- `debt_sustainability` – Cleaned text (footnote markers removed) with consistent labels (`Sustainable`, `Unsustainable`).
- `joint_with_wb` – Boolean flag indicating whether the assessment was prepared jointly with the World Bank.

Rows without a recognised country are dropped to ensure the final DataFrame contains only country rows.


If you encounter format changes in the source PDF, please [open an issue](https://github.com/ONEcampaign/bblocks_data_importers/issues)
so we can update the parser accordingly.
