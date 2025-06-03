# BACI trade database Importer

The `BACI` importer provides access to BACI database on international trade.

## About the BACI database

The BACI database is an international trade dataset developed by CEPII. It provides detailed bilateral trade flows for
more than 200 countries and territories, covering over 5,000 products at the 6-digit Harmonized System (HS) level.
Trade values are reported in thousands of USD and quantities in metric tons. BACI is built using the UN COMTRADE
database and includes reconciliation procedures.

More information and access to the raw data can be found at: https://www.cepii.fr/CEPII/en/bdd_modele/presentation.asp?id=37

This importer provides functionality to easily access the latest BACI data (or data from a specific version),
automatically download and extract data if not already available locally, and return formatted trade data.

## Basic usage

Begin by instantiating a `BACI` object with a path to save the data locally. 

```python
import bblocks.data_importers as bbdata

# Create an importer instance
baci = bbdata.BACI(data_path="my/local/folder")

# Get data from the latest release
df = baci.get_data(
    # years=range(2022, 2024) # Optional: filter years
)

# Preview
print(df.sample(3))

# Output:
#           year exporter_iso3    exporter_name  ... product_code      value quantity
# 2685311   2022           FIN          Finland  ...       320990     16.292    4.972
# 17120141  2023           MDA  Rep. of Moldova  ...       841459      0.575    0.019
# 10229678  2022           GBR   United Kingdom  ...       621143  44.633999     1.31
```

The traded amounts are specified in columns `value` (current thousand USD) and `quantity` (metric tons).

## Specify a version

By default, the importer will return the latest BACI version available, but you may specify a different one. You may 
also indicate an HS classification. Note that hs_version determines how far back in time the data goes. For example, 
the default value "22" returns data from 2022 onward.

The `get_baci_versions()` method returns a dictionary with the different BACI versions available and their supported HS
classifications, as well as bool indicator to identify the latest BACI version.

```python
versions = bbdata.get_baci_versions()
```

```python
baci = bbdata.BACI(
    data_path="my/local/folder",
    baci_version="202301",
    hs_version="07"
)

df = baci.get_data()
```

## Metadata

To access metadata from a BACI object:

```python
metadata = baci.get_metadata()
```

A dictionary that maps HS codes to product descriptions is available with:

```python
hs_map = baci.get_hs_map()
```

## Data caching

The data and metadata are cached to avoid loading the dataset repeatedly. Use the `clear_cache()` method to delete this
data. You can set clear_disk = True to delete the local directory where the BACI data was saver (defaults to False).

```python
baci.clear_cache(clear_disk=True)
```
