# BACI trade database importer

The `BACI` importer provides access to the BACI database on international trade.

## About the BACI database

The BACI database is an international trade dataset developed by CEPII. It provides detailed bilateral trade flows for
more than 200 countries and territories, covering over 5,000 products at the 6-digit Harmonized System (HS) level.
Trade values are reported in thousands of USD and quantities in metric tons. BACI is built using the UN COMTRADE
database and includes reconciliation procedures.

More information and access to the raw data can be found [here](https://www.cepii.fr/CEPII/en/bdd_modele/presentation.asp?id=37).

This importer provides functionality to easily access the BACI data for different versions and HS classifications.

## Basic usage

Begin by instantiating a `BACI` object. You must specify an HS classification in the `get_data()` method. 

```python
from bblocks.data_importers import BACI

# Create an importer instance
baci = BACI()

# Get all data from the latest release and HS22 classification
df = baci.get_data(hs_version="HS22")

# Preview
print(df.sample(3))

# Output:
#           year  exporter_code  importer_code  product_code    value  quantity
# 21252611  2023            804            246        910199    0.182     0.001
# 4855297   2022            392            446        151319    2.415     0.649
# 7142310   2022            620            276        871494  180.266     5.269
```

The traded amounts are specified in columns `value` (current thousands of USD) and `quantity` (metric tons).

You can also include country and product labels in the returned DataFrame by setting the `incl_country_labels` and 
`incl_product_labels` parameters to `True`:

```python
df = baci.get_data(hs_version="HS22", incl_country_labels=True, incl_product_labels=True)
```

## Specify a version

A `BACI` object gives you access to all available BACI versions and their supported HS classifications.
To see the available versions and HS classifications, call the `get_available_versions()` method:

```python
versions = baci.get_available_versions()


print(versions)

# Output:
# {
#   '202501': {'hs_versions': ['HS92', 'HS96', 'HS02', 'HS07', 'HS12', 'HS17', 'HS22'], 'latest': True}, 
#   '202401b': {'hs_versions': ['HS92', 'HS96', 'HS02', 'HS07', 'HS12', 'HS17']},
#   ...
# }

# Get all data from the 202401b release and HS17 classification
df = baci.get_data(hs_version="HS17", baci_version="202401b")
```

## Filtering

You can filter the data for specific years or products. To view the available options for a specific HS version:
```python
product_descriptions = baci.get_product_descriptions(hs_version="HS22")
available_years = baci.get_available_years(hs_version="HS22")
```

Depending on your needs, you can pass single values, lists, ranges, or tuples to the `years` and `products` parameters 
in `get_data()`.

```python
# Returns data for the years 2022 and product code 10121 - "Horses: live, pure-bred breeding animals"
df_value = baci.get_data(hs_version="HS22", years=2022, products=10121)

# Returns data for years 2020 and 2022, and products 10121 and 10190
df_list = baci.get_data(hs_version="HS22", years=[2020, 2022], products=[10121, 10190])

# Returns data for the years 2020 to 2022, and products 10121 to 10189
df_range = baci.get_data(hs_version="HS22", years=range(2020, 2023), products=range(10121, 10190))

# Returns data for the years 2020 to 2023, and products 10121 to 10190
df_tuple = baci.get_data(hs_version="HS22", years=(2020, 2023), products=(10121, 10190))
```

## Metadata

You can access metadata for a specific BACI version and HS classification with the `get_metadata()` method. By default,
`baci_version="latest"`. 

```python
metadata = baci.get_metadata(hs_version="HS22")
```

## Saving data locally

To save the raw data to a local directory as a zip file, use the `save_raw_data()` method:

```python
baci.save_raw_data(path="path/to/save/baci_data.zip", hs_version="HS22")
```

## Data caching

The data is cached to avoid unnecessary downloads. Because the BACI dataset is large, the data is cached to
a temporary directory as Parquet files. The cache is deleted automatically when the object is deleted or the
session ends. To clear the cache manually, call the `clear_cache()` method:

```python
baci.clear_cache(clear_disk=True)
```
