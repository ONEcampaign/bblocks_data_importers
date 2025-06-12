# Human Development Report Importer

The `HumanDevelopmentIndex` importer provides structured access to Human Development Index (HDI) indicators
from the Human Development Reports database of the United Nations Development Program (UNDP)

## About the HDI and complementary indices

The Human Development Index (HDI) is a summary measure of average achievement in key dimensions of human development: 
a long and healthy life, being knowledgeable and having a decent standard of living. 

In addition to the HDI, the UNDP also computes and publishes  composite indices to capture broader dimensions of human 
development, identify groups falling behind in human progress and monitor the distribution of human development. These 
include Inequality-adjusted Human Development Index (IHDI), Gender Inequality Index (GII), Gender Development Index 
(GDI) and Planetary pressures-adjusted HDI (PHDI), among others

For more information and access to the raw data, visit the [UNDP website](https://hdr.undp.org/)

## Basi usage

To start using the importer, instantiate the importer and use the `get_data` method to get the latest WEO data.

```python
from bblocks.data_importers import HumanDevelopmentIndex

# Create an importer instance
hdi = HumanDevelopmentIndex()

# Get the latest data
df = hdi.get_data()

# Preview
print(df.head())

# Output: 
#   entity_code  entity_name region_code  ...  value  year  indicator_name
# 0         AFG  Afghanistan          SA  ...  182.0  2022        HDI Rank
# 1         ALB      Albania         ECA  ...   74.0  2022        HDI Rank
# ...
```

## Access metadata

The `get_metadata()` method provides information about the indicators featured in the data

```python
metadata = hdi.get_metadata()
```

## Data caching
The data and metadata are cached to avoid repeated downloads within a session. Cached data is tied to the importer 
instance and cleared automatically when the session ends. You can also manually clear the cache whenever you need.

```python
hdi.clear_cache()
```