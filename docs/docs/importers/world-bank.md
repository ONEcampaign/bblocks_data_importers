# World Bank Importer

The `WorldBank` importer provides access to World Bank databases accessible through
the World Bank public API. 

## About World Bank data

The World Bank is one of the best known aggregators and publishers of 
development data on a broad range of topics including poverty, debt, health, 
environmental sustainability and many others. The World Bank makes its data
available through several databases such as the 
[World Development Indicators (WDI)](https://datatopics.worldbank.org/world-development-indicators/)
and the [International Debt Statistics (IDS)](https://www.worldbank.org/en/programs/debt-statistics/ids).

The `WorldBank` importer is built on top of the 
[wbgapi](https://pypi.org/project/wbgapi/) Python package, a powerful
and complete wrapper for the World Bank API. The `WorldBank` importer offers simple
access to World Bank data with a consistent interface. For more granular control of API
settings and additional functionality, consider using the 
[wbgapi](https://pypi.org/project/wbgapi/) package.

## Basic Usage

To access World Bank data, first instantiate a `WorldBank` importer

```python
from bblocks.data_importers import WorldBank

wb = WorldBank()
```

By default, the importer will connect to the World Development Indicators (WDI) database 
(WDI database id = `2`). This database contains the largest collection of development
indicators. 

To get data for an indicator, use the `get_data` method. For example, to get 
GDP data (indicator code `NY.GDP.MKTP.CD`):

```python
df = wb.get_data(series="NY.GDP.MKTP.CD")

print(df.head())
# Output:
#       year    entity_code entity_name indicator_code  value
# 0     2023    ZWE         Zimbabwe    NY.GDP.MKTP.CD  35231367885.8554
# 1     2022    ZWE         Zimbabwe    NY.GDP.MKTP.CD  32789751736.332298
# 2     2021    ZWE         Zimbabwe    NY.GDP.MKTP.CD  27240515108.804901
# ...
```

## Access different databases

The `WorldBank` importer gives you access to any database available through the 
World Bank API.

To see all available databases, call the `get_available_databases` method.

```python
wb.get_available_databases()

# Output:
# {'1': 'Doing Business',
# '2': 'World Development Indicators',
# '3': 'Worldwide Governance Indicators',
# '5': 'Subnational Malnutrition Database',
# '6': 'International Debt Statistics',
# ...}
```

By default, the importer will query the WDI database (id=`2`), but you can query any
other database by setting the database class attribute to the desired database ID.

For example, to access International Debt Statistics (IDS) data set the database to `6`.

```python
wb.set_database(6)
```

Now you can use `get_data` to access any series in the IDS database.


## Indicator metadata

To get the metadata including indicator name, description
notes, aggregations, etc., for an indicator use the `get_indicator_metadata` method.

```python
wb.get_indicator_metadata("NY.GDP.MKTP.CD")

# Output:
# {'Aggregationmethod': 'Gap-filled total',
# 'IndicatorName': 'GDP (current US$)',
# 'License_Type': 'CC BY-4.0',
# ...}
```

## Specialized World Bank importers

In addition to the `WorldBank` importer, specialized importers exist 
for specific databases with additional database specific functionality.

- [`InternationalDebtStatistics`](./ids.md) - Access International Debt Statistics (IDS) data on long-term debt