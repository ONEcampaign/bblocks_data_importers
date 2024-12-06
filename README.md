[![PyPI](https://img.shields.io/pypi/v/bblocks_data_importers.svg)](https://pypi.org/project/bblocks_data_importers/)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/bblocks_data_importers.svg)](https://pypi.org/project/bblocks_data_importers/)
[![Documentation Status](https://readthedocs.org/projects/bblocks-data-importers/badge/?version=latest)](https://bblocks-data-importers.readthedocs.io/en/latest/?badge=latest)
[![codecov](https://codecov.io/gh/ONEcampaign/bblocks_data_importers/branch/main/graph/badge.svg?token=YN8S1719NH)](https://codecov.io/gh/ONEcampaign/bblocks_data_importers)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)


# bblocks_data_importers

A package to import data from major humanitarian, development and financial institutions.

## Installation

```bash
$ pip install bblocks_data_importers
```

## Example Usage

Import the package and use the various importer objects to access data from different sources.

```python
import bblocks_data_importers as bbdata
```

To access data from the IMF's World Economic Outlook use a WEO object:

```python
weo = bbdata.WEO() # Create a WEO object
df = weo.get_data() # Get the data as a pandas DataFrame
```

Available importers include:
- `WEO` to access the IMF's World Economic Outlook
- `GHED` to access WHO's Global Health Expenditure Database
- `WFPFoodSecurity` and `WFPInflation` to access the World Food Programme's Food Security and Inflation data
- `WorldBank` to access the World Bank data
- `InternationalDebtStatistics` to access the World Bank's International Debt Statistics

Read the documentation for more details on each importer [here](https://bblocks-data-importers.readthedocs.io/en/latest/Importers/index.html)


## Contributing

Interested in contributing? Check out the contributing guidelines. Please note that this project is released with a Code of Conduct. By contributing to this project, you agree to abide by its terms.

## License

`bblocks_data_importers` was created by The ONE Campaign. It is licensed under the terms of the MIT license.

## Credits
This package is maintained by Luca Picci and Jorge Rivera

`bblocks_data_importers` was created with [`cookiecutter`](https://cookiecutter.readthedocs.io/en/latest/) and the `py-pkgs-cookiecutter` [template](https://github.com/py-pkgs/py-pkgs-cookiecutter).
