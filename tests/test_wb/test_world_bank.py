import pytest

from bblocks_data_importers.world_bank.ids import InternationalDebtStatistics
from bblocks_data_importers.world_bank.wb_api import WorldBank


def test_set_database():
    wb = WorldBank()
    wb.set_database(3)
    assert wb.config["database"] == 3
    assert wb.api.db == 3


@pytest.mark.parametrize(
    "economies_input,expected",
    [("GTM", ["GTM"]), (["GTM", "TGO"], ["GTM", "TGO"])],
)
def test_get_data_single_economy(economies_input, expected):
    wb = WorldBank()
    wb.set_economies(economies_input)
    wb.set_years(2022)
    data = wb.get_data(series="NY.GDP.MKTP.CD")
    assert set(data["entity_code"].unique()) == set(expected)


@pytest.mark.parametrize(
    "years_input,expected",
    [(2022, [2022]), ([2022, 2023], [2022, 2023])],
)
def test_get_data_single_year(years_input, expected):
    wb = WorldBank()
    wb.set_economies("GTM")
    wb.set_years(years_input)
    data = wb.get_data(series="NY.GDP.MKTP.CD")
    assert set(data["year"].unique()) == set(expected)


@pytest.mark.parametrize("years_input,expected", [(2, 4), (1, 2)])
def test_get_most_recent_value(years_input, expected):
    wb = WorldBank()
    wb.set_economies(["GTM", "USA"])
    wb.set_most_recent_values_to_get(years_input)
    data = wb.get_data(series="NY.GDP.MKTP.CD")
    assert data["value"].nunique() == expected
    assert "year" in data.columns


@pytest.mark.parametrize("years_input,expected", [(2, 4), (1, 2)])
def test_get_most_recent_non_empty_value(years_input, expected):
    wb = WorldBank()
    wb.set_economies(["GTM", "USA"])
    wb.set_most_recent_non_empty_value(years_input)
    data = wb.get_data(series="NY.GDP.MKTP.CD")
    assert data["value"].nunique() == expected
    assert "year" in data.columns


def test_get_african_countries():
    wb = WorldBank()
    countries = wb.get_african_countries()
    assert 50 < len(countries) < 60


def test_get_data_ids_data():
    wb = InternationalDebtStatistics()
    wb.set_economies("GTM")
    wb.set_years(2024)
    debt_service = wb.debt_service_indicators(detailed_category=True)
    assert len(debt_service) > 0
    data = wb.get_data(series=list(debt_service))

    assert "counterpart_code" in data.columns
    assert "counterpart_name" in data.columns
    assert "counterpart_entity_code" in data.columns
    assert "DT.AMT.BLAT.CD" in data["indicator_code"].unique()


def test_get_data_ids_debt_stocks_data():
    wb = InternationalDebtStatistics()
    wb.set_economies("GTM")
    wb.set_years(2021)

    data = wb.get_debt_stocks_data(detailed_category=False)

    assert "Private" in data["indicator"].unique()
