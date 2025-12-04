import pandas as pd
import pytest

from bblocks.data_importers.world_bank import (
    international_debt_statistics as ids_module,
)
from bblocks.data_importers.world_bank import world_bank


@pytest.fixture(autouse=True)
def _clear_world_bank_caches():
    world_bank.get_wb_databases.cache_clear()
    world_bank.get_wb_entities.cache_clear()
    world_bank.get_wb_indicators.cache_clear()
    world_bank._get_cached_metadata.cache_clear()


def test_initializes_with_ids_database(monkeypatch):
    captured = []
    monkeypatch.setattr(world_bank, "_check_valid_db", lambda db: captured.append(db))

    importer = ids_module.InternationalDebtStatistics()

    assert importer.db == 6
    assert captured == [6]


def test_repr_includes_database(monkeypatch):
    monkeypatch.setattr(world_bank, "_check_valid_db", lambda db: None)

    importer = ids_module.InternationalDebtStatistics()

    assert repr(importer) == "InternationalDebtStatistics(db=6)"


def test_last_updated_reads_from_database_table(monkeypatch):
    monkeypatch.setattr(world_bank, "_check_valid_db", lambda db: None)
    sample = pd.DataFrame(
        {
            "id": [6, 7],
            "last_updated": [
                pd.Timestamp("2024-04-01"),
                pd.Timestamp("2020-01-01"),
            ],
        }
    )
    monkeypatch.setattr(ids_module, "get_wb_databases", lambda: sample)

    importer = ids_module.InternationalDebtStatistics()

    assert importer.last_updated == pd.Timestamp("2024-04-01")


def test_debt_stock_indicators_fetch_metadata(monkeypatch):
    monkeypatch.setattr(world_bank, "_check_valid_db", lambda db: None)
    captured = {}

    def fake_get_indicator_metadata(self, indicator_code):
        captured["indicator_code"] = indicator_code
        return pd.DataFrame({"indicator_code": indicator_code})

    monkeypatch.setattr(
        ids_module.WorldBank, "get_indicator_metadata", fake_get_indicator_metadata
    )

    importer = ids_module.InternationalDebtStatistics()
    df = importer.debt_stock_indicators

    expected = [
        "DT.DOD.BLAT.CD",
        "DT.DOD.MLAT.CD",
        "DT.DOD.PBND.CD",
        "DT.DOD.PCBK.CD",
        "DT.DOD.PROP.CD",
    ]

    assert captured["indicator_code"] == expected
    assert df["indicator_code"].tolist() == expected


def test_debt_service_indicators_fetch_metadata(monkeypatch):
    monkeypatch.setattr(world_bank, "_check_valid_db", lambda db: None)
    captured = {}

    def fake_get_indicator_metadata(self, indicator_code):
        captured["indicator_code"] = indicator_code
        return pd.DataFrame({"indicator_code": indicator_code})

    monkeypatch.setattr(
        ids_module.WorldBank, "get_indicator_metadata", fake_get_indicator_metadata
    )

    importer = ids_module.InternationalDebtStatistics()
    df = importer.debt_service_indicators

    expected = [
        "DT.AMT.BLAT.CD",
        "DT.AMT.MLAT.CD",
        "DT.AMT.PBND.CD",
        "DT.AMT.PCBK.CD",
        "DT.AMT.PROP.CD",
        "DT.INT.BLAT.CD",
        "DT.INT.MLAT.CD",
        "DT.INT.PBND.CD",
        "DT.INT.PCBK.CD",
        "DT.INT.PROP.CD",
    ]

    assert captured["indicator_code"] == expected
    assert df["indicator_code"].tolist() == expected
