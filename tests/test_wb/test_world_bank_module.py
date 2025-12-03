import pandas as pd
import pytest
from unittest.mock import Mock

from bblocks.data_importers.config import DataExtractionError, Fields
from bblocks.data_importers.protocols import DataImporter
from bblocks.data_importers.utilities import convert_dtypes
from bblocks.data_importers.world_bank import world_bank


class _DummyMetadata:
    def __init__(self, metadata: dict):
        self.metadata = metadata


@pytest.fixture(autouse=True)
def _clear_world_bank_caches():
    world_bank.get_wb_databases.cache_clear()
    world_bank.get_wb_entities.cache_clear()
    world_bank.get_wb_indicators.cache_clear()
    world_bank._get_cached_metadata.cache_clear()


def test_batch_splits_iterable_into_chunks():
    chunks = list(world_bank._batch(["a", "b", "c", "d", "e"], 2))
    assert chunks == [["a", "b"], ["c", "d"], ["e"]]


def test_make_hashable_handles_common_containers():
    assert world_bank._make_hashable([1, 2]) == (1, 2)
    assert world_bank._make_hashable({"b": 2, "a": 1}) == (("a", 1), ("b", 2))
    assert world_bank._make_hashable("value") == "value"


def test_clean_df_standardizes_column_names():
    raw = pd.DataFrame(
        {
            "time_id": [1],
            "time_value": [2020],
            "economy": ["USA"],
            "economy_value": ["United States"],
            "economy_aggregate": [False],
            "series": ["SP.POP"],
            "series_value": ["Population"],
            "counterpart_area": ["WLD"],
            "counterpart_area_value": ["World"],
            "value": [123],
        }
    )

    cleaned = world_bank._clean_df(raw)

    assert Fields.year in cleaned.columns
    assert Fields.entity_code in cleaned.columns
    assert Fields.indicator_code in cleaned.columns
    assert "time_id" not in cleaned.columns
    assert cleaned.loc[0, Fields.entity_name] == "United States"
    assert cleaned.loc[0, Fields.counterpart_name] == "World"


def test_get_time_range_handles_missing_bounds():
    assert world_bank._get_time_range(None, None) is None
    assert world_bank._get_time_range(2000, 2005) == range(2000, 2006)
    assert world_bank._get_time_range(None, 1900).start == 1800
    assert world_bank._get_time_range(2010, None).stop == 2100


def test_get_wb_databases_formats_source_data(monkeypatch):
    sample = [
        {"id": "2", "name": "WDI", "code": "WDI", "lastupdated": "2024-01-15"}
    ]
    monkeypatch.setattr(world_bank.wb.source, "list", lambda: sample)

    df = world_bank.get_wb_databases()

    assert df.loc[0, "id"] == 2
    assert df.loc[0, "name"] == "WDI"
    assert pd.Timestamp("2024-01-15") == df.loc[0, "last_updated"]


def test_get_wb_entities_renames_and_cleans(monkeypatch):
    sample = [
        {
            "id": "USA",
            "value": "United States",
            "aggregate": False,
            "longitude": "",
            "latitude": "38.0",
            "capitalCity": "Washington",
            "region": {"id": "NA", "value": "North America"},
            "adminregion": {"id": "AR", "value": "Americas"},
            "lendingType": {"id": "LT", "value": "Lending"},
            "incomeLevel": {"id": "HIC", "value": "High income"},
        }
    ]
    monkeypatch.setattr(
        world_bank.wb.economy, "list", lambda **kwargs: sample
    )

    df = world_bank.get_wb_entities(db=1, skip_aggs=True)

    assert df.loc[0, Fields.entity_code] == "USA"
    assert df.loc[0, Fields.region_name] == "North America"
    assert df.loc[0, Fields.income_level_code] == "HIC"
    assert pd.isna(df.loc[0, "longitude"])


def test_get_wb_indicators_returns_expected_columns(monkeypatch):
    sample = [{"id": "SP.POP.TOTL", "value": "Population"}]
    monkeypatch.setattr(world_bank.wb.series, "list", lambda **kwargs: sample)

    df = world_bank.get_wb_indicators(db=4)

    assert df.loc[0, Fields.indicator_code] == "SP.POP.TOTL"
    assert df.loc[0, Fields.indicator_name] == "Population"


def test_check_valid_db_raises_for_unknown_database(monkeypatch):
    monkeypatch.setattr(
        world_bank,
        "get_wb_databases",
        lambda: pd.DataFrame({"id": [1]}),
    )

    world_bank._check_valid_db(1)
    with pytest.raises(ValueError):
        world_bank._check_valid_db(99)


def test_get_indicator_metadata_builds_dataframe(monkeypatch):
    captured = {}

    def fake_metadata(**kwargs):
        captured.update(kwargs)
        return [
            _DummyMetadata({"IndicatorName": "Population", "Unitofmeasure": "people"})
        ]

    monkeypatch.setattr(world_bank, "_get_cached_metadata", fake_metadata)

    df = world_bank.get_indicator_metadata(["SP.POP.TOTL"], db=2)

    assert captured["id"] == ("SP.POP.TOTL",)
    assert df.loc[0, Fields.indicator_code] == "SP.POP.TOTL"
    assert df.loc[0, Fields.indicator_name] == "Population"
    assert df.loc[0, Fields.unit] == "people"


def test_get_indicator_metadata_raises_when_missing(monkeypatch):
    monkeypatch.setattr(world_bank, "_get_cached_metadata", lambda **kwargs: [])

    with pytest.raises(DataExtractionError):
        world_bank.get_indicator_metadata("SP.POP.TOTL")


def test_get_data_returns_clean_dataframe(monkeypatch):
    response = [
        {
            "economy": "USA",
            "time": 2020,
            "series": "SP.POP.TOTL",
            "value": 1,
        }
    ]
    monkeypatch.setattr(world_bank.wb.data, "fetch", lambda **kwargs: response)

    df = world_bank.get_data({"series": ("SP.POP.TOTL",)})

    assert df.loc[0, Fields.entity_code] == "USA"
    assert df.loc[0, Fields.year] == 2020


def test_get_data_returns_empty_dataframe_when_no_results(monkeypatch):
    monkeypatch.setattr(world_bank.wb.data, "fetch", lambda **kwargs: [])

    df = world_bank.get_data({"series": ("SP.POP.TOTL",)})

    assert df.empty


def test_get_data_raises_data_extraction_error_on_failure(monkeypatch):
    def _boom(**kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(world_bank.wb.data, "fetch", _boom)

    with pytest.raises(DataExtractionError):
        world_bank.get_data({"series": ("SP.POP.TOTL",)})


def test_fetch_data_batches_and_validates(monkeypatch):
    calls = []

    def fake_get_data(api_params):
        calls.append(api_params)
        data = [
            {
                Fields.year: 2020,
                Fields.indicator_code: code,
                Fields.entity_code: "KEN",
                Fields.value: idx,
            }
            for idx, code in enumerate(api_params["series"])
        ]
        return convert_dtypes(pd.DataFrame(data))

    monkeypatch.setattr(world_bank, "get_data", fake_get_data)
    monkeypatch.setattr(world_bank.wb, "db", 2)

    wb_instance = world_bank.WorldBank()

    df = wb_instance._fetch_data(
        indicators=("B", "A", "C"),
        db=wb_instance.db,
        entity_code=("KEN",),
        time=range(2000, 2003),
        skip_blanks=True,
        skip_aggs=False,
        include_labels=False,
        params_items=(("per_page", 1000),),
        extra_items=(("version", "MRV"),),
        batch_size=2,
        thread_num=1,
    )

    assert set(df[Fields.indicator_code]) == {"A", "B", "C"}
    assert all(call["params"]["per_page"] == 1000 for call in calls)
    assert all(call["version"] == "MRV" for call in calls)


def test_fetch_data_raises_when_no_rows(monkeypatch):
    monkeypatch.setattr(
        world_bank, "get_data", lambda api_params: pd.DataFrame()
    )
    monkeypatch.setattr(world_bank.wb, "db", 2)

    wb_instance = world_bank.WorldBank()

    with pytest.raises(DataExtractionError):
        wb_instance._fetch_data(
            indicators=("A",),
            db=wb_instance.db,
            entity_code=None,
            time=None,
            skip_blanks=False,
            skip_aggs=False,
            include_labels=False,
            params_items=None,
            extra_items=(),
            batch_size=1,
            thread_num=1,
        )


def test_world_bank_get_data_prepares_parameters(monkeypatch):
    monkeypatch.setattr(world_bank.wb, "db", 2)
    captured = {}

    def fake_fetch(
        self,
        indicators,
        db,
        entity_code,
        time,
        skip_blanks,
        skip_aggs,
        include_labels,
        params_items,
        extra_items,
        batch_size,
        thread_num,
    ):
        captured.update(
            {
                "indicators": indicators,
                "db": db,
                "entity_code": entity_code,
                "time": time,
                "skip_blanks": skip_blanks,
                "skip_aggs": skip_aggs,
                "include_labels": include_labels,
                "params_items": params_items,
                "extra_items": extra_items,
                "batch_size": batch_size,
                "thread_num": thread_num,
            }
        )
        data = pd.DataFrame(
            {
                Fields.year: [2010],
                Fields.indicator_code: ["A"],
                Fields.entity_code: ["KEN"],
                Fields.value: [1],
            }
        )
        return convert_dtypes(data)

    fake_fetch.cache_clear = lambda: None
    monkeypatch.setattr(world_bank.WorldBank, "_fetch_data", fake_fetch)

    wb_instance = world_bank.WorldBank()
    wb_instance.get_data(
        indicator_code=["B", "A"],
        entity_code=["USA", "KEN"],
        start_year=2010,
        end_year=2012,
        skip_blanks=True,
        skip_aggs=True,
        include_labels=True,
        params={"per_page": 100},
        batch_size=3,
        thread_num=4,
        version="MRV",
    )

    assert captured["indicators"] == ("A", "B")
    assert captured["entity_code"] == ("KEN", "USA")
    assert captured["time"] == range(2010, 2013)
    assert captured["params_items"] == (("per_page", 100),)
    assert captured["extra_items"] == (("version", "MRV"),)


def test_world_bank_clear_cache_invokes_underlying_cache(monkeypatch):
    monkeypatch.setattr(world_bank.wb, "db", 2)

    cache_clear = Mock()

    def fake_fetch(self, *args, **kwargs):
        data = pd.DataFrame(
            {
                Fields.year: [2020],
                Fields.indicator_code: ["A"],
                Fields.entity_code: ["KEN"],
                Fields.value: [1],
            }
        )
        return convert_dtypes(data)

    fake_fetch.cache_clear = cache_clear
    monkeypatch.setattr(world_bank.WorldBank, "_fetch_data", fake_fetch)

    wb_instance = world_bank.WorldBank()
    wb_instance.clear_cache()

    cache_clear.assert_called_once_with()


def test_world_bank_implements_data_importer_protocol(monkeypatch):
    monkeypatch.setattr(world_bank.wb, "db", 2)
    importer = world_bank.WorldBank()

    assert isinstance(importer, DataImporter)
    assert callable(getattr(importer, "get_data"))
    assert callable(getattr(importer, "clear_cache"))
