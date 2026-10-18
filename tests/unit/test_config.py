from trade_intel.config import load_settings


def test_env_overrides_and_rendering():
    s = load_settings("dev")
    assert s.catalog == "trade_dev"
    assert s.raw["storage"]["landing_root"] == "abfss://landing@sttradeinteldev.dfs.core.windows.net"
    assert s.source("comtrade")["years_back"] == 6
    assert "080550" in s.product_universe


def test_local_has_no_catalog():
    s = load_settings("local")
    assert s.catalog is None and s.storage_format == "parquet"


def test_weights_sum_to_one():
    w = load_settings("prod").gold["opportunity_weights"]
    assert abs(sum(w.values()) - 1.0) < 1e-9
