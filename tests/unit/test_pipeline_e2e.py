"""Integración: corre bronze -> silver -> gold sobre la muestra y verifica los casos inyectados."""

import shutil
import subprocess
import sys
from pathlib import Path

import pytest
from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module")
def lake(spark, tmp_path_factory):
    from trade_intel.bronze.ingest import SOURCES, ingest
    from trade_intel.common.table_io import TableIO
    from trade_intel.config import Settings, load_settings
    from trade_intel.gold.build import build_gold
    from trade_intel.jobs import silver_build

    sample = tmp_path_factory.mktemp("sample")
    subprocess.run([sys.executable, str(ROOT / "scripts/generate_sample_data.py"), "--out", str(sample)], check=True)
    base = load_settings("local")
    raw = {**base.raw, "storage": {**base.raw["storage"], "landing_root": str(sample)},
           "reference_dir": str(ROOT / "data/reference")}
    settings = Settings(env="local", raw=raw)
    io = TableIO(spark, settings)
    io.ensure_schemas()
    for src in SOURCES:
        ingest(spark, settings, io, src)
    silver_build.run(io, settings)
    silver_build.run(io, settings)  # re-proceso: debe ser idempotente
    build_gold(io, settings)
    yield io
    shutil.rmtree(sample, ignore_errors=True)


def test_silver_is_idempotent(lake):
    df = lake.read("silver", "trade_flows")
    from trade_intel.silver.trade_flows import KEY
    assert df.count() == df.select(*KEY).distinct().count()


def test_bad_rows_quarantined(lake):
    assert lake.read("ops", "quarantine_trade_flows").count() >= 2


def test_injected_mirror_discrepancy_found(lake):
    hit = lake.read("gold", "fact_mirror_gap").filter(
        (F.col("partner_iso3") == "NLD") & (F.col("hs6") == "080550") & (F.col("year") == 2024)
    ).first()
    assert hit["mirror_status"] == "discrepancia" and hit["gap_ratio"] > 0.4


def test_injected_price_outlier_found(lake):
    hit = lake.read("gold", "fact_unit_value").filter(
        (F.col("market_iso3") == "CHN") & (F.col("hs6") == "220421") & (F.col("supplier_iso3") == "FRA")
    )
    assert hit.filter("is_price_outlier").count() == hit.count() > 0


def test_monthly_exports_converted_to_ars(lake):
    m = lake.read("gold", "fact_ar_exports_monthly")
    assert m.filter("fob_ars IS NULL").count() == 0
    assert m.filter("fob_ars <= fob_usd").count() == 0
