"""Tarea: bronze -> silver (referencias, flujos unificados, FX) con calidad y cuarentena."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import functions as F

from trade_intel.common.audit import audited
from trade_intel.common.logging import get_logger
from trade_intel.jobs._common import bootstrap
from trade_intel.quality.expectations import FX_RULES, TRADE_FLOW_RULES, split_valid_invalid
from trade_intel.silver import fx_rates, reference, trade_flows

log = get_logger(__name__)
REF_DIR = Path(__file__).resolve().parents[3] / "data" / "reference"


def _write_dq(io, table: str, metrics: dict) -> None:
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    rows = [(table, k, int(v), now) for k, v in metrics.items()]
    df = io.spark.createDataFrame(rows, "table string, metric string, value long, measured_at timestamp")
    io.write(df, "ops", "dq_metrics", mode="append")


def run(io, settings, ref_dir: str | None = None) -> dict:
    spark = io.spark
    ref = ref_dir or settings.raw.get("reference_dir") or str(REF_DIR)
    countries = reference.load_countries(spark, f"{ref}/countries.csv").cache()
    products = reference.load_products(spark, f"{ref}/products.csv")
    io.write(countries, "silver", "dim_country")
    io.write(products, "silver", "dim_product")

    parts = []
    if io.exists("bronze", "comtrade_raw"):
        parts.append(trade_flows.normalize_comtrade(io.read("bronze", "comtrade_raw"), countries))
    if io.exists("bronze", "indec_exports_raw"):
        parts.append(trade_flows.normalize_indec_exports(io.read("bronze", "indec_exports_raw"), countries))
    if not parts:
        raise RuntimeError("No hay datos en bronze (comtrade_raw ni indec_exports_raw). Corré antes la ingesta y bronze.")
    flows = parts[0]
    for p in parts[1:]:
        flows = flows.unionByName(p)

    valid, invalid, metrics = split_valid_invalid(flows, TRADE_FLOW_RULES)
    io.upsert(valid, "silver", "trade_flows", keys=trade_flows.KEY, partition_by=["source", "year"])
    io.write(invalid.withColumn("_quarantined_at", F.current_timestamp()), "ops", "quarantine_trade_flows", mode="append")
    _write_dq(io, "silver.trade_flows", metrics)
    log.info("silver trade_flows", extra={"ctx": {"dq": json.dumps(metrics)}})

    if io.exists("bronze", "bcra_fx_raw"):
        fx = fx_rates.normalize_fx(io.read("bronze", "bcra_fx_raw"))
        fx_valid, _, fx_metrics = split_valid_invalid(fx, FX_RULES)
        io.upsert(fx_valid, "silver", "fx_daily", keys=["rate_date", "currency"])
        io.write(fx_rates.monthly_average(io.read("silver", "fx_daily")), "silver", "fx_monthly")
        _write_dq(io, "silver.fx_daily", fx_metrics)
    return metrics


def main() -> None:
    ns, settings, spark, io = bootstrap("Bronze -> Silver")
    with audited(io, "silver_build") as stats:
        metrics = run(io, settings)
        stats["rows_out"] = metrics["rows_in"] - metrics["rows_quarantined"]


if __name__ == "__main__":
    main()
