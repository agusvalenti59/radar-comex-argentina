# Copyright (c) 2026 Agustin Valenti Boursiac. Todos los derechos reservados.
# Prohibido copiar, modificar o redistribuir sin permiso escrito del autor. Ver LICENSE.
"""Silver -> Gold: modelo dimensional para consumo (Power BI / Databricks SQL)."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from trade_intel.common.table_io import TableIO
from trade_intel.config import Settings
from trade_intel.gold import metrics as m


def build_dim_date(spark, start_year: int, end_year: int) -> DataFrame:
    return (
        spark.sql(
            f"SELECT explode(sequence(to_date('{start_year}-01-01'), to_date('{end_year}-12-01'), interval 1 month)) AS period_date"
        )
        .withColumn("date_key", F.date_format("period_date", "yyyyMM").cast("int"))
        .withColumn("year", F.year("period_date"))
        .withColumn("month", F.month("period_date"))
        .withColumn("quarter", F.quarter("period_date"))
        .withColumn("semester", F.when(F.col("month") <= 6, 1).otherwise(2))
        # Campaña agrícola argentina (julio-junio), clave para granos y oleaginosas
        .withColumn(
            "crop_year",
            F.when(F.col("month") >= 7, F.concat_ws("/", F.col("year"), (F.col("year") + 1) % 100))
            .otherwise(F.concat_ws("/", F.col("year") - 1, F.col("year") % 100)),
        )
    )


def ar_exports_monthly(silver: DataFrame, fx_monthly: DataFrame) -> DataFrame:
    """Exportaciones mensuales de ARG (INDEC) con conversión a pesos al tipo de cambio promedio."""
    usd = fx_monthly.filter(F.col("currency") == "USD").select("period_date", F.col("avg_rate").alias("usd_ars"))
    return (
        silver.filter(F.col("source") == "indec")
        .select("period_date", "year", "month", "partner_iso3", "hs6",
                F.col("trade_value_usd").alias("fob_usd"), "net_weight_kg", "unit_value_usd_kg")
        .join(usd, "period_date", "left")
        .withColumn("fob_ars", F.col("fob_usd") * F.col("usd_ars"))
        .withColumn("date_key", F.date_format("period_date", "yyyyMM").cast("int"))
    )


def build_gold(io: TableIO, settings: Settings) -> dict[str, int]:
    g = settings.gold
    home = settings.home_iso3
    silver = io.read("silver", "trade_flows")
    flows = m.annual_flows(silver, "comtrade").cache()

    imports = m.market_imports(flows)
    shares = m.supplier_shares(imports).cache()
    conc = m.concentration(shares).cache()
    growth = m.market_growth(conc, g["cagr_years"])
    rca = m.revealed_comparative_advantage(flows).cache()
    uv = m.unit_value_benchmark(imports, home, g["unit_value_outlier_z"], g["unit_value_min_ratio"]).cache()
    mirror = m.mirror_gap(flows, home, g["mirror_gap"]["cif_fob_factor"], g["mirror_gap"]["tolerance"])
    opp = m.opportunity_score(conc, growth, shares, rca, uv, home, g["opportunity_weights"], g["min_market_size_usd"])

    years = [r["year"] for r in flows.select("year").distinct().collect()]
    outputs: dict[str, DataFrame] = {
        "dim_country": io.read("silver", "dim_country"),
        "dim_product": io.read("silver", "dim_product"),
        "dim_date": build_dim_date(io.spark, min(years), max(years) + 1),
        "fact_trade_annual": flows,
        "fact_supplier_share": shares,
        "fact_market_concentration": conc.join(growth, ["year", "market_iso3", "hs6"], "left"),
        "fact_rca": rca,
        "fact_unit_value": uv,
        "fact_mirror_gap": mirror,
        "fact_export_opportunity": opp,
    }
    if io.exists("silver", "fx_monthly"):
        outputs["fact_ar_exports_monthly"] = ar_exports_monthly(silver, io.read("silver", "fx_monthly"))

    counts = {}
    for table, df in outputs.items():
        part = ["year"] if "year" in df.columns and table.startswith("fact_") and table != "fact_ar_exports_monthly" else None
        io.write(df, "gold", table, partition_by=part)
        counts[table] = io.read("gold", table).count()
    return counts
