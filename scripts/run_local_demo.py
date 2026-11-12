# Copyright (c) 2026 Agustin Boursiac. Todos los derechos reservados.
# Prohibido copiar, modificar o redistribuir sin permiso escrito del autor. Ver LICENSE.
"""Corre el pipeline completo en local: landing (data/sample) -> bronze -> silver -> gold.

Usa conf/local.yml (Spark + Parquet). Al final imprime el top de oportunidades y las
alertas espejo/precio, que es lo que vería un analista en el dashboard.
"""

from __future__ import annotations

import argparse
import os
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
os.chdir(ROOT)

from pyspark.sql import functions as F  # noqa: E402

from trade_intel.bronze.ingest import SOURCES, ingest  # noqa: E402
from trade_intel.common.audit import audited  # noqa: E402
from trade_intel.common.spark import get_spark  # noqa: E402
from trade_intel.common.table_io import TableIO  # noqa: E402
from trade_intel.config import load_settings  # noqa: E402
from trade_intel.gold.build import build_gold  # noqa: E402
from trade_intel.jobs import silver_build  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--landing", default=None,
                    help="carpeta landing alternativa (ej. data/public_sample bajada con fetch_comtrade_public.py)")
    args = ap.parse_args()
    settings = load_settings("local")
    if args.landing:
        from trade_intel.config import Settings

        raw = {**settings.raw, "storage": {**settings.raw["storage"], "landing_root": args.landing}}
        settings = Settings(env="local", raw=raw)
    shutil.rmtree(ROOT / "_local_lake", ignore_errors=True)
    if not args.landing and not (ROOT / "data/sample/comtrade").exists():
        import subprocess

        subprocess.run([sys.executable, "scripts/generate_sample_data.py"], check=True)

    spark = get_spark("trade-intel-local", settings.raw["storage"]["warehouse_dir"])
    spark.sparkContext.setLogLevel("ERROR")
    io = TableIO(spark, settings)
    io.ensure_schemas()

    for src, spec in SOURCES.items():
        if not (Path(settings.landing_root) / spec.subdir).exists():
            print(f"(sin datos de {src} en {settings.landing_root}, se omite)")
            continue
        with audited(io, f"bronze_{src}") as st:
            st["rows_out"] = ingest(spark, settings, io, src)
    with audited(io, "silver_build") as st:
        dq = silver_build.run(io, settings)
        st["rows_out"] = dq["rows_in"] - dq["rows_quarantined"]
    print("\nCalidad silver.trade_flows:", dq)
    with audited(io, "gold_build") as st:
        counts = build_gold(io, settings)
        st["rows_out"] = sum(counts.values())
    print("Tablas gold:", counts)

    opp = io.read("gold", "fact_export_opportunity")
    last = opp.agg(F.max("year")).first()[0]
    print(f"\nTop 10 oportunidades para ARG ({last})")
    (opp.filter(F.col("year") == last)
        .join(io.read("gold", "dim_country").select(F.col("iso3").alias("market_iso3"), "name_es"), "market_iso3")
        .join(io.read("gold", "dim_product").select("hs6", "description_es"), "hs6")
        .select("name_es", "description_es", F.round(F.col("market_value_usd") / 1e6, 1).alias("mercado_musd"),
                F.round(F.col("cagr") * 100, 1).alias("cagr_pct"), F.round(F.col("home_share") * 100, 1).alias("share_arg_pct"),
                F.round("home_rca", 2).alias("rca_arg"), F.round("hhi", 0).alias("hhi"),
                "strategic_quadrant", "opportunity_score")
        .orderBy(F.desc("opportunity_score")).show(10, truncate=28))

    print("Alertas espejo (ARG exporta vs. socio importa)")
    (io.read("gold", "fact_mirror_gap").filter((F.col("year") == last) & (F.col("mirror_status") == "discrepancia"))
        .select("partner_iso3", "hs6", F.round(F.col("exports_reported_usd") / 1e6, 1).alias("x_arg_musd"),
                F.round(F.col("mirror_imports_usd") / 1e6, 1).alias("m_socio_musd"), F.round("gap_ratio", 2).alias("gap"))
        .show(10))

    print("Outliers de precio unitario")
    (io.read("gold", "fact_unit_value").filter((F.col("year") == last) & F.col("is_price_outlier"))
        .select("market_iso3", "hs6", "supplier_iso3", F.round("unit_value_usd_kg", 2).alias("usd_kg"),
                F.round("median_uv", 2).alias("mediana"), F.round("robust_z", 1).alias("z"))
        .show(10))

    print("Corridas auditadas:")
    io.read("ops", "pipeline_runs").select("task", "status", F.round("duration_s", 1).alias("s"), "rows_out").show()
    spark.stop()


if __name__ == "__main__":
    main()
