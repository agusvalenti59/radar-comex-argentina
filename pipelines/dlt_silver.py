# Copyright (c) 2026 Agustin Valenti Boursiac. Todos los derechos reservados.
# Prohibido copiar, modificar o redistribuir sin permiso escrito del autor. Ver LICENSE.
"""Lakeflow Declarative Pipeline (DLT): landing -> bronze -> silver para Comtrade.

Mismas reglas (`TRADE_FLOW_RULES`) y mismas funciones de normalización que el camino Jobs:
la lógica de negocio vive en el paquete y es testeable sin DLT.
"""

import dlt  # type: ignore  # disponible sólo dentro de un pipeline
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from trade_intel.bronze.ingest import parse_comtrade
from trade_intel.quality.expectations import TRADE_FLOW_RULES
from trade_intel.silver.reference import load_countries
from trade_intel.silver.trade_flows import normalize_comtrade

spark = SparkSession.getActiveSession()
LANDING = spark.conf.get("trade_intel.landing_root")
REF_DIR = spark.conf.get("trade_intel.reference_dir")

DROP_RULES = {r.name: r.expr for r in TRADE_FLOW_RULES if r.severity == "drop"}
WARN_RULES = {r.name: r.expr for r in TRADE_FLOW_RULES if r.severity == "warn"}
FAIL_RULES = {r.name: r.expr for r in TRADE_FLOW_RULES if r.severity == "fail"}


@dlt.table(name="comtrade_bronze", comment="Comtrade crudo, un registro por fila de la API",
           table_properties={"quality": "bronze"})
def comtrade_bronze():
    raw = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("multiLine", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(f"{LANDING}/comtrade")
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_file_mod_ts", F.col("_metadata.file_modification_time"))
        .withColumn("_ingest_ts", F.current_timestamp())
    )
    return parse_comtrade(raw)


@dlt.view(name="dim_country_v")
def dim_country_v():
    return load_countries(spark, f"{REF_DIR}/countries.csv")


@dlt.view(name="comtrade_normalized")
@dlt.expect_all(WARN_RULES)
@dlt.expect_all_or_drop(DROP_RULES)
@dlt.expect_all_or_fail(FAIL_RULES)
def comtrade_normalized():
    return normalize_comtrade(dlt.read_stream("comtrade_bronze"), dlt.read("dim_country_v"))


dlt.create_streaming_table(
    name="trade_flows",
    comment="Flujos comerciales unificados (SCD1 por clave de negocio)",
    table_properties={"quality": "silver", "delta.enableChangeDataFeed": "true"},
    cluster_by=["hs6", "reporter_iso3"],
)

dlt.apply_changes(
    target="trade_flows",
    source="comtrade_normalized",
    keys=["source", "freq", "flow", "period_date", "reporter_iso3", "partner_iso3", "hs6"],
    sequence_by=F.col("_ingest_ts"),
    stored_as_scd_type=1,
)
