"""Landing -> Bronze.

En Databricks usa Auto Loader (cloudFiles) en modo `availableNow`: procesa incrementalmente
sólo archivos nuevos y termina, lo que permite orquestarlo como tarea batch de un Workflow
manteniendo semántica exactly-once vía checkpoint.

En local usa lectura batch de los mismos archivos: la lógica de parseo es idéntica porque
ambos caminos comparten las funciones `parse_*`.

Bronze guarda el dato tal cual vino (tipos string en CSV, struct original en JSON) +
metadata técnica: archivo de origen, fecha de ingesta y `_rescued_data`.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from trade_intel.common.table_io import TableIO
from trade_intel.config import Settings


@dataclass(frozen=True)
class BronzeSource:
    name: str
    subdir: str
    fmt: str
    options: dict
    table: str


SOURCES: dict[str, BronzeSource] = {
    "comtrade": BronzeSource("comtrade", "comtrade", "json", {"multiLine": "true"}, "comtrade_raw"),
    "bcra": BronzeSource("bcra", "bcra", "json", {"multiLine": "true"}, "bcra_fx_raw"),
    "indec_exports": BronzeSource(
        "indec_exports", "indec/exports", "csv",
        {"header": "true", "sep": ";", "encoding": "ISO-8859-1"}, "indec_exports_raw",
    ),
}


def _with_tech_cols(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_file_mod_ts", F.col("_metadata.file_modification_time"))
        .withColumn("_ingest_ts", F.current_timestamp())
    )


def parse_comtrade(df: DataFrame) -> DataFrame:
    """Un registro por fila del array `data`, conservando el request que lo generó."""
    return (
        df.select(
            F.col("_request"),
            F.col("_fetched_at"),
            F.col("_truncated"),
            F.explode_outer("data").alias("r"),
            "_source_file", "_file_mod_ts", "_ingest_ts",
        )
        .select("r.*", "_request", "_fetched_at", "_truncated",
                "_source_file", "_file_mod_ts", "_ingest_ts")
    )


def parse_bcra(df: DataFrame) -> DataFrame:
    return (
        df.select("_request", F.explode("results").alias("res"), "_source_file", "_file_mod_ts", "_ingest_ts")
        .select(
            F.col("res.fecha").alias("fecha"),
            F.explode("res.detalle").alias("d"),
            "_request", "_source_file", "_file_mod_ts", "_ingest_ts",
        )
        .select("fecha", "d.*", "_request", "_source_file", "_file_mod_ts", "_ingest_ts")
    )


def parse_indec(df: DataFrame) -> DataFrame:
    # Normaliza encabezados de INDEC ("Pnet(kg)", "FOB(u$s)", "Año") a snake_case
    rename = {"Año": "anio", "Mes": "mes", "NCM": "ncm", "Pdes": "pdes",
              "Pnet(kg)": "pnet_kg", "FOB(u$s)": "fob_usd"}
    for old, new in rename.items():
        if old in df.columns:
            df = df.withColumnRenamed(old, new)
    return df


PARSERS = {"comtrade": parse_comtrade, "bcra": parse_bcra, "indec_exports": parse_indec}


def read_landing_batch(spark: SparkSession, settings: Settings, src: BronzeSource) -> DataFrame:
    path = f"{settings.landing_root}/{src.subdir}"
    reader = spark.read.format(src.fmt).options(**src.options)
    if src.fmt == "json":
        reader = reader.option("recursiveFileLookup", "true")
    else:
        reader = reader.option("recursiveFileLookup", "true").option("inferSchema", "false")
    return _with_tech_cols(reader.load(path))


def run_autoloader(spark: SparkSession, settings: Settings, io: TableIO, src: BronzeSource) -> None:
    """Camino productivo: Auto Loader incremental con schema evolution y rescued data."""
    path = f"{settings.landing_root}/{src.subdir}"
    ckpt = f"{settings.checkpoint_root}/bronze/{src.table}"
    stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", src.fmt)
        .option("cloudFiles.schemaLocation", f"{ckpt}/_schema")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.inferColumnTypes", "false")
        .option("rescuedDataColumn", "_rescued_data")
        .options(**src.options)
        .load(path)
    )
    parsed = PARSERS[src.name](_with_tech_cols(stream))
    (
        parsed.writeStream.option("checkpointLocation", ckpt)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(io.name("bronze", src.table))
        .awaitTermination()
    )


def ingest(spark: SparkSession, settings: Settings, io: TableIO, source: str) -> int:
    src = SOURCES[source]
    if settings.storage_format == "delta":
        run_autoloader(spark, settings, io, src)
        return io.read("bronze", src.table).count()
    df = PARSERS[source](read_landing_batch(spark, settings, src))
    io.write(df, "bronze", src.table, mode="overwrite")
    return df.count()
