# Copyright (c) 2026 Agustin Boursiac. Todos los derechos reservados.
# Prohibido copiar, modificar o redistribuir sin permiso escrito del autor. Ver LICENSE.
"""Bronze -> Silver: modelo unificado de flujos comerciales.

Unifica dos fuentes con semánticas distintas en un solo contrato:

| fuente   | granularidad          | país         | producto | valor            |
|----------|-----------------------|--------------|----------|------------------|
| Comtrade | anual, bilateral      | M49          | HS6      | primaryValue USD |
| INDEC    | mensual, sólo ARG     | código INDEC | NCM 8d   | FOB USD          |

Decisiones:
- NCM (Mercosur) a HS6 = primeros 6 dígitos (NCM es una extensión del SA a 8 dígitos).
- "World" (M49 = 0) se mapea a ISO3 'WLD' y se conserva: lo usa el RCA como denominador.
- Re-exportaciones/re-importaciones (RX/RM) se descartan del modelo analítico.
- Se deduplica por clave de negocio quedándose con la última ingesta (re-descargas de la API).
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

KEY = ["source", "freq", "flow", "period_date", "reporter_iso3", "partner_iso3", "hs6"]

SILVER_COLUMNS = [
    "source", "freq", "flow", "period_date", "year", "month",
    "reporter_iso3", "partner_iso3", "hs6",
    "trade_value_usd", "fob_value_usd", "cif_value_usd",
    "net_weight_kg", "qty", "qty_unit_code", "unit_value_usd_kg",
    "_ingest_ts",
]


def _to_double(col: str) -> F.Column:
    # Soporta "1234.5", "1.234,5" y "1234,5" (formatos presentes en archivos INDEC)
    c = F.trim(F.col(col).cast("string"))
    c = F.when(c.contains(",") & c.contains("."), F.regexp_replace(F.regexp_replace(c, r"\.", ""), ",", ".")) \
         .otherwise(F.regexp_replace(c, ",", "."))
    return c.cast(T.DoubleType())


def normalize_comtrade(bronze: DataFrame, countries: DataFrame) -> DataFrame:
    c_rep = countries.select(F.col("m49").alias("_rep_m49"), F.col("iso3").alias("reporter_iso3"))
    c_par = countries.select(F.col("m49").alias("_par_m49"), F.col("iso3").alias("partner_iso3"))

    df = (
        bronze.filter(F.col("flowCode").isin("X", "M"))
        .filter(F.coalesce(F.col("partner2Code").cast("int"), F.lit(0)) == 0)
        .filter(F.coalesce(F.col("customsCode"), F.lit("C00")) == "C00")
        .filter(F.coalesce(F.col("motCode").cast("int"), F.lit(0)) == 0)
        .select(
            F.lit("comtrade").alias("source"),
            F.col("freqCode").alias("freq"),
            F.col("flowCode").alias("flow"),
            F.col("refYear").cast("int").alias("year"),
            F.when(F.col("freqCode") == "M", F.col("refMonth").cast("int")).alias("month"),
            F.col("reporterCode").cast("int").alias("_rep_m49"),
            F.col("partnerCode").cast("int").alias("_par_m49"),
            F.when(F.upper("cmdCode") == "TOTAL", F.lit("TOTAL"))
             .otherwise(F.lpad(F.col("cmdCode").cast("string"), 6, "0")).alias("hs6"),
            F.col("primaryValue").cast("double").alias("trade_value_usd"),
            F.col("fobvalue").cast("double").alias("fob_value_usd"),
            F.col("cifvalue").cast("double").alias("cif_value_usd"),
            F.col("netWgt").cast("double").alias("net_weight_kg"),
            F.col("qty").cast("double").alias("qty"),
            F.col("qtyUnitCode").cast("int").alias("qty_unit_code"),
            F.col("_ingest_ts"),
        )
        .join(F.broadcast(c_rep), "_rep_m49", "left")
        .join(F.broadcast(c_par), "_par_m49", "left")
    )
    return _finalize(df)


def normalize_indec_exports(bronze: DataFrame, countries: DataFrame) -> DataFrame:
    c_par = countries.filter(F.col("indec_code").isNotNull()).select(
        F.col("indec_code").alias("_pdes"), F.col("iso3").alias("partner_iso3")
    )
    df = (
        bronze.select(
            F.col("anio").cast("int").alias("year"),
            F.col("mes").cast("int").alias("month"),
            F.substring(F.regexp_replace("ncm", r"[^0-9]", ""), 1, 6).alias("hs6"),
            F.col("pdes").cast("int").alias("_pdes"),
            _to_double("pnet_kg").alias("net_weight_kg"),
            _to_double("fob_usd").alias("fob_value_usd"),
            "_ingest_ts",
        )
        .join(F.broadcast(c_par), "_pdes", "left")
        # NCM 8d -> HS6: varias posiciones NCM colapsan en una HS6
        .groupBy("year", "month", "hs6", "partner_iso3")
        .agg(
            F.sum("fob_value_usd").alias("fob_value_usd"),
            F.sum("net_weight_kg").alias("net_weight_kg"),
            F.max("_ingest_ts").alias("_ingest_ts"),
        )
        .select(
            F.lit("indec").alias("source"),
            F.lit("M").alias("freq"),
            F.lit("X").alias("flow"),
            "year", "month",
            F.lit("ARG").alias("reporter_iso3"),
            "partner_iso3", "hs6",
            F.col("fob_value_usd").alias("trade_value_usd"),
            "fob_value_usd",
            F.lit(None).cast("double").alias("cif_value_usd"),
            "net_weight_kg",
            F.lit(None).cast("double").alias("qty"),
            F.lit(None).cast("int").alias("qty_unit_code"),
            "_ingest_ts",
        )
    )
    return _finalize(df)


def _finalize(df: DataFrame) -> DataFrame:
    df = (
        df.withColumn("period_date", F.make_date("year", F.coalesce("month", F.lit(1)), F.lit(1)))
        .withColumn(
            "unit_value_usd_kg",
            F.when(F.col("net_weight_kg") > 0, F.col("trade_value_usd") / F.col("net_weight_kg")),
        )
    )
    return df.select(*SILVER_COLUMNS)
