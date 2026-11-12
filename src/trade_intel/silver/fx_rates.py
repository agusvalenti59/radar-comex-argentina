# Copyright (c) 2026 Agustin Valenti Boursiac. Todos los derechos reservados.
# Prohibido copiar, modificar o redistribuir sin permiso escrito del autor. Ver LICENSE.
"""Bronze -> Silver: cotizaciones BCRA (moneda/ARS) diarias y promedio mensual."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def normalize_fx(bronze: DataFrame) -> DataFrame:
    return bronze.select(
        F.to_date("fecha").alias("rate_date"),
        F.upper("codigoMoneda").alias("currency"),
        F.col("tipoCotizacion").cast("double").alias("rate"),
        F.col("tipoPase").cast("double").alias("usd_parity"),
        "_ingest_ts",
    )


def monthly_average(fx: DataFrame) -> DataFrame:
    return (
        fx.withColumn("period_date", F.trunc("rate_date", "month"))
        .groupBy("period_date", "currency")
        .agg(
            F.avg("rate").alias("avg_rate"),
            F.max_by("rate", "rate_date").alias("eom_rate"),
            F.count("*").alias("n_days"),
        )
    )
