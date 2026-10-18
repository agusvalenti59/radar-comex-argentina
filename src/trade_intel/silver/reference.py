"""Dimensiones de referencia (seeds versionados en data/reference).

- countries.csv: M49 (Comtrade) <-> ISO3 <-> código de país INDEC, región y bloque comercial.
- products.csv: HS6 del universo del radar con descripción en español y sector.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def load_countries(spark: SparkSession, path: str) -> DataFrame:
    return (
        spark.read.option("header", "true").csv(path)
        .select(
            F.col("m49").cast("int").alias("m49"),
            F.upper("iso3").alias("iso3"),
            F.col("name_es"),
            F.col("region"),
            F.col("trade_bloc"),
            F.col("indec_code").cast("int").alias("indec_code"),
        )
    )


def load_products(spark: SparkSession, path: str) -> DataFrame:
    return (
        spark.read.option("header", "true").option("dtype", "str").csv(path)
        .select(
            F.lpad("hs6", 6, "0").alias("hs6"),
            F.substring(F.lpad("hs6", 6, "0"), 1, 2).alias("hs2"),
            F.substring(F.lpad("hs6", 6, "0"), 1, 4).alias("hs4"),
            "description_es",
            "sector",
        )
    )
