"""Mantenimiento Delta: OPTIMIZE + VACUUM de tablas gold/silver y ANALYZE para el optimizador.

En prod se puede reemplazar por Predictive Optimization de Unity Catalog (ver ADR 0001).
"""

import argparse

from pyspark.sql import SparkSession

ap = argparse.ArgumentParser()
ap.add_argument("--catalog", required=True)
ap.add_argument("--retain-hours", type=int, default=168)
args = ap.parse_args()
spark = SparkSession.builder.getOrCreate()

for schema in ("silver", "gold"):
    for t in spark.sql(f"SHOW TABLES IN {args.catalog}.{schema}").collect():
        fq = f"{args.catalog}.{schema}.{t.tableName}"
        spark.sql(f"OPTIMIZE {fq}")
        spark.sql(f"VACUUM {fq} RETAIN {args.retain_hours} HOURS")
        spark.sql(f"ANALYZE TABLE {fq} COMPUTE STATISTICS FOR ALL COLUMNS")
        print(f"maintained {fq}")
