# Copyright (c) 2026 Agustin Valenti Boursiac. Todos los derechos reservados.
# Prohibido copiar, modificar o redistribuir sin permiso escrito del autor. Ver LICENSE.
"""Mantenimiento Delta: OPTIMIZE + VACUUM de tablas gold/silver y ANALYZE para el optimizador.

En prod se puede reemplazar por Predictive Optimization de Unity Catalog (ver ADR 0001).
"""

import argparse
import os

from pyspark.sql import SparkSession

ap = argparse.ArgumentParser()
ap.add_argument("--catalog", default=f"trade_{os.environ.get('TRADE_INTEL_ENV', 'dev')}")
ap.add_argument("--retain-hours", type=int, default=168)
args, _ = ap.parse_known_args()  # tolera --python-params pensados para otras tareas
spark = SparkSession.builder.getOrCreate()

for schema in ("silver", "gold"):
    for t in spark.sql(f"SHOW TABLES IN {args.catalog}.{schema}").collect():
        fq = f"{args.catalog}.{schema}.{t.tableName}"
        spark.sql(f"OPTIMIZE {fq}")
        spark.sql(f"VACUUM {fq} RETAIN {args.retain_hours} HOURS")
        spark.sql(f"ANALYZE TABLE {fq} COMPUTE STATISTICS FOR ALL COLUMNS")
        print(f"maintained {fq}")
