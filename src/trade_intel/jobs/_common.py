# Copyright (c) 2026 Agustin Boursiac. Todos los derechos reservados.
# Prohibido copiar, modificar o redistribuir sin permiso escrito del autor. Ver LICENSE.
from __future__ import annotations

import argparse

from trade_intel.common.spark import get_spark
from trade_intel.common.table_io import TableIO
from trade_intel.config import load_settings


def bootstrap(description: str, extra_args=None):
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--env", default=None, help="dev | prod | local")
    for args, kwargs in extra_args or []:
        parser.add_argument(*args, **kwargs)
    # parse_known_args: `bundle run --python-params` manda los mismos parámetros a todas las tareas
    ns, _ = parser.parse_known_args()
    settings = load_settings(ns.env)
    spark = get_spark(warehouse_dir=settings.raw["storage"].get("warehouse_dir"))
    io = TableIO(spark, settings)
    io.ensure_schemas()
    return ns, settings, spark, io


def get_dbutils(spark):
    try:
        from pyspark.dbutils import DBUtils  # type: ignore

        return DBUtils(spark)
    except ImportError:
        return None
