# Copyright (c) 2026 Agustin Boursiac. Todos los derechos reservados.
# Prohibido copiar, modificar o redistribuir sin permiso escrito del autor. Ver LICENSE.
"""Creación de SparkSession: en Databricks reutiliza la sesión activa, en local arma una."""

from __future__ import annotations

from pyspark.sql import SparkSession


def get_spark(app_name: str = "trade-intel", warehouse_dir: str | None = None) -> SparkSession:
    active = SparkSession.getActiveSession()
    if active is not None:
        return active
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    )
    if warehouse_dir:
        builder = builder.config("spark.sql.warehouse.dir", warehouse_dir)
    return builder.getOrCreate()
