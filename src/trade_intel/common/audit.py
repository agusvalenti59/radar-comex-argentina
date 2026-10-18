"""Auditoría de corridas: una fila por tarea en ops.pipeline_runs (lineage operativo + SLAs)."""

from __future__ import annotations

import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone

from pyspark.sql import types as T

from trade_intel.common.logging import get_logger
from trade_intel.common.table_io import TableIO

log = get_logger(__name__)

RUN_SCHEMA = T.StructType(
    [
        T.StructField("run_id", T.StringType()),
        T.StructField("task", T.StringType()),
        T.StructField("status", T.StringType()),
        T.StructField("started_at", T.TimestampType()),
        T.StructField("duration_s", T.DoubleType()),
        T.StructField("rows_out", T.LongType()),
        T.StructField("error", T.StringType()),
    ]
)


@contextmanager
def audited(io: TableIO, task: str, run_id: str | None = None):
    run_id = run_id or str(uuid.uuid4())
    started = datetime.now(timezone.utc).replace(tzinfo=None)
    t0 = time.monotonic()
    stats = {"rows_out": None}
    status, error = "SUCCESS", None
    try:
        yield stats
    except Exception as exc:  # se re-lanza: el Workflow marca la tarea como fallida
        status, error = "FAILED", repr(exc)[:2000]
        raise
    finally:
        row = (run_id, task, status, started, time.monotonic() - t0, stats["rows_out"], error)
        df = io.spark.createDataFrame([row], RUN_SCHEMA)
        io.write(df, "ops", "pipeline_runs", mode="append")
        log.info("task finished", extra={"ctx": {"task": task, "status": status, "run_id": run_id}})
