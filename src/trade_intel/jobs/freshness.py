# Copyright (c) 2026 Agustin Boursiac. Todos los derechos reservados.
# Prohibido copiar, modificar o redistribuir sin permiso escrito del autor. Ver LICENSE.
"""Tarea: control de frescura (SLA) por tabla. Falla el job si una fuente quedó desactualizada.

Comtrade tiene rezago estructural (los países reportan con meses de demora), por eso cada
tabla tiene su propio SLA en horas desde la última ingesta.
"""

from __future__ import annotations

from datetime import datetime, timezone

from pyspark.sql import functions as F

from trade_intel.common.logging import get_logger
from trade_intel.jobs._common import bootstrap

log = get_logger(__name__)

SLA_HOURS = {
    ("bronze", "bcra_fx_raw"): 72,          # fines de semana + feriados
    ("bronze", "indec_exports_raw"): 24 * 40,
    ("bronze", "comtrade_raw"): 24 * 10,
}


def check(io) -> list[dict]:
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    results = []
    for (layer, table), sla in SLA_HOURS.items():
        if not io.exists(layer, table):
            results.append({"table": table, "status": "MISSING", "age_h": None, "sla_h": sla})
            continue
        last = io.read(layer, table).agg(F.max("_ingest_ts")).first()[0]
        age = (now - last).total_seconds() / 3600 if last else None
        status = "OK" if age is not None and age <= sla else "STALE"
        results.append({"table": table, "status": status, "age_h": round(age or -1, 1), "sla_h": sla})
    return results


def main() -> None:
    _, _, spark, io = bootstrap("Freshness SLA")
    results = check(io)
    io.write(spark.createDataFrame(results).withColumn("checked_at", F.current_timestamp()), "ops", "freshness", mode="append")
    bad = [r for r in results if r["status"] != "OK"]
    log.info("freshness", extra={"ctx": {"results": results}})
    if bad:
        raise RuntimeError(f"SLA de frescura incumplido: {bad}")


if __name__ == "__main__":
    main()
