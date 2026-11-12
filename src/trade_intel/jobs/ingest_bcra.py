# Copyright (c) 2026 Agustin Boursiac. Todos los derechos reservados.
# Prohibido copiar, modificar o redistribuir sin permiso escrito del autor. Ver LICENSE.
"""Tarea: cotizaciones BCRA -> landing (ventana móvil de 2 años, re-escritura idempotente)."""

from __future__ import annotations

import json
from datetime import date

from trade_intel.common.audit import audited
from trade_intel.ingestion.bcra import BcraClient, year_windows
from trade_intel.ingestion.landing import write_landing
from trade_intel.jobs._common import bootstrap, get_dbutils


def main() -> None:
    ns, settings, spark, io = bootstrap("Ingesta BCRA")
    cfg = settings.source("bcra")
    client = BcraClient(cfg["base_url"])
    dbutils = get_dbutils(spark)
    today = date.today()
    with audited(io, "ingest_bcra") as stats:
        n = 0
        for cur in cfg["currencies"]:
            for start, end in year_windows(date(today.year - 1, 1, 1), today):
                env = client.fetch_quotes(cur, start, end)
                key = f"bcra/currency={cur}/year={start.year}/quotes.json"
                write_landing(settings.landing_root, key, json.dumps(env).encode(), dbutils)
                n += len(env["results"])
        stats["rows_out"] = n


if __name__ == "__main__":
    main()
