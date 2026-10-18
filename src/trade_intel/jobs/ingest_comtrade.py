"""Tarea: extraer UN Comtrade -> landing. Idempotente: re-escribe la misma clave por partición."""

from __future__ import annotations

from datetime import date

from trade_intel.common.audit import audited
from trade_intel.common.logging import get_logger
from trade_intel.ingestion.comtrade import ComtradeClient, plan_requests, to_landing_bytes
from trade_intel.ingestion.landing import write_landing
from trade_intel.jobs._common import bootstrap, get_dbutils

log = get_logger(__name__)


def main() -> None:
    ns, settings, spark, io = bootstrap(
        "Ingesta UN Comtrade",
        [(("--year",), {"type": int, "default": None, "help": "Un año puntual (backfill)"})],
    )
    cfg = settings.source("comtrade")
    dbutils = get_dbutils(spark)
    key = dbutils.secrets.get(settings.raw["secrets"]["scope"], settings.raw["secrets"]["comtrade_key"])
    client = ComtradeClient(cfg["base_url"], key, cfg["requests_per_minute"], cfg["max_records"])

    last = date.today().year - 1  # Comtrade anual tiene ~6-12 meses de rezago
    years = [ns.year] if ns.year else list(range(last - cfg["years_back"] + 1, last + 1))
    with audited(io, "ingest_comtrade") as stats:
        n = 0
        for req in plan_requests(cfg["reporters_m49"], years, settings.product_universe):
            envelope = client.fetch(req)
            write_landing(settings.landing_root, req.landing_key, to_landing_bytes(envelope), dbutils)
            n += envelope["count"]
        stats["rows_out"] = n


if __name__ == "__main__":
    main()
