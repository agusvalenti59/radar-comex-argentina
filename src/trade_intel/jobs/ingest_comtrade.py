"""Tarea: extraer UN Comtrade -> landing. Idempotente: re-escribe la misma clave por partición.

Elige la API según `sources.comtrade.api_mode`:
- `keyed`: lee la clave del secret scope (falla si no está).
- `public`: API pública sin clave (tope 500 filas, un pedido por producto).
- `auto` (default): usa la clave si existe y no es el placeholder; si no, la pública.
"""

from __future__ import annotations

from datetime import date

from trade_intel.common.audit import audited
from trade_intel.common.logging import get_logger
from trade_intel.ingestion.comtrade import ComtradeClient, plan_requests, to_landing_bytes
from trade_intel.ingestion.landing import write_landing
from trade_intel.jobs._common import bootstrap, get_dbutils

log = get_logger(__name__)
PLACEHOLDER = "CHANGE_ME"


def resolve_key(mode: str, read_secret) -> str | None:
    """Devuelve la clave a usar o None (=API pública) según el modo configurado."""
    if mode == "public":
        return None
    try:
        key = read_secret()
    except Exception as exc:  # secreto inexistente o scope sin permisos
        if mode == "keyed":
            raise
        log.warning("comtrade key not available, using public API", extra={"ctx": {"reason": repr(exc)[:200]}})
        return None
    if not key or key == PLACEHOLDER:
        if mode == "keyed":
            raise RuntimeError("api_mode=keyed pero el secreto de Comtrade está vacío o es el placeholder")
        log.warning("comtrade key is placeholder, using public API")
        return None
    return key


def build_client(cfg: dict, key: str | None) -> ComtradeClient:
    rpm = cfg["requests_per_minute"] if key else cfg.get("public_requests_per_minute", 12)
    return ComtradeClient(cfg["base_url"], key, rpm, cfg["max_records"],
                          public_base_url=cfg.get("public_base_url"))


def main() -> None:
    ns, settings, spark, io = bootstrap(
        "Ingesta UN Comtrade",
        [
            (("--year",), {"type": int, "default": None, "help": "Un año puntual (backfill)"}),
            (("--api-mode",), {"choices": ["auto", "keyed", "public"], "default": None}),
        ],
    )
    cfg = settings.source("comtrade")
    mode = ns.api_mode or cfg.get("api_mode", "auto")
    dbutils = get_dbutils(spark)
    secrets = settings.raw["secrets"]
    key = resolve_key(mode, lambda: dbutils.secrets.get(secrets["scope"], secrets["comtrade_key"]))
    client = build_client(cfg, key)

    last = date.today().year - 1  # Comtrade anual tiene ~6-12 meses de rezago
    years = [ns.year] if ns.year else list(range(last - cfg["years_back"] + 1, last + 1))
    plan = list(plan_requests(cfg["reporters_m49"], years, settings.product_universe,
                              split_by_product=client.split_by_product))
    log.info("comtrade plan", extra={"ctx": {"mode": client.mode, "requests": len(plan), "years": years}})
    with audited(io, f"ingest_comtrade_{client.mode}") as stats:
        n = truncated = 0
        for req in plan:
            envelope = client.fetch(req)
            write_landing(settings.landing_root, req.landing_key, to_landing_bytes(envelope), dbutils)
            n += envelope["count"]
            truncated += envelope["_truncated"]
        stats["rows_out"] = n
        if truncated:
            log.warning("truncated responses", extra={"ctx": {"count": truncated}})


if __name__ == "__main__":
    main()
