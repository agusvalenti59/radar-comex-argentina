"""Tarea: silver -> gold (modelo dimensional + métricas de inteligencia comercial)."""

from trade_intel.common.audit import audited
from trade_intel.common.logging import get_logger
from trade_intel.gold.build import build_gold
from trade_intel.jobs._common import bootstrap

log = get_logger(__name__)


def main() -> None:
    ns, settings, spark, io = bootstrap("Silver -> Gold")
    with audited(io, "gold_build") as stats:
        counts = build_gold(io, settings)
        stats["rows_out"] = sum(counts.values())
        log.info("gold built", extra={"ctx": counts})


if __name__ == "__main__":
    main()
