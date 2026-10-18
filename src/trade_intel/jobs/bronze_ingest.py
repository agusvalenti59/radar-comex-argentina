"""Tarea: landing -> bronze para una fuente (Auto Loader availableNow)."""

from trade_intel.bronze.ingest import SOURCES, ingest
from trade_intel.common.audit import audited
from trade_intel.jobs._common import bootstrap


def main() -> None:
    ns, settings, spark, io = bootstrap(
        "Landing -> Bronze", [(("--source",), {"required": True, "choices": sorted(SOURCES)})]
    )
    with audited(io, f"bronze_{ns.source}") as stats:
        stats["rows_out"] = ingest(spark, settings, io, ns.source)


if __name__ == "__main__":
    main()
