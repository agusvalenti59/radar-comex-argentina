# Copyright (c) 2026 Agustin Boursiac. Todos los derechos reservados.
# Prohibido copiar, modificar o redistribuir sin permiso escrito del autor. Ver LICENSE.
"""Reglas de calidad declarativas.

Cada regla es una expresión SQL que debe ser verdadera. La misma definición se usa en:
- Jobs PySpark: `split_valid_invalid` separa filas válidas y manda las inválidas a
  `ops.quarantine_<tabla>` con la lista de reglas que fallaron (nada se pierde en silencio).
- Lakeflow Declarative Pipelines (DLT): `pipelines/dlt_silver.py` las pasa a `@dlt.expect_all`.

Severidad:
- `drop`: la fila va a cuarentena.
- `warn`: se registra la métrica pero la fila sigue.
- `fail`: si alguna fila la viola, la tarea falla (protege contratos críticos).
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


@dataclass(frozen=True)
class Rule:
    name: str
    expr: str
    severity: str = "drop"  # drop | warn | fail


TRADE_FLOW_RULES: list[Rule] = [
    Rule("valid_hs6", "hs6 RLIKE '^[0-9]{6}$' OR hs6 = 'TOTAL'"),
    Rule("known_reporter", "reporter_iso3 IS NOT NULL"),
    Rule("known_partner", "partner_iso3 IS NOT NULL"),
    Rule("valid_flow", "flow IN ('X', 'M')", "fail"),
    Rule("non_negative_value", "trade_value_usd >= 0"),
    Rule("plausible_year", "year BETWEEN 1990 AND year(current_date()) + 1"),
    Rule("non_negative_weight", "net_weight_kg IS NULL OR net_weight_kg >= 0", "warn"),
    Rule("weight_reported", "net_weight_kg IS NOT NULL AND net_weight_kg > 0", "warn"),
]

FX_RULES: list[Rule] = [
    Rule("positive_rate", "rate > 0", "fail"),
    Rule("known_currency", "currency IS NOT NULL", "drop"),
]


class DataQualityError(RuntimeError):
    pass


def evaluate(df: DataFrame, rules: list[Rule]) -> DataFrame:
    """Agrega `_dq_failed` (array de reglas `drop` violadas) y `_dq_warn`."""
    def failed(sev: str):
        exprs = [F.when(~F.coalesce(F.expr(r.expr), F.lit(False)), F.lit(r.name)) for r in rules if r.severity == sev]
        return F.filter(F.array(*exprs), lambda x: x.isNotNull()) if exprs else F.array().cast("array<string>")

    return df.withColumn("_dq_failed", failed("drop")).withColumn("_dq_warn", failed("warn")).withColumn(
        "_dq_fatal", failed("fail")
    )


def split_valid_invalid(df: DataFrame, rules: list[Rule]) -> tuple[DataFrame, DataFrame, dict]:
    evaluated = evaluate(df, rules).cache()
    fatal = evaluated.filter(F.size("_dq_fatal") > 0).count()
    if fatal:
        sample = [r.asDict() for r in evaluated.filter(F.size("_dq_fatal") > 0).limit(5).collect()]
        raise DataQualityError(f"{fatal} filas violan reglas críticas. Ejemplos: {sample}")

    valid = evaluated.filter(F.size("_dq_failed") == 0).drop("_dq_failed", "_dq_fatal")
    invalid = evaluated.filter(F.size("_dq_failed") > 0).drop("_dq_fatal")

    metrics = {"rows_in": evaluated.count(), "rows_quarantined": invalid.count()}
    for r in rules:
        col = "_dq_warn" if r.severity == "warn" else "_dq_failed"
        if r.severity == "fail":
            continue
        metrics[f"violations.{r.name}"] = evaluated.filter(F.array_contains(col, r.name)).count()
    return valid.drop("_dq_warn"), invalid, metrics
