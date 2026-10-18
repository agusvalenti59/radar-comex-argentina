import pytest

from trade_intel.quality.expectations import (
    TRADE_FLOW_RULES,
    DataQualityError,
    Rule,
    split_valid_invalid,
)

COLS = "flow string, hs6 string, reporter_iso3 string, partner_iso3 string, trade_value_usd double, year int, net_weight_kg double"


def test_invalid_rows_go_to_quarantine_with_reasons(spark):
    df = spark.createDataFrame(
        [("M", "080550", "ESP", "ARG", 10.0, 2024, 5.0),
         ("M", "80550", "ESP", None, -1.0, 2024, 5.0),
         ("X", "TOTAL", "ARG", "WLD", 1e9, 2024, None)],
        COLS,
    )
    valid, invalid, metrics = split_valid_invalid(df, TRADE_FLOW_RULES)
    assert valid.count() == 2
    reasons = set(invalid.first()["_dq_failed"])
    assert reasons == {"valid_hs6", "known_partner", "non_negative_value"}
    assert metrics["rows_quarantined"] == 1
    assert metrics["violations.weight_reported"] == 1  # warn: no descarta


def test_fatal_rule_raises(spark):
    df = spark.createDataFrame([("RX", "080550", "ESP", "ARG", 10.0, 2024, 5.0)], COLS)
    with pytest.raises(DataQualityError):
        split_valid_invalid(df, TRADE_FLOW_RULES)


def test_null_is_treated_as_violation(spark):
    df = spark.createDataFrame([("M", None, "ESP", "ARG", 1.0, 2024, 1.0)], COLS)
    _, invalid, _ = split_valid_invalid(df, [Rule("valid_hs6", "hs6 RLIKE '^[0-9]{6}$'")])
    assert invalid.count() == 1
