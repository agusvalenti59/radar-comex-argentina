import math

import pytest
from pyspark.sql import functions as F

from trade_intel.gold import metrics as m


def _row(df, **filters):
    for k, v in filters.items():
        df = df.filter(F.col(k) == v)
    rows = df.collect()
    assert len(rows) == 1, rows
    return rows[0]


def test_market_imports_excludes_world_and_totals(flows):
    imp = m.market_imports(flows)
    assert imp.filter("supplier_iso3 = 'WLD'").count() == 0
    assert imp.count() == 5


def test_supplier_shares_sum_to_one_and_rank(flows):
    shares = m.supplier_shares(m.market_imports(flows)).filter("year = 2024")
    total = shares.agg(F.sum("share")).first()[0]
    assert total == pytest.approx(1.0)
    assert _row(shares, supplier_iso3="ARG")["rank"] == 1
    assert _row(shares, supplier_iso3="TUR")["share"] == pytest.approx(0.1)


def test_hhi_and_cr3(flows):
    conc = m.concentration(m.supplier_shares(m.market_imports(flows)))
    r = _row(conc, year=2024)
    # 0.6^2 + 0.3^2 + 0.1^2 = 0.46 -> 4600
    assert r["hhi"] == pytest.approx(4600.0)
    assert r["cr3"] == pytest.approx(1.0)
    assert r["concentration_level"] == "alta"
    assert r["top_supplier_iso3"] == "ARG"


def test_cagr_over_five_years(flows):
    conc = m.concentration(m.supplier_shares(m.market_imports(flows)))
    g = _row(m.market_growth(conc, 5), year=2024)
    assert g["cagr"] == pytest.approx((100 / 50) ** (1 / 5) - 1)


def test_cagr_null_without_history(flows):
    conc = m.concentration(m.supplier_shares(m.market_imports(flows)))
    assert _row(m.market_growth(conc, 5), year=2019)["cagr"] is None


def test_balassa_rca(flows):
    rca = m.revealed_comparative_advantage(flows)
    arg = _row(rca, exporter_iso3="ARG")
    # (80/1000) / (100/10000) = 8
    assert arg["rca"] == pytest.approx(8.0)
    assert arg["has_advantage"] is True
    assert _row(rca, exporter_iso3="ZAF")["rca"] == pytest.approx((20 / 9000) / (100 / 10000))


def test_mirror_gap_detects_discrepancy(flows):
    mirror = m.mirror_gap(flows, "ARG", cif_fob_factor=1.08, tolerance=0.25)
    r = _row(mirror, year=2024, partner_iso3="ESP")
    assert r["gap_ratio"] == pytest.approx(60 / (50 * 1.08) - 1)
    assert r["mirror_status"] == "consistente"
    strict = _row(m.mirror_gap(flows, "ARG", 1.0, 0.1), year=2024, partner_iso3="ESP")
    assert strict["mirror_status"] == "discrepancia"


def test_mirror_gap_only_mirror_side(flows):
    r = _row(m.mirror_gap(flows, "ARG", 1.08, 0.25), year=2019, partner_iso3="ESP")
    assert r["mirror_status"] == "solo_espejo"


def test_unit_value_outlier_needs_statistical_and_economic_signal(spark):
    rows = [(2024, "080550", "USA", s, v, 100.0) for s, v in
            [("A", 100.0), ("B", 105.0), ("C", 98.0), ("D", 102.0), ("E", 101.0), ("F", 500.0), ("G", 120.0)]]
    imp = spark.createDataFrame(rows, "year int, hs6 string, market_iso3 string, supplier_iso3 string, value_usd double, net_weight_kg double")
    uv = m.unit_value_benchmark(imp, "A", z_threshold=3.5, min_ratio=2.0)
    flagged = {r["supplier_iso3"] for r in uv.filter("is_price_outlier").collect()}
    assert flagged == {"F"}  # G tiene z alto pero sólo 1.2x la mediana
    a = _row(uv, supplier_iso3="A")
    assert a["is_home"] is True
    assert a["premium_vs_median"] == pytest.approx(100 / 102 - 1)


def test_opportunity_score_bounds_and_weights(flows):
    imp = m.market_imports(flows)
    shares = m.supplier_shares(imp)
    conc = m.concentration(shares)
    opp = m.opportunity_score(
        conc, m.market_growth(conc, 5), shares, m.revealed_comparative_advantage(flows),
        m.unit_value_benchmark(imp, "ARG"), "ARG",
        {"market_growth": 1, "market_size": 1, "rca": 1, "headroom": 1, "openness": 1, "price_competitiveness": 1},
        min_market_size_usd=0,
    )
    for r in opp.collect():
        assert 0 <= r["opportunity_score"] <= 100
        assert r["strategic_quadrant"] in {"estrella", "oportunidad", "defender", "monitorear"}
    r = _row(opp, year=2024)
    assert r["home_share"] == pytest.approx(0.6)
    assert r["home_rca"] == pytest.approx(8.0)
    assert not math.isnan(r["opportunity_score"])


def test_opportunity_filters_small_markets(flows):
    imp = m.market_imports(flows)
    shares = m.supplier_shares(imp)
    conc = m.concentration(shares)
    opp = m.opportunity_score(conc, m.market_growth(conc, 5), shares, m.revealed_comparative_advantage(flows),
                              m.unit_value_benchmark(imp, "ARG"), "ARG", {"rca": 1}, min_market_size_usd=1e9)
    assert opp.count() == 0
