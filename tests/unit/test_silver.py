from pathlib import Path

import pytest
from chispa import assert_df_equality
from pyspark.sql import functions as F

from trade_intel.silver import fx_rates, reference, trade_flows

REF = Path(__file__).resolve().parents[2] / "data" / "reference"


@pytest.fixture()
def countries(spark):
    return reference.load_countries(spark, str(REF / "countries.csv"))


def _comtrade(spark, rows):
    cols = ("freqCode string, refYear int, refMonth int, reporterCode int, flowCode string, partnerCode int, "
            "partner2Code int, cmdCode string, customsCode string, motCode int, primaryValue double, "
            "fobvalue double, cifvalue double, netWgt double, qty double, qtyUnitCode int, _ingest_ts timestamp")
    return spark.createDataFrame(rows, cols)


def test_reference_seeds_are_unique(spark, countries):
    assert countries.count() == countries.select("iso3").distinct().count()
    assert countries.filter("iso3 = 'WLD' AND m49 = 0").count() == 1
    products = reference.load_products(spark, str(REF / "products.csv"))
    assert products.filter(F.length("hs6") != 6).count() == 0


def test_normalize_comtrade_maps_codes_and_filters(spark, countries):
    from datetime import datetime

    ts = datetime(2025, 1, 1)
    rows = [
        ("A", 2024, 52, 724, "M", 32, 0, "80550", "C00", 0, 120.0, None, 120.0, 100.0, 100.0, 8, ts),
        ("A", 2024, 52, 724, "RM", 32, 0, "080550", "C00", 0, 5.0, None, 5.0, 1.0, 1.0, 8, ts),   # re-import
        ("A", 2024, 52, 724, "M", 32, 76, "080550", "C00", 0, 9.0, None, 9.0, 1.0, 1.0, 8, ts),   # partner2
        ("A", 2024, 52, 32, "X", 0, 0, "TOTAL", "C00", 0, 1e9, 1e9, None, None, None, None, ts),
    ]
    out = trade_flows.normalize_comtrade(_comtrade(spark, rows), countries)
    assert out.count() == 2
    lemons = out.filter("hs6 = '080550'").first()
    assert (lemons.reporter_iso3, lemons.partner_iso3, lemons.flow) == ("ESP", "ARG", "M")
    assert lemons.unit_value_usd_kg == pytest.approx(1.2)
    assert str(lemons.period_date) == "2024-01-01"
    total = out.filter("hs6 = 'TOTAL'").first()
    assert total.partner_iso3 == "WLD" and total.unit_value_usd_kg is None


def test_normalize_indec_collapses_ncm_to_hs6(spark, countries):
    from datetime import datetime

    ts = datetime(2025, 1, 1)
    bronze = spark.createDataFrame(
        [("2024", "5", "0805.50.00", "410", "1000", "1.500,50", ts),
         ("2024", "5", "0805.50.90", "410", "500", "499,50", ts),
         ("2024", "5", "0805.50.00", "999", "10", "10", ts)],
        "anio string, mes string, ncm string, pdes string, pnet_kg string, fob_usd string, _ingest_ts timestamp",
    )
    out = trade_flows.normalize_indec_exports(bronze, countries)
    esp = out.filter("partner_iso3 = 'ESP'").first()
    assert esp.hs6 == "080550"
    assert esp.trade_value_usd == pytest.approx(2000.0)
    assert esp.net_weight_kg == pytest.approx(1500.0)
    assert esp.freq == "M" and esp.reporter_iso3 == "ARG" and esp.month == 5
    # Código INDEC desconocido -> partner nulo (lo atrapa la regla known_partner)
    assert out.filter("partner_iso3 IS NULL").count() == 1


def test_fx_monthly_average(spark):
    from datetime import datetime

    ts = datetime(2025, 1, 1)
    bronze = spark.createDataFrame(
        [("2024-01-02", "usd", 800.0, 1.0, ts), ("2024-01-31", "usd", 820.0, 1.0, ts),
         ("2024-02-01", "usd", 830.0, 1.0, ts)],
        "fecha string, codigoMoneda string, tipoCotizacion double, tipoPase double, _ingest_ts timestamp",
    )
    monthly = fx_rates.monthly_average(fx_rates.normalize_fx(bronze)).select("period_date", "avg_rate", "eom_rate", "n_days")
    expected = spark.createDataFrame(
        [(datetime(2024, 1, 1).date(), 810.0, 820.0, 2), (datetime(2024, 2, 1).date(), 830.0, 830.0, 1)],
        "period_date date, avg_rate double, eom_rate double, n_days long",
    )
    assert_df_equality(monthly, expected, ignore_row_order=True, ignore_nullable=True)
