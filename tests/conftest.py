import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
os.environ.setdefault("TRADE_INTEL_CONF_DIR", str(ROOT / "conf"))


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    from pyspark.sql import SparkSession

    wh = tmp_path_factory.mktemp("warehouse")
    session = (
        SparkSession.builder.master("local[2]")
        .appName("trade-intel-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.warehouse.dir", str(wh))
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture()
def flows(spark):
    """Panel mínimo de flujos anuales (ya agregados) para testear métricas.

    Mercado ESP importa limones (080550) de ARG, ZAF y TUR. Totales de exportación por país
    para el RCA.
    """
    rows = [
        # year, flow, reporter, partner, hs6, value_usd, kg
        (2024, "M", "ESP", "ARG", "080550", 60.0, 50.0),
        (2024, "M", "ESP", "ZAF", "080550", 30.0, 30.0),
        (2024, "M", "ESP", "TUR", "080550", 10.0, 10.0),
        (2024, "M", "ESP", "WLD", "080550", 100.0, 90.0),
        (2019, "M", "ESP", "ARG", "080550", 40.0, 40.0),
        (2019, "M", "ESP", "ZAF", "080550", 10.0, 10.0),
        (2024, "X", "ARG", "WLD", "080550", 80.0, None),
        (2024, "X", "ZAF", "WLD", "080550", 20.0, None),
        (2024, "X", "ARG", "WLD", "TOTAL", 1000.0, None),
        (2024, "X", "ZAF", "WLD", "TOTAL", 9000.0, None),
        (2024, "X", "ARG", "ESP", "080550", 50.0, 50.0),
    ]
    return spark.createDataFrame(
        rows, "year int, flow string, reporter_iso3 string, partner_iso3 string, hs6 string, value_usd double, net_weight_kg double"
    )
