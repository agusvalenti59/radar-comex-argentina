from datetime import date

import responses

from trade_intel.ingestion.bcra import BcraClient, year_windows
from trade_intel.ingestion.comtrade import ComtradeClient, ComtradeRequest, plan_requests


def test_plan_requests_covers_flows_and_totals():
    reqs = list(plan_requests([32, 724], [2023, 2024], ["080550", "120242"]))
    assert len(reqs) == 2 * 2 * 3
    assert {r.flow for r in reqs} == {"X", "M"}
    assert any(r.hs_codes == ("TOTAL",) for r in reqs)


def test_landing_key_is_deterministic():
    r = ComtradeRequest(reporter_m49=32, year=2024, flow="X")
    assert r.landing_key == "comtrade/freq=A/year=2024/reporter=32/flow=X/part-0000.json"


@responses.activate
def test_comtrade_fetch_sends_key_and_flags_truncation():
    responses.get(
        "https://comtradeapi.un.org/data/v1/get/C/A/HS",
        json={"data": [{"cmdCode": "080550"}, {"cmdCode": "120242"}]},
    )
    client = ComtradeClient("https://comtradeapi.un.org/data/v1/get", "secret", requests_per_minute=6000, max_records=2)
    env = client.fetch(ComtradeRequest(32, 2024, "X", hs_codes=("080550", "120242")))
    call = responses.calls[0].request
    assert call.headers["Ocp-Apim-Subscription-Key"] == "secret"
    assert "cmdCode=080550%2C120242" in call.url
    assert env["count"] == 2 and env["_truncated"] is True


def test_bcra_year_windows():
    w = year_windows(date(2023, 6, 1), date(2024, 3, 1))
    assert w == [(date(2023, 6, 1), date(2023, 12, 31)), (date(2024, 1, 1), date(2024, 3, 1))]


@responses.activate
def test_bcra_fetch():
    responses.get(
        "https://api.bcra.gob.ar/estadisticascambiarias/v1.0/Cotizaciones/USD",
        json={"status": 200, "results": [{"fecha": "2024-01-02", "detalle": []}]},
    )
    env = BcraClient("https://api.bcra.gob.ar/estadisticascambiarias/v1.0", requests_per_minute=6000).fetch_quotes(
        "USD", date(2024, 1, 1), date(2024, 1, 31)
    )
    assert env["results"][0]["fecha"] == "2024-01-02"
