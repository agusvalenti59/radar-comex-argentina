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


def test_plan_split_by_product_for_public_api():
    reqs = list(plan_requests([32], [2023], ["080550", "120242"], split_by_product=True))
    # 2 flujos x 2 productos + 1 total
    assert len(reqs) == 5
    assert all(len(r.hs_codes) == 1 for r in reqs)
    keys = {r.landing_key for r in reqs}
    assert len(keys) == 5  # un archivo por pedido, sin pisarse
    assert "comtrade/freq=A/year=2023/reporter=32/flow=X/part-080550.json" in keys


@responses.activate
def test_public_mode_without_key():
    responses.get(
        "https://comtradeapi.un.org/public/v1/preview/C/A/HS",
        json={"count": 1, "data": [{"cmdCode": "080550"}], "error": ""},
    )
    client = ComtradeClient("https://comtradeapi.un.org/data/v1/get", None, requests_per_minute=6000)
    assert client.mode == "public" and client.split_by_product and client.max_records == 500
    env = client.fetch(ComtradeRequest(32, 2023, "X", hs_codes=("080550",)))
    call = responses.calls[0].request
    assert "Ocp-Apim-Subscription-Key" not in call.headers
    assert "maxRecords=500" in call.url
    assert env["_api_mode"] == "public" and env["_truncated"] is False


@responses.activate
def test_api_error_field_raises():
    import pytest

    responses.get("https://comtradeapi.un.org/public/v1/preview/C/A/HS", json={"data": None, "error": "bad period"})
    client = ComtradeClient("x", None, requests_per_minute=6000)
    with pytest.raises(RuntimeError, match="bad period"):
        client.fetch(ComtradeRequest(32, 1800, "X"))


def test_resolve_key_modes():
    import pytest

    from trade_intel.jobs.ingest_comtrade import resolve_key

    def missing():
        raise KeyError("no secret")

    assert resolve_key("public", lambda: "abc") is None
    assert resolve_key("auto", lambda: "abc") == "abc"
    assert resolve_key("auto", lambda: "CHANGE_ME") is None
    assert resolve_key("auto", missing) is None
    with pytest.raises(RuntimeError):
        resolve_key("keyed", lambda: "CHANGE_ME")
    with pytest.raises(KeyError):
        resolve_key("keyed", missing)
