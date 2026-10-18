"""Cliente de UN Comtrade API v1 (https://comtradeapi.un.org).

Estrategia de extracción:
- Se particiona el pedido por (reporter, año, flujo) para quedar debajo del límite de
  registros por request y para que cada archivo en landing sea re-procesable de forma aislada.
- Cada respuesta se guarda cruda (JSON) en landing con metadata del request: bronze nunca
  depende de la API, sólo de lo que ya aterrizó.
- Si una respuesta llega al tope de registros se marca `truncated=true` y se loguea: es la
  señal para partir más el pedido (por capítulo HS).
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import datetime, timezone

from trade_intel.common.logging import get_logger
from trade_intel.ingestion.http import RateLimiter, build_session

log = get_logger(__name__)


@dataclass(frozen=True)
class ComtradeRequest:
    reporter_m49: int
    year: int
    flow: str  # "X" exportaciones | "M" importaciones
    freq: str = "A"
    hs_codes: tuple[str, ...] = ()

    @property
    def landing_key(self) -> str:
        return (
            f"comtrade/freq={self.freq}/year={self.year}/reporter={self.reporter_m49}/"
            f"flow={self.flow}/part-0000.json"
        )


class ComtradeClient:
    def __init__(self, base_url: str, subscription_key: str, requests_per_minute: int = 30,
                 max_records: int = 100_000, session=None):
        self.base_url = base_url.rstrip("/")
        self.key = subscription_key
        self.max_records = max_records
        self.session = session or build_session()
        self.limiter = RateLimiter(requests_per_minute)

    def _url(self, req: ComtradeRequest) -> str:
        return f"{self.base_url}/C/{req.freq}/HS"

    def params(self, req: ComtradeRequest) -> dict:
        params = {
            "reporterCode": str(req.reporter_m49),
            "period": str(req.year),
            "flowCode": req.flow,
            "partner2Code": "0",
            "customsCode": "C00",
            "motCode": "0",
            "includeDesc": "false",
            "maxRecords": str(self.max_records),
        }
        if req.hs_codes:
            params["cmdCode"] = ",".join(req.hs_codes)
        return params

    def fetch(self, req: ComtradeRequest) -> dict:
        self.limiter.wait()
        resp = self.session.get(
            self._url(req),
            params=self.params(req),
            headers={"Ocp-Apim-Subscription-Key": self.key},
            timeout=120,
        )
        resp.raise_for_status()
        body = resp.json()
        rows = body.get("data") or []
        envelope = {
            "_source": "comtrade",
            "_request": {**self.params(req), "freq": req.freq},
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "_truncated": len(rows) >= self.max_records,
            "count": len(rows),
            "data": rows,
        }
        if envelope["_truncated"]:
            log.warning("comtrade response truncated", extra={"ctx": envelope["_request"]})
        return envelope


def plan_requests(reporters: Iterable[int], years: Iterable[int], hs_codes: Iterable[str],
                  home_m49: int = 32) -> Iterator[ComtradeRequest]:
    """Arma el plan de extracción.

    - Argentina (32): exportaciones (para RCA y espejo) + importaciones.
    - Mercados objetivo: importaciones (tamaño de mercado, competidores, precios) y
      exportaciones (necesarias para el denominador mundial del RCA).
    """
    hs = tuple(sorted(set(hs_codes)))
    for year in years:
        for rep in reporters:
            for flow in ("X", "M"):
                yield ComtradeRequest(reporter_m49=rep, year=year, flow=flow, hs_codes=hs)
            # Totales por reporter (todas las mercaderías) para el denominador del RCA
            yield ComtradeRequest(reporter_m49=rep, year=year, flow="X", hs_codes=("TOTAL",))


def to_landing_bytes(envelope: dict) -> bytes:
    return json.dumps(envelope, ensure_ascii=False).encode("utf-8")
