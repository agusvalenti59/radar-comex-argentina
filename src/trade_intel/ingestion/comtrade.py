# Copyright (c) 2026 Agustin Boursiac. Todos los derechos reservados.
# Prohibido copiar, modificar o redistribuir sin permiso escrito del autor. Ver LICENSE.
"""Cliente de UN Comtrade (https://comtradeapi.un.org).

Dos modos con el mismo contrato de salida (el envelope que va a landing es idéntico):

| modo      | endpoint                          | clave | tope por request |
|-----------|-----------------------------------|-------|------------------|
| `keyed`   | /data/v1/get/C/{freq}/HS          | sí    | 100.000 (según plan) |
| `public`  | /public/v1/preview/C/{freq}/HS    | no    | 500              |

El modo público sirve para probar el pipeline con datos reales sin registrarse. Como el
tope es 500 registros, el plan de extracción se parte por producto: un pedido por
(reporter, año, flujo, HS6) devuelve una fila por socio (~200 como máximo), debajo del tope.
Eso multiplica la cantidad de llamadas, así que en modo público conviene acotar
reporters/años (ver `scripts/fetch_comtrade_public.py`).

Estrategia general:
- Cada respuesta se guarda cruda (JSON) en landing con metadata del request: bronze nunca
  depende de la API, sólo de lo que ya aterrizó.
- Si una respuesta llega al tope de registros se marca `_truncated=true` y se loguea: es la
  señal para partir más el pedido.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import datetime, timezone

from trade_intel.common.logging import get_logger
from trade_intel.ingestion.http import RateLimiter, build_session

log = get_logger(__name__)

PUBLIC_MAX_RECORDS = 500


@dataclass(frozen=True)
class ComtradeRequest:
    reporter_m49: int
    year: int
    flow: str  # "X" exportaciones | "M" importaciones
    freq: str = "A"
    hs_codes: tuple[str, ...] = ()

    @property
    def landing_key(self) -> str:
        # Un archivo por pedido. Con un solo código se usa como sufijo (modo público, split
        # por producto); con varios, part-0000. Misma ruta => re-ingesta idempotente.
        part = self.hs_codes[0] if len(self.hs_codes) == 1 else "0000"
        return (
            f"comtrade/freq={self.freq}/year={self.year}/reporter={self.reporter_m49}/"
            f"flow={self.flow}/part-{part}.json"
        )


class ComtradeClient:
    def __init__(self, base_url: str, subscription_key: str | None = None, requests_per_minute: int = 30,
                 max_records: int = 100_000, session=None, public_base_url: str | None = None):
        self.key = subscription_key or None
        self.mode = "keyed" if self.key else "public"
        if self.mode == "public":
            self.base_url = (public_base_url or "https://comtradeapi.un.org/public/v1/preview").rstrip("/")
            self.max_records = min(max_records, PUBLIC_MAX_RECORDS)
        else:
            self.base_url = base_url.rstrip("/")
            self.max_records = max_records
        self.session = session or build_session()
        self.limiter = RateLimiter(requests_per_minute)

    @property
    def split_by_product(self) -> bool:
        return self.mode == "public"

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
        headers = {"Ocp-Apim-Subscription-Key": self.key} if self.key else {}
        resp = self.session.get(self._url(req), params=self.params(req), headers=headers, timeout=120)
        resp.raise_for_status()
        body = resp.json()
        if body.get("error"):
            raise RuntimeError(f"Comtrade devolvió error para {self.params(req)}: {body['error']}")
        rows = body.get("data") or []
        envelope = {
            "_source": "comtrade",
            "_api_mode": self.mode,
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
                  split_by_product: bool = False, flows: Iterable[str] = ("X", "M"),
                  include_totals: bool = True) -> Iterator[ComtradeRequest]:
    """Arma el plan de extracción.

    - Argentina (32): exportaciones (para RCA y espejo) + importaciones.
    - Mercados objetivo: importaciones (tamaño de mercado, competidores, precios) y
      exportaciones (necesarias para el denominador mundial del RCA).
    - `split_by_product`: un pedido por HS6 (necesario en la API pública, tope 500 filas).
    """
    hs = tuple(sorted(set(hs_codes)))
    groups = [(code,) for code in hs] if split_by_product else [hs]
    reporters = list(reporters)
    for year in years:
        for rep in reporters:
            for flow in flows:
                for group in groups:
                    yield ComtradeRequest(reporter_m49=rep, year=year, flow=flow, hs_codes=group)
            if include_totals:
                # Totales por reporter (todas las mercaderías) para el denominador del RCA
                yield ComtradeRequest(reporter_m49=rep, year=year, flow="X", hs_codes=("TOTAL",))


def to_landing_bytes(envelope: dict) -> bytes:
    return json.dumps(envelope, ensure_ascii=False).encode("utf-8")
