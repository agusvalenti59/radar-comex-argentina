"""Cliente de la API de Estadísticas Cambiarias del BCRA (v1.0).

GET /Cotizaciones/{moneda}?fechadesde=YYYY-MM-DD&fechahasta=YYYY-MM-DD
Se pagina por ventanas de 1 año (la API limita el rango por request).
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

from trade_intel.ingestion.http import RateLimiter, build_session


def year_windows(start: date, end: date) -> list[tuple[date, date]]:
    windows, cur = [], start
    while cur <= end:
        nxt = min(date(cur.year, 12, 31), end)
        windows.append((cur, nxt))
        cur = nxt + timedelta(days=1)
    return windows


class BcraClient:
    def __init__(self, base_url: str, session=None, requests_per_minute: int = 60):
        self.base_url = base_url.rstrip("/")
        self.session = session or build_session()
        self.limiter = RateLimiter(requests_per_minute)

    def fetch_quotes(self, currency: str, start: date, end: date) -> dict:
        self.limiter.wait()
        resp = self.session.get(
            f"{self.base_url}/Cotizaciones/{currency}",
            params={"fechadesde": start.isoformat(), "fechahasta": end.isoformat()},
            timeout=60,
        )
        resp.raise_for_status()
        body = resp.json()
        return {
            "_source": "bcra",
            "_request": {"currency": currency, "from": start.isoformat(), "to": end.isoformat()},
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "results": body.get("results", []),
        }
