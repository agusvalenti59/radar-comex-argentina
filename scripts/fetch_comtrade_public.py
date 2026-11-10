"""Baja una muestra REAL de UN Comtrade usando la API pública (sin clave) a una landing local.

Por defecto: Argentina + 5 mercados, 3 productos, 1 año = 42 pedidos (~4 min a 12 pedidos/min).
Después se puede correr todo el pipeline sobre esos datos:

    python scripts/fetch_comtrade_public.py --year 2023
    python scripts/run_local_demo.py --landing data/public_sample

Límites de la API pública: 500 registros por pedido y sin cuota documentada. Por eso se pide
un producto por vez y despacio. Para el universo completo, usar la API con clave.
"""

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from trade_intel.ingestion.comtrade import ComtradeClient, plan_requests, to_landing_bytes  # noqa: E402
from trade_intel.ingestion.landing import write_landing  # noqa: E402

# M49: Argentina, España, Países Bajos, EE.UU., China, Alemania
DEFAULT_REPORTERS = [32, 724, 528, 842, 156, 276]
DEFAULT_HS = ["080550", "120242", "220421"]  # limones, maní, vino


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--year", type=int, nargs="+", default=[date.today().year - 2])
    ap.add_argument("--reporters", type=int, nargs="+", default=DEFAULT_REPORTERS)
    ap.add_argument("--hs", nargs="+", default=DEFAULT_HS)
    ap.add_argument("--out", default=str(ROOT / "data/public_sample"))
    ap.add_argument("--rpm", type=int, default=12, help="pedidos por minuto")
    args = ap.parse_args()

    client = ComtradeClient("unused", None, requests_per_minute=args.rpm)  # sin clave => pública
    plan = list(plan_requests(args.reporters, args.year, args.hs, split_by_product=True))
    print(f"API pública · {len(plan)} pedidos · ~{len(plan) / args.rpm:.0f} min")
    rows = truncated = failed = 0
    for i, req in enumerate(plan, 1):
        try:
            env = client.fetch(req)
        except Exception as exc:  # seguimos con el resto; al final se informa
            failed += 1
            print(f"[{i}/{len(plan)}] ERROR {req.reporter_m49} {req.year} {req.flow} {req.hs_codes}: {exc}")
            continue
        write_landing(args.out, req.landing_key, to_landing_bytes(env))
        rows += env["count"]
        truncated += env["_truncated"]
        print(f"[{i}/{len(plan)}] {req.reporter_m49} {req.year} {req.flow} {req.hs_codes[0]}: {env['count']} filas")
    print(f"\nListo: {rows} filas en {args.out} · truncados: {truncated} · fallidos: {failed}")
    if truncated:
        print("Hay respuestas en el tope de 500 filas: faltan datos en esos pedidos.")


if __name__ == "__main__":
    main()
