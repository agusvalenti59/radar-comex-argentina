"""Baja datos REALES de UN Comtrade a una landing local, con o sin clave.

Sin clave (por defecto): API pública. Argentina + 5 mercados, 3 productos, 1 año,
un pedido por producto (tope de 500 filas por pedido). Sirve para probar.

    python scripts/fetch_comtrade_public.py --year 2023
    python scripts/run_local_demo.py --landing data/public_sample

Con clave: si existe la variable de entorno COMTRADE_API_KEY se usa la API con clave
(https://comtradeapi.un.org/data/v1/get, header Ocp-Apim-Subscription-Key). Por defecto
baja el radar completo definido en conf/base.yml: los 20 países de `reporters_m49`, todo
el `product_universe` y los últimos 6 años cerrados, a data/full_sample.

    $env:COMTRADE_API_KEY = Read-Host Clave        # PowerShell; no queda en el historial
    python scripts/fetch_comtrade_public.py
    python scripts/run_local_demo.py --landing data/full_sample

La clave nunca se pasa por línea de comandos ni se imprime. El plan gratuito permite unas
500 llamadas por día; el radar completo usa ~360 (20 países x 6 años x 3 pedidos).
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from trade_intel.config import load_settings  # noqa: E402
from trade_intel.ingestion.comtrade import ComtradeClient, plan_requests, to_landing_bytes  # noqa: E402
from trade_intel.ingestion.landing import write_landing  # noqa: E402

KEY_ENV = "COMTRADE_API_KEY"

# Muestra chica para la API pública. M49: Argentina, España, Países Bajos, EE.UU., China, Alemania
PUBLIC_REPORTERS = [32, 724, 528, 842, 156, 276]
PUBLIC_HS = ["080550", "120242", "220421"]  # limones, maní, vino


def read_key() -> str | None:
    key = os.environ.get(KEY_ENV, "").strip().strip('"').strip("'")
    return key or None


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--year", type=int, nargs="+", default=None,
                    help="años a bajar (default: público = hace 2 años; con clave = últimos years_back años)")
    ap.add_argument("--reporters", type=int, nargs="+", default=None, help="códigos M49 de los países")
    ap.add_argument("--hs", nargs="+", default=None, help="códigos HS6")
    ap.add_argument("--out", default=None, help="carpeta landing (default: data/public_sample o data/full_sample)")
    ap.add_argument("--rpm", type=int, default=None, help="pedidos por minuto")
    ap.add_argument("--public", action="store_true", help=f"forzar la API pública aunque exista {KEY_ENV}")
    args = ap.parse_args()

    settings = load_settings("local")
    cfg = settings.source("comtrade")
    key = None if args.public else read_key()
    last_closed = date.today().year - 2  # el anual de Comtrade llega con ~1 año de rezago

    if key:
        years = args.year or list(range(last_closed - cfg["years_back"] + 1, last_closed + 1))
        reporters = args.reporters or cfg["reporters_m49"]
        hs = args.hs or settings.product_universe
        out = args.out or str(ROOT / "data/full_sample")
        rpm = args.rpm or cfg["requests_per_minute"]
    else:
        years = args.year or [last_closed]
        reporters = args.reporters or PUBLIC_REPORTERS
        hs = args.hs or PUBLIC_HS
        out = args.out or str(ROOT / "data/public_sample")
        rpm = args.rpm or cfg.get("public_requests_per_minute", 12)

    client = ComtradeClient(cfg["base_url"], key, rpm, cfg["max_records"],
                            public_base_url=cfg.get("public_base_url"))
    plan = list(plan_requests(reporters, years, hs, split_by_product=client.split_by_product))

    if client.mode == "keyed":
        print(f"Modo: API CON CLAVE (clave leída de {KEY_ENV}, no se muestra)", flush=True)
    else:
        print(f"Modo: API PÚBLICA sin clave (tope 500 filas por pedido). Para usar la clave, definí {KEY_ENV}.",
              flush=True)
    print(f"Años {min(years)}-{max(years)} · {len(reporters)} países · {len(hs)} productos · "
          f"{len(plan)} pedidos · ~{len(plan) / rpm:.0f} min · destino {out}", flush=True)

    rows = truncated = failed = 0
    for i, req in enumerate(plan, 1):
        try:
            env = client.fetch(req)
        except Exception as exc:  # seguimos con el resto; al final se informa
            failed += 1
            msg = str(exc)
            if key:
                msg = msg.replace(key, "***")
            print(f"[{i}/{len(plan)}] ERROR {req.reporter_m49} {req.year} {req.flow}: {msg[:300]}", flush=True)
            if key and failed == i and ("401" in msg or "403" in msg):
                print("La API rechazó la clave (401/403). Revisá que copiaste la Clave principal completa.")
                sys.exit(1)
            continue
        write_landing(out, req.landing_key, to_landing_bytes(env))
        rows += env["count"]
        truncated += env["_truncated"]
        label = req.hs_codes[0] if len(req.hs_codes) == 1 else f"{len(req.hs_codes)} productos"
        print(f"[{i}/{len(plan)}] {req.reporter_m49} {req.year} {req.flow} {label}: {env['count']} filas", flush=True)

    print(f"\nListo: {rows} filas en {out} · truncados: {truncated} · fallidos: {failed}")
    if truncated:
        print(f"Hay respuestas en el tope de {client.max_records} filas: faltan datos en esos pedidos.")


if __name__ == "__main__":
    main()
