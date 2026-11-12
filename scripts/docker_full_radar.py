# Copyright (c) 2026 Agustin Boursiac. Todos los derechos reservados.
# Prohibido copiar, modificar o redistribuir sin permiso escrito del autor. Ver LICENSE.
"""Corre el radar completo dentro de Docker (imagen python:3.12-bookworm), sin comillas en PowerShell.

1. Instala Java y las dependencias del proyecto dentro del contenedor.
2. Baja Comtrade con la clave de COMTRADE_API_KEY a data/full_sample (se omite con --skip-fetch).
3. Corre el pipeline local (bronze, silver, gold) sobre esa carpeta.

Uso desde PowerShell, parado en la carpeta del proyecto:

    $env:COMTRADE_API_KEY = Read-Host Clave
    docker run --rm -e COMTRADE_API_KEY -v ${PWD}:/work -w /work python:3.12-bookworm python scripts/docker_full_radar.py

`--skip-fetch` re-corre solo el pipeline sin gastar llamadas a la API (el plan gratuito
permite ~500 por día y la descarga completa usa ~360).
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LANDING = "data/full_sample"


def run(cmd: list[str], quiet: bool = False) -> None:
    print("\n>>", " ".join(cmd), flush=True)
    out = subprocess.DEVNULL if quiet else None
    subprocess.run(cmd, cwd=ROOT, check=True, stdout=out)


def main() -> None:
    skip_fetch = "--skip-fetch" in sys.argv[1:]
    if not skip_fetch and not os.environ.get("COMTRADE_API_KEY", "").strip():
        sys.exit("Falta COMTRADE_API_KEY. En PowerShell: $env:COMTRADE_API_KEY = Read-Host Clave "
                 "y agregá -e COMTRADE_API_KEY al docker run.")

    print("Paso 1/3: instalando Java y dependencias (tarda unos minutos la primera vez)...", flush=True)
    run(["apt-get", "update", "-qq"], quiet=True)
    run(["apt-get", "install", "-y", "-qq", "default-jre-headless"], quiet=True)
    run([sys.executable, "-m", "pip", "install", "-q", "--root-user-action=ignore", "-e", ".[dev]"])

    if skip_fetch:
        print(f"\nPaso 2/3: se omite la descarga, se usa lo que ya hay en {LANDING}", flush=True)
    else:
        print("\nPaso 2/3: bajando Comtrade con tu clave...", flush=True)
        run([sys.executable, "-u", "scripts/fetch_comtrade_public.py", "--out", LANDING])

    print("\nPaso 3/3: corriendo el pipeline (bronze, silver, gold)...", flush=True)
    run([sys.executable, "-u", "scripts/run_local_demo.py", "--landing", LANDING])


if __name__ == "__main__":
    main()
