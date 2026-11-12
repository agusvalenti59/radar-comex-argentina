# Databricks notebook source
# Copyright (c) 2026 Agustin Boursiac. Todos los derechos reservados.
# Prohibido copiar, modificar o redistribuir sin permiso escrito del autor. Ver LICENSE.
# MAGIC %md
# MAGIC # Actualización automática desde UN Comtrade
# MAGIC Pensado para correr programado (por ejemplo, todos los lunes):
# MAGIC 1. Pregunta a Comtrade (API pública de disponibilidad, sin gastar llamadas de la clave)
# MAGIC    qué países y años hay publicados y con qué versión (checksum).
# MAGIC 2. Compara con lo que ya está cargado. Hay novedad si:
# MAGIC    - salió un año nuevo y ya lo publicaron Argentina y al menos el 80% de los países del radar,
# MAGIC    - un país publicó un año que nos faltaba, o
# MAGIC    - Comtrade revisó (cambió) los datos de un país-año ya cargado.
# MAGIC 3. Si hay novedades: baja solo esos país-año con tu clave (3 llamadas cada uno) y corre `comex_pipeline`.
# MAGIC    Si no hay: termina sin hacer nada.
# MAGIC
# MAGIC Requisitos: haber corrido `comex_pipeline` una vez y guardado la clave con `comex_guardar_clave`.

# COMMAND ----------

VOLUME = "/Volumes/workspace/comex/archivos"
PROJECT = f"{VOLUME}/proyecto"
EXTRA = f"{VOLUME}/landing_extra"        # descargas nuevas (sobreviven a cada re-corrida)
SNAPSHOT = "workspace.comex_ops.comtrade_disponibilidad"
PIPELINE_NOTEBOOK = "./comex_pipeline"
MAX_LLAMADAS = 300                       # tope por corrida (el plan gratis da ~500 por día)
COBERTURA_MINIMA = 0.8                   # % de países que tienen que tener un año nuevo

# COMMAND ----------

import os, shutil, sys, json, glob
from datetime import date, datetime, timezone
import requests

assert os.path.exists(f"{PROJECT}/src"), "Primero corré comex_pipeline una vez."
shutil.rmtree("/tmp/comex_src", ignore_errors=True)
shutil.copytree(f"{PROJECT}/src", "/tmp/comex_src")
os.environ["TRADE_INTEL_CONF_DIR"] = f"{PROJECT}/conf"
sys.path.insert(0, "/tmp/comex_src")
for mod in [m for m in sys.modules if m.startswith("trade_intel")]:
    del sys.modules[mod]

from trade_intel.config import load_settings
from trade_intel.ingestion.comtrade import ComtradeClient, plan_requests, to_landing_bytes

settings = load_settings("dev")
cfg = settings.source("comtrade")
REPORTERS = list(cfg["reporters_m49"])
HS = settings.product_universe

# COMMAND ----------

# 1) Qué hay cargado hoy: archivos de landing del zip + descargas anteriores
def cargados():
    pares = set()
    for root in (f"{PROJECT}/data/full_sample", EXTRA):
        for p in glob.glob(f"{root}/comtrade/freq=A/year=*/reporter=*"):
            parts = p.rstrip("/").split("/")
            pares.add((int(parts[-1].split("=")[1]), int(parts[-2].split("=")[1])))
    return pares

ya = cargados()
anios_cargados = sorted({y for _, y in ya})
assert anios_cargados, "No encuentro datos cargados de Comtrade."
ultimo_cargado = max(anios_cargados)
print(f"Cargado: {len(ya)} país-año, años {min(anios_cargados)}-{ultimo_cargado}")

# COMMAND ----------

# 2) Qué publicó Comtrade (API pública de disponibilidad, una sola llamada, sin clave)
anios = list(range(min(anios_cargados), date.today().year))
r = requests.get(
    "https://comtradeapi.un.org/public/v1/getDA/C/A/HS",
    params={"reporterCode": ",".join(map(str, REPORTERS)), "period": ",".join(map(str, anios))},
    timeout=60,
)
r.raise_for_status()
disp = {}
for d in r.json().get("data") or []:
    k = (int(d["reporterCode"]), int(d["period"]))
    disp.setdefault(k, []).append(str(d.get("datasetChecksum")))
version = {k: "|".join(sorted(v)) for k, v in disp.items()}
print(f"Comtrade tiene publicados {len(version)} país-año del radar")

# COMMAND ----------

# 3) Comparar contra la última foto guardada
if spark.catalog.tableExists(SNAPSHOT):
    previo = {(row.reporter, row.anio): row.version for row in spark.table(SNAPSHOT).collect()}
    primera_vez = False
else:
    previo, primera_vez = {}, True

nuevos = []
for (rep, y), v in version.items():
    if (rep, y) in ya:
        # Ya cargado: se baja de nuevo solo si Comtrade lo revisó.
        # La primera vez se toma lo cargado como vigente (no se gasta nada).
        if not primera_vez and previo.get((rep, y)) not in (None, v):
            nuevos.append((rep, y, "revisado"))
    elif y <= ultimo_cargado:
        nuevos.append((rep, y, "faltaba"))

# Años nuevos: solo cuando ya los publicó ARG y suficientes países (evita un año a medias en el dashboard)
for y in [a for a in anios if a > ultimo_cargado]:
    paises = {rep for (rep, yy) in version if yy == y}
    cobertura = len(paises) / len(REPORTERS)
    print(f"Año {y}: publicado por {len(paises)}/{len(REPORTERS)} países ({cobertura:.0%}), ARG: {'sí' if 32 in paises else 'no'}")
    if 32 in paises and cobertura >= COBERTURA_MINIMA:
        nuevos += [(rep, y, "año nuevo") for rep in sorted(paises)]

nuevos.sort(key=lambda t: -t[1])  # primero lo más reciente
llamadas_por_par = 3                # exportaciones, importaciones y total
nuevos = nuevos[: MAX_LLAMADAS // llamadas_por_par]
print(f"Novedades a bajar: {len(nuevos)} país-año (~{len(nuevos) * llamadas_por_par} llamadas)")
for t in nuevos[:30]:
    print("  ", t)

# COMMAND ----------

# 4) Bajar las novedades con la clave y guardarlas en landing_extra
descargados = []
if nuevos:
    key = dbutils.secrets.get("comex", "comtrade")
    client = ComtradeClient(cfg["base_url"], key, cfg["requests_per_minute"], cfg["max_records"])
    for rep, y, motivo in nuevos:
        try:
            for req in plan_requests([rep], [y], HS):
                env = client.fetch(req)
                path = f"{EXTRA}/{req.landing_key}"
                os.makedirs(os.path.dirname(path), exist_ok=True)
                with open(path, "wb") as f:
                    f.write(to_landing_bytes(env))
            descargados.append((rep, y))
            print(f"OK {rep} {y} ({motivo})")
        except Exception as e:
            print(f"ERROR {rep} {y}: {str(e).replace(key, '***')[:200]}")
    del key

# COMMAND ----------

# 5) Guardar la foto nueva (lo descargado + lo que ya estaba cargado)
ya = cargados()
ahora = datetime.now(timezone.utc).isoformat()
filas = []
for (rep, y), v in version.items():
    if (rep, y) not in ya:
        continue
    # Si Comtrade lo revisó y no se pudo re-bajar, queda la versión anterior y se reintenta la próxima
    vigente = v if (primera_vez or (rep, y) in descargados or (rep, y) not in previo) else previo[(rep, y)]
    filas.append((rep, y, vigente, ahora))
spark.createDataFrame(filas, "reporter int, anio int, version string, actualizado string") \
    .write.mode("overwrite").saveAsTable(SNAPSHOT)
print(f"Foto guardada: {len(filas)} país-año")

# COMMAND ----------

# 6) Si hubo datos nuevos, correr el pipeline completo
if descargados:
    print(f"Hay {len(descargados)} país-año nuevos: corriendo comex_pipeline...")
    dbutils.notebook.run(PIPELINE_NOTEBOOK, 7200)
    print("Pipeline terminado. El dashboard ya muestra los datos nuevos.")
else:
    print("Sin novedades en Comtrade. No se corre el pipeline.")

# COMMAND ----------

# 7) Si alguna descarga falló, la corrida termina en error (así llega el mail de aviso)
assert len(descargados) == len(nuevos), len(nuevos) - len(descargados)
