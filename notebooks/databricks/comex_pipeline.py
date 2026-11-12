# Databricks notebook source
# Copyright (c) 2026 Agustin Boursiac. Todos los derechos reservados.
# Prohibido copiar, modificar o redistribuir sin permiso escrito del autor. Ver LICENSE.
# MAGIC %md
# MAGIC # Pipeline COMEX en Databricks Free Edition
# MAGIC Corre landing -> bronze -> silver -> gold con el mismo código del repo (`src/trade_intel`),
# MAGIC sobre computo serverless y tablas Delta en Unity Catalog (catálogo `workspace`).
# MAGIC
# MAGIC Requisito: haber subido `comex.zip` (carpetas `src`, `conf` y `data` del proyecto) al volumen
# MAGIC `/Volumes/workspace/comex/archivos/`. Después: **Run all**.

# COMMAND ----------

# Parámetros (no hace falta tocarlos)
CATALOG = "workspace"
VOLUME = "/Volumes/workspace/comex/archivos"
ZIP_PATH = f"{VOLUME}/comex.zip"
PROJECT = f"{VOLUME}/proyecto"

# COMMAND ----------

# 1) Descomprimir el zip en el volumen.
#    Se arreglan las barras "\" que pone Compress-Archive de Windows PowerShell 5.1.
import os
import shutil
import zipfile

assert os.path.exists(ZIP_PATH), f"No encuentro {ZIP_PATH}. Subí comex.zip al volumen."
shutil.rmtree(PROJECT, ignore_errors=True)
n = 0
with zipfile.ZipFile(ZIP_PATH) as z:
    for info in z.infolist():
        name = info.filename.replace("\\", "/").lstrip("/")
        if not name or name.endswith("/") or "__pycache__" in name:
            continue
        dest = os.path.join(PROJECT, name)
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        with z.open(info) as src, open(dest, "wb") as out:
            shutil.copyfileobj(src, out)
        n += 1
print(f"{n} archivos extraídos en {PROJECT}")

# Sumar las descargas nuevas que dejó el notebook comex_actualizar (si las hay)
EXTRA = f"{VOLUME}/landing_extra"
if os.path.exists(f"{EXTRA}/comtrade"):
    shutil.copytree(EXTRA, f"{PROJECT}/data/full_sample", dirs_exist_ok=True)
    print("Se sumaron las descargas nuevas de", EXTRA)
print("Carpetas:", sorted(os.listdir(PROJECT)))

# COMMAND ----------

# 2) Preparar Python: código del repo + pyyaml.
import importlib.util
import subprocess
import sys

if importlib.util.find_spec("yaml") is None:
    subprocess.run([sys.executable, "-m", "pip", "install", "-q", "pyyaml"], check=True)

# Copia local del código (importar desde disco local es más confiable que desde el volumen)
shutil.rmtree("/tmp/comex_src", ignore_errors=True)
shutil.copytree(f"{PROJECT}/src", "/tmp/comex_src")
os.environ["TRADE_INTEL_CONF_DIR"] = f"{PROJECT}/conf"
sys.path.insert(0, "/tmp/comex_src")
for mod in [m for m in sys.modules if m.startswith("trade_intel")]:
    del sys.modules[mod]

# Serverless no permite df.cache()/persist(): los convertimos en no-op (solo afecta rendimiento).
from pyspark.sql import DataFrame as _DF
_classes = [_DF]
try:
    from pyspark.sql.connect.dataframe import DataFrame as _CDF
    _classes.append(_CDF)
except ImportError:
    pass
for _c in _classes:
    _c.cache = lambda self: self
    _c.persist = lambda self, *a, **k: self

spark.conf.set("spark.sql.session.timeZone", "UTC")

# COMMAND ----------

# 3) Configuración: conf/base.yml + overrides para Free Edition.
from trade_intel.config import Settings, load_settings

landing = f"{PROJECT}/data/full_sample"
if not os.path.exists(f"{landing}/comtrade"):
    landing = f"{PROJECT}/data/sample"
print("Landing usado:", landing)

base = load_settings("dev")
raw = dict(base.raw)
raw["catalog"] = CATALOG
raw["reference_dir"] = f"{PROJECT}/data/reference"
raw["schemas"] = {"bronze": "comex_bronze", "silver": "comex_silver", "gold": "comex_gold", "ops": "comex_ops"}
raw["storage"] = {**raw["storage"], "format": "delta", "landing_root": landing,
                  "checkpoint_root": f"{VOLUME}/_checkpoints"}
settings = Settings(env="free", raw=raw)

from trade_intel.common.table_io import TableIO
io = TableIO(spark, settings)
io.ensure_schemas()
print("Schemas listos:", [f"{CATALOG}.{settings.schema(l)}" for l in ("bronze", "silver", "gold", "ops")])

# COMMAND ----------

# 4) Bronze: lectura batch de los JSON/CSV del landing (mismo camino que en local) -> Delta.
from trade_intel.bronze.ingest import PARSERS, SOURCES, read_landing_batch
from trade_intel.common.audit import audited

for name, src in SOURCES.items():
    if not os.path.exists(f"{settings.landing_root}/{src.subdir}"):
        print(f"(sin datos de {name}, se omite)")
        continue
    with audited(io, f"bronze_{name}") as st:
        df = PARSERS[name](read_landing_batch(spark, settings, src))
        io.write(df, "bronze", src.table, mode="overwrite")
        st["rows_out"] = io.read("bronze", src.table).count()
    print(f"bronze.{src.table}: {st['rows_out']} filas")

# COMMAND ----------

# 5) Silver
from trade_intel.jobs import silver_build

with audited(io, "silver_build") as st:
    dq = silver_build.run(io, settings)
    st["rows_out"] = dq["rows_in"] - dq["rows_quarantined"]
print("Calidad silver.trade_flows:", dq)

# COMMAND ----------

# 6) Gold
from trade_intel.gold.build import build_gold

with audited(io, "gold_build") as st:
    counts = build_gold(io, settings)
    st["rows_out"] = sum(counts.values())
print("Tablas gold:", counts)

# COMMAND ----------

# 7) Top 10 oportunidades para ARG (último año)
from pyspark.sql import functions as F

opp = io.read("gold", "fact_export_opportunity")
last = opp.agg(F.max("year")).first()[0]
print("Año:", last)
display(
    opp.filter(F.col("year") == last)
    .join(io.read("gold", "dim_country").select(F.col("iso3").alias("market_iso3"), "name_es"), "market_iso3")
    .join(io.read("gold", "dim_product").select("hs6", "description_es"), "hs6")
    .select("name_es", "description_es", F.round(F.col("market_value_usd") / 1e6, 1).alias("mercado_musd"),
            F.round(F.col("cagr") * 100, 1).alias("cagr_pct"), F.round(F.col("home_share") * 100, 1).alias("share_arg_pct"),
            F.round("home_rca", 2).alias("rca_arg"), F.round("hhi", 0).alias("hhi"),
            "strategic_quadrant", "opportunity_score")
    .orderBy(F.desc("opportunity_score")).limit(10)
)

# COMMAND ----------

# 8) Corridas auditadas
display(io.read("ops", "pipeline_runs").select("task", "status", F.round("duration_s", 1).alias("s"), "rows_out", "error"))
