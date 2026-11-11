# Databricks notebook source
# MAGIC %md
# MAGIC # Guardar la clave de UN Comtrade (una sola vez)
# MAGIC 1. Corré la celda 1: aparece arriba un cuadro **clave_comtrade**.
# MAGIC 2. Pegá tu clave en ese cuadro (no en el código).
# MAGIC 3. Corré la celda 2: la guarda cifrada como secreto `comex / comtrade` y borra el cuadro.

# COMMAND ----------

dbutils.widgets.text("clave_comtrade", "", "Clave Comtrade")
print("Pegá la clave en el cuadro de arriba y después corré la celda 2.")

# COMMAND ----------

from databricks.sdk import WorkspaceClient

clave = dbutils.widgets.get("clave_comtrade").strip().strip('"').strip("'")
assert clave, "El cuadro clave_comtrade está vacío: pegá la clave y volvé a correr esta celda."

w = WorkspaceClient()
if "comex" not in [s.name for s in w.secrets.list_scopes()]:
    w.secrets.create_scope(scope="comex")
w.secrets.put_secret(scope="comex", key="comtrade", string_value=clave)
dbutils.widgets.removeAll()
del clave

ok = len(dbutils.secrets.get("comex", "comtrade")) > 0
print("Clave guardada como secreto comex/comtrade:", "OK" if ok else "ERROR")
