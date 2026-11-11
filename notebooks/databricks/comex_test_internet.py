# Databricks notebook source
# MAGIC %md
# MAGIC # Prueba: ¿Databricks llega a la API de UN Comtrade?
# MAGIC No usa la clave. Solo consulta la API pública de disponibilidad de datos. **Run all**.

# COMMAND ----------

import requests

URL = "https://comtradeapi.un.org/public/v1/getDA/C/A/HS"
try:
    r = requests.get(URL, params={"reporterCode": "32", "period": "2024,2025"}, timeout=30)
    print("Respuesta HTTP:", r.status_code)
    print(r.text[:500])
    print("\nRESULTADO: Databricks SI llega a Comtrade" if r.ok else "\nRESULTADO: llega, pero respondio con error")
except Exception as e:
    print("RESULTADO: Databricks NO llega a Comtrade")
    print(type(e).__name__, str(e)[:300])
