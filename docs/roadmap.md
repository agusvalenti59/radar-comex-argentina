# Plan por fases

Cada fase termina en algo demostrable. El estado actual del repo cubre las fases 0 a 3 en código; las fases 4 y 5 están diseñadas y parcialmente implementadas.

| Fase | Objetivo | Entregables | Estado |
|------|----------|-------------|--------|
| **0. Fundaciones** | Repo ejecutable en local | Estructura, config por entorno, paquete Python, tests con Spark, CI | ✅ |
| **1. Infraestructura** | Plataforma Azure reproducible | Terraform: RG, ADLS Gen2, Key Vault, Databricks + Access Connector, ADF, Log Analytics, Unity Catalog (catálogo, schemas, volume, grants, warehouse) | ✅ código · ⏳ `apply` en suscripción real |
| **2. Ingesta + bronze** | Datos oficiales aterrizando solos | Clientes Comtrade/BCRA con rate limit y reintentos, pipeline ADF para INDEC, Auto Loader, file-arrival trigger | ✅ código · ⏳ validar layout INDEC real |
| **3. Silver + gold** | Métricas de negocio confiables | Modelo unificado Comtrade+INDEC, reglas DQ + cuarentena, RCA, HHI, CAGR, valores unitarios, espejo, score de oportunidad, estrella | ✅ |
| **4. Consumo** | Analistas usando el radar | Vistas semánticas, row filters por sector, SQL Warehouse, reporte Power BI de 4 páginas | ✅ SQL · ⏳ `.pbix` |
| **5. Operación** | Correr sin supervisión | Auditoría, frescura con SLA, dashboard operativo con system tables, alertas, runbook, mantenimiento Delta | ✅ base · ⏳ alertas SQL en workspace |

## Próximas extensiones (fase 6+), por valor de negocio

1. **Aranceles y preferencias** (WITS/TRAINS, MacMap): sumar al score el arancel efectivo que paga ARG vs. competidores (ej. ventaja Mercosur-UE cuando entre en vigor).
2. **Distancia y costo logístico** (CEPII GeoDist): ajustar el score por gravedad.
3. **Nowcasting mensual**: usar Comtrade mensual + INDEC para anticipar el anual con 6-9 meses de ventaja.
4. **Concordancias HS entre versiones** (HS2017↔HS2022) con tablas de UN para series largas sin quiebres.
5. **Alertas proactivas**: notificar cuando un competidor pierde > 5 pp de share en un mercado donde ARG tiene RCA > 1 (ventana de entrada).
6. **Importaciones argentinas** desde INDEC para análisis de dependencia de insumos.

## Riesgos y mitigaciones

| Riesgo | Mitigación |
|--------|-----------|
| Cuota/límite de la API de Comtrade | Landing como caché persistente; pedidos particionados; flag `_truncated` |
| Cambios de layout en archivos INDEC | Bronze sin tipar + `_rescued_data`; reglas DQ detectan códigos de país sin mapear |
| Rezago de reporte de países en Comtrade | MERGE idempotente + corrida semanal; el último año se marca como provisorio |
| Cambio de versión HS | `classificationCode` en bronze; concordancias en fase 6 |
