# ADR 0002: ADF para archivos, Databricks Workflows para APIs y transformación

**Estado**: aceptada

## Contexto
Hay dos tipos de ingesta: archivos zip públicos (INDEC) y APIs REST con autenticación, cuota y paginación (Comtrade, BCRA).

## Opciones
1. Todo en ADF (Copy + REST + Databricks activity).
2. Todo en Databricks Workflows.
3. Híbrido.

## Decisión
**Híbrido**, desacoplado por storage:
- ADF baja y descomprime los archivos de INDEC a landing (sin código, con reintentos y monitoreo nativo).
- El job `trade-intel-daily` arranca por **file-arrival trigger** sobre `landing/indec/`. ADF no necesita conocer Databricks.
- Las APIs se consumen desde Python (rate limit, partición de pedidos, detección de truncado, tests con `responses`).

## Consecuencias
- (+) Cada herramienta hace lo que mejor sabe; no hay acoplamiento ADF→Databricks por token.
- (−) Dos lugares para mirar el monitoreo (mitigado: ambos mandan logs a Log Analytics).
