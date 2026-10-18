# ADR 0001: Medallion con landing persistente sobre Unity Catalog

**Estado**: aceptada

## Contexto
Las fuentes son APIs con cuota (Comtrade) y archivos que se revisan retroactivamente (INDEC). Necesitamos poder re-procesar sin volver a descargar y gobernar el acceso por grupo.

## Decisión
- Zona **landing** separada con la respuesta cruda y metadata del request; bronze se alimenta sólo de landing.
- **Bronze/silver/gold/ops** como schemas de un catálogo de Unity Catalog por entorno (`trade_<env>`), tablas managed sobre el container `lakehouse`.
- Acceso a ADLS sólo vía Access Connector (managed identity); storage sin shared keys.
- Mantenimiento con `OPTIMIZE/VACUUM` programado; migrable a Predictive Optimization.

## Consecuencias
- (+) Re-procesamiento barato y auditable; linaje automático en UC; permisos centralizados.
- (−) Landing duplica storage (mitigado con lifecycle a cool a 30 días y borrado al año).
