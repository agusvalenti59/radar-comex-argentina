# ADR 0003: Jobs PySpark como camino principal, DLT como variante

**Estado**: aceptada

## Contexto
Lakeflow Declarative Pipelines (DLT) da expectativas, linaje y manejo de CDC listos. Pero las métricas gold usan ventanas, percentiles y joins multi-tabla que conviene testear como funciones puras.

## Decisión
- La lógica vive en `src/trade_intel` como funciones `DataFrame -> DataFrame` con tests unitarios.
- El camino principal son Workflows con tareas wheel (bronze → silver → gold).
- `pipelines/dlt_silver.py` implementa bronze+silver de Comtrade en DLT **importando las mismas funciones y reglas**, para comparar operación y costo con datos reales antes de elegir uno.

## Consecuencias
- (+) El cambio de motor no reescribe lógica de negocio. Tests corren en CI sin workspace.
- (−) Dos caminos que mantener hasta decidir (criterio de salida: 1 mes en dev con ambos).
