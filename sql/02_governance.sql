-- Copyright (c) 2026 Agustin Valenti Boursiac. Todos los derechos reservados.
-- Prohibido copiar, modificar o redistribuir sin permiso escrito del autor. Ver LICENSE.
-- Gobierno de datos en Unity Catalog: tags, comentarios, column masks y row filters.

-- Clasificación
ALTER TABLE gold.fact_export_opportunity SET TAGS ('domain' = 'trade-intelligence', 'tier' = 'gold', 'pii' = 'none');
ALTER TABLE silver.trade_flows SET TAGS ('domain' = 'trade-intelligence', 'tier' = 'silver', 'source' = 'comtrade,indec');

COMMENT ON COLUMN gold.fact_export_opportunity.opportunity_score IS
  'Score 0-100. Ponderación de percentiles: crecimiento, tamaño, RCA, headroom, apertura y precio. Ver docs/metrics.md';

-- Row filter: los analistas de una cámara sectorial sólo ven su sector (ej. grupo "camara-citricos").
CREATE OR REPLACE FUNCTION gold.sector_filter(hs6 STRING)
RETURN is_account_group_member('data-engineers')
    OR is_account_group_member('trade-analysts')
    OR (is_account_group_member('camara-citricos') AND hs6 = '080550');

ALTER TABLE gold.fact_export_opportunity SET ROW FILTER gold.sector_filter ON (hs6);

-- Linaje: Unity Catalog lo captura automáticamente (system.access.table_lineage).
