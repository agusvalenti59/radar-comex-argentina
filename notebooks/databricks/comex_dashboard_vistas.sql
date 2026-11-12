-- Databricks notebook source
-- Copyright (c) 2026 Agustin Valenti Boursiac. Todos los derechos reservados.
-- Prohibido copiar, modificar o redistribuir sin permiso escrito del autor. Ver LICENSE.
-- MAGIC %md
-- MAGIC # Vistas para el dashboard COMEX
-- MAGIC Crea el esquema `workspace.comex_dash` con 4 vistas listas para el dashboard.
-- MAGIC Las vistas leen siempre el último año disponible, así que cuando el pipeline se re-corre
-- MAGIC con datos nuevos el dashboard se actualiza solo. Ejecutar una vez con **Run all**.

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS workspace.comex_dash;

-- COMMAND ----------

-- 1) Oportunidades de exportación para Argentina (último año)
CREATE OR REPLACE VIEW workspace.comex_dash.v_oportunidades AS
WITH ultimo AS (SELECT max(year) AS y FROM workspace.comex_gold.fact_export_opportunity)
SELECT
  o.year                                   AS anio,
  coalesce(c.name_es, o.market_iso3)       AS mercado,
  o.market_iso3                            AS mercado_iso3,
  coalesce(c.region, 'Otra')               AS region,
  o.hs6,
  coalesce(p.description_es, o.hs6)        AS producto,
  coalesce(p.sector, 'Otro')               AS sector,
  round(o.opportunity_score, 1)            AS score,
  o.strategic_quadrant                     AS cuadrante,
  round(o.market_value_usd / 1e6, 1)       AS mercado_musd,
  round(o.cagr * 100, 1)                   AS crecimiento_anual_pct,
  round(o.home_share * 100, 1)             AS participacion_arg_pct,
  round(o.home_rca, 2)                     AS ventaja_comparativa_arg,
  round(o.hhi, 0)                          AS hhi,
  o.concentration_level                    AS concentracion,
  coalesce(ct.name_es, o.top_supplier_iso3) AS proveedor_principal,
  round(o.home_price_premium * 100, 1)     AS precio_arg_vs_mediana_pct
FROM workspace.comex_gold.fact_export_opportunity o
JOIN ultimo u ON o.year = u.y
LEFT JOIN workspace.comex_gold.dim_country c  ON c.iso3 = o.market_iso3
LEFT JOIN workspace.comex_gold.dim_country ct ON ct.iso3 = o.top_supplier_iso3
LEFT JOIN workspace.comex_gold.dim_product p  ON p.hs6 = o.hs6
ORDER BY score DESC;

-- COMMAND ----------

-- 2) Alertas de estadística espejo: lo que ARG declara exportar vs. lo que el socio declara importar.
--    Solo discrepancias mayores al 25% (configurable en base.yml) y de al menos USD 1 millón.
CREATE OR REPLACE VIEW workspace.comex_dash.v_alertas_espejo AS
WITH ultimo AS (SELECT max(year) AS y FROM workspace.comex_gold.fact_mirror_gap)
SELECT
  g.year                                         AS anio,
  coalesce(c.name_es, g.partner_iso3)            AS socio,
  g.hs6,
  coalesce(p.description_es, g.hs6)              AS producto,
  coalesce(p.sector, 'Otro')                     AS sector,
  round(g.exports_reported_usd / 1e6, 2)         AS exporta_arg_musd,
  round(g.mirror_imports_usd / 1e6, 2)           AS importa_socio_musd,
  round(g.gap_ratio * 100, 1)                    AS brecha_pct,
  round(g.gap_usd / 1e6, 2)                      AS brecha_musd,
  CASE WHEN g.gap_usd > 0 THEN 'Socio declara mas' ELSE 'Socio declara menos' END AS sentido
FROM workspace.comex_gold.fact_mirror_gap g
JOIN ultimo u ON g.year = u.y
LEFT JOIN workspace.comex_gold.dim_country c ON c.iso3 = g.partner_iso3
LEFT JOIN workspace.comex_gold.dim_product p ON p.hs6 = g.hs6
WHERE g.mirror_status = 'discrepancia'
  AND abs(g.gap_usd) >= 1000000
ORDER BY abs(g.gap_usd) DESC;

-- COMMAND ----------

-- 3) Precios atípicos (outliers de USD/kg) sin el ruido de envíos chicos:
--    se exige al menos 20 toneladas y USD 250 mil en el flujo.
CREATE OR REPLACE VIEW workspace.comex_dash.v_precios_atipicos AS
WITH ultimo AS (SELECT max(year) AS y FROM workspace.comex_gold.fact_unit_value)
SELECT
  v.year                                          AS anio,
  coalesce(cm.name_es, v.market_iso3)             AS mercado,
  coalesce(cs.name_es, v.supplier_iso3)           AS proveedor,
  v.is_home                                       AS es_argentina,
  v.hs6,
  coalesce(p.description_es, v.hs6)               AS producto,
  round(v.value_usd / 1e6, 2)                     AS valor_musd,
  round(v.net_weight_kg / 1000, 0)                AS toneladas,
  round(v.unit_value_usd_kg, 2)                   AS precio_usd_kg,
  round(v.median_uv, 2)                           AS mediana_mercado_usd_kg,
  round(v.premium_vs_median * 100, 0)             AS dif_vs_mediana_pct,
  CASE WHEN v.premium_vs_median > 0 THEN 'Mas caro' ELSE 'Mas barato' END AS tipo
FROM workspace.comex_gold.fact_unit_value v
JOIN ultimo u ON v.year = u.y
LEFT JOIN workspace.comex_gold.dim_country cm ON cm.iso3 = v.market_iso3
LEFT JOIN workspace.comex_gold.dim_country cs ON cs.iso3 = v.supplier_iso3
LEFT JOIN workspace.comex_gold.dim_product p  ON p.hs6 = v.hs6
WHERE v.is_price_outlier
  AND v.net_weight_kg >= 20000
  AND v.value_usd >= 250000
ORDER BY abs(v.premium_vs_median) DESC;

-- COMMAND ----------

-- 4) Números de resumen para la fila de KPIs
CREATE OR REPLACE VIEW workspace.comex_dash.v_resumen AS
SELECT
  (SELECT max(anio) FROM workspace.comex_dash.v_oportunidades)                                   AS anio,
  (SELECT count(*) FROM workspace.comex_dash.v_oportunidades WHERE score >= 70)                  AS oportunidades_score_70,
  (SELECT count(*) FROM workspace.comex_dash.v_oportunidades WHERE cuadrante = 'oportunidad')    AS cuadrante_oportunidad,
  (SELECT count(*) FROM workspace.comex_dash.v_alertas_espejo)                                   AS alertas_espejo,
  (SELECT count(*) FROM workspace.comex_dash.v_precios_atipicos)                                 AS precios_atipicos,
  (SELECT max(_ingest_ts) FROM workspace.comex_bronze.comtrade_raw)                              AS ultima_carga;

-- COMMAND ----------

-- Control: cuántas filas trae cada vista
SELECT 'oportunidades' AS vista, count(*) AS filas FROM workspace.comex_dash.v_oportunidades
UNION ALL SELECT 'alertas_espejo', count(*) FROM workspace.comex_dash.v_alertas_espejo
UNION ALL SELECT 'precios_atipicos', count(*) FROM workspace.comex_dash.v_precios_atipicos;

-- COMMAND ----------

SELECT * FROM workspace.comex_dash.v_resumen;
