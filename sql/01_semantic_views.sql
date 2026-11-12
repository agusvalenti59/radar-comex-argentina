-- Copyright (c) 2026 Agustin Valenti Boursiac. Todos los derechos reservados.
-- Prohibido copiar, modificar o redistribuir sin permiso escrito del autor. Ver LICENSE.
-- Capa semántica sobre gold: vistas que consume Power BI (DirectQuery vía SQL Warehouse).
-- Ejecutar con: USE CATALOG trade_<env>;

CREATE OR REPLACE VIEW gold.vw_opportunity_radar
COMMENT 'Radar de oportunidades de exportación para Argentina: un registro por mercado-producto-año'
AS
SELECT
  o.year,
  o.market_iso3,
  c.name_es            AS market_name,
  c.region,
  c.trade_bloc,
  o.hs6,
  p.description_es     AS product,
  p.sector,
  o.market_value_usd,
  o.cagr               AS market_cagr_5y,
  o.home_share         AS arg_share,
  o.home_rank          AS arg_rank,
  o.home_rca           AS arg_rca,
  o.hhi,
  o.concentration_level,
  o.top_supplier_iso3,
  o.home_price_premium AS arg_price_premium,
  o.opportunity_score,
  o.strategic_quadrant,
  -- USD de crecimiento potencial si ARG alcanzara el share del proveedor líder
  o.market_value_usd * GREATEST(o.cr3 / 3 - o.home_share, 0) AS headroom_usd
FROM gold.fact_export_opportunity o
JOIN gold.dim_country c ON c.iso3 = o.market_iso3
JOIN gold.dim_product p ON p.hs6  = o.hs6;

CREATE OR REPLACE VIEW gold.vw_competitor_landscape
COMMENT 'Competidores por mercado-producto: share, ranking, variación interanual y precio'
AS
SELECT
  s.year, s.market_iso3, s.hs6, s.supplier_iso3,
  cs.name_es AS supplier_name,
  s.value_usd, s.share, s.rank,
  s.share - LAG(s.share) OVER (PARTITION BY s.market_iso3, s.hs6, s.supplier_iso3 ORDER BY s.year) AS share_change_pp,
  u.unit_value_usd_kg, u.median_uv, u.premium_vs_median, u.is_price_outlier,
  s.supplier_iso3 = 'ARG' AS is_argentina
FROM gold.fact_supplier_share s
LEFT JOIN gold.fact_unit_value u
  ON u.year = s.year AND u.market_iso3 = s.market_iso3 AND u.hs6 = s.hs6 AND u.supplier_iso3 = s.supplier_iso3
JOIN gold.dim_country cs ON cs.iso3 = s.supplier_iso3;

CREATE OR REPLACE VIEW gold.vw_mirror_alerts
COMMENT 'Discrepancias entre exportaciones declaradas por ARG e importaciones declaradas por el socio'
AS
SELECT m.*, c.name_es AS partner_name, p.description_es AS product
FROM gold.fact_mirror_gap m
JOIN gold.dim_country c ON c.iso3 = m.partner_iso3
JOIN gold.dim_product p ON p.hs6  = m.hs6
WHERE m.mirror_status IN ('discrepancia', 'solo_espejo');

CREATE OR REPLACE VIEW gold.vw_ar_exports_monthly
COMMENT 'Exportaciones mensuales ARG (INDEC) en USD y ARS, con campaña agrícola'
AS
SELECT e.*, d.quarter, d.crop_year, p.description_es AS product, p.sector, c.name_es AS partner_name
FROM gold.fact_ar_exports_monthly e
JOIN gold.dim_date d    ON d.date_key = e.date_key
JOIN gold.dim_product p ON p.hs6 = e.hs6
LEFT JOIN gold.dim_country c ON c.iso3 = e.partner_iso3;
