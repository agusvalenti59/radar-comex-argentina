# Capa de consumo: Power BI

## Conexión

- Conector **Azure Databricks** → SQL Warehouse `wh-trade-bi-<env>` (serverless).
- Autenticación: Entra ID (SSO). Los permisos los aplica Unity Catalog (grants + row filters), no el modelo de Power BI.
- Modo: **DirectQuery** para `vw_opportunity_radar` y `vw_competitor_landscape` (volumen chico, siempre fresco); **Import** con refresh diario para `vw_ar_exports_monthly`.

## Modelo (estrella)

```
dim_country (iso3) ──┐
dim_product (hs6) ───┼── vw_opportunity_radar   (year, market_iso3, hs6)
dim_date (date_key) ─┤── vw_competitor_landscape (year, market_iso3, hs6, supplier_iso3)
                     └── vw_ar_exports_monthly   (date_key, partner_iso3, hs6)
```

`dim_country` se usa dos veces (rol mercado y rol proveedor): relación activa con `market_iso3`, inactiva con `supplier_iso3` y `USERELATIONSHIP` en las medidas de competidores.

## Páginas del reporte

1. **Radar de oportunidades**: scatter CAGR (x) vs. share ARG (y), tamaño = mercado USD, color = cuadrante estratégico. Tabla top-N por score.
2. **Paisaje competitivo**: por mercado-producto, ranking de proveedores, variación de share en pp y precio unitario vs. mediana.
3. **Alertas espejo**: mapa de calor socio × producto con `gap_ratio`, drill a la serie histórica.
4. **Exportaciones mensuales**: USD y ARS, campaña agrícola, estacionalidad.

## Medidas DAX

```DAX
Mercado USD = SUM ( vw_opportunity_radar[market_value_usd] )

Share ARG = DIVIDE (
    CALCULATE ( SUM ( vw_competitor_landscape[value_usd] ), vw_competitor_landscape[is_argentina] = TRUE () ),
    SUM ( vw_competitor_landscape[value_usd] )
)

Share ARG Δ pp =
VAR cur = [Share ARG]
VAR prev = CALCULATE ( [Share ARG], vw_competitor_landscape[year] = MAX ( vw_competitor_landscape[year] ) - 1 )
RETURN ( cur - prev ) * 100

Score Oportunidad = AVERAGE ( vw_opportunity_radar[opportunity_score] )

Exportaciones ARS YoY % =
VAR cur = SUM ( vw_ar_exports_monthly[fob_ars] )
VAR prev = CALCULATE ( SUM ( vw_ar_exports_monthly[fob_ars] ), SAMEPERIODLASTYEAR ( dim_date[period_date] ) )
RETURN DIVIDE ( cur - prev, prev )
```

> En contexto de alta inflación, comparar ARS nominales interanuales distorsiona: el reporte muestra USD como métrica principal y ARS como referencia.
