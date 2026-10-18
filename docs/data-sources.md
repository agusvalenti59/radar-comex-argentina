# Fuentes de datos

## UN Comtrade (API v1)

- **Endpoint**: `GET https://comtradeapi.un.org/data/v1/get/C/{freq}/HS` con header `Ocp-Apim-Subscription-Key`.
- **Parámetros usados**: `reporterCode`, `period`, `flowCode` (X/M), `cmdCode` (lista HS6 o `TOTAL`), `partner2Code=0`, `customsCode=C00`, `motCode=0`.
- **Cobertura**: anual, bilateral, HS6. Códigos de país M49 (ojo: India = 699, Francia = 251, EE.UU. = 842 en Comtrade).
- **Valores**: `primaryValue` (USD). Importaciones en CIF, exportaciones en FOB.
- **Limitaciones**: cuota diaria y tope de registros por request según plan; países reportan con rezago de 6-18 meses; revisiones retroactivas.
- **Plan de extracción**: por (reporter, año, flujo) para el universo HS6 del radar + `TOTAL` de exportaciones para el denominador del RCA.

## INDEC: comercio exterior por NCM

- **Origen**: bases mensuales de exportaciones e importaciones (zip con CSV) publicadas por INDEC.
- **Layout esperado** (separador `;`, encoding ISO-8859-1): `Año;Mes;NCM;Pdes;Pnet(kg);FOB(u$s)`.
- **NCM → HS6**: primeros 6 dígitos (la NCM es la extensión Mercosur del Sistema Armonizado).
- **Países**: código propio de INDEC (`Pdes`), mapeado a ISO3 en `data/reference/countries.csv` (`indec_code`).
- ⚠️ **A validar en la primera descarga real**: la URL exacta de cada zip (parametrizada en el pipeline de ADF), el layout de columnas y la tabla de códigos de país de INDEC. Los códigos cargados en el seed son un punto de partida y deben contrastarse con la tabla oficial; las filas con códigos sin mapear caen en cuarentena (`known_partner`), así que un error de mapeo es visible, no silencioso.

## BCRA: Estadísticas Cambiarias (API v1.0)

- **Endpoint**: `GET https://api.bcra.gob.ar/estadisticascambiarias/v1.0/Cotizaciones/{moneda}?fechadesde=&fechahasta=`.
- **Uso**: cotización diaria (`tipoCotizacion`) para convertir exportaciones mensuales a ARS; promedio y cierre mensual en `silver.fx_monthly`.
- **Paginación**: ventanas anuales.

## Seeds de referencia

| Archivo | Contenido | Mantenimiento |
|---------|-----------|---------------|
| `data/reference/countries.csv` | M49, ISO3, nombre, región, bloque, código INDEC | Versionado en git, subido al Volume `ops.reference` por el CD |
| `data/reference/products.csv` | Universo HS6 del radar con descripción y sector | Ídem; agregar un producto = PR + agregarlo en `conf/base.yml` |

## Datos de muestra

`data/sample/` es **sintético**: lo genera `scripts/generate_sample_data.py` con semilla fija y respeta el formato exacto de cada fuente. No usar para conclusiones de negocio.
