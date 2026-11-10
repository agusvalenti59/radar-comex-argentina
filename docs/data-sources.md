# Fuentes de datos

## UN Comtrade (API v1)

- **Endpoint**: `GET https://comtradeapi.un.org/data/v1/get/C/{freq}/HS` con header `Ocp-Apim-Subscription-Key`.
- **Parámetros usados**: `reporterCode`, `period`, `flowCode` (X/M), `cmdCode` (lista HS6 o `TOTAL`), `partner2Code=0`, `customsCode=C00`, `motCode=0`.
- **Cobertura**: anual, bilateral, HS6. Códigos de país M49 (ojo: India = 699, Francia = 251, EE.UU. = 842 en Comtrade).
- **Valores**: `primaryValue` (USD). Importaciones en CIF, exportaciones en FOB.
- **Limitaciones**: cuota diaria y tope de registros por request según plan; países reportan con rezago de 6-18 meses; revisiones retroactivas.
- **Plan de extracción**: por (reporter, año, flujo) para el universo HS6 del radar + `TOTAL` de exportaciones para el denominador del RCA.

### API pública (sin clave)

- **Base**: `https://comtradeapi.un.org/public/v1`. Endpoint de datos: `GET /preview/C/{freq}/HS` con los mismos parámetros que la API con clave y sin header.
- **Tope**: 500 registros por pedido. El cliente (`ComtradeClient` sin clave) lo usa automáticamente y el plan se parte por producto: un pedido por (reporter, año, flujo, HS6) devuelve una fila por socio, debajo del tope. Respuestas con 500 filas se marcan `_truncated`.
- **Cuándo se usa**: `sources.comtrade.api_mode` = `auto` (default: pública si el secreto no existe o es `CHANGE_ME`), `public` o `keyed`. Localmente, `scripts/fetch_comtrade_public.py` baja una muestra real chica.
- **Costo**: unas 10 veces más pedidos que con clave para el mismo universo. Sirve para pruebas y demos; para la carga productiva, usar la clave.
- **Otros endpoints útiles de la pública** (no integrados todavía):
  - `/getComtradeReleases`: qué países publicaron o revisaron datos y cuándo. Permitiría re-descargar sólo los reporters con novedades en vez de todo el plan (candidato para la fase 6).
  - `/getDA/C/{freq}/HS`: disponibilidad de datos por reporter y período, útil para no pedir años que todavía no existen.
  - `/getMetadata/C/{freq}/HS`: notas metodológicas por reporter.

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
