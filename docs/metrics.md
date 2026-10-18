# Métricas y fórmulas

Notación: *c* país exportador, *p* mercado importador, *s* proveedor, *k* producto HS6, *t* año.

## Ventaja Comparativa Revelada (Balassa)

RCA<sub>ck</sub> = (X<sub>ck</sub> / X<sub>c</sub>) / (X<sub>wk</sub> / X<sub>w</sub>)

- RCA ≥ 1: el país exporta proporcionalmente más de *k* que el mundo → especialización.
- *Limitación*: "mundo" = panel de reporters extraídos, no los ~200 países. Ampliar `reporters_m49` acerca el índice al valor oficial.

## Share de proveedor y concentración

- share<sub>spk</sub> = M<sub>p←s,k</sub> / Σ<sub>s</sub> M<sub>p←s,k</sub> (datos declarados por el importador, más confiables que las exportaciones declaradas por cada proveedor).
- **HHI** = Σ share² × 10.000. < 1.500 baja, 1.500-2.500 moderada, > 2.500 alta.
- **CR3** = suma de los 3 mayores shares.

## Crecimiento del mercado

CAGR<sub>pk</sub> = (V<sub>t</sub> / V<sub>t−n</sub>)<sup>1/n</sup> − 1, con *n* = `gold.cagr_years` (5). Nulo si falta el año base.

## Valor unitario y outliers

- UV = valor USD / kg (CIF en destino).
- Mediana del mercado y **MAD**; robust z = (UV − mediana) / (1,4826 × MAD).
- Outlier si |z| > 3,5 **y** UV ≥ 2× o ≤ 0,5× la mediana. La segunda condición evita falsos positivos cuando hay pocos proveedores y el MAD es chico.
- `premium_vs_median` = UV / mediana − 1: posicionamiento de precio de ARG.

## Estadística espejo

- Esperado = X<sub>ARG→p</sub> × factor CIF/FOB (1,08 por defecto).
- gap = M<sub>p←ARG</sub> / esperado − 1.
- Estados: `consistente` (|gap| ≤ 25%), `discrepancia`, `sin_espejo` (el socio no reporta), `solo_espejo` (ARG no reporta).
- Causas típicas de discrepancia: triangulación vía hubs (Rotterdam, Uruguay), destino final desconocido al embarcar, diferencias de clasificación, sub/sobrefacturación.

## Score de oportunidad (0-100)

Para cada (año, mercado, producto) con mercado ≥ USD 5 M y mercado ≠ ARG:

| Componente | Variable | Mejor cuando | Peso |
|-----------|----------|-------------|------|
| Crecimiento | CAGR 5 años | más alto | 0,25 |
| Tamaño | valor de mercado | más alto | 0,20 |
| Competitividad | RCA de ARG | más alto | 0,20 |
| Headroom | 1 − share ARG | más espacio | 0,15 |
| Apertura | 10.000 − HHI | menos concentrado | 0,10 |
| Precio | premium ARG vs. mediana | más barato | 0,10 |

Cada variable se transforma a percentil dentro del año (`percent_rank`) y se pondera. Datos faltantes = 0,5 (neutro). Pesos configurables en `conf/base.yml`. Justificación en [ADR 0004](adr/0004-score-de-oportunidad.md).

**Cuadrante estratégico** (vs. crecimiento mediano y share 10%): `estrella` (crece y ARG es fuerte), `oportunidad` (crece y ARG es débil), `defender` (no crece y ARG es fuerte), `monitorear`.
