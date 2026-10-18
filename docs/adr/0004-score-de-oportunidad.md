# ADR 0004: Score de oportunidad por percentiles ponderados

**Estado**: aceptada

## Contexto
Hay que priorizar cientos de pares mercado-producto combinando variables con escalas muy distintas (USD, %, índices).

## Opciones
1. Min-max normalización: sensible a outliers (China domina todo por tamaño).
2. Z-score: asume normalidad; los tamaños de mercado son log-normales.
3. **Percentiles** (`percent_rank`) dentro del año.

## Decisión
Percentiles ponderados con pesos configurables, más un cuadrante estratégico interpretable. Datos faltantes = 0,5 para no premiar ni castigar ausencia de información. Filtro de tamaño mínimo para evitar mercados irrelevantes.

## Consecuencias
- (+) Robusto, explicable a no técnicos ("está en el top 10% de crecimiento").
- (−) Relativo al panel: el score de un par cambia si se agregan mercados. Se documenta y se compara siempre dentro del mismo corte.
- Revisión: calibrar pesos con el equipo comercial y backtesting (¿los pares con score alto en t−3 crecieron en exportaciones ARG?).
