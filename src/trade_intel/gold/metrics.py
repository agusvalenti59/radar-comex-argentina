"""Métricas de inteligencia comercial (funciones puras sobre DataFrames, testeadas en tests/unit).

Notación: X = exportaciones, M = importaciones, k = producto HS6, p = mercado (importador),
s = proveedor (exportador), t = año. Todas las fórmulas están en docs/metrics.md.
"""

from __future__ import annotations

import math

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def annual_flows(silver: DataFrame, source: str = "comtrade") -> DataFrame:
    """Agrega a nivel anual (INDEC viene mensual; Comtrade anual pasa sin cambios)."""
    return (
        silver.filter(F.col("source") == source)
        .groupBy("year", "flow", "reporter_iso3", "partner_iso3", "hs6")
        .agg(
            F.sum("trade_value_usd").alias("value_usd"),
            F.sum("net_weight_kg").alias("net_weight_kg"),
        )
    )


def market_imports(flows: DataFrame) -> DataFrame:
    """Importaciones bilaterales del mercado p desde cada proveedor s (dato reportado por p)."""
    return (
        flows.filter((F.col("flow") == "M") & (F.col("partner_iso3") != "WLD") & (F.col("hs6") != "TOTAL"))
        .select(
            "year", "hs6",
            F.col("reporter_iso3").alias("market_iso3"),
            F.col("partner_iso3").alias("supplier_iso3"),
            "value_usd", "net_weight_kg",
        )
    )


def supplier_shares(imports: DataFrame) -> DataFrame:
    """Share de cada proveedor en el mercado p para el producto k y ranking competitivo."""
    w_mkt = Window.partitionBy("year", "market_iso3", "hs6")
    return (
        imports.withColumn("market_value_usd", F.sum("value_usd").over(w_mkt))
        .withColumn("share", F.col("value_usd") / F.col("market_value_usd"))
        .withColumn("rank", F.dense_rank().over(w_mkt.orderBy(F.col("value_usd").desc())))
    )


def concentration(shares: DataFrame) -> DataFrame:
    """Herfindahl-Hirschman Index (0-10.000) y CR3 por mercado-producto.

    HHI > 2.500 = mercado muy concentrado (criterio DOJ/FTC); entrar implica desplazar a un
    proveedor dominante.
    """
    return (
        shares.groupBy("year", "market_iso3", "hs6")
        .agg(
            F.first("market_value_usd").alias("market_value_usd"),
            (F.sum(F.col("share") ** 2) * 10_000).alias("hhi"),
            F.sum(F.when(F.col("rank") <= 3, F.col("share")).otherwise(0)).alias("cr3"),
            F.countDistinct("supplier_iso3").alias("n_suppliers"),
            F.max_by("supplier_iso3", "value_usd").alias("top_supplier_iso3"),
        )
        .withColumn(
            "concentration_level",
            F.when(F.col("hhi") > 2500, "alta").when(F.col("hhi") >= 1500, "moderada").otherwise("baja"),
        )
    )


def revealed_comparative_advantage(flows: DataFrame) -> DataFrame:
    """RCA de Balassa: (X_ck / X_c) / (X_wk / X_w), usando exportaciones al Mundo (WLD).

    El "mundo" es el universo de reporters extraídos: el índice es relativo a ese panel, lo
    que está documentado como limitación en docs/metrics.md.
    """
    x = flows.filter((F.col("flow") == "X") & (F.col("partner_iso3") == "WLD"))
    totals = x.filter(F.col("hs6") == "TOTAL").select(
        "year", F.col("reporter_iso3"), F.col("value_usd").alias("x_c_total")
    )
    by_prod = x.filter(F.col("hs6") != "TOTAL").select(
        "year", "reporter_iso3", "hs6", F.col("value_usd").alias("x_ck")
    )
    world_k = by_prod.groupBy("year", "hs6").agg(F.sum("x_ck").alias("x_wk"))
    world_total = totals.groupBy("year").agg(F.sum("x_c_total").alias("x_w_total"))
    return (
        by_prod.join(totals, ["year", "reporter_iso3"])
        .join(world_k, ["year", "hs6"])
        .join(world_total, "year")
        .withColumn("rca", (F.col("x_ck") / F.col("x_c_total")) / (F.col("x_wk") / F.col("x_w_total")))
        .withColumn("has_advantage", F.col("rca") >= 1)
        .select(F.col("reporter_iso3").alias("exporter_iso3"), "year", "hs6", "x_ck", "rca", "has_advantage")
    )


def market_growth(conc: DataFrame, years: int) -> DataFrame:
    """CAGR del tamaño de mercado entre t-n y t: (V_t / V_{t-n})^(1/n) - 1."""
    base = conc.select("year", "market_iso3", "hs6", "market_value_usd")
    past = base.select(
        (F.col("year") + years).alias("year"), "market_iso3", "hs6",
        F.col("market_value_usd").alias("market_value_past"),
    )
    return (
        base.join(past, ["year", "market_iso3", "hs6"], "left")
        .withColumn(
            "cagr",
            F.when(
                (F.col("market_value_past") > 0) & (F.col("market_value_usd") > 0),
                F.pow(F.col("market_value_usd") / F.col("market_value_past"), F.lit(1.0 / years)) - 1,
            ),
        )
        .drop("market_value_usd")
    )


def unit_value_benchmark(
    imports: DataFrame, home_iso3: str, z_threshold: float = 3.5, min_ratio: float = 2.0
) -> DataFrame:
    """Precio unitario (USD/kg CIF en destino) por proveedor vs. la mediana del mercado.

    - premium_vs_median: >0 se vende más caro que la mediana del mercado.
    - robust_z: (uv - mediana) / (1.4826 * MAD). |z| > umbral => outlier de precio (posible
      error de registro, nicho premium o subfacturación).
    - Con pocos proveedores el MAD es chico y el z se dispara, por eso además se exige
      significancia económica: precio >= `min_ratio` veces la mediana o <= 1/`min_ratio`.
    """
    uv = imports.filter(F.col("net_weight_kg") > 0).withColumn(
        "unit_value_usd_kg", F.col("value_usd") / F.col("net_weight_kg")
    )
    w = Window.partitionBy("year", "market_iso3", "hs6")
    uv = uv.withColumn("median_uv", F.percentile_approx("unit_value_usd_kg", 0.5, 10_000).over(w))
    uv = uv.withColumn("_abs_dev", F.abs(F.col("unit_value_usd_kg") - F.col("median_uv")))
    uv = uv.withColumn("mad", F.percentile_approx("_abs_dev", 0.5, 10_000).over(w))
    return (
        uv.withColumn(
            "robust_z",
            F.when(F.col("mad") > 0, (F.col("unit_value_usd_kg") - F.col("median_uv")) / (1.4826 * F.col("mad"))),
        )
        .withColumn("premium_vs_median", F.col("unit_value_usd_kg") / F.col("median_uv") - 1)
        .withColumn(
            "is_price_outlier",
            F.coalesce(
                (F.abs("robust_z") > z_threshold)
                & (F.abs(F.log(F.col("unit_value_usd_kg") / F.col("median_uv"))) >= math.log(min_ratio)),
                F.lit(False),
            ),
        )
        .withColumn("is_home", F.col("supplier_iso3") == home_iso3)
        .drop("_abs_dev")
    )


def mirror_gap(flows: DataFrame, home_iso3: str, cif_fob_factor: float, tolerance: float) -> DataFrame:
    """Estadística espejo: X declaradas por el país vs. M declaradas por el socio desde ese país.

    M_p<-c debería ser ~ X_c->p * factor CIF/FOB. Brechas grandes y persistentes indican
    triangulación, diferencias de clasificación, sub/sobrefacturación o destino final distinto.
    """
    x = flows.filter(
        (F.col("flow") == "X") & (F.col("reporter_iso3") == home_iso3)
        & (F.col("partner_iso3") != "WLD") & (F.col("hs6") != "TOTAL")
    ).select("year", "hs6", F.col("partner_iso3").alias("partner_iso3"),
             F.col("value_usd").alias("exports_reported_usd"),
             F.col("net_weight_kg").alias("exports_kg"))
    m = flows.filter(
        (F.col("flow") == "M") & (F.col("partner_iso3") == home_iso3) & (F.col("hs6") != "TOTAL")
    ).select("year", "hs6", F.col("reporter_iso3").alias("partner_iso3"),
             F.col("value_usd").alias("mirror_imports_usd"),
             F.col("net_weight_kg").alias("mirror_kg"))
    return (
        x.join(m, ["year", "hs6", "partner_iso3"], "full_outer")
        .withColumn("expected_imports_usd", F.col("exports_reported_usd") * F.lit(cif_fob_factor))
        .withColumn(
            "gap_ratio",
            F.when(F.col("expected_imports_usd") > 0,
                   F.col("mirror_imports_usd") / F.col("expected_imports_usd") - 1),
        )
        .withColumn("gap_usd", F.coalesce("mirror_imports_usd", F.lit(0.0)) - F.coalesce("expected_imports_usd", F.lit(0.0)))
        .withColumn(
            "mirror_status",
            F.when(F.col("exports_reported_usd").isNull(), "solo_espejo")
            .when(F.col("mirror_imports_usd").isNull(), "sin_espejo")
            .when(F.abs("gap_ratio") > tolerance, "discrepancia")
            .otherwise("consistente"),
        )
    )


def opportunity_score(
    conc: DataFrame,
    growth: DataFrame,
    shares: DataFrame,
    rca: DataFrame,
    uv: DataFrame,
    home_iso3: str,
    weights: dict,
    min_market_size_usd: float,
) -> DataFrame:
    """Score 0-100 por (año, mercado, producto) para el país exportador `home_iso3`.

    Cada componente se convierte a percentil (percent_rank) dentro del año para que sean
    comparables entre sí y robustos a outliers; luego se pondera con `weights`.
    """
    home_share = shares.filter(F.col("supplier_iso3") == home_iso3).select(
        "year", "market_iso3", "hs6", F.col("share").alias("home_share"), F.col("rank").alias("home_rank")
    )
    home_rca = rca.filter(F.col("exporter_iso3") == home_iso3).select("year", "hs6", F.col("rca").alias("home_rca"))
    home_uv = uv.filter("is_home").select(
        "year", "market_iso3", "hs6", F.col("premium_vs_median").alias("home_price_premium"),
        F.col("unit_value_usd_kg").alias("home_uv"), "median_uv",
    )

    base = (
        conc.join(growth, ["year", "market_iso3", "hs6"], "left")
        .join(home_share, ["year", "market_iso3", "hs6"], "left")
        .join(home_rca, ["year", "hs6"], "left")
        .join(home_uv, ["year", "market_iso3", "hs6"], "left")
        .filter(F.col("market_iso3") != home_iso3)
        .filter(F.col("market_value_usd") >= min_market_size_usd)
        .withColumn("home_share", F.coalesce("home_share", F.lit(0.0)))
    )

    w = Window.partitionBy("year")

    def pct(col: F.Column, name: str) -> tuple[str, F.Column]:
        # Nulos => 0.5 (neutro): no premiar ni castigar falta de dato
        return name, F.coalesce(F.percent_rank().over(w.orderBy(col)), F.lit(0.5))

    comps = dict(
        [
            pct(F.col("cagr").asc_nulls_first(), "s_market_growth"),
            pct(F.col("market_value_usd"), "s_market_size"),
            pct(F.col("home_rca").asc_nulls_first(), "s_rca"),
            pct(F.lit(1.0) - F.col("home_share"), "s_headroom"),
            pct(F.lit(10_000.0) - F.col("hhi"), "s_openness"),
        ]
    )
    scored = base
    for name, expr in comps.items():
        scored = scored.withColumn(name, expr)
    # Precio: más barato que la mediana = más competitivo. Sin presencia => neutro.
    scored = scored.withColumn(
        "s_price_competitiveness",
        F.when(F.col("home_price_premium").isNull(), F.lit(0.5)).otherwise(
            F.percent_rank().over(w.orderBy(F.col("home_price_premium").desc()))
        ),
    )

    score = sum(F.col(f"s_{k}") * F.lit(float(v)) for k, v in weights.items())
    total_w = float(sum(weights.values()))
    median_growth = F.percentile_approx("cagr", 0.5).over(w)
    return (
        scored.withColumn("opportunity_score", F.round(score / total_w * 100, 1))
        .withColumn("_median_growth", median_growth)
        .withColumn(
            "strategic_quadrant",
            F.when((F.col("cagr") >= F.col("_median_growth")) & (F.col("home_share") >= 0.10), "estrella")
            .when((F.col("cagr") >= F.col("_median_growth")) & (F.col("home_share") < 0.10), "oportunidad")
            .when(F.col("home_share") >= 0.10, "defender")
            .otherwise("monitorear"),
        )
        .drop("_median_growth")
    )
