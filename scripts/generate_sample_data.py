# Copyright (c) 2026 Agustin Valenti Boursiac. Todos los derechos reservados.
# Prohibido copiar, modificar o redistribuir sin permiso escrito del autor. Ver LICENSE.
"""Genera datos de MUESTRA SINTÉTICOS con el layout exacto de cada fuente.

No son datos oficiales: sirven para correr el pipeline y los tests sin credenciales.
Las magnitudes son plausibles (orden de magnitud de mercados reales) y se inyectan a
propósito casos que el pipeline debe detectar:
  - Discrepancia espejo: ARG -> NLD en limones (Rotterdam como hub de re-despacho).
  - Outlier de precio: un proveedor de vino en CHN con USD/kg 12x la mediana.
  - Filas inválidas: socio con código desconocido y valor negativo (van a cuarentena).

Uso: python scripts/generate_sample_data.py [--out data/sample]
"""

from __future__ import annotations

import argparse
import csv
import json
import random
from datetime import date, timedelta
from pathlib import Path

SEED = 42
YEARS = list(range(2018, 2025))
M49 = {"ARG": 32, "BRA": 76, "CHN": 156, "USA": 842, "DEU": 276, "ESP": 724, "ITA": 380,
       "NLD": 528, "CHL": 152, "MEX": 484, "RUS": 643, "ZAF": 710, "TUR": 792, "AUS": 36,
       "URY": 858, "FRA": 251, "IND": 699, "VNM": 704, "KOR": 410, "WLD": 0}
INDEC = {"BRA": 203, "CHN": 310, "USA": 212, "DEU": 438, "ESP": 410, "ITA": 417, "NLD": 423,
         "CHL": 208, "MEX": 218, "RUS": 444, "VNM": 337, "IND": 315}

# producto: (precio base USD/kg, {mercado: (tamaño USD 2018, crecimiento anual)}, proveedores {iso3: peso})
PRODUCTS = {
    "080550": (1.1, {"ESP": (90e6, 0.03), "NLD": (260e6, 0.05), "USA": (520e6, 0.06),
                     "RUS": (330e6, -0.04), "DEU": (310e6, 0.02), "ITA": (180e6, 0.04)},
               {"ESP": 3.0, "MEX": 2.5, "ARG": 2.0, "ZAF": 2.2, "TUR": 2.0, "CHL": 0.8}),
    "120242": (1.4, {"NLD": (420e6, 0.04), "DEU": (160e6, 0.03), "MEX": (150e6, 0.07),
                     "RUS": (140e6, 0.05), "VNM": (60e6, 0.12), "ESP": (55e6, 0.02)},
               {"ARG": 3.5, "USA": 2.5, "BRA": 1.5, "CHN": 1.0, "IND": 1.2}),
    "220421": (3.2, {"USA": (4200e6, 0.03), "DEU": (2300e6, 0.0), "CHN": (1900e6, -0.06),
                     "BRA": (380e6, 0.06), "MEX": (290e6, 0.08), "NLD": (900e6, 0.02)},
               {"FRA": 3.5, "ITA": 3.0, "ESP": 1.8, "CHL": 1.5, "ARG": 1.0, "AUS": 1.5}),
    "020230": (5.5, {"CHN": (4800e6, 0.09), "USA": (2600e6, 0.05), "RUS": (550e6, -0.05),
                     "CHL": (600e6, 0.04), "ITA": (420e6, 0.01), "DEU": (380e6, 0.02)},
               {"BRA": 4.0, "ARG": 2.5, "AUS": 3.0, "URY": 1.5, "USA": 1.0}),
    "283691": (14.0, {"CHN": (900e6, 0.35), "KOR": (500e6, 0.25), "USA": (200e6, 0.20),
                      "DEU": (60e6, 0.30), "IND": (40e6, 0.30)},
               {"CHL": 4.0, "ARG": 1.8, "CHN": 0.6, "USA": 0.4}),
}
EXPORT_TOTAL_2018 = {"ARG": 61e9, "BRA": 231e9, "CHN": 2487e9, "USA": 1664e9, "DEU": 1560e9,
                     "ESP": 345e9, "ITA": 547e9, "NLD": 726e9, "CHL": 75e9, "MEX": 450e9,
                     "RUS": 450e9, "ZAF": 94e9, "TUR": 177e9, "AUS": 257e9, "URY": 9e9,
                     "FRA": 568e9, "IND": 324e9, "VNM": 243e9, "KOR": 605e9}
CIF_FOB = 1.08


def rec(year, reporter, flow, partner, hs6, value, kg):
    return {"typeCode": "C", "freqCode": "A", "refPeriodId": int(f"{year}0101"), "refYear": year,
            "refMonth": 52, "period": str(year), "reporterCode": M49.get(reporter, reporter),
            "flowCode": flow, "partnerCode": M49.get(partner, partner), "partner2Code": 0,
            "classificationCode": "H5", "cmdCode": hs6, "customsCode": "C00", "mosCode": "0",
            "motCode": 0, "qtyUnitCode": 8, "qty": round(kg, 1) if kg else None,
            "netWgt": round(kg, 1) if kg else None, "grossWgt": None,
            "cifvalue": round(value, 2) if flow == "M" else None,
            "fobvalue": round(value, 2) if flow == "X" else None,
            "primaryValue": round(value, 2), "isReported": True, "isAggregate": False}


def build(out: Path) -> None:
    rnd = random.Random(SEED)
    data: dict[tuple, list] = {}  # (year, reporter, flow) -> records

    def add(year, reporter, flow, r):
        data.setdefault((year, reporter, flow), []).append(r)

    for hs6, (base_uv, markets, suppliers) in PRODUCTS.items():
        sup_uv = {s: base_uv * rnd.uniform(0.75, 1.3) for s in suppliers}
        for year in YEARS:
            t = year - YEARS[0]
            exports_by_sup = {s: 0.0 for s in suppliers}
            for mkt, (size0, g) in markets.items():
                size = size0 * (1 + g) ** t * rnd.uniform(0.92, 1.08)
                w = {s: wt * rnd.uniform(0.6, 1.4) for s, wt in suppliers.items() if s != mkt}
                tot_w = sum(w.values())
                wld_val = wld_kg = 0.0
                for s, wt in w.items():
                    val = size * wt / tot_w
                    uv = sup_uv[s] * (1 + 0.02 * t) * rnd.uniform(0.9, 1.1)
                    if hs6 == "220421" and mkt == "CHN" and s == "FRA":
                        uv *= 12  # outlier de precio inyectado
                    kg = val / uv
                    add(year, mkt, "M", rec(year, mkt, "M", s, hs6, val, kg))
                    wld_val += val
                    wld_kg += kg
                    # Exportación espejo declarada por el proveedor (FOB)
                    x_val = val / CIF_FOB * rnd.uniform(0.95, 1.05)
                    if s == "ARG" and mkt == "NLD" and hs6 == "080550":
                        x_val *= 0.6  # ARG declara menos de lo que NLD registra
                    add(year, s, "X", rec(year, s, "X", mkt, hs6, x_val, kg))
                    exports_by_sup[s] += x_val
                add(year, mkt, "M", rec(year, mkt, "M", "WLD", hs6, wld_val, wld_kg))
            for s, v in exports_by_sup.items():
                rest_of_world = rnd.uniform(1.2, 1.8)  # mercados fuera del panel
                add(year, s, "X", rec(year, s, "X", "WLD", hs6, v * rest_of_world, None))

    for year in YEARS:
        t = year - YEARS[0]
        for rep, tot in EXPORT_TOTAL_2018.items():
            add(year, rep, "X", rec(year, rep, "X", "WLD", "TOTAL", tot * (1.03 ** t), None))
    # Filas inválidas para demostrar cuarentena
    add(2024, "ESP", "M", rec(2024, "ESP", "M", 999, "080550", 1_000_000, 900_000))
    add(2024, "USA", "M", rec(2024, "USA", "M", "MEX", "080550", -50_000, 10_000))

    for (year, rep, flow), rows in data.items():
        p = out / f"comtrade/freq=A/year={year}/reporter={M49[rep]}/flow={flow}/part-0000.json"
        p.parent.mkdir(parents=True, exist_ok=True)
        env = {"_source": "comtrade", "_request": {"reporterCode": str(M49[rep]), "period": str(year),
               "flowCode": flow, "freq": "A"}, "_fetched_at": f"{year + 1}-07-01T00:00:00+00:00",
               "_truncated": False, "count": len(rows), "data": rows}
        p.write_text(json.dumps(env))

    # INDEC: exportaciones mensuales de ARG por NCM 8d (2023-2024), con estacionalidad
    seasonality = {"080550": [0.2, 0.3, 0.6, 1.2, 1.9, 2.1, 2.0, 1.6, 1.0, 0.5, 0.3, 0.3],
                   "120242": [0.6, 0.5, 0.6, 0.9, 1.2, 1.4, 1.4, 1.3, 1.1, 1.0, 1.0, 1.0]}
    for year in (2023, 2024):
        p = out / f"indec/exports/expo_{year}.csv"
        p.parent.mkdir(parents=True, exist_ok=True)
        with p.open("w", newline="", encoding="ISO-8859-1") as fh:
            wr = csv.writer(fh, delimiter=";")
            wr.writerow(["Año", "Mes", "NCM", "Pdes", "Pnet(kg)", "FOB(u$s)"])
            for (y, rep, flow), rows in data.items():
                if y != year or rep != "ARG" or flow != "X":
                    continue
                for r in rows:
                    iso = next((k for k, v in M49.items() if v == r["partnerCode"]), None)
                    if iso not in INDEC or r["cmdCode"] == "TOTAL":
                        continue
                    seas = seasonality.get(r["cmdCode"], [1.0] * 12)
                    s_tot = sum(seas)
                    for mes in range(1, 13):
                        share = seas[mes - 1] / s_tot
                        for suffix, split in (("00", 0.7), ("90", 0.3)):  # 2 NCM por HS6
                            fob = r["primaryValue"] * share * split
                            kg = (r["netWgt"] or 0) * share * split
                            wr.writerow([year, mes, f"{r['cmdCode'][:4]}.{r['cmdCode'][4:]}.{suffix}",
                                         INDEC[iso], f"{kg:.0f}", f"{fob:.2f}".replace(".", ",")])

    # BCRA: tipo de cambio diario USD/ARS sintético con salto discreto (dic-2023)
    d, rate, results = date(2023, 1, 2), 178.0, []
    while d <= date(2024, 12, 31):
        if d.weekday() < 5:
            if d == date(2023, 12, 13):
                rate = 800.0
            rate *= 1 + (0.0022 if d < date(2023, 12, 13) else 0.0009) + rnd.uniform(-0.001, 0.001)
            results.append({"fecha": d.isoformat(), "detalle": [
                {"codigoMoneda": "USD", "descripcion": "DOLAR ESTADOUNIDENSE", "tipoPase": 1.0,
                 "tipoCotizacion": round(rate, 4)}]})
        d += timedelta(days=1)
    p = out / "bcra/currency=USD/quotes.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps({"_source": "bcra", "_request": {"currency": "USD"},
                             "_fetched_at": "2025-01-01T00:00:00+00:00", "results": results}))
    print(f"sample data written to {out} ({sum(len(v) for v in data.values())} comtrade rows)")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="data/sample")
    build(Path(ap.parse_args().out))
