"""Modelo planta-mes de E2 con medidas de Silver y catálogos de SQLite.

No modifica las reglas de limpieza ni deduplica lote_id: concilia las métricas E1.
"""
from __future__ import annotations

import sqlite3
import pandas as pd
import numpy as np

from config import ANALYSIS_YEAR, MT_TO_LB, RAW_DIR, SILVER_DIR

PLANT_ALIASES = {
    "CERRO NORTE": "Cerro Norte", "P. CERRO NORTE": "Cerro Norte",
    "PLANTA CERRO NORTE": "Cerro Norte", "LOA": "Loa", "P. LOA": "Loa",
    "PLANTA LOA": "Loa", "SALADO": "Salado", "P. SALADO": "Salado",
    "PLANTA SALADO": "Salado", "ILLAPEL": "Illapel", "P. ILLAPEL": "Illapel",
    "PLANTA ILLAPEL": "Illapel",
}


def canonical_plant(value: object) -> str | None:
    key = " ".join(str(value).upper().strip().split())
    return PLANT_ALIASES.get(key)


def load_catalogues() -> tuple[pd.DataFrame, pd.DataFrame]:
    with sqlite3.connect(RAW_DIR / "origen_mina.sqlite") as db:
        plants = pd.read_sql_query("SELECT planta_id, nombre, region_id FROM plantas", db)
        regions = pd.read_sql_query("SELECT region_id, region FROM regiones", db)
    return plants, regions


def load_silver() -> dict[str, pd.DataFrame]:
    return {
        name: pd.read_parquet(SILVER_DIR / f"silver_{name}.parquet")
        for name in ["lotes", "costos", "laboratorio", "precio_cobre", "tipo_cambio"]
    }


def _last_available_by_month(source: pd.DataFrame, field: str) -> pd.DataFrame:
    df = source[["fecha", "valor"]].copy()
    df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
    df["valor"] = pd.to_numeric(df["valor"], errors="coerce")
    df = df.dropna(subset=["fecha", "valor"])
    df = df[df["fecha"].dt.year == ANALYSIS_YEAR]
    df["mes"] = df["fecha"].dt.strftime("%Y-%m")
    df = df.sort_values("fecha").drop_duplicates("mes", keep="last")
    return df[["mes", "valor"]].rename(columns={"valor": field})


def build_model(
    silver: dict[str, pd.DataFrame], plants: pd.DataFrame, regions: pd.DataFrame
) -> dict[str, pd.DataFrame]:
    """Produce dimensiones y hecho COMPLETO de 4 plantas x 12 meses.

    Las 48 claves existen aunque no haya costo, sin imputarlo. El denominador
    del costo por libra se construye lote a lote y se suma al grano planta-mes.
    """
    p = plants[["planta_id", "nombre", "region_id"]].copy()
    p["planta"] = p["nombre"].map(canonical_plant)
    if len(p) != 4 or p["planta_id"].duplicated().any() or p["planta"].isna().any() or p["planta"].duplicated().any():
        raise ValueError("El catálogo SQLite no identifica exactamente cuatro plantas únicas")
    region = regions[["region_id", "region"]].copy()
    p = p.merge(region, on="region_id", how="left", validate="many_to_one")
    # La región 9 de Illapel no existe en regiones; decisión documentada del equipo.
    illapel = p["planta"].eq("Illapel") & p["region"].isna()
    p.loc[illapel, "region"] = "Coquimbo"
    if p["region"].isna().any():
        raise ValueError("Existen otras plantas sin región válida; no se imputan")
    dim_planta = p[["planta_id", "planta", "region", "region_id"]].sort_values("planta_id").reset_index(drop=True)

    dim_tiempo = pd.DataFrame({"mes": [f"{ANALYSIS_YEAR}-{m:02d}" for m in range(1, 13)]})
    months = pd.to_datetime(dim_tiempo["mes"] + "-01")
    dim_tiempo["anio"] = months.dt.year
    dim_tiempo["trimestre"] = months.dt.quarter
    dim_tiempo["semestre"] = np.where(months.dt.month <= 6, 1, 2)
    for key, out_col in [("precio_cobre", "precio_cobre_usd_lb"), ("tipo_cambio", "dolar_observado_clp")]:
        dim_tiempo = dim_tiempo.merge(_last_available_by_month(silver[key], out_col), on="mes", how="left", validate="one_to_one")

    lotes = silver["lotes"]
    valid = lotes.loc[lotes["es_elegible"].fillna(False)].copy()
    if valid[["planta_id", "mes", "toneladas_tratadas", "ley_pct", "recuperacion_pct"]].isna().any().any():
        raise ValueError("Lote elegible con atributo indispensable nulo")
    ids = set(dim_planta["planta_id"].astype(int))
    if not set(valid["planta_id"].astype(int)).issubset(ids):
        raise ValueError("Lotes elegibles con planta_id inexistente")
    valid["fino_lb"] = valid["toneladas_tratadas"] * valid["ley_pct"] / 100 * valid["recuperacion_pct"] / 100 * MT_TO_LB
    valid["ton_por_rec"] = valid["toneladas_tratadas"] * valid["recuperacion_pct"] / 100
    grp = valid.groupby(["planta_id", "mes"], as_index=False, observed=True).agg(
        n_lotes=("lote_id", "size"),
        toneladas_tratadas=("toneladas_tratadas", "sum"),
        cobre_fino_lb_operacional=("fino_lb", "sum"),
        toneladas_por_recuperacion=("ton_por_rec", "sum"),
        ley_simple_pct=("ley_pct", "mean"),
    )
    grp["cobre_fino_lb_simple"] = grp["toneladas_por_recuperacion"] * grp["ley_simple_pct"] / 100 * MT_TO_LB
    grp = grp.drop(columns=["toneladas_por_recuperacion", "ley_simple_pct"])

    lab = silver["laboratorio"].dropna(subset=["lote_id", "ley_lab_prom"])[["lote_id", "ley_lab_prom"]].copy()
    if lab["lote_id"].duplicated().any():
        raise ValueError("Laboratorio contiene lote_id ambiguos; no se puede calibrar")
    matched = valid.merge(lab, on="lote_id", how="inner", validate="many_to_one")
    matched["num_lab"] = matched["toneladas_tratadas"] * matched["ley_lab_prom"]
    matched["den_lab"] = matched["toneladas_tratadas"] * matched["ley_pct"]
    calibration = matched.groupby("mes", as_index=False).agg(num_lab=("num_lab", "sum"), den_lab=("den_lab", "sum"))
    calibration["factor_calibracion"] = calibration["num_lab"] / calibration["den_lab"]
    n_pairs = matched.groupby(["planta_id", "mes"], as_index=False).agg(n_pares_laboratorio=("lote_id", "nunique"))

    costos = silver["costos"].copy()
    costos["planta"] = costos["planta"].map(canonical_plant)
    if costos["planta"].isna().any():
        raise ValueError("Costo con planta no reconocida: detener antes de publicar")
    costos = costos.merge(dim_planta[["planta", "planta_id"]], on="planta", how="left", validate="many_to_one")
    if costos.duplicated(subset=["planta_id", "mes"]).any():
        raise ValueError("Costos con dos filas para una misma clave planta-mes")
    cost_fields = ["energia_clp", "insumos_clp", "mano_obra_clp"]
    costos["costo_completo"] = costos[cost_fields].notna().all(axis=1)
    costos["costo_directo_clp"] = costos[cost_fields].sum(axis=1, min_count=3)
    costos.loc[~costos["costo_completo"], "costo_directo_clp"] = np.nan

    cross = (dim_planta[["planta_id"]].assign(_k=1)
             .merge(dim_tiempo[["mes"]].assign(_k=1), on="_k")
             .drop(columns="_k"))
    fact = cross.merge(grp, on=["planta_id", "mes"], how="left", validate="one_to_one")
    fact = fact.merge(costos[["planta_id", "mes", *cost_fields, "costo_directo_clp", "costo_completo"]],
                      on=["planta_id", "mes"], how="left", validate="one_to_one")
    fact = fact.merge(calibration[["mes", "factor_calibracion"]], on="mes", how="left", validate="many_to_one")
    fact = fact.merge(n_pairs, on=["planta_id", "mes"], how="left", validate="one_to_one")
    for column in ["n_lotes", "toneladas_tratadas", "cobre_fino_lb_operacional", "cobre_fino_lb_simple", "n_pares_laboratorio"]:
        fact[column] = fact[column].fillna(0)
    fact["costo_completo"] = fact["costo_completo"].map(lambda value: False if pd.isna(value) else bool(value)).astype(bool)
    fact["cobre_fino_lb_laboratorio"] = fact["cobre_fino_lb_operacional"] * fact["factor_calibracion"]
    fact = fact.merge(dim_tiempo[["mes", "dolar_observado_clp"]], on="mes", validate="many_to_one")
    fact["costo_directo_usd"] = fact["costo_directo_clp"] / fact["dolar_observado_clp"]
    fact = fact.drop(columns=["factor_calibracion", "dolar_observado_clp"])
    fact = fact[["planta_id", "mes", "n_lotes", "toneladas_tratadas", "cobre_fino_lb_operacional",
                 "cobre_fino_lb_laboratorio", "cobre_fino_lb_simple", *cost_fields,
                 "costo_directo_clp", "costo_directo_usd", "costo_completo", "n_pares_laboratorio"]]
    fact = fact.sort_values(["planta_id", "mes"]).reset_index(drop=True)
    return {"dim_planta": dim_planta, "dim_tiempo": dim_tiempo, "fact_planta_mes": fact}
