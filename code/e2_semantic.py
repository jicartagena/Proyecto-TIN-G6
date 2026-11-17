"""Definición semántica única de la E2: costo directo de planta por libra.

Usar este módulo desde prototipo, gráficos e informe; no promediar tasas.
"""
from __future__ import annotations
import pandas as pd
import numpy as np

DENOMINATORS = {
    "operacional": "cobre_fino_lb_operacional",
    "laboratorio": "cobre_fino_lb_laboratorio",
    "simple": "cobre_fino_lb_simple",
}

KPI_COLUMN = "costo_directo_planta_por_libra_usd_lb"
COST_COMPONENTS = ["energia_clp", "insumos_clp", "mano_obra_clp"]
EXPECTED_PLANTS = 4


def costo_directo_planta_por_libra(
    fact: pd.DataFrame, *, definicion: str = "operacional", nivel: str = "empresa_mes"
) -> pd.DataFrame:
    """Calcula USD/lb con SUM(costos) / SUM(lb) a la granularidad elegida.

    Empresa-mes solo se calcula si las 4 plantas tienen costo completo; si no,
    el costo por libra queda nulo, sin sumar costos parciales ni perder libras.
    """
    if definicion not in DENOMINATORS:
        raise ValueError("Definición desconocida")
    col = DENOMINATORS[definicion]
    if nivel not in {"empresa_mes", "planta_mes"}:
        raise ValueError("Nivel desconocido")
    if nivel == "planta_mes":
        out = fact[["planta_id", "mes", "costo_directo_usd", "costo_completo", col]].copy()
        out["costo_directo_planta_por_libra_usd_lb"] = np.where(
            out["costo_completo"] & out[col].gt(0), out["costo_directo_usd"] / out[col], np.nan
        )
        return out
    if fact.duplicated(["planta_id", "mes"]).any():
        raise ValueError("El consolidado requiere una fila por planta y mes")
    group = fact.groupby("mes", as_index=False).agg(
        n_plantas=("planta_id", "nunique"),
        n_completas=("costo_completo", "sum"),
        n_costos_usd=("costo_directo_usd", "count"),
        n_denominadores=(col, "count"),
        costo_directo_usd=("costo_directo_usd", lambda s: s.sum(min_count=1)),
        cobre_fino_lb=(col, lambda s: s.sum(min_count=EXPECTED_PLANTS)),
    )
    complete_cost = group[["n_plantas", "n_completas", "n_costos_usd"]].eq(EXPECTED_PLANTS).all(axis=1)
    group.loc[~complete_cost, "costo_directo_usd"] = np.nan
    valid_copper = group["n_denominadores"].eq(EXPECTED_PLANTS) & group["cobre_fino_lb"].gt(0)
    group[KPI_COLUMN] = group["costo_directo_usd"] / group["cobre_fino_lb"].where(valid_copper)
    return group.drop(columns=["n_costos_usd", "n_denominadores"])


def historia_costo_empresa_mes(
    fact: pd.DataFrame,
    dim_tiempo: pd.DataFrame,
    dim_planta: pd.DataFrame,
    *,
    definicion: str = "operacional",
) -> pd.DataFrame:
    """Materializa el número principal y los datos necesarios para explicarlo.

    La salida no redacta conclusiones. Expone evidencia para responder, por mes:
    cuánto fue el costo USD/lb, si el dato está completo, qué cambió en costos y
    producción, cómo se compone el costo, qué planta tuvo el mayor costo unitario
    entre las disponibles y cuál fue la referencia de precio del cobre.

    Los efectos de costos y producción usan una descomposición simétrica. Su suma
    coincide exactamente con la variación del KPI respecto del último mes anterior
    con información empresarial completa, identificado en ``mes_comparacion``.
    """
    time_columns = ["mes", "precio_cobre_usd_lb", "dolar_observado_clp"]
    missing_time = set(time_columns) - set(dim_tiempo.columns)
    missing_plant = {"planta_id", "planta"} - set(dim_planta.columns)
    missing_fact = set(COST_COMPONENTS) - set(fact.columns)
    if missing_time or missing_plant or missing_fact:
        raise ValueError(
            "Columnas faltantes para construir la historia: "
            f"tiempo={sorted(missing_time)}, planta={sorted(missing_plant)}, "
            f"hecho={sorted(missing_fact)}"
        )

    number = costo_directo_planta_por_libra(fact, definicion=definicion)
    out = (
        dim_tiempo[time_columns]
        .merge(number, on="mes", how="left", validate="one_to_one")
        .sort_values("mes")
        .reset_index(drop=True)
    )
    complete_cost = out["costo_directo_usd"].notna()
    out["estado_dato"] = np.where(out[KPI_COLUMN].notna(), "COMPLETO", "INCOMPLETO")

    components = fact.groupby("mes", as_index=False)[COST_COMPONENTS].sum(min_count=1)
    out = out.merge(components, on="mes", how="left", validate="one_to_one")
    out.loc[~complete_cost, COST_COMPONENTS] = np.nan
    total_clp = out[COST_COMPONENTS].sum(axis=1, min_count=len(COST_COMPONENTS))
    for component in COST_COMPONENTS:
        share_name = "participacion_" + component.removesuffix("_clp") + "_pct"
        out[share_name] = 100 * out[component] / total_clp

    plant_number = costo_directo_planta_por_libra(
        fact, definicion=definicion, nivel="planta_mes"
    ).merge(dim_planta[["planta_id", "planta"]], on="planta_id", validate="many_to_one")
    top_plant = (
        plant_number.dropna(subset=[KPI_COLUMN])
        .sort_values(["mes", KPI_COLUMN, "planta"], ascending=[True, False, True])
        .drop_duplicates("mes", keep="first")
        [["mes", "planta", KPI_COLUMN]]
        .rename(
            columns={
                "planta": "planta_mayor_costo_unitario",
                KPI_COLUMN: "mayor_costo_unitario_planta_usd_lb",
            }
        )
    )
    out = out.merge(top_plant, on="mes", how="left", validate="one_to_one")

    out["margen_referencia_usd_lb"] = out["precio_cobre_usd_lb"] - out[KPI_COLUMN]
    valid_number = out[KPI_COLUMN].notna()
    previous_month = out["mes"].where(valid_number).ffill().shift(1)
    previous_kpi = out[KPI_COLUMN].where(valid_number).ffill().shift(1)
    previous_cost = out["costo_directo_usd"].where(valid_number).ffill().shift(1)
    previous_copper = out["cobre_fino_lb"].where(valid_number).ffill().shift(1)
    comparable = out[KPI_COLUMN].notna() & previous_kpi.notna()

    out["mes_comparacion"] = previous_month.where(comparable)
    out["variacion_kpi_usd_lb"] = (out[KPI_COLUMN] - previous_kpi).where(comparable)
    out["variacion_kpi_pct"] = (100 * (out[KPI_COLUMN] / previous_kpi - 1)).where(comparable)
    out["variacion_costo_pct"] = (100 * (out["costo_directo_usd"] / previous_cost - 1)).where(comparable)
    out["variacion_cobre_fino_pct"] = (100 * (out["cobre_fino_lb"] / previous_copper - 1)).where(comparable)

    out["efecto_costos_usd_lb"] = (
        0.5
        * (out["costo_directo_usd"] - previous_cost)
        * (1 / previous_copper + 1 / out["cobre_fino_lb"])
    ).where(comparable)
    out["efecto_produccion_usd_lb"] = (
        0.5
        * (previous_cost + out["costo_directo_usd"])
        * (1 / out["cobre_fino_lb"] - 1 / previous_copper)
    ).where(comparable)

    return out[
        [
            "mes",
            "mes_comparacion",
            "estado_dato",
            "n_plantas",
            "n_completas",
            "costo_directo_usd",
            "cobre_fino_lb",
            KPI_COLUMN,
            "variacion_kpi_usd_lb",
            "variacion_kpi_pct",
            "efecto_costos_usd_lb",
            "efecto_produccion_usd_lb",
            "variacion_costo_pct",
            "variacion_cobre_fino_pct",
            "energia_clp",
            "insumos_clp",
            "mano_obra_clp",
            "participacion_energia_pct",
            "participacion_insumos_pct",
            "participacion_mano_obra_pct",
            "planta_mayor_costo_unitario",
            "mayor_costo_unitario_planta_usd_lb",
            "dolar_observado_clp",
            "precio_cobre_usd_lb",
            "margen_referencia_usd_lb",
        ]
    ]
