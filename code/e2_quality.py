"""Contrato de calidad E2 (ejecutado por run_e2.py en cada corrida).

PASA: satisface contrato. AVISA: limitación conocida y acotada.
BLOQUEA: la salida Gold de E2 se conserva.
"""
from __future__ import annotations
import numpy as np
import pandas as pd

from config import ANALYSIS_YEAR
from e2_semantic import costo_directo_planta_por_libra


class QualitySuite:
    def __init__(self):
        self.records = []

    def check(self, codigo, origen, ok, detalle, *, warning=False, accion="Revisar el origen y no publicar"):
        state = "PASA" if ok else ("AVISA" if warning else "BLOQUEA")
        self.records.append({"codigo": codigo, "origen": origen, "estado": state,
                             "detalle": str(detalle), "accion": "Ninguna" if ok else accion})

    def as_frame(self):
        return pd.DataFrame(self.records, columns=["codigo", "origen", "estado", "detalle", "accion"])

    def blocking(self):
        return [x for x in self.records if x["estado"] == "BLOQUEA"]


def validate_e2(silver, model, legacy_gold):
    """Controles explícitos de las cuatro fuentes y del indicador principal."""
    suite = QualitySuite()
    lotes, costos, lab = [silver[k] for k in ("lotes", "costos", "laboratorio")]
    fx, copper = [silver[k] for k in ("tipo_cambio", "precio_cobre")]
    fact, plants, months = [model[k] for k in ("fact_planta_mes", "dim_planta", "dim_tiempo")]

    # FUENTE 1: SQLite (se documentan exclusiones, pero no se ocultan ni rellenan).
    needed = {"planta_id", "mes", "fecha", "toneladas_tratadas", "ley_pct", "recuperacion_pct", "es_elegible"}
    suite.check("SRC-SQLITE-01", "SQLite", len(lotes) > 0 and needed.issubset(lotes.columns),
                f"filas={len(lotes)}, columnas indispensables presentes={needed.issubset(lotes.columns)}")
    eligible = lotes.loc[lotes["es_elegible"].fillna(False)]
    valid_year = eligible["fecha"].astype("string").str.startswith(f"{ANALYSIS_YEAR}-").fillna(False).all()
    valid_ranges = (eligible["toneladas_tratadas"].ge(100) & eligible["ley_pct"].gt(0) & eligible["ley_pct"].lt(5) & eligible["recuperacion_pct"].between(50, 100)).all()
    suite.check("SRC-SQLITE-02", "SQLite", bool(valid_year and valid_ranges),
                f"elegibles={len(eligible)}; no elegibles={len(lotes) - len(eligible)}; fecha/rangos validados")
    suite.check("SRC-SQLITE-03", "SQLite", len(lotes) == 14598 and len(eligible) == 14240,
                f"base E1: 14598 lotes/14240 elegibles; actual={len(lotes)}/{len(eligible)}",
                warning=True, accion="Conciliar con E1 antes de actualizar las cifras")

    # FUENTE 2: CSV de costos.
    suite.check("SRC-CSV-01", "CSV", not costos[["planta", "mes"]].isna().any().any() and not costos.duplicated(["planta", "mes"]).any(),
                f"filas={len(costos)}; clave candidata planta-mes sin nulos ni duplicados")
    complete_cost = costos[["energia_clp", "insumos_clp", "mano_obra_clp"]].notna().all(axis=1)
    n_complete = int(complete_cost.sum())
    suite.check("SRC-CSV-02", "CSV", len(costos) == 48 and n_complete == 48,
                f"celdas con costo completo={n_complete}/48; filas presentes={len(costos)}/48; no se imputan faltantes",
                warning=True, accion="Mostrar 'sin dato' en el prototipo y solicitar costos a Control de Gestión")

    # FUENTE 3: laboratorio. No se exige cobertura 100% (muestra, no censo).
    lab_ok = {"lote_id", "ley_lab_prom"}.issubset(lab.columns) and lab["lote_id"].notna().any() and not lab["lote_id"].dropna().duplicated().any()
    suite.check("SRC-JSON-01", "JSON laboratorio", lab_ok,
                f"filas laboratorio={len(lab)}; identificadores no ambiguos")
    matched = lab[lab["lote_id"].isin(eligible["lote_id"].dropna()) & lab["ley_lab_prom"].notna()]
    lab_in_range = matched["ley_lab_prom"].between(0, 5, inclusive="neither")
    invalid_lab = int((~lab_in_range).sum())
    suite.check("SRC-JSON-02", "JSON laboratorio", len(matched) > 0 and invalid_lab == 0,
                f"pares únicos con lotes elegibles={len(matched)}; fuera de rango (0,5)={invalid_lab}")
    suite.check("SRC-JSON-03", "JSON laboratorio", len(matched) == 843,
                f"base E1=843 pares; E2={len(matched)}; cobertura de muestra limitada",
                warning=True, accion="Revisar vínculo de identificadores y documentar cambio")

    # FUENTE 4: API (dos indicadores, 12 cierres en 2025, sin interpolación).
    for field, dataset in [("precio_cobre_usd_lb", copper), ("dolar_observado_clp", fx)]:
        values = months[field]
        suite.check(f"SRC-API-{field}", "API mindicador.cl", len(dataset) > 0 and np.isfinite(values).all() and values.gt(0).all(),
                    f"serie={field}; {values.notna().sum()}/12 cierres válidos; snapshot RAW reutilizable")

    # MODELO: 48 celdas, dos dimensiones y medidas coherentes.
    pk = not fact.duplicated(["planta_id", "mes"]).any() and not fact[["planta_id", "mes"]].isna().any().any()
    suite.check("FACT-01", "Gold", len(fact) == 48 and pk,
                f"filas={len(fact)}; PK (planta_id,mes) única")
    dim_ok = (len(plants) == 4 and plants["planta_id"].is_unique and len(months) == 12 and months["mes"].is_unique)
    suite.check("DIM-01", "Gold", dim_ok, "dim_planta=4 y dim_tiempo=12, claves únicas")
    refs_ok = set(fact["planta_id"]).issubset(set(plants["planta_id"])) and set(fact["mes"]).issubset(set(months["mes"]))
    suite.check("FACT-02", "Gold", refs_ok, "sin claves huérfanas planta/tiempo")
    suite.check("FACT-03", "Gold", fact["mes"].astype(str).str.match(rf"^{ANALYSIS_YEAR}-(0[1-9]|1[0-2])$").all(),
                "fechas dentro del período histórico 2025")
    numeric_ok = fact["cobre_fino_lb_operacional"].gt(0).all() and fact["toneladas_tratadas"].gt(0).all() and fact.loc[fact["costo_completo"], "costo_directo_usd"].gt(0).all()
    suite.check("FACT-04", "Gold", numeric_ok, "producción positiva y costos completos positivos")

    # CONCILIACIÓN: los ocho meses completos deben reproducir el cálculo E1.
    summary = costo_directo_planta_por_libra(fact)
    old = legacy_gold[["mes", "cobre_fino_lb", "costos_totales_usd", "cash_cost_usd_lb"]]
    comp = summary.merge(old, on="mes", how="left", validate="one_to_one", suffixes=("_e2", "_e1"))
    comparable = comp["cash_cost_usd_lb"].notna()
    results = []
    for _, row in comp.loc[comparable].iterrows():
        for e2, e1 in [("cobre_fino_lb_e2", "cobre_fino_lb_e1"),
                       ("costo_directo_usd", "costos_totales_usd"),
                       ("costo_directo_planta_por_libra_usd_lb", "cash_cost_usd_lb")]:
            results.append(np.isclose(row[e2], row[e1], rtol=1e-10, atol=1e-5))
    suite.check("MET-01", "Métrica", len(comp) == 12 and comparable.sum() == 8 and all(results),
                f"meses E1 comparables={int(comparable.sum())}; conciliación costos/cobre fino/USD-lb")
    missing = set(comp.loc[comp["costo_directo_planta_por_libra_usd_lb"].isna(), "mes"])
    suite.check("MET-02", "Métrica", missing == {"2025-03", "2025-06", "2025-08", "2025-11"},
                f"meses empresa sin costo completo={sorted(missing)}")
    suite.check("SLA-01", "SLA completitud", int(fact["costo_completo"].sum()) / 48 >= .95,
                f"completitud={int(fact['costo_completo'].sum())}/48 ({100 * fact['costo_completo'].mean():.2f}%) vs meta propuesta >=95%",
                warning=True, accion="Escalar a Control de Gestión; no publicar como completo el mes de la empresa")
    return suite
