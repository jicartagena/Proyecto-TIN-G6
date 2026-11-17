"""Pruebas unitarias del grano planta-mes, agregación correcta y bloqueo."""
from __future__ import annotations
import sys
import unittest
from pathlib import Path
import pandas as pd
import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "code"))
from e2_model import build_model, load_catalogues
from e2_quality import validate_e2
from e2_semantic import costo_directo_planta_por_libra, historia_costo_empresa_mes


class TestE2Model(unittest.TestCase):
    @staticmethod
    def fixture():
        plants = pd.DataFrame({"planta_id": [1, 2, 3, 4],
                               "nombre": ["Planta Cerro Norte", "Planta Loa", "Salado", "Illapel"],
                               "region_id": [1, 2, 3, 9]})
        regions = pd.DataFrame({"region_id": [1, 2, 3], "region": ["Antofagasta", "Tarapacá", "Atacama"]})
        lots, costs, lab, price, fx = [], [], [], [], []
        idx = 0
        for month in range(1, 13):
            mes = f"2025-{month:02d}"
            price.append({"fecha": mes + "-27", "valor": 4.0})
            fx.append({"fecha": mes + "-27", "valor": 1000.0})
            for i, name in enumerate(["Cerro Norte", "Loa", "Salado", "Illapel"], start=1):
                idx += 1
                lots.append({"lote_id": idx, "planta_id": i, "mes": mes, "fecha": mes + "-02",
                             "toneladas_tratadas": 1000.0, "ley_pct": 1.0,
                             "recuperacion_pct": 90.0, "es_elegible": True})
                lab.append({"lote_id": idx, "ley_lab_prom": 0.98})
                if (i, month) not in {(2, 3), (2, 8), (3, 11)}:
                    costs.append({"planta": name, "mes": mes,
                                  "energia_clp": 1_000_000., "insumos_clp": np.nan if (i,month)==(1,6) else 1_000_000.,
                                  "mano_obra_clp": 1_000_000.})
        return ({"lotes": pd.DataFrame(lots), "costos": pd.DataFrame(costs),
                 "laboratorio": pd.DataFrame(lab), "precio_cobre": pd.DataFrame(price),
                 "tipo_cambio": pd.DataFrame(fx)}, plants, regions)

    @classmethod
    def setUpClass(cls):
        cls.silver, cls.plants, cls.regions = cls.fixture()
        cls.model = build_model(cls.silver, cls.plants, cls.regions)

    def test_esquema_estrella_48_celdas_y_claves(self):
        m = self.model
        self.assertEqual(len(m["dim_planta"]), 4)
        self.assertEqual(len(m["dim_tiempo"]), 12)
        self.assertEqual(len(m["fact_planta_mes"]), 48)
        self.assertFalse(m["fact_planta_mes"].duplicated(["planta_id", "mes"]).any())
        self.assertEqual(m["dim_planta"].loc[m["dim_planta"].planta == "Illapel", "region"].iloc[0], "Coquimbo")

    def test_incompletitud_planta_vs_empresa(self):
        fact = self.model["fact_planta_mes"]
        self.assertEqual(int(fact["costo_completo"].sum()), 44)
        enterprise = costo_directo_planta_por_libra(fact)
        self.assertEqual(set(enterprise.loc[enterprise["costo_directo_planta_por_libra_usd_lb"].isna(), "mes"]),
                         {"2025-03", "2025-06", "2025-08", "2025-11"})
        planta = costo_directo_planta_por_libra(fact, nivel="planta_mes")
        self.assertEqual(planta["costo_directo_planta_por_libra_usd_lb"].notna().sum(), 44)

        filtered = costo_directo_planta_por_libra(fact.loc[fact["planta_id"] != 1])
        self.assertTrue(filtered["costo_directo_planta_por_libra_usd_lb"].isna().all())
        self.assertTrue(filtered["costo_directo_usd"].isna().all())

    def test_definicion_laboratorio_y_simple(self):
        fact = self.model["fact_planta_mes"]
        self.assertTrue(np.allclose(fact["cobre_fino_lb_laboratorio"], fact["cobre_fino_lb_operacional"] * .98))
        self.assertTrue(np.allclose(fact["cobre_fino_lb_simple"], fact["cobre_fino_lb_operacional"]))

        missing = fact.copy()
        missing.loc[missing.index[0], "cobre_fino_lb_laboratorio"] = np.nan
        enterprise = costo_directo_planta_por_libra(missing, definicion="laboratorio").set_index("mes")
        self.assertTrue(pd.isna(enterprise.loc["2025-01", "costo_directo_planta_por_libra_usd_lb"]))

    def test_calidad_y_conciliacion(self):
        f = self.model["fact_planta_mes"]
        sem = costo_directo_planta_por_libra(f)
        legacy = sem.rename(columns={"cobre_fino_lb": "cobre_fino_lb",
                                     "costo_directo_usd": "costos_totales_usd",
                                     "costo_directo_planta_por_libra_usd_lb": "cash_cost_usd_lb"})
        suite = validate_e2(self.silver, self.model, legacy)
        self.assertFalse(suite.blocking(), suite.as_frame().to_string())
        self.assertTrue((suite.as_frame().query("codigo == 'SLA-01'")["estado"] == "AVISA").all())
        bad = legacy.copy()
        bad.loc[bad["mes"] == "2025-01", "cash_cost_usd_lb"] += 0.1
        self.assertTrue(any(x["codigo"] == "MET-01" for x in validate_e2(self.silver, self.model, bad).blocking()))

    def test_duplicado_de_costos_se_rechaza(self):
        silver = {k:v.copy() for k,v in self.silver.items()}
        silver["costos"] = pd.concat([silver["costos"], silver["costos"].iloc[[0]]], ignore_index=True)
        with self.assertRaises(ValueError):
            build_model(silver, self.plants, self.regions)

    def test_catalogos_reales_respetan_esquema_sqlite(self):
        plants, regions = load_catalogues()
        self.assertEqual(set(plants.columns), {"planta_id", "nombre", "region_id"})
        self.assertEqual(set(regions.columns), {"region_id", "region"})

    def test_historia_conserva_numero_contexto_y_cobertura(self):
        story = historia_costo_empresa_mes(
            self.model["fact_planta_mes"], self.model["dim_tiempo"], self.model["dim_planta"]
        ).set_index("mes")
        self.assertEqual(len(story), 12)
        self.assertEqual(story.loc["2025-01", "estado_dato"], "COMPLETO")
        self.assertEqual(story.loc["2025-03", "estado_dato"], "INCOMPLETO")
        self.assertEqual(story.loc["2025-04", "mes_comparacion"], "2025-02")
        self.assertTrue(pd.isna(story.loc["2025-03", "costo_directo_planta_por_libra_usd_lb"]))
        valid = story["variacion_kpi_usd_lb"].notna()
        total_effect = story.loc[valid, "efecto_costos_usd_lb"] + story.loc[valid, "efecto_produccion_usd_lb"]
        self.assertTrue(np.allclose(total_effect, story.loc[valid, "variacion_kpi_usd_lb"]))


if __name__ == "__main__":
    unittest.main()
