"""Pruebas unitarias y de conciliación del pipeline completo."""

import hashlib
import math
import sys
import unittest
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "code"))

import validation_log as vlog  # noqa: E402
from cleaning import (  # noqa: E402
    _clean_costos,
    _clean_laboratorio,
    _clean_lotes,
    _parse_clp_amount,
    _standardize_operational_date,
    evaluar_elegibilidad_lote,
)
from extraction import run_extraction  # noqa: E402
from cleaning import run_cleaning  # noqa: E402
from metrics import (  # noqa: E402
    _build_lab_sensitivity,
    _calcular_costos_clp_mensual,
    calcular_resumen_reconciliacion,
    run_metrics,
)


def _raw_hashes() -> dict[str, str]:
    return {
        path.name: hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted((ROOT / "raw").iterdir())
        if path.is_file()
    }


class TestTransformaciones(unittest.TestCase):
    def setUp(self) -> None:
        vlog.reset()

    def test_cuatro_formatos_de_fecha_sin_invertir_dia_mes(self) -> None:
        self.assertEqual(_standardize_operational_date("2025-08-04"), "2025-08-04")
        self.assertEqual(_standardize_operational_date("05/08/2025"), "2025-08-05")
        self.assertEqual(_standardize_operational_date("14-04-25"), "2025-04-14")
        self.assertEqual(_standardize_operational_date("5-ago-2025"), "2025-08-05")

    def test_fecha_2026_se_conserva_pero_no_es_elegible(self) -> None:
        date = _standardize_operational_date("09/06/2026")
        self.assertEqual(date, "2026-06-09")
        self.assertIn("fecha_fuera_de_2025", evaluar_elegibilidad_lote(date, 100, 1, 90))

    def test_limites_de_calidad(self) -> None:
        self.assertEqual(evaluar_elegibilidad_lote("2025-01-01", 100, 1, 50), [])
        self.assertEqual(evaluar_elegibilidad_lote("2025-01-01", 100, 1, 100), [])
        self.assertTrue(evaluar_elegibilidad_lote("2025-01-01", 99, 1, 90))
        self.assertTrue(evaluar_elegibilidad_lote("2025-01-01", 100, 0, 90))
        self.assertTrue(evaluar_elegibilidad_lote("2025-01-01", 100, 5, 90))
        self.assertTrue(evaluar_elegibilidad_lote("2025-01-01", 100, 1, 49.9))
        self.assertTrue(evaluar_elegibilidad_lote("2025-01-01", 100, 1, 100.1))

    def test_lote_id_repetido_se_conserva(self) -> None:
        frame = pd.DataFrame(
            [
                {"lote_id": "7", "fecha": "2025-01-01", "planta_id": "1",
                 "rajo_id": "1", "toneladas_tratadas": "100", "ley_pct": "1",
                 "recuperacion_pct": "90", "mes": "1", "source_file": "x",
                 "source_row_index": "1"},
                {"lote_id": "7", "fecha": "02/01/2025", "planta_id": "1",
                 "rajo_id": "1", "toneladas_tratadas": "101", "ley_pct": "1",
                 "recuperacion_pct": "90", "mes": "1", "source_file": "x",
                 "source_row_index": "2"},
            ]
        )
        result = _clean_lotes(frame)
        self.assertEqual(len(result), 2)
        self.assertEqual(result["lote_id"].tolist(), [7, 7])
        alerts = [failure for failure in vlog.get_failures() if failure["field"] == "lote_id"]
        self.assertEqual(len(alerts), 1)

    def test_planta_ausente_invalida_costo_mensual(self) -> None:
        frame = pd.DataFrame(
            {
                "mes": ["2025-01"] * 3,
                "planta": ["Cerro Norte", "Loa", "Salado"],
                "energia_clp": [1.0] * 3,
                "insumos_clp": [1.0] * 3,
                "mano_obra_clp": [1.0] * 3,
                "source_row_index": ["1", "2", "3"],
            }
        )
        result = _calcular_costos_clp_mensual(frame).set_index("mes")
        self.assertTrue(math.isnan(result.loc["2025-01", "costos_totales_clp"]))

    def test_insumos_na_no_se_imputa(self) -> None:
        self.assertIsNone(_parse_clp_amount("N/A"))
        frame = pd.DataFrame(
            [{"planta": "Cerro Norte", "mes": "2025-06", "energia_clp": "1.000",
              "insumos_clp": "N/A", "mano_obra_clp": "$500", "source_file": "x",
              "source_row_index": "1"}]
        )
        result = _clean_costos(frame)
        self.assertTrue(pd.isna(result.loc[0, "insumos_clp"]))

    def test_laboratorio_usa_ley_reportada_con_coma(self) -> None:
        frame = pd.DataFrame(
            [{"lote_id": "1", "laboratorio": "QMC", "fecha_muestreo": "01/01/2025",
              "ley_lab_prom": "0,87", "muestras_json": '[{"m":1,"ley":"0,90"}]',
              "source_file": "x", "source_row_index": "1"}]
        )
        cleaned = _clean_laboratorio(frame)
        self.assertAlmostEqual(cleaned.loc[0, "ley_lab_prom"], 0.87)
        self.assertIn('"ley":0.9', cleaned.loc[0, "muestras_json"])

    def test_formula_de_calibracion(self) -> None:
        lots = pd.DataFrame(
            {"lote_id": [1, 2], "mes": ["2025-01", "2025-01"],
             "toneladas_tratadas": [100.0, 200.0], "ley_pct": [1.0, 2.0],
             "es_elegible": [True, True]}
        )
        lab = pd.DataFrame({"lote_id": [1, 2], "ley_lab_prom": [2.0, 1.0]})
        gold = pd.DataFrame(
            {"mes": ["2025-01"], "cobre_fino_lb": [1000.0],
             "costos_totales_usd": [800.0], "cash_cost_usd_lb": [0.8]}
        )
        result = _build_lab_sensitivity(lots, lab, gold)
        self.assertAlmostEqual(result.loc[0, "factor_calibracion_ley"], 0.8)
        self.assertAlmostEqual(result.loc[0, "cobre_fino_lb_calibrado"], 800.0)
        self.assertAlmostEqual(result.loc[0, "cash_cost_usd_lb_calibrado"], 1.0)


class TestConciliacionIntegrada(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.raw_before = _raw_hashes()
        vlog.reset()
        run_extraction()
        run_cleaning()
        run_metrics()
        cls.raw_after = _raw_hashes()

    def test_raw_permanece_intacto(self) -> None:
        self.assertEqual(self.raw_before, self.raw_after)

    def test_conteos_de_lotes(self) -> None:
        lots = pd.read_parquet(ROOT / "silver" / "silver_lotes.parquet")
        self.assertEqual(len(lots), 14_598)
        self.assertEqual(lots["fecha"].str.startswith("2026-").sum(), 86)
        is_2025 = lots["fecha"].str.startswith("2025-")
        self.assertEqual((is_2025 & ~lots["es_elegible"]).sum(), 272)
        self.assertEqual((~lots["es_elegible"]).sum(), 358)
        self.assertEqual(lots["es_elegible"].sum(), 14_240)
        self.assertEqual((lots["lote_id"].value_counts() > 1).sum(), 144)

    def test_costos_incompletos_y_produccion_completa(self) -> None:
        gold = pd.read_parquet(ROOT / "gold" / "gold_metricas.parquet").set_index("mes")
        self.assertEqual(len(gold), 12)
        self.assertTrue((gold["cobre_fino_lb"] > 0).all())
        missing = set(gold.index[gold["cash_cost_usd_lb"].isna()])
        self.assertEqual(missing, {"2025-03", "2025-06", "2025-08", "2025-11"})

    def test_cobertura_laboratorio_y_revision_2026(self) -> None:
        sensitivity = pd.read_parquet(
            ROOT / "gold" / "gold_sensibilidad_laboratorio.parquet"
        )
        review = pd.read_parquet(
            ROOT / "gold" / "revision_lotes_2026_laboratorio.parquet"
        )
        self.assertEqual(sensitivity["registros_laboratorio_vinculables"].sum(), 843)
        self.assertEqual(len(review), 86)
        self.assertEqual(review["tiene_laboratorio"].sum(), 7)

    def test_cifras_reconciliadas(self) -> None:
        gold = pd.read_parquet(ROOT / "gold" / "gold_metricas.parquet")
        summary = calcular_resumen_reconciliacion(gold)
        self.assertAlmostEqual(summary["cash_cost_enero"], 1.8458, delta=0.0001)
        self.assertAlmostEqual(summary["cash_cost_diciembre"], 3.4090, delta=0.0001)
        self.assertAlmostEqual(summary["variacion_enero_diciembre_pct"], 84.7, delta=0.1)
        self.assertAlmostEqual(summary["cash_cost_ponderado_meses_completos"], 2.34, delta=0.01)
        self.assertAlmostEqual(summary["margen_unitario_s1"], 2.23, delta=0.01)
        self.assertAlmostEqual(summary["margen_unitario_s2"], 2.13, delta=0.01)
        self.assertAlmostEqual(summary["efecto_precio"], 0.60, delta=0.01)
        self.assertAlmostEqual(summary["efecto_costo"], -0.71, delta=0.01)

    def test_ejecucion_repetida_es_determinista(self) -> None:
        before = pd.read_parquet(ROOT / "gold" / "gold_metricas.parquet")
        vlog.reset()
        run_extraction()
        run_cleaning()
        run_metrics()
        after = pd.read_parquet(ROOT / "gold" / "gold_metricas.parquet")
        pd.testing.assert_frame_equal(before, after)


if __name__ == "__main__":
    unittest.main()
