"""Reproducción E2, sección B. Uso desde la raíz: python run_e2.py.

Reconstruye Bronze y Silver, concilia con E1 y publica tres Parquet en gold/e2.
El reporte de calidad queda separado del modelo.
"""
from __future__ import annotations

import shutil
import sys
from pathlib import Path
from tempfile import TemporaryDirectory

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "code"))

import pandas as pd
import config
import validation_log
from extraction import run_extraction
from cleaning import run_cleaning
from e2_model import build_model, load_catalogues, load_silver
from e2_quality import QualitySuite, validate_e2


def _guardar_modelo(model, destino: Path) -> None:
    """Prepara los tres archivos juntos y conserva la salida anterior si falla el reemplazo."""
    destino.parent.mkdir(parents=True, exist_ok=True)
    anterior = destino.with_name(".e2-anterior")
    if anterior.exists():
        if destino.exists():
            raise FileExistsError(f"Revisar la copia pendiente en {anterior}")
        anterior.rename(destino)

    with TemporaryDirectory(prefix=".e2-", dir=destino.parent) as tmp:
        nuevo = Path(tmp) / "modelo"
        nuevo.mkdir()
        for nombre, tabla in model.items():
            tabla.to_parquet(nuevo / f"{nombre}.parquet", index=False)
        if destino.exists():
            destino.rename(anterior)
        try:
            nuevo.rename(destino)
        except OSError:
            if anterior.exists():
                anterior.rename(destino)
            raise
        if anterior.exists():
            shutil.rmtree(anterior)


def _guardar_reporte(calidad: QualitySuite, ruta: Path):
    reporte = calidad.as_frame()
    reporte.to_csv(ruta, index=False, encoding="utf-8-sig")
    return reporte


def main():
    destino = config.GOLD_DIR / "e2"
    ruta_reporte = ROOT / "reporte_calidad.csv"
    calidad = QualitySuite()
    etapa = "Extracción RAW a Bronze"
    validation_log.reset()
    try:
        print(f"[E2] 1/4 - {etapa}")
        run_extraction()
        etapa = "Limpieza de Bronze a Silver"
        print(f"[E2] 2/4 - {etapa}")
        run_cleaning()
        etapa = "Construcción del modelo planta-mes"
        print(f"[E2] 3/4 - {etapa}")
        silver = load_silver()
        model = build_model(silver, *load_catalogues())

        etapa = "Validación y conciliación con E1"
        print(f"[E2] 4/4 - {etapa}")
        referencia_e1 = pd.read_parquet(config.GOLD_DIR / "gold_metricas.parquet")
        calidad = validate_e2(silver, model, referencia_e1)
        reporte = _guardar_reporte(calidad, ruta_reporte)
        print(reporte[["codigo", "estado", "detalle"]].to_string(index=False))
        if calidad.blocking():
            print("[E2] Ejecución bloqueada. Consultar reporte_calidad.csv.")
            return 1

        etapa = "Escritura de Gold E2"
        _guardar_modelo(model, destino)
        print("[E2] OK - tres Parquet en gold/e2/ y reporte_calidad.csv en la raíz")
        return 0
    except Exception as error:
        calidad.check("ETL-ERROR", etapa, False, f"{type(error).__name__}: {error}")
        print(f"[E2] {etapa}: {error}", file=sys.stderr)
        try:
            _guardar_reporte(calidad, ruta_reporte)
        except OSError as report_error:
            print(f"[E2] Error al escribir reporte_calidad.csv: {report_error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
