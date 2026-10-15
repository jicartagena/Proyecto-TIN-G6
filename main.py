"""
main.py
=======
Punto de entrada único del pipeline CuprumAndes.

Ejecuta las tres etapas en orden:
  1. Extracción   (extraction.py)  → bronze/
  2. Limpieza     (cleaning.py)    → silver/
  3. Métricas     (metrics.py)     → gold/

Al terminar, escribe el log de validaciones en disco y muestra un resumen
en pantalla de las fallas acumuladas durante la corrida.

Manejo de errores:
  - Los errores técnicos inesperados (excepciones no controladas) detienen
    la ejecución con un mensaje claro.
  - Las fallas de validación NUNCA detienen la ejecución; se acumulan en
    validation_log y se reportan al final.

Uso:
  Desde la raíz del proyecto (o desde el directorio code/):
      python main.py
"""

import sys
import traceback

# ---------------------------------------------------------------------------
# Agregar el directorio code/ al path para que los imports entre módulos
# funcionen tanto si se ejecuta desde la raíz como desde code/.
# ---------------------------------------------------------------------------
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent / "code"))

# Importar las etapas y el log de validaciones
from extraction import run_extraction
from cleaning import run_cleaning
from metrics import run_metrics
import validation_log


def main() -> None:
    """
    Orquesta la ejecución secuencial de las tres etapas del pipeline.

    Imprime mensajes de progreso antes y después de cada etapa para que
    el usuario sepa qué está ocurriendo. Captura errores técnicos inesperados
    para mostrar un mensaje legible antes de abortar.
    """
    validation_log.reset()
    print("=" * 60)
    print("  Pipeline CuprumAndes — inicio")
    print("=" * 60)

    # ------------------------------------------------------------------
    # Etapa 1: Extracción
    # ------------------------------------------------------------------
    print("\n[1/3] Iniciando extracción...")
    try:
        run_extraction()
        print("[1/3] Extracción completada.\n")
    except Exception:
        # Un error técnico aquí es irrecuperable: sin bronze no hay pipeline.
        print("[1/3] ERROR INESPERADO en la etapa de extracción:")
        traceback.print_exc()
        _finalize_and_exit(exit_code=1)

    # ------------------------------------------------------------------
    # Etapa 2: Limpieza y validación
    # ------------------------------------------------------------------
    print("[2/3] Iniciando limpieza y validación...")
    try:
        run_cleaning()
        print("[2/3] Limpieza y validación completadas.\n")
    except Exception:
        print("[2/3] ERROR INESPERADO en la etapa de limpieza:")
        traceback.print_exc()
        _finalize_and_exit(exit_code=1)

    # ------------------------------------------------------------------
    # Etapa 3: Métricas
    # ------------------------------------------------------------------
    print("[3/3] Iniciando cálculo de métricas...")
    try:
        run_metrics()
        print("[3/3] Métricas calculadas.\n")
    except Exception:
        print("[3/3] ERROR INESPERADO en la etapa de métricas:")
        traceback.print_exc()
        _finalize_and_exit(exit_code=1)

    # ------------------------------------------------------------------
    # Finalización: escribir log y mostrar resumen
    # ------------------------------------------------------------------
    _finalize_and_exit(exit_code=0)


def _finalize_and_exit(exit_code: int) -> None:
    """
    Escribe el log de validaciones en disco, imprime el resumen en pantalla
    y termina el proceso con el código de salida indicado.

    Se llama tanto en finalización normal (exit_code=0) como en error
    técnico (exit_code=1) para asegurar que el log parcial siempre se guarde.
    """
    print("\nGuardando log de validaciones...")
    try:
        validation_log.write_log()
    except Exception:
        print("ADVERTENCIA: no se pudo escribir el log de validaciones.")
        traceback.print_exc()

    validation_log.print_summary()

    if exit_code == 0:
        print("Pipeline finalizado correctamente.")
    else:
        print("Pipeline finalizado con errores. Revisar mensajes anteriores.")

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
