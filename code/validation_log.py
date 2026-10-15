"""
validation_log.py
=================
Módulo de registro de alertas y fallas de validación del pipeline.

Provee un mecanismo centralizado para que cualquier etapa registre una falla
de validación sin detener la ejecución. Las fallas se acumulan en memoria
durante la corrida y al final se escriben en un archivo Parquet persistente
legible con pandas.

El compañero que redacta el informe de validación puede leer este archivo
directamente para listar todos los casos fallidos sin necesidad de
reinterpretar el código.

Estructura de cada registro:
  - stage     : nombre de la etapa que detectó la falla (ej. "cleaning")
  - dataset   : nombre del conjunto de datos o archivo de origen
  - source_ref: referencia a la fila de origen (ej. índice, clave primaria)
  - field     : nombre del campo con la falla
  - value     : valor original que falló la validación
  - rule      : descripción de la regla violada
"""

import pandas as pd
from config import VALIDATION_LOG_PATH

# ---------------------------------------------------------------------------
# Almacén en memoria de las fallas de validación acumuladas durante la corrida.
# Es una lista de diccionarios; cada diccionario es un registro.
# Se inicializa vacía y se llena con register_failure().
# ---------------------------------------------------------------------------
_failures: list[dict] = []


def reset() -> None:
    """Vacia el acumulador para iniciar una corrida independiente."""
    _failures.clear()


def get_failures() -> list[dict]:
    """Retorna una copia de las fallas acumuladas (util para pruebas)."""
    return list(_failures)


def register_failure(
    stage: str,
    dataset: str,
    source_ref: str | int,
    field: str,
    value: object,
    rule: str,
) -> None:
    """
    Registra una falla de validación en el almacén en memoria.

    Parámetros
    ----------
    stage      : nombre de la etapa del pipeline que detectó la falla.
    dataset    : nombre del dataset o archivo de origen (ej. "bronze_costos").
    source_ref : referencia a la fila fuente (índice numérico, clave primaria
                 u otra identificación reproducible).
    field      : nombre del campo o columna que falló la validación.
    value      : valor original del campo, tal como estaba antes de limpiar.
    rule       : descripción breve y legible de la regla violada.

    Esta función NUNCA lanza excepciones; solo agrega al almacén interno.
    """
    _failures.append(
        {
            "stage": stage,
            "dataset": dataset,
            "source_ref": str(source_ref),
            "field": field,
            "value": str(value),
            "rule": rule,
        }
    )


def get_failure_count() -> int:
    """
    Retorna el número de fallas registradas hasta el momento.
    Útil para imprimir un resumen parcial durante la ejecución.
    """
    return len(_failures)


def write_log() -> None:
    """
    Escribe todas las fallas acumuladas en un archivo Parquet persistente.

    La ruta de destino está definida en config.VALIDATION_LOG_PATH.
    Si no hay fallas, escribe de todas formas un archivo vacío con el esquema
    correcto para que el compañero pueda leer el archivo sin errores.

    Este método debe llamarse UNA SOLA VEZ al final del pipeline, desde
    el punto de entrada (main.py).
    """
    # Crear DataFrame aunque no haya fallas (archivo vacío con esquema)
    df = pd.DataFrame(
        _failures,
        columns=["stage", "dataset", "source_ref", "field", "value", "rule"],
    )
    df.to_parquet(VALIDATION_LOG_PATH, index=False)


def print_summary() -> None:
    """
    Imprime en pantalla un resumen de las fallas de validación acumuladas.

    Muestra el total de fallas y un desglose por etapa y dataset.
    Se llama desde main.py al final de la ejecución, después de write_log().
    """
    total = len(_failures)
    print(f"\n{'=' * 60}")
    print(f"RESUMEN DE VALIDACIONES: {total} falla(s) registrada(s)")
    print(f"{'=' * 60}")

    if total == 0:
        print("  Sin fallas de validación.")
    else:
        # Agrupar por etapa para facilitar la lectura del resumen
        by_stage: dict[str, list[dict]] = {}
        for f in _failures:
            by_stage.setdefault(f["stage"], []).append(f)

        for stage, records in by_stage.items():
            print(f"\n  Etapa '{stage}': {len(records)} falla(s)")
            # Mostrar hasta 5 ejemplos por etapa para no saturar la pantalla
            for rec in records[:5]:
                print(
                    f"    dataset={rec['dataset']}  "
                    f"ref={rec['source_ref']}  "
                    f"campo={rec['field']}  "
                    f"valor='{rec['value']}'  "
                    f"regla={rec['rule']}"
                )
            if len(records) > 5:
                print(f"    ... y {len(records) - 5} falla(s) más (ver log).")

    print(f"\n  Log completo en: {VALIDATION_LOG_PATH}")
    print(f"{'=' * 60}\n")
