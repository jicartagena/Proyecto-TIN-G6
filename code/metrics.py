"""
metrics.py
==========
Etapa 3 del pipeline: Cálculo de métricas mensuales a nivel empresa.

Lee ÚNICAMENTE desde silver/ y escribe una tabla Parquet en gold/.

Responsabilidades:
  1. Leer los cinco archivos silver necesarios, incluido laboratorio.
  2. Calcular cobre fino por lote y agregar a nivel mensual (REGLA 2).
  3. Obtener el precio del cobre y el tipo de cambio del último día hábil
     de cada mes (REGLA 1 y REGLA 4).
  4. Agregar costos en CLP por mes y convertir a USD (REGLA 3).
  5. Calcular ingresos, cash cost y utilidades.
  6. Verificar que los 12 meses de 2025 estén presentes (REGLA 6).
  7. Ejecutar verificaciones de sanidad sobre el resultado (REGLA 7).
  8. Escribir gold/gold_metricas.parquet con exactamente 7 columnas:
       mes, cobre_fino_lb, precio_cobre_usd_lb, ingresos_usd,
       costos_totales_usd, cash_cost_usd_lb, utilidades_usd

Por qué esta etapa existe separada de cleaning:
  Los valores derivados viven únicamente en gold/. La limpieza no anticipa
  ninguna lógica de negocio; esta etapa tampoco modifica los datos limpios.
"""

import pandas as pd

import business_rules as br
import validation_log
from config import ANALYSIS_YEAR, GOLD_DIR, MT_TO_LB, SILVER_DIR


# ---------------------------------------------------------------------------
# Constantes internas
# ---------------------------------------------------------------------------

# Columnas exactas y orden de la tabla gold (spec del enunciado)
GOLD_COLUMNS = [
    "mes",
    "cobre_fino_lb",
    "precio_cobre_usd_lb",
    "ingresos_usd",
    "costos_totales_usd",
    "cash_cost_usd_lb",
    "utilidades_usd",
]

# Nombre del archivo de salida en gold/
GOLD_FILENAME = "gold_metricas.parquet"

# Etiqueta de etapa usada en todos los registros del log de validaciones
_STAGE = "metrics"

EXPECTED_MONTHS = [f"{ANALYSIS_YEAR}-{month:02d}" for month in range(1, 13)]
S1_MONTHS = ["2025-01", "2025-02", "2025-04", "2025-05"]
S2_MONTHS = ["2025-07", "2025-09", "2025-10", "2025-12"]


# ---------------------------------------------------------------------------
# Funciones de lectura
# ---------------------------------------------------------------------------


def _load_silver() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Lee los cinco archivos Parquet de silver/ que necesita esta etapa.

    Retorna una tupla (lotes, costos, laboratorio, precio_cobre, tipo_cambio).
    Lanza FileNotFoundError si algún archivo no existe, porque sin esos
    datos la etapa no puede continuar.
    """
    lotes = pd.read_parquet(SILVER_DIR / "silver_lotes.parquet")
    costos = pd.read_parquet(SILVER_DIR / "silver_costos.parquet")
    laboratorio = pd.read_parquet(SILVER_DIR / "silver_laboratorio.parquet")
    precio_cobre = pd.read_parquet(SILVER_DIR / "silver_precio_cobre.parquet")
    tipo_cambio = pd.read_parquet(SILVER_DIR / "silver_tipo_cambio.parquet")
    return lotes, costos, laboratorio, precio_cobre, tipo_cambio


# ---------------------------------------------------------------------------
# REGLA 2: cobre fino por lote → suma mensual → conversión a libras
# ---------------------------------------------------------------------------


def _calcular_cobre_fino_mensual(lotes: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica REGLA 2: calcula el cobre fino primero a nivel de lote y luego
    agrega por mes.

    Fórmula por lote:
        cobre_fino_lote_t = toneladas_tratadas
                            × (ley_pct / 100)
                            × (recuperacion_pct / 100)

    La conversión a libras se realiza UNA SOLA VEZ sobre la suma mensual:
        cobre_fino_lb = SUM(cobre_fino_lote_t) × MT_TO_LB

    Este enfoque implementa la ponderación por tonelaje de forma natural:
    los lotes grandes pesan más en la suma sin necesidad de una columna
    de ley ponderada separada.

    Retorna un DataFrame con columnas [mes, cobre_fino_lb].
    """
    # Cálculo a nivel de lote (en toneladas métricas)
    lotes = lotes.copy()
    if "es_elegible" in lotes.columns:
        lotes = lotes[lotes["es_elegible"]].copy()
    lotes["cobre_fino_lote_t"] = (
        lotes["toneladas_tratadas"]
        * (lotes["ley_pct"] / 100.0)
        * (lotes["recuperacion_pct"] / 100.0)
    )

    # Suma mensual a nivel empresa y conversión a libras
    mensual = (
        lotes.groupby("mes", as_index=False)["cobre_fino_lote_t"]
        .sum()
        .rename(columns={"cobre_fino_lote_t": "cobre_fino_t"})
    )
    mensual["cobre_fino_lb"] = mensual["cobre_fino_t"] * MT_TO_LB
    return mensual[["mes", "cobre_fino_lb"]]


# ---------------------------------------------------------------------------
# REGLA 1 y REGLA 4: último valor hábil del mes para precios
# ---------------------------------------------------------------------------


def _ultimo_valor_del_mes(serie: pd.DataFrame, nombre_metrica: str) -> pd.DataFrame:
    """
    Aplica REGLA 1: para cada mes, extrae el valor del último día hábil
    disponible en la serie diaria.

    La serie silver tiene columnas [fecha (YYYY-MM-DD), valor]. Puede estar
    ordenada en cualquier sentido; esta función no asume orden.

    Aplica REGLA 4: si no hay ninguna cotización para un mes, el valor queda
    como NaN y se registra una alerta en el log. No se inventa ningún respaldo.

    Parámetros:
        serie          : DataFrame con columnas [fecha, valor]
        nombre_metrica : nombre legible para los mensajes del log (ej. "precio_cobre")

    Retorna un DataFrame con columnas [mes, valor], una fila por mes presente.
    """
    serie = serie.copy()

    # Derivar columna mes desde la fecha en formato YYYY-MM
    serie["mes"] = pd.to_datetime(serie["fecha"]).dt.strftime(f"{ANALYSIS_YEAR}-%m")

    # Filtrar solo el año de análisis (por seguridad ante datos fuera de rango)
    serie = serie[serie["mes"].str.startswith(str(ANALYSIS_YEAR))]

    # Para cada mes, conservar la fila con la fecha más reciente (último día hábil)
    idx_max = serie.groupby("mes")["fecha"].idxmax()
    resultado = serie.loc[idx_max, ["mes", "valor"]].reset_index(drop=True)

    return resultado


# ---------------------------------------------------------------------------
# REGLA 3: costos totales en CLP por mes
# ---------------------------------------------------------------------------


def _calcular_costos_clp_mensual(costos: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica REGLA 3: suma los tres componentes de costo (energía, insumos,
    mano de obra) por mes a nivel empresa (todas las plantas juntas).

    Si CUALQUIER fila del mes tiene un componente en NaN (valor faltante
    heredado de silver), el total mensual queda como NaN — no se inventa
    el costo parcial sumando solo las plantas disponibles, porque el total
    resultante estaría subvaluado y sesgaría el cash cost.

    Esto implementa la instrucción: "si falta cualquier input de un mes,
    dejar las métricas dependientes vacías para ese mes."

    Una alerta se registra por cada componente faltante detectado (con
    referencia de fila rastreable).

    Retorna un DataFrame con columnas [mes, costos_totales_clp].
    """
    costos = costos.copy()

    # Identificar meses que tienen al menos un componente faltante en alguna fila.
    # Estos meses completos quedarán con costos_totales_clp = NaN.
    componentes = ["energia_clp", "insumos_clp", "mano_obra_clp"]
    meses_incompletos: set[str] = set()

    # La ausencia de una planta no genera NaN por sí sola al sumar; por eso se
    # valida explícitamente el catálogo completo antes de consolidar.
    for mes in EXPECTED_MONTHS:
        filas_mes = costos[costos["mes"] == mes]
        plantas_presentes = set(filas_mes["planta"].dropna().astype(str))
        plantas_faltantes = sorted(br.EXPECTED_PLANTS - plantas_presentes)
        for planta in plantas_faltantes:
            meses_incompletos.add(mes)
            validation_log.register_failure(
                stage=_STAGE,
                dataset="silver_costos",
                source_ref=mes,
                field="planta",
                value=planta,
                rule=(
                    f"REGLA 3: planta '{planta}' ausente en {mes}; "
                    "costos_totales_usd quedará NaN para ese mes."
                ),
            )

        duplicadas = filas_mes["planta"].value_counts()
        for planta in duplicadas[duplicadas > 1].index:
            meses_incompletos.add(mes)
            validation_log.register_failure(
                stage=_STAGE,
                dataset="silver_costos",
                source_ref=mes,
                field="planta",
                value=planta,
                rule=(
                    f"REGLA 3: más de una fila para planta '{planta}' en {mes}; "
                    "costo mensual no calculable sin una regla adicional."
                ),
            )

    for col in componentes:
        filas_nan = costos[costos[col].isna()]
        for _, fila in filas_nan.iterrows():
            mes = fila["mes"]
            meses_incompletos.add(mes)
            # REGLA 3: no imputar; alertar con referencia rastreable
            validation_log.register_failure(
                stage=_STAGE,
                dataset="silver_costos",
                source_ref=fila.get("source_row_index", "?"),
                field=col,
                value="NaN",
                rule=(
                    f"REGLA 3 / IMPUTE_MISSING_COST={br.IMPUTE_MISSING_COST}: "
                    f"componente de costo '{col}' faltante en mes {mes}; "
                    "costos_totales_usd quedará NaN para ese mes."
                ),
            )

    # Calcular el total por fila (suma de las tres columnas).
    # NaN se propaga naturalmente: fila sin insumos → total de esa fila = NaN.
    costos["costos_totales_clp"] = (
        costos["energia_clp"]
        + costos["insumos_clp"]
        + costos["mano_obra_clp"]
    )

    # Agregar a nivel mensual.
    # Para los meses incompletos, forzar NaN explícitamente después de agrupar,
    # porque pandas.sum() omite NaN por defecto y produce un total subvaluado.
    mensual = (
        costos.groupby("mes", as_index=False)["costos_totales_clp"]
        .sum(min_count=1)
    )
    mensual = pd.DataFrame({"mes": EXPECTED_MONTHS}).merge(
        mensual, on="mes", how="left"
    )
    if meses_incompletos:
        mascara = mensual["mes"].isin(meses_incompletos)
        mensual.loc[mascara, "costos_totales_clp"] = float("nan")

    return mensual[["mes", "costos_totales_clp"]]


# ---------------------------------------------------------------------------
# Verificación de meses faltantes (REGLA 6)
# ---------------------------------------------------------------------------


def _verificar_meses_completos(gold: pd.DataFrame) -> None:
    """
    Aplica REGLA 6: verifica que los 12 meses de ANALYSIS_YEAR estén
    presentes en la tabla gold resultante.

    Si falta algún mes, registra una alerta en el log de validaciones.
    No modifica la tabla ni detiene la ejecución.
    """
    meses_esperados = {
        f"{ANALYSIS_YEAR}-{m:02d}" for m in range(1, 13)
    }
    meses_presentes = set(gold["mes"].tolist())
    meses_faltantes = sorted(meses_esperados - meses_presentes)

    for mes in meses_faltantes:
        validation_log.register_failure(
            stage=_STAGE,
            dataset="gold_metricas",
            source_ref=mes,
            field="mes",
            value=mes,
            rule=(
                f"REGLA 6: mes {mes} ausente en gold; "
                "no hay datos de producción en silver_lotes para ese mes."
            ),
        )


# ---------------------------------------------------------------------------
# Verificaciones de sanidad sobre gold (REGLA 7)
# ---------------------------------------------------------------------------


def _verificar_sanidad(gold: pd.DataFrame) -> None:
    """
    Aplica REGLA 7: ejecuta verificaciones de sanidad sobre la tabla gold.

    Comprueba no-negatividad en las columnas que no pueden ser negativas
    y que el precio del cobre esté dentro del rango histórico plausible.

    Registra una alerta por cada valor fuera de rango, pero NUNCA modifica
    los datos ni detiene la ejecución.
    """
    # Columnas que deben ser >= 0 (utilidades puede ser negativa, se omite)
    no_negativas = [
        "cobre_fino_lb",
        "ingresos_usd",
        "costos_totales_usd",
        "cash_cost_usd_lb",
    ]
    for col in no_negativas:
        mask = gold[col].notna() & (gold[col] < 0)
        for _, fila in gold[mask].iterrows():
            validation_log.register_failure(
                stage=_STAGE,
                dataset="gold_metricas",
                source_ref=fila["mes"],
                field=col,
                value=fila[col],
                rule=f"REGLA 7: {col} es negativo ({fila[col]:.4f}); valor no plausible.",
            )

    # Precio del cobre dentro del rango histórico
    lo, hi = br.VALID_RANGES["precio_cobre_usd_lb"]
    mask_precio = gold["precio_cobre_usd_lb"].notna() & (
        (gold["precio_cobre_usd_lb"] < lo) | (gold["precio_cobre_usd_lb"] > hi)
    )
    for _, fila in gold[mask_precio].iterrows():
        validation_log.register_failure(
            stage=_STAGE,
            dataset="gold_metricas",
            source_ref=fila["mes"],
            field="precio_cobre_usd_lb",
            value=fila["precio_cobre_usd_lb"],
            rule=(
                f"REGLA 7: precio_cobre_usd_lb={fila['precio_cobre_usd_lb']:.4f} "
                f"fuera del rango plausible [{lo}, {hi}]."
            ),
        )


# ---------------------------------------------------------------------------
# Resultados auxiliares: laboratorio, repetidos y revisión 2026
# ---------------------------------------------------------------------------


def _build_lab_sensitivity(
    lotes: pd.DataFrame, laboratorio: pd.DataFrame, gold: pd.DataFrame
) -> pd.DataFrame:
    """Recalcula un cash cost alternativo con la calibración mensual acordada."""
    lotes_validos = lotes[lotes["es_elegible"]].copy()
    laboratorio_valido = laboratorio.dropna(subset=["lote_id", "ley_lab_prom"])[
        ["lote_id", "ley_lab_prom"]
    ]
    pareados = lotes_validos.merge(
        laboratorio_valido, on="lote_id", how="inner", validate="many_to_one"
    )
    pareados["numerador"] = (
        pareados["toneladas_tratadas"] * pareados["ley_lab_prom"]
    )
    pareados["denominador"] = (
        pareados["toneladas_tratadas"] * pareados["ley_pct"]
    )
    mensual = pareados.groupby("mes", as_index=False).agg(
        registros_operacionales_pareados=("lote_id", "size"),
        registros_laboratorio_vinculables=("lote_id", "nunique"),
        numerador=("numerador", "sum"),
        denominador=("denominador", "sum"),
    )
    mensual["factor_calibracion_ley"] = (
        mensual["numerador"] / mensual["denominador"]
    )

    resultado = gold[
        ["mes", "cobre_fino_lb", "costos_totales_usd", "cash_cost_usd_lb"]
    ].merge(
        mensual[
            [
                "mes", "registros_operacionales_pareados",
                "registros_laboratorio_vinculables", "factor_calibracion_ley",
            ]
        ],
        on="mes",
        how="left",
    )
    resultado = resultado.rename(columns={
        "cobre_fino_lb": "cobre_fino_lb_principal",
        "cash_cost_usd_lb": "cash_cost_usd_lb_principal",
    })
    resultado["cobre_fino_lb_calibrado"] = (
        resultado["cobre_fino_lb_principal"]
        * resultado["factor_calibracion_ley"]
    )
    resultado["cash_cost_usd_lb_calibrado"] = (
        resultado["costos_totales_usd"] / resultado["cobre_fino_lb_calibrado"]
    )
    resultado["variacion_cash_cost_pct"] = (
        resultado["cash_cost_usd_lb_calibrado"]
        / resultado["cash_cost_usd_lb_principal"] - 1
    ) * 100
    return resultado


def _build_duplicate_sensitivity(lotes: pd.DataFrame, gold: pd.DataFrame) -> pd.DataFrame:
    """Escenario no adoptado: conserva la primera ocurrencia de cada lote_id."""
    lotes_validos = lotes[lotes["es_elegible"]].copy()
    lotes_validos["source_order"] = pd.to_numeric(
        lotes_validos["source_row_index"], errors="coerce"
    )
    sin_repeticiones = (
        lotes_validos.sort_values("source_order")
        .drop_duplicates("lote_id", keep="first")
    )
    alternativo = _calcular_cobre_fino_mensual(sin_repeticiones).rename(
        columns={"cobre_fino_lb": "cobre_fino_lb_sin_repeticiones_adicionales"}
    )
    resultado = gold[["mes", "cobre_fino_lb"]].merge(
        alternativo, on="mes", how="left"
    )
    resultado["variacion_cobre_fino_pct"] = (
        resultado["cobre_fino_lb_sin_repeticiones_adicionales"]
        / resultado["cobre_fino_lb"] - 1
    ) * 100
    return resultado


def _build_2026_review(lotes: pd.DataFrame, laboratorio: pd.DataFrame) -> pd.DataFrame:
    """Conserva los lotes fechados en 2026 y sus correspondencias de laboratorio."""
    lotes_2026 = lotes[
        lotes["fecha"].astype("string").str.startswith("2026-", na=False)
    ].copy()
    lab = laboratorio[
        ["lote_id", "laboratorio", "fecha_muestreo", "ley_lab_prom", "source_row_index"]
    ].rename(columns={"source_row_index": "lab_source_row_index"})
    resultado = lotes_2026.merge(
        lab, on="lote_id", how="left", validate="many_to_one"
    )
    resultado["tiene_laboratorio"] = resultado["ley_lab_prom"].notna()
    return resultado


def _reconcile_sources(lotes: pd.DataFrame, laboratorio: pd.DataFrame) -> dict[str, int]:
    """Compara conteos observados con las aserciones acordadas, sin forzarlos."""
    fechas = lotes["fecha"].astype("string")
    es_2025 = fechas.str.startswith(f"{ANALYSIS_YEAR}-", na=False)
    es_2026 = fechas.str.startswith("2026-", na=False)
    ids_validos = set(lotes.loc[lotes["es_elegible"], "lote_id"].dropna())
    conteos = {
        "lotes_originales": len(lotes),
        "lotes_2026": int(es_2026.sum()),
        "exclusiones_calidad_2025": int((es_2025 & ~lotes["es_elegible"]).sum()),
        "exclusiones_unicas": int((~lotes["es_elegible"]).sum()),
        "lotes_elegibles": int(lotes["es_elegible"].sum()),
        "lote_id_repetidos": int((lotes["lote_id"].value_counts() > 1).sum()),
        "laboratorio_vinculable": int(laboratorio["lote_id"].isin(ids_validos).sum()),
    }
    for nombre, observado in conteos.items():
        esperado = br.EXPECTED_SOURCE_COUNTS[nombre]
        if observado != esperado:
            validation_log.register_failure(
                stage=_STAGE,
                dataset="reconciliacion",
                source_ref=nombre,
                field=nombre,
                value=observado,
                rule=f"control esperado={esperado}; observado={observado}",
            )
    return conteos


def _weighted_metrics(gold: pd.DataFrame, meses: list[str]) -> tuple[float, float, float]:
    subset = gold[gold["mes"].isin(meses)].dropna(subset=["costos_totales_usd"])
    libras = subset["cobre_fino_lb"].sum()
    precio = (
        subset["precio_cobre_usd_lb"] * subset["cobre_fino_lb"]
    ).sum() / libras
    costo = subset["costos_totales_usd"].sum() / libras
    return precio, costo, precio - costo


def calcular_resumen_reconciliacion(gold: pd.DataFrame) -> dict[str, float]:
    """Calcula las cifras de conciliación desde Gold, sin escribir valores manuales."""
    por_mes = gold.set_index("mes")
    completos = gold.dropna(subset=["costos_totales_usd"])
    precio_s1, costo_s1, margen_s1 = _weighted_metrics(gold, S1_MONTHS)
    precio_s2, costo_s2, margen_s2 = _weighted_metrics(gold, S2_MONTHS)
    return {
        "cash_cost_enero": float(por_mes.loc["2025-01", "cash_cost_usd_lb"]),
        "cash_cost_diciembre": float(por_mes.loc["2025-12", "cash_cost_usd_lb"]),
        "variacion_enero_diciembre_pct": float((
            por_mes.loc["2025-12", "cash_cost_usd_lb"]
            / por_mes.loc["2025-01", "cash_cost_usd_lb"] - 1
        ) * 100),
        "cash_cost_ponderado_meses_completos": float(
            completos["costos_totales_usd"].sum()
            / completos["cobre_fino_lb"].sum()
        ),
        "margen_unitario_s1": float(margen_s1),
        "margen_unitario_s2": float(margen_s2),
        "efecto_precio": float(precio_s2 - precio_s1),
        "efecto_costo": float(-(costo_s2 - costo_s1)),
    }


# ---------------------------------------------------------------------------
# Punto de entrada público
# ---------------------------------------------------------------------------


def run_metrics() -> None:
    """
    Ejecuta la etapa de métricas completa de punta a punta.

    Orquesta la lectura de silver, todos los cálculos intermedios,
    las verificaciones de completitud y sanidad, y la escritura de gold.

    Es el único punto de entrada público de este módulo.
    main.py la llama directamente.
    """
    print("[metrics] Leyendo datos desde silver/...")
    lotes, costos, laboratorio, precio_cobre, tipo_cambio = _load_silver()
    conteos = _reconcile_sources(lotes, laboratorio)

    # ------------------------------------------------------------------
    # REGLA 2: cobre fino mensual en libras
    # ------------------------------------------------------------------
    print("[metrics] Calculando cobre fino mensual (REGLA 2)...")
    df_cobre = _calcular_cobre_fino_mensual(lotes)

    # ------------------------------------------------------------------
    # REGLA 1 + REGLA 4: último valor hábil del mes para precio y dólar
    # ------------------------------------------------------------------
    print("[metrics] Extrayendo precio de cierre mensual (REGLA 1 / REGLA 4)...")
    df_precio = _ultimo_valor_del_mes(precio_cobre, "precio_cobre").rename(
        columns={"valor": "precio_cobre_usd_lb"}
    )
    df_dolar = _ultimo_valor_del_mes(tipo_cambio, "tipo_cambio").rename(
        columns={"valor": "tipo_cambio_clp_usd"}
    )

    # Alertar si falta cotización para algún mes presente en lotes (REGLA 4)
    meses_con_produccion = set(df_cobre["mes"])
    for nombre, df_serie in [("precio_cobre", df_precio), ("tipo_cambio", df_dolar)]:
        meses_con_cotizacion = set(df_serie["mes"])
        for mes in sorted(meses_con_produccion - meses_con_cotizacion):
            validation_log.register_failure(
                stage=_STAGE,
                dataset=f"silver_{nombre}",
                source_ref=mes,
                field="fecha",
                value=mes,
                rule=(
                    f"REGLA 4: sin cotización de {nombre} para el mes {mes}; "
                    "métricas dependientes quedarán NaN."
                ),
            )

    # ------------------------------------------------------------------
    # REGLA 3: costos totales en CLP por mes
    # ------------------------------------------------------------------
    print("[metrics] Calculando costos mensuales (REGLA 3)...")
    df_costos_clp = _calcular_costos_clp_mensual(costos)

    # ------------------------------------------------------------------
    # Ensamblaje: join por mes de todas las series
    # ------------------------------------------------------------------
    print("[metrics] Ensamblando tabla gold...")

    # Base: todos los meses con producción registrada
    gold = df_cobre.copy()

    # Unir precio, tipo de cambio y costos; LEFT JOIN para conservar meses
    # con producción aunque falte cotización o costo (quedarán NaN)
    gold = gold.merge(df_precio, on="mes", how="left")
    gold = gold.merge(df_dolar, on="mes", how="left")
    gold = gold.merge(df_costos_clp, on="mes", how="left")

    # ------------------------------------------------------------------
    # Cálculos finales de métricas
    # ------------------------------------------------------------------

    # Ingresos = cobre fino (lb) × precio (USD/lb)
    gold["ingresos_usd"] = gold["cobre_fino_lb"] * gold["precio_cobre_usd_lb"]

    # Costos totales en USD = costos CLP / tipo de cambio CLP-USD
    gold["costos_totales_usd"] = gold["costos_totales_clp"] / gold["tipo_cambio_clp_usd"]

    # Cash cost = costos USD / cobre fino (lb)
    # Se usa NaN automáticamente si cobre_fino_lb == 0 (división segura vía pandas)
    gold["cash_cost_usd_lb"] = gold["costos_totales_usd"] / gold["cobre_fino_lb"]

    # Utilidades = ingresos - costos en USD
    gold["utilidades_usd"] = gold["ingresos_usd"] - gold["costos_totales_usd"]

    # ------------------------------------------------------------------
    # Ordenar por mes y seleccionar solo las columnas del esquema gold
    # ------------------------------------------------------------------
    gold = gold.sort_values("mes").reset_index(drop=True)
    gold = gold[GOLD_COLUMNS]

    # Resultados separados: no alteran el esquema ni la definición principal.
    sensibilidad_lab = _build_lab_sensitivity(lotes, laboratorio, gold)
    sensibilidad_repetidos = _build_duplicate_sensitivity(lotes, gold)
    revision_2026 = _build_2026_review(lotes, laboratorio)
    resumen = calcular_resumen_reconciliacion(gold)

    # ------------------------------------------------------------------
    # REGLA 6: verificar que estén los 12 meses
    # ------------------------------------------------------------------
    print("[metrics] Verificando completitud de meses (REGLA 6)...")
    _verificar_meses_completos(gold)

    # ------------------------------------------------------------------
    # REGLA 7: verificaciones de sanidad sobre valores calculados
    # ------------------------------------------------------------------
    print("[metrics] Ejecutando verificaciones de sanidad (REGLA 7)...")
    _verificar_sanidad(gold)

    # ------------------------------------------------------------------
    # Escribir gold
    # ------------------------------------------------------------------
    GOLD_DIR.mkdir(parents=True, exist_ok=True)
    gold_path = GOLD_DIR / GOLD_FILENAME
    gold.to_parquet(gold_path, index=False)
    sensibilidad_lab.to_parquet(
        GOLD_DIR / "gold_sensibilidad_laboratorio.parquet", index=False
    )
    sensibilidad_repetidos.to_parquet(
        GOLD_DIR / "gold_sensibilidad_lotes_repetidos.parquet", index=False
    )
    revision_2026.to_parquet(
        GOLD_DIR / "revision_lotes_2026_laboratorio.parquet", index=False
    )
    print(f"[metrics] gold guardado en: {gold_path}")
    print(f"[metrics] Filas en gold: {len(gold)} | Columnas: {list(gold.columns)}")
    print(f"[metrics] Controles de conciliacion: {conteos}")
    print(f"[metrics] Resumen de indicadores: {resumen}")
