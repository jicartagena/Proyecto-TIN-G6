"""
business_rules.py
=================
Módulo de reglas de negocio del pipeline CuprumAndes.

Este es el único lugar donde se definen las reglas analíticas del proyecto.
Centralizar las reglas aquí facilita la revisión por parte del equipo y del
evaluador: cualquier cambio de criterio se hace en un solo lugar.

Cada regla está documentada con su nombre, descripción y el lugar donde se
aplicará. La lógica concreta se implementa en las etapas correspondientes;
este módulo declara las reglas, la configuración de comportamiento y los
rangos de sanidad usados en métricas.
"""

# ---------------------------------------------------------------------------
# REGLA 1: Precio del cobre y tipo de cambio USD-CLP
# ---------------------------------------------------------------------------
# Se usa el valor del ÚLTIMO DÍA HÁBIL del mes tal como lo entrega la API.
# El valor se aplica a TODA la extracción del mismo mes (no hay interpolación
# ni promedio): un precio por mes.
#
# Lugar de aplicación: etapa de métricas (metrics.py), al calcular ingresos
# y al convertir costos de CLP a USD.
#
# Justificación: simplifica el modelo y es consistente con el criterio
# de valorización al cierre del mes.

# ---------------------------------------------------------------------------
# REGLA 2: Cobre fino por lote y suma mensual
# ---------------------------------------------------------------------------
# El cobre fino se calcula PRIMERO a nivel de lote (batch):
#   cobre_fino_lote_t = toneladas_tratadas × ley_pct/100 × recuperacion_pct/100
# Luego se SUMA a nivel mensual a nivel empresa (sin distinción de planta).
# La conversión a libras ocurre UNA SOLA VEZ sobre la suma mensual:
#   cobre_fino_mes_lb = cobre_fino_mes_t × MT_TO_LB  (de config.py)
#
# Lugar de aplicación: etapa de métricas (metrics.py).
#
# Justificación: la ponderación por tonelaje surge automáticamente de operar
# a nivel de lote antes de agregar. No se agrega una columna de ley ponderada
# separada; el efecto del tonelaje queda implícito en el cálculo por lote.

# ---------------------------------------------------------------------------
# REGLA 3: Tratamiento de un componente de costo faltante
# ---------------------------------------------------------------------------
# Si falta el valor de un componente de costo (energía, insumos o mano de obra)
# para algún mes/planta, el comportamiento por defecto es:
#   - NO imputar el valor.
#   - Registrar una alerta en el log de validaciones.
#   - El mes afectado tendrá costos_totales_usd, cash_cost_usd_lb y
#     utilidades_usd como NaN (ningún valor inventado).
#
# Este comportamiento se controla con la constante siguiente:
IMPUTE_MISSING_COST = False  # True = imputar con cero; False = dejar NaN y alertar

# La completitud mensual requiere presencia de las cuatro plantas, además de
# los tres componentes de costo. Planta-mes sigue siendo el grano de origen;
# la agregación final se mantiene a nivel empresa-mes.
EXPECTED_PLANTS = {"Cerro Norte", "Loa", "Salado", "Illapel"}

# Controles de conciliación de la fuente vigente. Son aserciones para detectar
# diferencias; nunca se usan para rellenar ni forzar resultados.
EXPECTED_SOURCE_COUNTS = {
    "lotes_originales": 14_598,
    "lotes_2026": 86,
    "exclusiones_calidad_2025": 272,
    "exclusiones_unicas": 358,
    "lotes_elegibles": 14_240,
    "lote_id_repetidos": 144,
    "laboratorio_vinculable": 843,
}

# Elegibilidad de lotes para el cálculo principal. La fecha normalizada debe
# pertenecer a 2025; el campo mes original se usa sólo como control.
LOT_GRADE_MIN_EXCLUSIVE = 0.0
LOT_GRADE_MAX_EXCLUSIVE = 5.0
LOT_RECOVERY_MIN_INCLUSIVE = 50.0
LOT_RECOVERY_MAX_INCLUSIVE = 100.0
LOT_TONNES_MIN_INCLUSIVE = 100.0

# Lugar de aplicación: etapa de limpieza (cleaning.py) y métricas (metrics.py).
#
# Justificación: imputar un costo con cero distorsionaría el cash cost.
# La decisión de negocio (qué hacer con ese mes) queda en manos del analista
# a partir de la alerta, no del código.

# ---------------------------------------------------------------------------
# REGLA 4: Sin cotización en el último día del mes
# ---------------------------------------------------------------------------
# Si la API no entrega precio de cobre o tipo de cambio para el último día
# de un mes (por feriado o fin de semana), el comportamiento es:
#   - Mantener la regla: usar el último día hábil disponible del mes.
#   - Registrar una alerta en el log de validaciones con el mes afectado.
#   - NO inventar un valor: las métricas dependientes quedan como NaN.
#
# Lugar de aplicación: etapa de métricas (metrics.py) al buscar el precio
# de cierre de cada mes.
#
# Justificación: inventar un respaldo (día anterior, promedio) introduce un
# supuesto no acordado con la gerencia. El criterio conservador es alertar
# y esperar una decisión explícita.

# ---------------------------------------------------------------------------
# REGLA 5: Rangos de validez para validaciones de formato
# ---------------------------------------------------------------------------
# Estos rangos se usan en la etapa de limpieza (cleaning.py) para validar
# que los valores, una vez convertidos al tipo correcto, sean plausibles.
# Un valor fuera de rango NO detiene la ejecución: genera una alerta.

VALID_RANGES = {
    # Ley de cobre en porcentaje (%). Rango plausible para una concentradora.
    # Fuente: experiencia de dominio; ajustar si los datos lo requieren.
    "ley_pct": (0.0, 5.0),

    # Recuperación metalúrgica en porcentaje (%).
    # Valores fuera de [0, 100] son imposibles; se estrecha el rango inferior.
    "recuperacion_pct": (50.0, 100.0),

    # Toneladas tratadas por lote. Límite inferior > 0 (un lote vacío es error).
    # Límite superior orientativo; ajustar si las plantas son muy grandes.
    "toneladas_tratadas": (0.0, 500_000.0),

    # Costos en CLP por mes. Mínimo > 0 (no se espera costo cero).
    # Máximo orientativo; ajustar según la escala real de la empresa.
    "energia_clp": (0.0, 50_000_000_000.0),
    "insumos_clp": (0.0, 50_000_000_000.0),
    "mano_obra_clp": (0.0, 50_000_000_000.0),

    # Precio del cobre en USD/lb. Rango histórico amplio para 2025.
    "precio_cobre_usd_lb": (1.0, 20.0),

    # Tipo de cambio USD-CLP. Rango histórico amplio para 2025.
    "tipo_cambio_usd_clp": (500.0, 2000.0),
}

# ---------------------------------------------------------------------------
# REGLA 6: Meses faltantes en gold
# ---------------------------------------------------------------------------
# Se espera exactamente un registro por cada mes de ANALYSIS_YEAR (12 meses).
# Si algún mes no tiene datos de producción en silver_lotes, se registra
# una alerta pero la fila de ese mes no se agrega a gold (no se inventa).
#
# Lugar de aplicación: etapa de métricas (metrics.py) en la validación final.
#
# Justificación: una fila de métricas con todos los valores en NaN no aporta
# información y puede confundir al consumidor downstream.

# ---------------------------------------------------------------------------
# REGLA 7: Verificaciones de sanidad sobre gold
# ---------------------------------------------------------------------------
# Después de construir la tabla gold, se ejecutan verificaciones de sanidad
# que alertan si se detectan valores fuera de lo plausible, pero NUNCA
# detienen la ejecución ni modifican los valores calculados.
#
# Verificaciones:
#   - cobre_fino_lb >= 0
#   - precio_cobre_usd_lb en rango VALID_RANGES["precio_cobre_usd_lb"]
#   - ingresos_usd >= 0
#   - costos_totales_usd >= 0
#   - cash_cost_usd_lb >= 0
#   - utilidades_usd puede ser negativa (no se alerta por signo)
#
# Lugar de aplicación: etapa de métricas (metrics.py) al final de run_metrics().
#
# Justificación: detectar errores de cálculo o datos atípicos sin interrumpir
# la entrega de resultados al equipo.
