"""
cleaning.py
===========
Etapa 2 del pipeline: Limpieza y validacion.

Lee exclusivamente desde bronze/ (Parquet, todos los valores como texto) y escribe
datos limpios y tipados a silver/ (Parquet).

Responsabilidades:
  1. Leer los cinco conjuntos bronze, incluido laboratorio.
  2. Eliminar filas de cabecera repetida y la fila TOTAL de costos; registrar cada eliminacion.
  3. Estandarizar formatos:
       - fecha (lotes)   : cuatro formatos observados -> "YYYY-MM-DD"; mes derivado de fecha
       - mes (costos)    : "YYYY-MM" ya canonico -> validar y dejar como string
       - fecha (series)  : ISO 8601 UTC -> "YYYY-MM-DD"
       - numericos costos: eliminar "$", puntos de miles, comas decimales, ",00" -> float
       - numericos lotes : manejar punto-como-miles y coma-como-decimal -> float
  4. Validar CADA campo despues de la conversion usando VALID_RANGES de business_rules.
     Una falla de validacion se registra en el log y NUNCA detiene la ejecucion.
  5. Detectar duplicados y registrar alertas (no se eliminan).
  6. Verificar que las series de precios tengan cotizacion en el ultimo dia de cada mes.
  7. Escribir silver_costos, silver_lotes, silver_laboratorio y las dos series de precios.

Por que esta etapa existe separada de las metricas:
  Separa "limpiar" de "calcular". Silver contiene solo datos tipados y verificados;
  ninguna metrica derivada, ninguna agregacion y ninguna conversion de moneda.
"""

import json
import re
import unicodedata
from datetime import datetime
import pandas as pd

from config import BRONZE_DIR, SILVER_DIR, ANALYSIS_YEAR
import business_rules as br
import validation_log as vlog

# ---------------------------------------------------------------------------
# Constantes internas de este modulo
# ---------------------------------------------------------------------------

# Nombre de etapa que aparece en cada registro del log
_STAGE = "cleaning"

# Patron canonico de mes: "YYYY-MM"
_CANONICAL_MONTH_RE = re.compile(r"^\d{4}-\d{2}$")

# Patron canonico de fecha: "YYYY-MM-DD"
_CANONICAL_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

# Abreviaturas observadas en la fuente operacional.
_SPANISH_MONTHS = {
    "ene": 1, "feb": 2, "mar": 3, "abr": 4, "may": 5, "jun": 6,
    "jul": 7, "ago": 8, "sep": 9, "sept": 9, "oct": 10,
    "nov": 11, "dic": 12,
}

# Variantes observadas en el CSV de costos.
_PLANT_ALIASES = {
    "CERRO NORTE": "Cerro Norte",
    "P. CERRO NORTE": "Cerro Norte",
    "PLANTA CERRO NORTE": "Cerro Norte",
    "LOA": "Loa",
    "P. LOA": "Loa",
    "PLANTA LOA": "Loa",
    "SALADO": "Salado",
    "P. SALADO": "Salado",
    "PLANTA SALADO": "Salado",
    "ILLAPEL": "Illapel",
    "P. ILLAPEL": "Illapel",
    "PLANTA ILLAPEL": "Illapel",
}


# ===========================================================================
# FUNCIONES DE CONVERSION NUMERICA
# ===========================================================================

def _parse_clp_amount(raw: str):
    """
    Convierte un valor de costo en CLP desde texto a float.

    Maneja todos los formatos observados en bronze_costos:
      - Prefijo "$"                         -> se elimina
      - Puntos como separador de miles      -> se eliminan
      - Coma decimal (incluso trailing ",00") -> reemplazada por punto
      - Numero limpio sin formato especial  -> se convierte directamente

    Retorna None si la conversion falla; el llamador registrara la alerta.
    """
    s = str(raw).strip()
    if not s:
        return None
    # Eliminar simbolo de moneda
    s = s.replace("$", "").strip()
    # Punto Y coma presentes: puntos son miles, coma es decimal
    if "." in s and "," in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        # Solo coma: decimal (incluye trailing ",00")
        s = s.replace(",", ".")
    else:
        # Solo puntos o ninguno
        # Si hay exactamente un punto y exactamente 3 digitos tras el -> miles
        parts = s.split(".")
        if len(parts) > 1 and all(len(p) == 3 and p.isdigit() for p in parts[1:]):
            s = s.replace(".", "")
        # De lo contrario el punto es decimal (caso raro en CLP pero posible)
    try:
        return float(s)
    except ValueError:
        return None


def _parse_numeric_lotes(raw: str, field: str):
    """
    Convierte un valor numerico de lotes desde texto a float.

    En bronze_lotes coexisten dos convenciones de formato:
      - Punto como separador de miles (ej. "2.834", "3.191" en toneladas)
      - Coma como separador decimal   (ej. "0,85", "90,5")
      - Numeros enteros limpios       (ej. "2962", "85")

    La heuristica para el punto es DEPENDIENTE DEL CAMPO porque el patron
    "X.XXX" (un digito entero, punto, tres digitos) aparece tanto en toneladas
    como separador de miles (ej. "2.834" = 2834 t) como en ley como decimal
    (ej. "1.005" = 1.005 %Cu). No es posible distinguirlos solo por formato.

    Reglas por campo:
      - toneladas_tratadas: X.XXX con parte entera >= 1 -> miles (2.834 = 2834).
        Si parte entera == "0" -> decimal (no hay 0.XXX en toneladas en este dataset).
      - ley_pct: NUNCA se trata el punto como miles. Los valores de ley en este
        dataset son todos < 2 %Cu; un valor de 1000+ seria imposible.
        Excepcion: si hay coma en el mismo string, la coma es decimal y el punto
        seria miles, pero eso no ocurre en ley_pct en este dataset.
      - recuperacion_pct: igual que ley_pct; los valores son 85-115, nunca miles.
      - Cualquier otro campo: heuristica generica (parte entera != "0" y 3 digitos
        exactos tras el punto -> miles).

    Retorna None si la conversion falla.
    """
    s = str(raw).strip()
    if not s:
        return None

    if "," in s:
        # Coma decimal en todos los campos; puntos previos (si los hay) son miles
        s = s.replace(".", "").replace(",", ".")
    elif "." in s:
        parts = s.split(".")
        three_digit_decimal = (
            len(parts) == 2
            and len(parts[1]) == 3
            and parts[1].isdigit()
            and parts[0].isdigit()
        )
        if three_digit_decimal:
            if field == "toneladas_tratadas" and parts[0] != "0":
                # Toneladas: parte entera >= 1 y 3 digitos -> miles (2.834 = 2834)
                s = s.replace(".", "")
            elif field in ("ley_pct", "recuperacion_pct"):
                # Leyes y recuperacion: NUNCA son miles; el punto es siempre decimal
                pass  # dejar s sin cambio -> float(s) lo interpreta como decimal
            else:
                # Campo generico: parte entera != "0" -> miles
                if parts[0] != "0":
                    s = s.replace(".", "")
        # Si no cumple three_digit_decimal el punto ya es decimal; no se modifica s
    try:
        return float(s)
    except ValueError:
        return None


# ===========================================================================
# FUNCIONES DE ESTANDARIZACION DE FECHAS / MESES
# ===========================================================================

def _standardize_month_from_int(raw: str):
    """
    Convierte un numero de mes entero (ej. "3", "12") al formato canonico "YYYY-MM".

    El anio se toma de ANALYSIS_YEAR definido en config.py, porque bronze_lotes
    solo contiene el numero de mes sin anio (artefacto de la fuente original SQLite).
    Retorna None si el valor no puede interpretarse como mes valido (1-12).
    """
    s = raw.strip()
    try:
        m = int(s)
    except ValueError:
        return None
    if not (1 <= m <= 12):
        return None
    return f"{ANALYSIS_YEAR}-{m:02d}"


def _standardize_month_costos(raw: str):
    """
    Valida y normaliza el campo 'mes' de costos, que ya deberia venir en "YYYY-MM".

    Si el valor coincide con el patron canonico lo retorna sin modificaciones.
    Si no coincide retorna None para que el llamador registre la alerta.
    """
    s = raw.strip()
    if _CANONICAL_MONTH_RE.match(s):
        return s
    return None


def _standardize_date_iso(raw: str):
    """
    Convierte una fecha ISO 8601 UTC (ej. "2025-12-30T03:00:00.000Z") a "YYYY-MM-DD".

    La API de mindicador.cl entrega las fechas siempre en ese formato ISO.
    Se usa pandas.to_datetime con utc=True para manejar cualquier variante del formato.
    Retorna None si la conversion falla.
    """
    s = raw.strip()
    if not s:
        return None
    try:
        dt = pd.to_datetime(s, utc=True)
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return None


def _standardize_operational_date(raw: str):
    """
    Normaliza la fecha operacional usando únicamente los cuatro formatos
    observados: YYYY-MM-DD, DD/MM/YYYY, DD-MM-YY y D-mmm-YYYY con mes en
    español. Los formatos con día primero se interpretan explícitamente para
    no invertir día y mes de manera heurística.
    """
    s = str(raw).strip().lower()
    try:
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
            parsed = datetime.strptime(s, "%Y-%m-%d")
        elif re.fullmatch(r"\d{1,2}/\d{1,2}/\d{4}", s):
            parsed = datetime.strptime(s, "%d/%m/%Y")
        elif re.fullmatch(r"\d{1,2}-\d{1,2}-\d{2}", s):
            parsed = datetime.strptime(s, "%d-%m-%y")
        else:
            match = re.fullmatch(r"(\d{1,2})-([a-záéíóúñ]+)-(\d{4})", s)
            if not match:
                return None
            token = "".join(
                char for char in unicodedata.normalize("NFD", match.group(2))
                if unicodedata.category(char) != "Mn"
            )
            month = _SPANISH_MONTHS.get(token)
            if month is None:
                return None
            parsed = datetime(int(match.group(3)), month, int(match.group(1)))
    except ValueError:
        return None
    return parsed.strftime("%Y-%m-%d")


def _standardize_plant(raw: str):
    """Normaliza las variantes de planta presentes en el CSV."""
    key = " ".join(str(raw).strip().upper().split())
    return _PLANT_ALIASES.get(key)


def _parse_integer(raw: str):
    """Convierte un identificador entero; retorna None si no es interpretable."""
    try:
        return int(str(raw).strip())
    except (TypeError, ValueError):
        return None


def evaluar_elegibilidad_lote(fecha, toneladas, ley, recuperacion) -> list[str]:
    """Retorna las causas de exclusión del cálculo principal."""
    causas = []
    if fecha is None:
        causas.append("fecha_no_interpretable")
    elif int(fecha[:4]) != ANALYSIS_YEAR:
        causas.append(f"fecha_fuera_de_{ANALYSIS_YEAR}")
    if toneladas is None:
        causas.append("toneladas_no_interpretables")
    elif toneladas < br.LOT_TONNES_MIN_INCLUSIVE:
        causas.append("toneladas_menores_a_100")
    if ley is None:
        causas.append("ley_no_interpretable")
    elif not (br.LOT_GRADE_MIN_EXCLUSIVE < ley < br.LOT_GRADE_MAX_EXCLUSIVE):
        causas.append("ley_fuera_de_0_a_5_exclusivo")
    if recuperacion is None:
        causas.append("recuperacion_no_interpretable")
    elif not (
        br.LOT_RECOVERY_MIN_INCLUSIVE
        <= recuperacion
        <= br.LOT_RECOVERY_MAX_INCLUSIVE
    ):
        causas.append("recuperacion_fuera_de_50_a_100")
    return causas


# ===========================================================================
# FUNCIONES DE VALIDACION DE CAMPO
# ===========================================================================

def _validate_month(value, dataset: str, source_ref: str, field: str, original: str) -> None:
    """
    Valida que el mes canonico tenga formato "YYYY-MM" y pertenezca a ANALYSIS_YEAR.

    Registra una alerta si:
      - value es None (fallo la estandarizacion)
      - el formato no coincide con el patron canonico
      - el anio no es ANALYSIS_YEAR
    """
    if value is None:
        vlog.register_failure(
            stage=_STAGE, dataset=dataset, source_ref=source_ref,
            field=field, value=original,
            rule=f"mes no pudo estandarizarse a YYYY-MM (valor original: '{original}')",
        )
        return
    if not _CANONICAL_MONTH_RE.match(value):
        vlog.register_failure(
            stage=_STAGE, dataset=dataset, source_ref=source_ref,
            field=field, value=original,
            rule=f"mes estandarizado '{value}' no cumple formato YYYY-MM",
        )
        return
    year = int(value[:4])
    if year != ANALYSIS_YEAR:
        vlog.register_failure(
            stage=_STAGE, dataset=dataset, source_ref=source_ref,
            field=field, value=original,
            rule=f"mes '{value}' esta fuera del anio de analisis {ANALYSIS_YEAR}",
        )


def _validate_date(value, dataset: str, source_ref: str, field: str, original: str) -> None:
    """
    Valida que la fecha canonica tenga formato "YYYY-MM-DD" y pertenezca a ANALYSIS_YEAR.

    Registra una alerta si la conversion fallo, el formato es incorrecto o el anio
    no es ANALYSIS_YEAR.
    """
    if value is None:
        vlog.register_failure(
            stage=_STAGE, dataset=dataset, source_ref=source_ref,
            field=field, value=original,
            rule=f"fecha no pudo estandarizarse a YYYY-MM-DD (valor original: '{original}')",
        )
        return
    if not _CANONICAL_DATE_RE.match(value):
        vlog.register_failure(
            stage=_STAGE, dataset=dataset, source_ref=source_ref,
            field=field, value=original,
            rule=f"fecha estandarizada '{value}' no cumple formato YYYY-MM-DD",
        )
        return
    year = int(value[:4])
    if year != ANALYSIS_YEAR:
        vlog.register_failure(
            stage=_STAGE, dataset=dataset, source_ref=source_ref,
            field=field, value=original,
            rule=f"fecha '{value}' esta fuera del anio de analisis {ANALYSIS_YEAR}",
        )


def _validate_numeric(value, field: str, dataset: str, source_ref: str, original: str) -> None:
    """
    Valida que un campo numerico sea no-nulo, no-negativo y dentro del rango plausible
    definido en business_rules.VALID_RANGES.

    Tres reglas se verifican en secuencia:
      1. No nulo: el valor debe haber podido convertirse desde texto.
      2. No negativo: costos, toneladas y porcentajes no pueden ser negativos.
      3. Rango plausible: detecta transformaciones erroneas (ej. coma leida como miles).

    Se registra una alerta por cada regla que falle; multiples alertas en un mismo
    campo son posibles y deseadas para que el autor entienda exactamente que ocurrio.
    """
    if value is None:
        vlog.register_failure(
            stage=_STAGE, dataset=dataset, source_ref=source_ref,
            field=field, value=original,
            rule=f"campo '{field}' no pudo convertirse a numerico (valor original: '{original}')",
        )
        return
    if value < 0:
        vlog.register_failure(
            stage=_STAGE, dataset=dataset, source_ref=source_ref,
            field=field, value=str(value),
            rule=f"campo '{field}' es negativo ({value}); se espera >= 0",
        )
    rng = br.VALID_RANGES.get(field)
    if rng is not None:
        lo, hi = rng
        if not (lo <= value <= hi):
            vlog.register_failure(
                stage=_STAGE, dataset=dataset, source_ref=source_ref,
                field=field, value=str(value),
                rule=(
                    f"campo '{field}' = {value} fuera del rango plausible "
                    f"[{lo}, {hi}]; posible transformacion incorrecta"
                ),
            )


def _validate_percentage(value, field: str, dataset: str, source_ref: str, original: str) -> None:
    """
    Valida que un porcentaje este en unidades de porcentaje (0-100), no fracciones (0-1).

    Esta validacion es adicional a _validate_numeric y se aplica a los campos
    'ley_pct' y 'recuperacion_pct'. Alerta si el valor esta entre 0 y 1 (probable
    lectura como fraccion en lugar de porcentaje) o si supera 100.
    """
    _validate_numeric(value, field, dataset, source_ref, original)
    if value is None:
        return
    if 0 < value <= 1.0:
        vlog.register_failure(
            stage=_STAGE, dataset=dataset, source_ref=source_ref,
            field=field, value=str(value),
            rule=(
                f"campo '{field}' = {value} esta en rango (0,1]; "
                "se esperan unidades de porcentaje (0-100), no fraccion"
            ),
        )
    if value > 100:
        vlog.register_failure(
            stage=_STAGE, dataset=dataset, source_ref=source_ref,
            field=field, value=str(value),
            rule=f"campo '{field}' = {value} supera 100%; imposible para un porcentaje",
        )


# ===========================================================================
# LIMPIEZA POR DATASET
# ===========================================================================

def _clean_costos(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpia y valida bronze_costos -> produce silver_costos.

    Pasos:
      1. Detectar y descartar la fila de cabecera repetida (donde 'mes' == 'mes').
         Se registra en el log; no pasa a silver.
      2. Detectar y descartar la fila TOTAL/vacia (mes vacio o "TOTAL").
         Se registra en el log; no pasa a silver.
      3. Estandarizar 'mes' al formato canonico "YYYY-MM".
      4. Convertir energia_clp, insumos_clp, mano_obra_clp a float.
         Valores 'N/A' y vacios quedan como NaN; se registra una alerta por cada uno.
         El comportamiento esta controlado por business_rules.IMPUTE_MISSING_COST.
      5. Validar todos los campos con rango y tipo.
      6. Detectar filas duplicadas (misma combinacion mes+energia+insumos+mano_obra).
         Las filas duplicadas se mantienen en silver; solo se alerta.

    La planta se conserva y se estandariza porque la completitud mensual exige
    verificar la presencia de las cuatro plantas esperadas.
    """
    dataset = "bronze_costos"
    numeric_fields = ["energia_clp", "insumos_clp", "mano_obra_clp"]
    out_rows = []

    for _, row in df.iterrows():
        ref = row["source_row_index"]
        mes_raw = str(row["mes"]).strip()
        planta_raw = str(row["planta"]).strip()

        # Paso 1: detectar y descartar cabecera repetida
        if mes_raw.lower() == "mes":
            vlog.register_failure(
                stage=_STAGE, dataset=dataset, source_ref=ref,
                field="mes", value=mes_raw,
                rule="fila de cabecera repetida dentro de los datos; se elimina de silver",
            )
            continue

        # Paso 2: detectar y descartar fila TOTAL / vacia
        if mes_raw == "" or mes_raw.upper() == "TOTAL":
            vlog.register_failure(
                stage=_STAGE, dataset=dataset, source_ref=ref,
                field="mes", value=mes_raw,
                rule="fila de resumen/TOTAL sin periodo valido; se elimina de silver",
            )
            continue

        # Paso 3: estandarizar mes
        mes_clean = _standardize_month_costos(mes_raw)
        _validate_month(mes_clean, dataset, ref, "mes", mes_raw)
        planta_clean = _standardize_plant(planta_raw)
        if planta_clean is None:
            vlog.register_failure(
                stage=_STAGE, dataset=dataset, source_ref=ref,
                field="planta", value=planta_raw,
                rule="planta no reconocida entre las cuatro plantas esperadas",
            )

        # Paso 4: convertir numericos
        numeric_values = {}
        for field in numeric_fields:
            raw_val = str(row[field]).strip()
            if raw_val.upper() in ("N/A", "NA", ""):
                # Componente faltante: comportamiento controlado por regla de negocio
                if not br.IMPUTE_MISSING_COST:
                    vlog.register_failure(
                        stage=_STAGE, dataset=dataset, source_ref=ref,
                        field=field, value=raw_val,
                        rule=(
                            f"componente de costo '{field}' faltante "
                            f"(valor original: '{raw_val}'); "
                            "IMPUTE_MISSING_COST=False -> se deja como NaN sin imputar"
                        ),
                    )
                numeric_values[field] = None
            else:
                parsed = _parse_clp_amount(raw_val)
                _validate_numeric(parsed, field, dataset, ref, raw_val)
                numeric_values[field] = parsed

        out_rows.append({
            "planta_original": planta_raw,
            "planta": planta_clean,
            "mes": mes_clean,
            "energia_clp": numeric_values["energia_clp"],
            "insumos_clp": numeric_values["insumos_clp"],
            "mano_obra_clp": numeric_values["mano_obra_clp"],
            "source_file": row["source_file"],
            "source_row_index": ref,
        })

    result = pd.DataFrame(out_rows)
    if result.empty:
        return result

    result["mes"] = result["mes"].astype("string")
    result["planta_original"] = result["planta_original"].astype("string")
    result["planta"] = result["planta"].astype("string")
    for f in numeric_fields:
        result[f] = pd.to_numeric(result[f], errors="coerce")
    result["source_file"] = result["source_file"].astype("string")
    result["source_row_index"] = result["source_row_index"].astype("string")

    # Validar no-nulos residuales en mes
    for _, row in result.iterrows():
        if pd.isna(row["mes"]):
            vlog.register_failure(
                stage=_STAGE, dataset=dataset, source_ref=row["source_row_index"],
                field="mes", value="",
                rule="mes quedo nulo tras la estandarizacion; requiere revision manual",
            )

    # Detectar duplicados (se mantienen en silver; solo se alerta)
    dup_cols = ["planta", "mes", "energia_clp", "insumos_clp", "mano_obra_clp"]
    dup_mask = result.duplicated(subset=dup_cols, keep=False)
    if dup_mask.any():
        for _, row in result[dup_mask].iterrows():
            vlog.register_failure(
                stage=_STAGE, dataset=dataset, source_ref=row["source_row_index"],
                field="(planta,mes,energia_clp,insumos_clp,mano_obra_clp)",
                value=str(row[dup_cols].to_dict()),
                rule="fila duplicada detectada; se mantiene en silver para revision del autor",
            )

    return result


def _clean_lotes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpia y valida bronze_lotes -> produce silver_lotes.

    Pasos:
      1. Estandarizar 'fecha' y derivar 'mes' desde ella; conservar ambos originales.
      2. Convertir toneladas_tratadas a float (punto-miles / coma-decimal).
      3. Convertir ley_pct y recuperacion_pct a float.
      4. Marcar elegibilidad con las reglas acordadas, sin eliminar registros.
      5. Detectar lote_id repetidos (se mantienen en silver; solo se alerta).

    No hay cabeceras repetidas en bronze_lotes (la fuente es SQLite).
    """
    dataset = "bronze_lotes"
    out_rows = []

    for _, row in df.iterrows():
        ref = row["source_row_index"]

        # Paso 1: estandarizar la fecha operacional y derivar el mes oficial
        fecha_raw = str(row["fecha"]).strip()
        fecha_clean = _standardize_operational_date(fecha_raw)
        mes_clean = fecha_clean[:7] if fecha_clean is not None else None
        mes_raw = str(row["mes"]).strip()
        mes_original_comparable = (
            f"{ANALYSIS_YEAR}-{int(mes_raw):02d}"
            if mes_raw.isdigit() and 1 <= int(mes_raw) <= 12
            else None
        )
        if fecha_clean is None:
            vlog.register_failure(
                stage=_STAGE, dataset=dataset, source_ref=ref,
                field="fecha", value=fecha_raw,
                rule="fecha operacional no interpretable en los formatos observados",
            )
        if mes_clean != mes_original_comparable:
            vlog.register_failure(
                stage=_STAGE, dataset=dataset, source_ref=ref,
                field="mes", value=mes_raw,
                rule=(
                    f"mes original discrepa del mes derivado de fecha ({mes_clean}); "
                    "se usa la fecha operacional"
                ),
            )

        # Paso 2: convertir toneladas
        ton_raw = str(row["toneladas_tratadas"]).strip()
        ton = _parse_numeric_lotes(ton_raw, "toneladas_tratadas")

        # Paso 3: convertir y validar porcentajes
        ley_raw = str(row["ley_pct"]).strip()
        ley = _parse_numeric_lotes(ley_raw, "ley_pct")

        rec_raw = str(row["recuperacion_pct"]).strip()
        rec = _parse_numeric_lotes(rec_raw, "recuperacion_pct")

        # Paso 4: marcar elegibilidad sin descartar la fila de silver
        causas = evaluar_elegibilidad_lote(fecha_clean, ton, ley, rec)
        for causa in causas:
            vlog.register_failure(
                stage=_STAGE, dataset=dataset, source_ref=ref,
                field="elegibilidad", value=str(row["lote_id"]),
                rule=f"lote excluido del calculo principal: {causa}",
            )

        out_rows.append({
            "lote_id": _parse_integer(row["lote_id"]),
            "fecha_original": fecha_raw,
            "fecha": fecha_clean,
            "planta_id": _parse_integer(row["planta_id"]),
            "rajo_id": _parse_integer(row["rajo_id"]),
            "mes_original": mes_raw,
            "mes": mes_clean,
            "toneladas_tratadas": ton,
            "ley_pct": ley,
            "recuperacion_pct": rec,
            "es_elegible": not causas,
            "motivo_exclusion": "|".join(causas),
            "source_file": row["source_file"],
            "source_row_index": ref,
        })

    result = pd.DataFrame(out_rows)
    if result.empty:
        return result

    for field in ["lote_id", "planta_id", "rajo_id"]:
        result[field] = pd.array(result[field], dtype="Int64")
    for field in ["fecha_original", "fecha", "mes_original", "mes", "motivo_exclusion"]:
        result[field] = result[field].astype("string")
    result["toneladas_tratadas"] = pd.to_numeric(result["toneladas_tratadas"], errors="coerce")
    result["ley_pct"] = pd.to_numeric(result["ley_pct"], errors="coerce")
    result["recuperacion_pct"] = pd.to_numeric(result["recuperacion_pct"], errors="coerce")
    result["es_elegible"] = result["es_elegible"].astype(bool)
    result["source_file"] = result["source_file"].astype("string")
    result["source_row_index"] = result["source_row_index"].astype("string")

    # Paso 5: alertar una vez por cada identificador repetido, sin eliminarlo
    repeated = result[result["lote_id"].duplicated(keep=False)].groupby("lote_id")
    for lote_id, group in repeated:
        vlog.register_failure(
            stage=_STAGE, dataset=dataset,
            source_ref=",".join(group["source_row_index"].astype(str)),
            field="lote_id", value=lote_id,
            rule="identificador repetido; se conservan todas las ocurrencias sin asumir error",
        )

    return result


def _clean_laboratorio(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza bronze_leyes_laboratorio -> produce silver_laboratorio.

    Conserva ley_lab_prom como valor reportado y normaliza sus comas decimales.
    Las muestras se guardan nuevamente como JSON con leyes numericas, sin usar
    su promedio para reemplazar ley_lab_prom.
    """
    dataset = "bronze_leyes_laboratorio"
    out_rows = []

    for _, row in df.iterrows():
        ref = row["source_row_index"]
        fecha_raw = str(row["fecha_muestreo"]).strip()
        fecha_clean = _standardize_operational_date(fecha_raw)
        ley_raw = str(row["ley_lab_prom"]).strip()
        ley = _parse_numeric_lotes(ley_raw, "ley_lab_prom")

        try:
            muestras = json.loads(str(row["muestras_json"]))
        except (TypeError, ValueError, json.JSONDecodeError):
            muestras = []
            vlog.register_failure(
                stage=_STAGE, dataset=dataset, source_ref=ref,
                field="muestras_json", value=row["muestras_json"],
                rule="lista de muestras no interpretable; se conserva como lista vacia",
            )

        muestras_normalizadas = [
            {
                "m": _parse_integer(muestra.get("m")),
                "ley": _parse_numeric_lotes(muestra.get("ley", ""), "ley_lab"),
            }
            for muestra in muestras
        ]

        if fecha_clean is None:
            vlog.register_failure(
                stage=_STAGE, dataset=dataset, source_ref=ref,
                field="fecha_muestreo", value=fecha_raw,
                rule="fecha de muestreo no interpretable",
            )
        if ley is None:
            vlog.register_failure(
                stage=_STAGE, dataset=dataset, source_ref=ref,
                field="ley_lab_prom", value=ley_raw,
                rule="ley reportada no interpretable; no se reemplaza por el promedio de muestras",
            )

        out_rows.append({
            "lote_id": _parse_integer(row["lote_id"]),
            "laboratorio": str(row["laboratorio"]).strip(),
            "fecha_muestreo_original": fecha_raw,
            "fecha_muestreo": fecha_clean,
            "ley_lab_prom_original": ley_raw,
            "ley_lab_prom": ley,
            "muestras_json": json.dumps(
                muestras_normalizadas, ensure_ascii=False, separators=(",", ":")
            ),
            "cantidad_muestras": len(muestras_normalizadas),
            "source_file": row["source_file"],
            "source_row_index": ref,
        })

    result = pd.DataFrame(out_rows)
    if result.empty:
        return result

    result["lote_id"] = pd.array(result["lote_id"], dtype="Int64")
    result["ley_lab_prom"] = pd.to_numeric(result["ley_lab_prom"], errors="coerce")
    result["cantidad_muestras"] = pd.array(result["cantidad_muestras"], dtype="Int64")
    for field in [
        "laboratorio", "fecha_muestreo_original", "fecha_muestreo",
        "ley_lab_prom_original", "muestras_json", "source_file", "source_row_index",
    ]:
        result[field] = result[field].astype("string")
    return result


def _check_last_day_of_month(df: pd.DataFrame, dataset: str, value_field: str) -> None:
    """
    Verifica que la serie tenga al menos una cotizacion disponible en cada mes
    de ANALYSIS_YEAR.

    La etapa de metricas selecciona luego la fecha maxima disponible, que
    corresponde al ultimo dia habil informado por la serie. No se exige una
    cotizacion en fines de semana o feriados.
    """
    fechas_dt = pd.to_datetime(df["fecha"].dropna().values, format="%Y-%m-%d", errors="coerce")
    meses_presentes = set(pd.Series(fechas_dt).dropna().dt.strftime("%Y-%m"))

    for month in range(1, 13):
        mes_str = f"{ANALYSIS_YEAR}-{month:02d}"
        if mes_str not in meses_presentes:
            vlog.register_failure(
                stage=_STAGE, dataset=dataset, source_ref="serie_completa",
                field="fecha", value=mes_str,
                rule=(
                    f"no existe ninguna cotizacion de '{value_field}' para {mes_str}; "
                    "las metricas dependientes quedaran vacias (REGLA 4)"
                ),
            )


def _clean_price_series(df: pd.DataFrame, dataset: str, value_field: str) -> pd.DataFrame:
    """
    Limpia y valida una serie de precios (bronze_precio_cobre o bronze_tipo_cambio).

    Parametros
    ----------
    df          : DataFrame leido de bronze (todas las columnas como texto).
    dataset     : nombre del dataset para el log.
    value_field : nombre semantico del campo de valor para buscar en VALID_RANGES
                  ("precio_cobre_usd_lb" o "tipo_cambio_usd_clp").

    Pasos:
      1. Convertir 'fecha' de ISO 8601 UTC a "YYYY-MM-DD".
      2. Convertir 'valor' a float.
      3. Validar fecha (formato, anio de analisis).
      4. Validar valor (rango plausible definido en business_rules.VALID_RANGES).
      5. Detectar duplicados por fecha.
      6. Verificar cotizacion en el ultimo dia de cada mes del anio de analisis.
    """
    out_rows = []

    for _, row in df.iterrows():
        ref = row["source_row_index"]

        fecha_raw = str(row["fecha"]).strip()
        fecha_clean = _standardize_date_iso(fecha_raw)
        _validate_date(fecha_clean, dataset, ref, "fecha", fecha_raw)

        valor_raw = str(row["valor"]).strip()
        try:
            valor = float(valor_raw)
        except ValueError:
            valor = None
        _validate_numeric(valor, value_field, dataset, ref, valor_raw)

        out_rows.append({
            "fecha": fecha_clean,
            "valor": valor,
            "source_file": row["source_file"],
            "source_row_index": ref,
        })

    result = pd.DataFrame(out_rows)
    if result.empty:
        return result

    result["fecha"] = result["fecha"].astype("string")
    result["valor"] = pd.to_numeric(result["valor"], errors="coerce")
    result["source_file"] = result["source_file"].astype("string")
    result["source_row_index"] = result["source_row_index"].astype("string")

    # Detectar duplicados por fecha
    dup_mask = result.duplicated(subset=["fecha"], keep=False)
    if dup_mask.any():
        for _, row in result[dup_mask].iterrows():
            vlog.register_failure(
                stage=_STAGE, dataset=dataset, source_ref=row["source_row_index"],
                field="fecha", value=str(row["fecha"]),
                rule="fecha duplicada en la serie; se mantiene en silver para revision",
            )

    # Verificar cotizacion en el ultimo dia de cada mes
    _check_last_day_of_month(result, dataset, value_field)

    return result


# ===========================================================================
# PUNTO DE ENTRADA PUBLICO
# ===========================================================================

def run_cleaning() -> None:
    """
    Ejecuta la etapa completa de limpieza y validacion.

    Orquesta la lectura desde bronze/, la limpieza de cada dataset y la escritura
    a silver/. Imprime resumenes de filas por dataset para auditoria rapida.

    Es el unico punto de entrada publico del modulo; main.py lo llama directamente.
    """
    print("[cleaning] Etapa de limpieza y validacion iniciada.")
    SILVER_DIR.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # 1. COSTOS
    # ------------------------------------------------------------------
    print("[cleaning] Procesando bronze_costos...")
    df_costos_bronze = pd.read_parquet(BRONZE_DIR / "bronze_costos.parquet")
    df_costos_silver = _clean_costos(df_costos_bronze)
    df_costos_silver.to_parquet(SILVER_DIR / "silver_costos.parquet", index=False)
    print(
        f"[cleaning] silver_costos: {len(df_costos_bronze)} filas bronze -> "
        f"{len(df_costos_silver)} filas silver "
        f"({len(df_costos_bronze) - len(df_costos_silver)} descartadas)"
    )

    # ------------------------------------------------------------------
    # 2. LOTES
    # ------------------------------------------------------------------
    print("[cleaning] Procesando bronze_lotes...")
    df_lotes_bronze = pd.read_parquet(BRONZE_DIR / "bronze_lotes.parquet")
    df_lotes_silver = _clean_lotes(df_lotes_bronze)
    df_lotes_silver.to_parquet(SILVER_DIR / "silver_lotes.parquet", index=False)
    print(
        f"[cleaning] silver_lotes: {len(df_lotes_bronze)} filas bronze -> "
        f"{len(df_lotes_silver)} filas silver "
        f"({len(df_lotes_bronze) - len(df_lotes_silver)} descartadas)"
    )

    # ------------------------------------------------------------------
    # 3. LEYES DE LABORATORIO
    # ------------------------------------------------------------------
    print("[cleaning] Procesando bronze_leyes_laboratorio...")
    df_lab_bronze = pd.read_parquet(BRONZE_DIR / "bronze_leyes_laboratorio.parquet")
    df_lab_silver = _clean_laboratorio(df_lab_bronze)
    df_lab_silver.to_parquet(SILVER_DIR / "silver_laboratorio.parquet", index=False)
    print(
        f"[cleaning] silver_laboratorio: {len(df_lab_bronze)} filas bronze -> "
        f"{len(df_lab_silver)} filas silver"
    )

    # ------------------------------------------------------------------
    # 4. PRECIO COBRE
    # ------------------------------------------------------------------
    print("[cleaning] Procesando bronze_precio_cobre...")
    df_pc_bronze = pd.read_parquet(BRONZE_DIR / "bronze_precio_cobre.parquet")
    df_pc_silver = _clean_price_series(df_pc_bronze, "bronze_precio_cobre", "precio_cobre_usd_lb")
    df_pc_silver.to_parquet(SILVER_DIR / "silver_precio_cobre.parquet", index=False)
    print(
        f"[cleaning] silver_precio_cobre: {len(df_pc_bronze)} filas bronze -> "
        f"{len(df_pc_silver)} filas silver"
    )

    # ------------------------------------------------------------------
    # 5. TIPO DE CAMBIO
    # ------------------------------------------------------------------
    print("[cleaning] Procesando bronze_tipo_cambio...")
    df_tc_bronze = pd.read_parquet(BRONZE_DIR / "bronze_tipo_cambio.parquet")
    df_tc_silver = _clean_price_series(df_tc_bronze, "bronze_tipo_cambio", "tipo_cambio_usd_clp")
    df_tc_silver.to_parquet(SILVER_DIR / "silver_tipo_cambio.parquet", index=False)
    print(
        f"[cleaning] silver_tipo_cambio: {len(df_tc_bronze)} filas bronze -> "
        f"{len(df_tc_silver)} filas silver"
    )

    print("[cleaning] Etapa de limpieza y validacion completada.")
