"""
extraction.py
=============
Etapa 1 del pipeline: Extracción.

Responsabilidades:
  1. Descargar o reutilizar las respuestas de la API de precios (cobre y dólar)
     y guardarlas tal como llegan en raw/ como JSON para permitir ejecuciones offline.
  2. Leer las cuatro fuentes desde raw/:
       - Base operacional SQLite (tabla 'lotes', columnas relevantes).
       - Costos de planta CSV (columnas relevantes).
       - Leyes de laboratorio JSON (extraído a Parquet manteniendo estructura como texto).
       - Series históricas de la API de precios (cobre y dólar).
  3. Agregar a cada registro en bronze trazabilidad sobre su origen (archivo y fila).
  4. Escribir cada conjunto en bronze/ en formato Parquet, con todos los valores
     como texto plano (dtype object/string) sin alterar ni limpiar los datos.
  5. Imprimir en pantalla los conteos de filas extraídas y compararlos con las expectativas.
"""

import json
import sqlite3
import urllib.request
from pathlib import Path
import pandas as pd

from config import BRONZE_DIR, RAW_DIR, API_COPPER_PRICE_URL, API_USD_CLP_URL

# Expectativas de filas según el enunciado/especificación
EXPECTED_LOTES_ROWS = 14598
EXPECTED_COSTOS_ROWS = 45


def fetch_or_load_api_json(url: str, target_path: Path) -> dict:
    """
    Obtiene la respuesta JSON desde una API externa o la carga desde raw/ si ya existe.

    Esta función garantiza que la extracción sea reproducible de manera offline:
    si el archivo ya fue descargado previamente en la carpeta raw/, no realiza una
    nueva petición HTTP y reutiliza el archivo existente. Si no existe, realiza la
    petición, guarda el contenido crudo exactamente como fue recibido y retorna el JSON.

    Parameters
    ----------
    url : str
        Dirección URL del endpoint de la API.
    target_path : Path
        Ruta del archivo en raw/ donde se almacena o desde donde se lee la respuesta.

    Returns
    -------
    dict
        Diccionario con la respuesta estructurada de la API.
    """
    if target_path.exists():
        print(f"[extraction] Reutilizando respuesta existente de API desde {target_path.name}")
        with open(target_path, "r", encoding="utf-8") as f:
            return json.load(f)

    print(f"[extraction] Descargando datos desde {url} hacia {target_path.name}...")
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=30) as response:
        raw_bytes = response.read()

    # Guardar exactamente tal como se recibió en raw/
    with open(target_path, "wb") as f:
        f.write(raw_bytes)

    return json.loads(raw_bytes.decode("utf-8"))


def extract_lotes(db_path: Path, output_path: Path) -> int:
    """
    Extrae la tabla 'lotes' de la base SQLite origen_mina.sqlite hacia bronze.

    Lee las columnas necesarias para auditar fecha, lote, planta, rajo y producción,
    y preserva cada valor exactamente como texto sin inferencia de tipos ni
    conversiones. Además, agrega las columnas de trazabilidad 'source_file' y
    'source_row_index' (representando la posición ordinal 1-indexed del registro en la tabla).

    Parameters
    ----------
    db_path : Path
        Ruta a la base de datos SQLite en raw/.
    output_path : Path
        Ruta del archivo Parquet de salida en bronze/.

    Returns
    -------
    int
        Número de filas extraídas.
    """
    columns = [
        "lote_id", "fecha", "planta_id", "rajo_id", "mes",
        "toneladas_tratadas", "ley_pct", "recuperacion_pct",
    ]
    conn = sqlite3.connect(db_path)
    try:
        # Extraer usando SELECT preservando el orden natural de la base de datos
        query = f"SELECT {', '.join(columns)} FROM lotes;"
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
    finally:
        conn.close()

    # Construir DataFrame convirtiendo explícitamente todo a texto para evitar conversiones
    data_dict = {col: [str(r[i]) if r[i] is not None else "" for r in rows] for i, col in enumerate(columns)}
    df = pd.DataFrame(data_dict, dtype="object")

    # Trazabilidad de origen
    df["source_file"] = db_path.name
    df["source_row_index"] = [str(i) for i in range(1, len(df) + 1)]

    df.to_parquet(output_path, index=False)
    return len(df)


def extract_costos(csv_path: Path, output_path: Path) -> int:
    """
    Extrae las columnas solicitadas del archivo origen_costos_planta.csv hacia bronze.

    Lee las líneas del archivo CSV delimitado por punto y coma, extrayendo las columnas
    'planta', 'mes', 'energia_clp', 'insumos_clp' y 'mano_obra_clp'. Preserva los valores en crudo
    (incluyendo headers repetidos y filas de resumen como 'TOTAL' si existen en los datos)
    sin recortar espacios ni alterar tipos. Agrega la trazabilidad 'source_file' y
    'source_row_index' (número de línea 1-indexed en el archivo original).

    Parameters
    ----------
    csv_path : Path
        Ruta al archivo CSV de costos en raw/.
    output_path : Path
        Ruta del archivo Parquet de salida en bronze/.

    Returns
    -------
    int
        Número de filas de datos extraídas (excluyendo únicamente la fila 1 de cabecera inicial).
    """
    target_cols = ["planta", "mes", "energia_clp", "insumos_clp", "mano_obra_clp"]

    with open(csv_path, "r", encoding="utf-8", errors="replace") as f:
        lines = f.readlines()

    if not lines:
        empty_df = pd.DataFrame(columns=target_cols + ["source_file", "source_row_index"], dtype="object")
        empty_df.to_parquet(output_path, index=False)
        return 0

    header_line = lines[0].rstrip("\r\n")
    headers = [h.strip() for h in header_line.split(";")]
    col_indices = {col: headers.index(col) for col in target_cols if col in headers}

    records = []
    # Procesar desde la línea 2 en adelante; no descartar encabezados repetidos ni totales en bronze
    for line_num, line in enumerate(lines[1:], start=2):
        raw_line = line.rstrip("\r\n")
        parts = raw_line.split(";")
        row_dict = {}
        for col in target_cols:
            idx = col_indices.get(col)
            if idx is not None and idx < len(parts):
                row_dict[col] = parts[idx]
            else:
                row_dict[col] = ""
        row_dict["source_file"] = csv_path.name
        row_dict["source_row_index"] = str(line_num)
        records.append(row_dict)

    df = pd.DataFrame(records, dtype="object")
    df.to_parquet(output_path, index=False)
    return len(df)


def extract_leyes_laboratorio(json_path: Path, output_path: Path) -> int:
    """
    Extrae el archivo de leyes de laboratorio origen_leyes_laboratorio.json hacia bronze.

    Cada registro conserva su contenido íntegro en 'record_content' y expone además
    los campos necesarios para normalizarlo en silver: lote_id, laboratorio,
    fecha_muestreo, ley_lab_prom y muestras_json. Se añade 'source_file' y
    'source_row_index' (posición 1-indexed de cada registro dentro del arreglo).

    Parameters
    ----------
    json_path : Path
        Ruta al archivo JSON en raw/.
    output_path : Path
        Ruta del archivo Parquet de salida en bronze/.

    Returns
    -------
    int
        Número de registros extraídos.
    """
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    registros = data.get("registros", [])
    records = []
    for idx, reg in enumerate(registros, start=1):
        records.append({
            "lote_id": str(reg.get("lote_id", "")),
            "laboratorio": str(reg.get("laboratorio", "")),
            "fecha_muestreo": str(reg.get("fecha_muestreo", "")),
            "ley_lab_prom": str(reg.get("ley_lab_prom", "")),
            "muestras_json": json.dumps(reg.get("muestras", []), ensure_ascii=False),
            "record_content": json.dumps(reg, ensure_ascii=False),
            "source_file": json_path.name,
            "source_row_index": str(idx),
        })

    df = pd.DataFrame(records, dtype="object")
    df.to_parquet(output_path, index=False)
    return len(df)


def extract_price_series(api_json: dict, source_filename: str, output_path: Path) -> int:
    """
    Extrae la serie de valores fechados de un indicador de precios a una tabla bronze Parquet.

    Extrae la lista 'serie' contenida en la respuesta de la API, manteniendo cada campo
    ('fecha' y 'valor') como texto sin conversiones ni formateos. Incluye columnas
    de trazabilidad 'source_file' y 'source_row_index' (índice 1-indexed en la serie).

    Parameters
    ----------
    api_json : dict
        Diccionario con la respuesta de la API de mindicador.cl.
    source_filename : str
        Nombre del archivo JSON en raw/ de donde provienen los datos.
    output_path : Path
        Ruta del archivo Parquet de destino en bronze/.

    Returns
    -------
    int
        Número de registros extraídos en la serie.
    """
    serie = api_json.get("serie", [])
    records = []
    for idx, item in enumerate(serie, start=1):
        records.append({
            "fecha": str(item.get("fecha", "")),
            "valor": str(item.get("valor", "")),
            "source_file": source_filename,
            "source_row_index": str(idx),
        })

    df = pd.DataFrame(records, dtype="object")
    df.to_parquet(output_path, index=False)
    return len(df)


def run_extraction() -> None:
    """
    Ejecuta el flujo completo de la etapa de extracción.

    Coordina:
      1. Creación/verificación del directorio bronze/.
      2. Descarga o carga offline de los archivos JSON de precios de la API en raw/.
      3. Extracción de las cuatro fuentes hacia sus respectivas tablas Parquet en bronze/.
      4. Impresión de conteos de filas extraídas y comparación con las expectativas.
    """
    print("[extraction] Etapa de extracción iniciada.")
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    # Nombres de archivos crudos para las respuestas de la API
    copper_raw_path = RAW_DIR / "precio_cobre_2025.json"
    dolar_raw_path = RAW_DIR / "tipo_cambio_2025.json"

    # 1. Obtener respuestas de la API (descargar o reutilizar si ya existen)
    copper_json = fetch_or_load_api_json(API_COPPER_PRICE_URL, copper_raw_path)
    dolar_json = fetch_or_load_api_json(API_USD_CLP_URL, dolar_raw_path)

    # 2. Extraer origen_mina.sqlite (tabla lotes)
    lotes_path = RAW_DIR / "origen_mina.sqlite"
    lotes_bronze_path = BRONZE_DIR / "bronze_lotes.parquet"
    lotes_count = extract_lotes(lotes_path, lotes_bronze_path)

    # 3. Extraer origen_costos_planta.csv
    costos_path = RAW_DIR / "origen_costos_planta.csv"
    costos_bronze_path = BRONZE_DIR / "bronze_costos.parquet"
    costos_count = extract_costos(costos_path, costos_bronze_path)

    # 4. Extraer origen_leyes_laboratorio.json
    lab_path = RAW_DIR / "origen_leyes_laboratorio.json"
    lab_bronze_path = BRONZE_DIR / "bronze_leyes_laboratorio.parquet"
    lab_count = extract_leyes_laboratorio(lab_path, lab_bronze_path)

    # 5. Extraer series de precios de la API a bronze
    copper_bronze_path = BRONZE_DIR / "bronze_precio_cobre.parquet"
    copper_count = extract_price_series(copper_json, copper_raw_path.name, copper_bronze_path)

    dolar_bronze_path = BRONZE_DIR / "bronze_tipo_cambio.parquet"
    dolar_count = extract_price_series(dolar_json, dolar_raw_path.name, dolar_bronze_path)

    # 6. Reporte de conteos y comparación con lo esperado
    print("\n" + "=" * 60)
    print("RESUMEN DE EXTRACCIÓN A BRONZE")
    print("=" * 60)
    print(f"1. Lotes (origen_mina.sqlite):")
    print(f"   - Extraídos: {lotes_count} filas")
    print(f"   - Esperados: {EXPECTED_LOTES_ROWS} filas")
    diff_lotes = lotes_count - EXPECTED_LOTES_ROWS
    print(f"   - Diferencia: {diff_lotes:+d} filas")

    print(f"\n2. Costos de Planta (origen_costos_planta.csv):")
    print(f"   - Extraídos: {costos_count} filas (excluyendo cabecera inicial)")
    print(f"   - Esperados: {EXPECTED_COSTOS_ROWS} filas")
    diff_costos = costos_count - EXPECTED_COSTOS_ROWS
    print(f"   - Diferencia: {diff_costos:+d} filas")

    print(f"\n3. Leyes de Laboratorio (origen_leyes_laboratorio.json):")
    print(f"   - Extraídos: {lab_count} registros")

    print(f"\n4. Precio Cobre API (precio_cobre_2025.json):")
    print(f"   - Extraídos: {copper_count} observaciones")

    print(f"\n5. Tipo de Cambio API (tipo_cambio_2025.json):")
    print(f"   - Extraídos: {dolar_count} observaciones")
    print("=" * 60 + "\n")
    print("[extraction] Etapa de extracción completada exitosamente.")
