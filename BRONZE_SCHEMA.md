# Esquema y Documentación de la Capa Bronze

Este documento describe el esquema, estructura, trazabilidad y anomalías observadas en los conjuntos de datos extraídos en la capa `bronze` del proyecto CuprumAndes SpA.

---

## 1. Principios de Extracción a Bronze

- **Preservación literal (fidelidad al origen):** Todos los valores se almacenan como texto plano (`dtype object` / string). No se realiza casting, redondeo, eliminación de espacios en blanco (`trim`), inferencia de tipo ni corrección de formatos.
- **Trazabilidad:** Cada registro contiene dos columnas de metadatos de auditoría:
  - `source_file`: Nombre del archivo de origen en la capa `raw/`.
  - `source_row_index`: Posición ordinal del registro en la fuente de origen (1-indexed).
- **Almacenamiento uniforme:** Todos los conjuntos de datos se guardan como archivos Parquet independientes dentro de la carpeta `bronze/`.
- **Reproducibilidad offline:** Las llamadas a la API de precios guardan la respuesta JSON cruda en `raw/` y la reutilizan en ejecuciones posteriores si el archivo ya existe.

---

## 2. Inventario de Conjuntos de Datos

### 2.1. Lotes de Producción (`bronze_lotes.parquet`)

- **Archivo de origen:** `raw/origen_mina.sqlite` (tabla `lotes`).
- **Archivo de destino:** `bronze/bronze_lotes.parquet`
- **Total de filas extraídas:** 14,598
- **Total de filas esperadas:** 14,598
- **Diferencia observada:** 0 filas (coincidencia exacta).
- **Esquema de columnas:**
  | Columna | Tipo de dato en Bronze | Significado / Descripción |
  |---|---|---|
  | `lote_id` | Texto (`object`) | Identificador original del lote. |
  | `fecha` | Texto (`object`) | Fecha operacional original, sin interpretar. |
  | `planta_id` | Texto (`object`) | Identificador de planta asociado al lote. |
  | `rajo_id` | Texto (`object`) | Identificador de rajo asociado al lote. |
  | `mes` | Texto (`object`) | Mes original, conservado para conciliación. |
  | `toneladas_tratadas` | Texto (`object`) | Tonelaje procesado del lote mineral. |
  | `ley_pct` | Texto (`object`) | Ley de cobre del lote en porcentaje (%Cu). |
  | `recuperacion_pct` | Texto (`object`) | Recuperación metalúrgica del lote en porcentaje (%). |
  | `source_file` | Texto (`object`) | Trazabilidad: archivo de origen (`origen_mina.sqlite`). |
  | `source_row_index` | Texto (`object`) | Trazabilidad: índice ordinal 1-indexed del registro en la tabla `lotes`. |

- **Anomalías observadas (sin corregir en bronze):**
  - Formatos numéricos heterogéneos: convivencia de punto y coma como separador decimal (ej. `'0.646'` junto a `'0,85'`; `'90,5'` junto a `'85.9'`).
  - Formatos de miles con puntos en tonelaje (ej. `'2.834'` y `'3.191'` junto a valores enteros directos como `'2962'`).
  - Formato de mes heterogéneo a nivel operacional (ej. `'12'`, `'8'`, `'3'`).
  - Cuatro formatos de fecha: `YYYY-MM-DD`, `DD/MM/YYYY`, `DD-MM-YY` y abreviaturas de meses en español.

---

### 2.2. Costos de Planta (`bronze_costos.parquet`)

- **Archivo de origen:** `raw/origen_costos_planta.csv`
- **Archivo de destino:** `bronze/bronze_costos.parquet`
- **Total de filas extraídas:** 47 filas (excluyendo únicamente la fila 1 de cabecera inicial).
- **Total de filas esperadas:** ~45 filas.
- **Diferencia observada:** +2 filas adicionales (47 vs 45).
- **Explicación de la diferencia:**
  1. **Cabecera duplicada interna:** En la línea 16 del archivo CSV original existe una fila que repite textualmente los nombres de las columnas (`planta ; mes ; energia_clp ; insumos_clp ; mano_obra_clp ; moneda; glosa`). Siguiendo la regla de no limpiar ni descartar en extracción, fue preservada en bronze con `source_row_index = '16'`.
  2. **Fila de resumen/totalizador:** En la línea 48 (última fila del archivo) existe un registro de agregación con `planta = TOTAL` y campos vacíos. Fue preservada en bronze con `source_row_index = '48'`.
- **Esquema de columnas:**
  | Columna | Tipo de dato en Bronze | Significado / Descripción |
  |---|---|---|
  | `planta` | Texto (`object`) | Nombre original de la planta, incluidos sus alias. |
  | `mes` | Texto (`object`) | Período de costo (formato esperado `YYYY-MM` o cabecera). |
  | `energia_clp` | Texto (`object`) | Costo de energía en pesos chilenos (CLP). |
  | `insumos_clp` | Texto (`object`) | Costo de insumos en pesos chilenos (CLP). |
  | `mano_obra_clp` | Texto (`object`) | Costo de mano de obra en pesos chilenos (CLP). |
  | `source_file` | Texto (`object`) | Trazabilidad: archivo de origen (`origen_costos_planta.csv`). |
  | `source_row_index` | Texto (`object`) | Trazabilidad: número de línea original en el archivo CSV (1-indexed). |

- **Anomalías observadas (sin corregir en bronze):**
  - **Encabezado repetido:** En la fila con `source_row_index = '16'`, las columnas contienen los literales `'mes'`, `'energia_clp'`, `'insumos_clp'` y `'mano_obra_clp'`.
  - **Fila TOTAL:** En la fila con `source_row_index = '48'`, los valores de las columnas extraídas están vacíos.
  - **Símbolos monetarios y separadores mixtos:** Se observan prefijos de moneda como `'$'`, separadores de miles con puntos (ej. `'10.800.686.617'`), decimales con comas y sufijo `',00'` (ej. `'8.400.534.036,00'`), y números planos sin formato (ej. `'11834227655'`).
  - **Valores nulos/faltantes textuales:** Presencia de la cadena `'N/A'` en insumos de planta Cerro Norte para el mes `2025-06` (línea 7).

---

### 2.3. Leyes de Laboratorio (`bronze_leyes_laboratorio.parquet`)

- **Archivo de origen:** `raw/origen_leyes_laboratorio.json`
- **Archivo de destino:** `bronze/bronze_leyes_laboratorio.parquet`
- **Total de filas extraídas:** 879 registros.
- **Esquema de columnas:**
  | Columna | Tipo de dato en Bronze | Significado / Descripción |
  |---|---|---|
  | `lote_id` | Texto (`object`) | Identificador usado para vincular laboratorio con lotes. |
  | `laboratorio` | Texto (`object`) | Laboratorio reportante. |
  | `fecha_muestreo` | Texto (`object`) | Fecha original de muestreo. |
  | `ley_lab_prom` | Texto (`object`) | Ley promedio reportada, sin recalcular. |
  | `muestras_json` | Texto (`object`) | Lista de muestras anidadas serializada como JSON. |
  | `record_content` | Texto (`object`) | Contenido JSON serializado completo de cada registro del arreglo `registros`. |
  | `source_file` | Texto (`object`) | Trazabilidad: archivo de origen (`origen_leyes_laboratorio.json`). |
  | `source_row_index` | Texto (`object`) | Trazabilidad: índice ordinal del registro dentro de la lista (1-indexed). |

- **Documentación de la decisión de estructura:**
  - El archivo JSON contiene metadatos a nivel raíz (`faena`, `unidad_ley`, `reporta`) y una lista `registros`.
  - Cada elemento de `registros` incluye campos escalares (`lote_id`, `laboratorio`, `fecha_muestreo`, `ley_lab_prom`) y una lista anidada de sub-registros: `'muestras'` (ej. `[{"m": 1, "ley": "0,45"}, {"m": 2, "ley": "0,42"}, ...]`).
  - Debido a que la estructura contiene listas con cantidades variables, las muestras permanecen serializadas; no se duplica la granularidad del análisis.
  - Cada registro se conserva íntegro en `record_content`, y los campos escalares se exponen para que Silver pueda normalizarlos y reproducir la sensibilidad sin alterar el valor reportado.

---

### 2.4. Serie de Precio del Cobre (`bronze_precio_cobre.parquet`)

- **Fuente externa / Archivo crudo en raw:** `https://mindicador.cl/api/libra_cobre/2025` → `raw/precio_cobre_2025.json`
- **Archivo de destino:** `bronze/bronze_precio_cobre.parquet`
- **Total de filas extraídas:** 248 observaciones diarias del año 2025.
- **Esquema de columnas:**
  | Columna | Tipo de dato en Bronze | Significado / Descripción |
  |---|---|---|
  | `fecha` | Texto (`object`) | Fecha de la cotización diaria (formato ISO 8601 UTC según API, ej. `'2025-12-30T03:00:00.000Z'`). |
  | `valor` | Texto (`object`) | Precio de la libra de cobre en dólares estadounidenses (USD/lb) en texto plano (ej. `'5.53'`). |
  | `source_file` | Texto (`object`) | Trazabilidad: archivo crudo en raw (`precio_cobre_2025.json`). |
  | `source_row_index` | Texto (`object`) | Trazabilidad: posición ordinal en la serie histórica (1-indexed). |

- **Observaciones:**
  - Los datos corresponden a días hábiles bursátiles.

---

### 2.5. Serie de Tipo de Cambio USD-CLP (`bronze_tipo_cambio.parquet`)

- **Fuente externa / Archivo crudo en raw:** `https://mindicador.cl/api/dolar/2025` → `raw/tipo_cambio_2025.json`
- **Archivo de destino:** `bronze/bronze_tipo_cambio.parquet`
- **Total de filas extraídas:** 248 observaciones diarias del año 2025.
- **Esquema de columnas:**
  | Columna | Tipo de dato en Bronze | Significado / Descripción |
  |---|---|---|
  | `fecha` | Texto (`object`) | Fecha de la cotización observada (formato ISO 8601 UTC según API, ej. `'2025-12-30T03:00:00.000Z'`). |
  | `valor` | Texto (`object`) | Valor del dólar observado en pesos chilenos (CLP) en texto plano (ej. `'911.18'`). |
  | `source_file` | Texto (`object`) | Trazabilidad: archivo crudo en raw (`tipo_cambio_2025.json`). |
  | `source_row_index` | Texto (`object`) | Trazabilidad: posición ordinal en la serie histórica (1-indexed). |

- **Observaciones:**
  - Los datos corresponden a días hábiles bancarios/financieros en Chile.
