# Esquema de la Capa Silver — CuprumAndes SpA

Este documento describe el esquema de los archivos Parquet que produce la etapa
de limpieza (`code/cleaning.py`) en la carpeta `silver/`.

---

## Principios de la capa silver

- **Datos limpios y tipados:** todos los campos tienen el tipo correcto (string, float64).
  No quedan simbolos de moneda, separadores de miles, comas decimales ni sufijos ",00".
- **Sin metricas derivadas:** no hay columnas calculadas, agregaciones ni conversiones
  de moneda. Silver es la entrada de la etapa de metricas; no anticipa ninguna logica de esa etapa.
- **Granularidad de origen conservada:** las filas de costos permanecen a nivel planta-mes;
  los lotes conservan sus identificadores de planta y rajo. No se crean dimensiones fisicas nuevas.
- **Trazabilidad conservada:** cada fila mantiene `source_file` y `source_row_index`
  heredados de bronze para poder rastrear cualquier valor hasta su origen en `raw/`.
- **Fallas visibles:** los valores que no pudieron transformarse quedan como `NaN`/`None`
  (nunca se imputan silenciosamente). Cada falla tiene un registro en `validation_log.parquet`.

---

## Formatos canonicos

| Concepto        | Formato canonico | Ejemplo        | Notas                                      |
|-----------------|------------------|----------------|--------------------------------------------|
| Mes operacional | `YYYY-MM` string | `"2025-06"`    | Derivado de la fecha normalizada del lote   |
| Fecha operacional | `YYYY-MM-DD` string | `"2025-06-09"` | Soporta los cuatro formatos observados   |
| Fecha diaria    | `YYYY-MM-DD` string | `"2025-06-30"` | Derivado de ISO 8601 UTC de la API      |
| Monto en CLP    | `float64`        | `12652294704.0`| Sin "$", sin puntos de miles, sin coma     |
| Porcentaje      | `float64`        | `0.646`, `90.5`| En unidades de porcentaje (no fraccion)    |
| Toneladas       | `float64`        | `2834.0`       | Sin separadores de miles                   |
| Precio USD/lb   | `float64`        | `5.53`         | Directo desde la API, ya en formato limpio |
| Tipo cambio CLP | `float64`        | `911.18`       | Directo desde la API, ya en formato limpio |

---

## Tablas

### silver_costos.parquet

**Origen:** `bronze/bronze_costos.parquet`
**Filas esperadas:** ~45 (47 bronze menos 1 cabecera repetida y 1 fila TOTAL)

| Columna            | Tipo     | Significado                                                              |
|--------------------|----------|--------------------------------------------------------------------------|
| `planta_original`  | string   | Nombre tal como aparece en el CSV.                                      |
| `planta`           | string   | Nombre canonico de una de las cuatro plantas esperadas.                 |
| `mes`              | string   | Periodo de costo en formato canonico `YYYY-MM`.                         |
| `energia_clp`      | float64  | Costo de energia electrica en pesos chilenos (CLP).                     |
| `insumos_clp`      | float64  | Costo de insumos en CLP. Puede ser NaN si el valor original era `N/A`.  |
| `mano_obra_clp`    | float64  | Costo de mano de obra en CLP.                                           |
| `source_file`      | string   | Archivo de origen en `raw/` (trazabilidad).                             |
| `source_row_index` | string   | Numero de linea original en el CSV de origen (1-indexed, trazabilidad). |

**Notas:**
- Cada fila corresponde a una planta para un mes dado. Esta columna permite comprobar
  que esten Cerro Norte, Loa, Salado e Illapel antes de consolidar a nivel empresa.
- El valor `N/A` de `insumos_clp` para una planta en junio 2025 queda como `NaN`.
  Se registra una alerta en `validation_log.parquet` con `rule = IMPUTE_MISSING_COST=False`.

---

### silver_lotes.parquet

**Origen:** `bronze/bronze_lotes.parquet`
**Filas esperadas:** 14,598 (misma cantidad que bronze; no se descartan filas de lotes)

| Columna              | Tipo     | Significado                                                                |
|----------------------|----------|----------------------------------------------------------------------------|
| `lote_id`            | Int64    | Identificador original; puede repetirse y no se elimina.                   |
| `fecha_original`     | string   | Fecha tal como aparece en SQLite.                                           |
| `fecha`              | string   | Fecha operacional normalizada `YYYY-MM-DD`.                                |
| `planta_id`          | Int64    | Planta asociada al lote.                                                    |
| `rajo_id`            | Int64    | Rajo asociado al lote.                                                      |
| `mes_original`       | string   | Campo mes original, conservado sólo para auditoria.                         |
| `mes`                | string   | Mes `YYYY-MM` derivado de `fecha`, usado por Gold.                          |
| `toneladas_tratadas` | float64  | Tonelaje del lote procesado (toneladas metricas). No negativo.            |
| `ley_pct`            | float64  | Ley de cobre del lote en porcentaje (%Cu). Rango plausible: 0-5 %.       |
| `recuperacion_pct`   | float64  | Recuperacion metalurgica en porcentaje (%). Rango plausible: 50-100 %.   |
| `es_elegible`        | bool     | Cumple fecha 2025 y las tres reglas de calidad acordadas.                  |
| `motivo_exclusion`   | string   | Causas de exclusión separadas por `|`; vacío si es elegible.               |
| `source_file`        | string   | Archivo de origen en `raw/` (trazabilidad).                               |
| `source_row_index`   | string   | Posicion ordinal del registro en la tabla `lotes` de SQLite (trazabilidad).|

**Notas:**
- Los porcentajes se expresan en unidades de porcentaje (ej. `0.646` es 0.646 %Cu),
  no como fraccion (no se divide por 100). La etapa de metricas divide por 100 al calcular.
- La fecha normalizada es la fuente temporal oficial. Las discrepancias con
  `mes_original` se registran, pero no cambian la fecha ni se corrigen automaticamente.
- Silver conserva 14.598 filas: 14.240 elegibles y 358 excluidas. Incluye los 86
  registros fechados en 2026 y los 144 `lote_id` repetidos para auditoria.

---

### silver_laboratorio.parquet

**Origen:** `bronze/bronze_leyes_laboratorio.parquet`
**Filas esperadas:** 879

| Columna | Tipo | Significado |
|---------|------|-------------|
| `lote_id` | Int64 | Clave de vinculacion con los lotes. |
| `laboratorio` | string | Laboratorio reportante. |
| `fecha_muestreo_original` | string | Fecha original. |
| `fecha_muestreo` | string | Fecha normalizada `YYYY-MM-DD`. |
| `ley_lab_prom_original` | string | Valor reportado original. |
| `ley_lab_prom` | float64 | El mismo valor normalizado; no se reemplaza por el promedio de muestras. |
| `muestras_json` | string | Muestras con leyes numericas normalizadas. |
| `cantidad_muestras` | Int64 | Cantidad de muestras informadas. |
| `source_file`, `source_row_index` | string | Trazabilidad al JSON original. |

---

### silver_precio_cobre.parquet

**Origen:** `bronze/bronze_precio_cobre.parquet`
**Filas esperadas:** 248 (dias habiles bursatiles de 2025)

| Columna            | Tipo     | Significado                                                              |
|--------------------|----------|--------------------------------------------------------------------------|
| `fecha`            | string   | Fecha de la cotizacion en formato canonico `YYYY-MM-DD`.                |
| `valor`            | float64  | Precio de la libra de cobre en dolares estadounidenses (USD/lb).        |
| `source_file`      | string   | Archivo de origen en `raw/` (trazabilidad).                             |
| `source_row_index` | string   | Posicion ordinal en la serie historica (1-indexed, trazabilidad).       |

**Notas:**
- La serie cubre solo dias habiles bursatiles; Gold selecciona la fecha mas reciente
  realmente disponible de cada mes.
- La serie viene ordenada de mas reciente a mas antigua (como la entrega la API).

---

### silver_tipo_cambio.parquet

**Origen:** `bronze/bronze_tipo_cambio.parquet`
**Filas esperadas:** 248 (dias habiles bancarios/financieros de 2025 en Chile)

| Columna            | Tipo     | Significado                                                              |
|--------------------|----------|--------------------------------------------------------------------------|
| `fecha`            | string   | Fecha de la cotizacion en formato canonico `YYYY-MM-DD`.                |
| `valor`            | float64  | Valor del dolar observado en pesos chilenos (CLP/USD).                  |
| `source_file`      | string   | Archivo de origen en `raw/` (trazabilidad).                             |
| `source_row_index` | string   | Posicion ordinal en la serie historica (1-indexed, trazabilidad).       |

**Notas:**
- Mismas consideraciones de dias habiles que `silver_precio_cobre`.
- El tipo de cambio observado en Chile puede diferir del spot interbancario;
  se usa el valor entregado directamente por la API de mindicador.cl.

---

## Que NO esta en silver

| Dataset                   | Razon                                                                      |
|---------------------------|----------------------------------------------------------------------------|
| Metricas derivadas        | Cobre fino, cash cost, ingresos, costos en USD: se calculan en la etapa de metricas desde silver. |
| Dimension de tiempo       | Eliminada del modelo dimensional (ARCHITECTURE.md, decision 11).           |
| Dimension fisica de planta| No se crea una tabla dimensional; la planta queda como atributo del dato de origen. |

---

## Archivo de fallas de validacion

`validation_log.parquet` (en la raiz del proyecto) registra cada falla detectada
durante la limpieza. Columnas: `stage`, `dataset`, `source_ref`, `field`, `value`, `rule`.
Legible directamente con `pandas.read_parquet("validation_log.parquet")`.
