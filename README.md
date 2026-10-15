# CuprumAndes — Pipeline de Procesamiento de Datos (Proyecto TIN G6)

Pipeline de datos reproducible para la extracción, limpieza, validación y cálculo de métricas operacionales y financieras de la minera de cobre **CuprumAndes SpA** para el ejercicio 2025.

---

## 1. Propósito del proyecto

El objetivo analítico central del proyecto es responder una pregunta estratégica planteada por el directorio y la gerencia de CuprumAndes:

> **¿El cash cost por libra de cobre subió o bajó durante 2025, y cómo se compara el efecto del precio de referencia del cobre frente a la evolución del costo directo de producción?**

Para resolver esta interrogante, el pipeline integra cuatro fuentes de datos dispersas:
1. Base operacional SQLite (`origen_mina.sqlite`), que contiene los lotes de producción diaria de mineral.
2. Archivo CSV de costos de planta (`origen_costos_planta.csv`), que desglosa los costos operacionales mensuales en energía, insumos y mano de obra.
3. Archivo JSON de leyes de laboratorio (`origen_leyes_laboratorio.json`), utilizado además para reproducir la sensibilidad de calibración de ley.
4. API pública de indicadores económicos (`mindicador.cl`), que provee la serie diaria del precio internacional del cobre (USD/lb) y del tipo de cambio dólar observado (USD/CLP).

El pipeline procesa estas fuentes a través de una arquitectura Medallion (`raw` → `bronze` → `silver` → `gold`), generando una tabla final consolidada a nivel empresa y frecuencia mensual (`gold/gold_metricas.parquet`) que permite comparar la evolución mensual del cash cost frente al precio internacional y determinar con evidencia cuantitativa los causantes del desempeño económico del año.

---

## 2. Prerrequisitos

- **Entorno probado:** Python **3.12.13** en **Windows con PowerShell**. Las instrucciones para Linux, macOS y WSL se mantienen como guía de instalación, pero no fueron verificadas en esta revisión.
- **Acceso a Internet:** Se requiere conexión a Internet **únicamente durante la primera ejecución**. En dicha corrida, la etapa de extracción descarga las series del año 2025 desde la API de `mindicador.cl` (`precio_cobre_2025.json` y `tipo_cambio_2025.json`) y las persiste en la carpeta `raw/`. Todas las ejecuciones subsecuentes pueden realizarse de forma 100% offline.

---

## 3. Instalación y preparación del entorno

Se recomienda ejecutar el proyecto dentro de un entorno virtual limpio para garantizar el aislamiento de dependencias.

### Paso 1: Clonar o posicionarse en la raíz del proyecto
```bash
cd Proyecto-TIN-G6
```

### Paso 2: Crear un entorno virtual limpio
- En **Linux / macOS / WSL (Bash)**:
  ```bash
  python3 -m venv .venv
  ```
- En **Windows (PowerShell o CMD)**:
  ```powershell
  python -m venv .venv
  ```

### Paso 3: Activar el entorno virtual
- En **Linux / macOS / WSL (Bash)**:
  ```bash
  source .venv/bin/activate
  ```
- En **Windows (PowerShell)**:
  ```powershell
  .venv\Scripts\Activate.ps1
  ```
- En **Windows (CMD)**:
  ```cmd
  .venv\Scripts\activate.bat
  ```

### Paso 4: Instalar las dependencias fijadas
Instale las dependencias exactas indicadas en `requirements.txt`:
```bash
pip install -r requirements.txt
```

> **Nota sobre dependencias:** El proyecto sigue el principio de dependencias mínimas de terceros. Solo instala `pandas==2.2.3` y `pyarrow==17.0.0` para el procesamiento tabular y la lectura/escritura de archivos Parquet. Las demás funcionalidades (conexión SQLite, peticiones HTTP, parseo JSON/CSV y rutas) utilizan la biblioteca estándar de Python (`sqlite3`, `urllib.request`, `json`, `csv`, `pathlib`).

---

## 4. Ejecución del pipeline

Para ejecutar el pipeline completo de punta a punta (las tres etapas en secuencia más la generación del log de validaciones), ejecute un único comando desde la raíz del proyecto:

```bash
python main.py
```

Durante la ejecución, el script mostrará en consola el avance de cada una de las tres etapas (`[1/3]`, `[2/3]`, `[3/3]`) y concluirá desplegando un resumen de las validaciones registradas.

### Verificación y resultados esperados

Después de ejecutar `python main.py`, la consistencia del pipeline se comprueba con:

```bash
python -m unittest discover -s tests -v
```

La ejecución validada finalizó satisfactoriamente con **14 pruebas aprobadas** (`Ran 14 tests ... OK`). Sus controles principales son:

- 14.598 registros operacionales originales.
- 358 registros no elegibles para el indicador principal: 86 fechados en 2026 y 272 adicionales por calidad.
- 14.240 registros elegibles, conservando los identificadores repetidos según la decisión del equipo.
- 843 análisis de laboratorio vinculables a los registros elegibles.
- 12 meses de producción en Gold; ocho con cash cost calculable y cuatro con costos incompletos.

El cash cost de referencia pasa de aproximadamente **1,85 USD/lb en enero a 3,41 USD/lb en diciembre de 2025**, un incremento de **84,7 %**.

La corrida revisada generó 1.517 eventos en `validation_log.parquet`. Estos eventos incluyen alertas de calidad, excepciones de negocio y exclusiones documentadas; no son necesariamente errores de ejecución ni equivalen al número de registros excluidos.

El CSV de costos contiene 47 filas extraídas. Una corresponde a una cabecera repetida y otra a una fila de resumen, por lo que Silver conserva 45 registros útiles.

**Tiempo medido del pipeline:** 1,584 segundos para `python main.py` en Windows PowerShell con Python 3.12.13 y las respuestas de la API ya disponibles localmente. Esta medición no incluye la creación del entorno, la instalación de dependencias ni la descarga inicial.

---

## 5. Estructura de carpetas y arquitectura de datos

El repositorio implementa una arquitectura por capas Medallion:

```text
Proyecto-TIN-G6/
├── main.py                    # Punto de entrada único del pipeline
├── requirements.txt           # Dependencias fijadas (pandas y pyarrow)
├── README.md                  # Este archivo de instrucciones y documentación
├── PROJECT_STRUCTURE.md       # Mapa detallado de navegación del repositorio
├── BRONZE_SCHEMA.md           # Especificación de esquema y trazabilidad de bronze
├── SILVER_SCHEMA.md           # Especificación de esquema y formatos de silver
├── GOLD_SCHEMA.md             # Especificación de columnas y fórmulas de gold
├── validation_log.parquet     # Archivo persistente con las alertas de validación
│
├── raw/                       # Capa Cruda: fuentes originales (inmutables) + JSON de API
│   ├── origen_mina.sqlite
│   ├── origen_costos_planta.csv
│   ├── origen_leyes_laboratorio.json
│   ├── precio_cobre_2025.json    (generado en la primera corrida)
│   └── tipo_cambio_2025.json     (generado en la primera corrida)
│
├── bronze/                    # Capa Bronze: datos extraídos sin limpiar en Parquet (texto plano)
│   ├── bronze_lotes.parquet
│   ├── bronze_costos.parquet
│   ├── bronze_leyes_laboratorio.parquet
│   ├── bronze_precio_cobre.parquet
│   └── bronze_tipo_cambio.parquet
│
├── silver/                    # Capa Silver: datos limpios, validados y tipados en Parquet
│   ├── silver_lotes.parquet
│   ├── silver_costos.parquet
│   ├── silver_laboratorio.parquet
│   ├── silver_precio_cobre.parquet
│   └── silver_tipo_cambio.parquet
│
├── gold/                      # Capa Gold: tabla analítica agregada mensual a nivel empresa
│   ├── gold_metricas.parquet
│   ├── gold_sensibilidad_laboratorio.parquet
│   ├── gold_sensibilidad_lotes_repetidos.parquet
│   └── revision_lotes_2026_laboratorio.parquet
│
├── code/                      # Módulos de lógica del pipeline
│   ├── config.py              # Rutas a directorios y constantes analíticas compartidas
│   ├── business_rules.py      # Declaración de reglas de negocio y rangos de validez
│   ├── validation_log.py      # Acumulación en memoria y exportación del log de validaciones
│   ├── extraction.py          # Etapa 1: Extracción hacia bronze/
│   ├── cleaning.py            # Etapa 2: Limpieza y validación hacia silver/
│   └── metrics.py             # Etapa 3: Cálculo de métricas analíticas hacia gold/
│
└── tests/
    └── test_pipeline.py       # Pruebas unitarias y controles de conciliación
```

---

## 6. Etapas del pipeline: qué hace cada una, qué lee y qué escribe

### Etapa 1: Extracción (`code/extraction.py`)
- **Qué hace:**
  1. Si no existen en `raw/`, consulta las URLs de la API de `mindicador.cl` para precio del cobre y dólar, y almacena las respuestas JSON crudas en `raw/`. Si ya existen, las reutiliza sin usar la red.
  2. Lee las columnas relevantes de cada fuente en `raw/`.
  3. Asigna a cada registro atributos de trazabilidad de auditoría: `source_file` (nombre del archivo origen) y `source_row_index` (número de línea o posición ordinal 1-indexed original).
  4. Extrae las leyes de laboratorio preservando `record_content` y exponiendo los campos necesarios para su normalización posterior.
  5. Almacena todos los valores como texto plano (`dtype object/string`) sin inferir tipos ni modificar valores para evitar pérdida de información previa a la limpieza.
- **Qué lee:**
  - `https://mindicador.cl/api/libra_cobre/2025` y `https://mindicador.cl/api/dolar/2025` (vía red, solo si no existen los archivos locales).
  - `raw/origen_mina.sqlite`
  - `raw/origen_costos_planta.csv`
  - `raw/origen_leyes_laboratorio.json`
  - `raw/precio_cobre_2025.json` y `raw/tipo_cambio_2025.json`
- **Qué escribe:**
  - `raw/precio_cobre_2025.json` y `raw/tipo_cambio_2025.json` (solo en la primera corrida).
  - `bronze/bronze_lotes.parquet`
  - `bronze/bronze_costos.parquet`
  - `bronze/bronze_leyes_laboratorio.parquet`
  - `bronze/bronze_precio_cobre.parquet`
  - `bronze/bronze_tipo_cambio.parquet`

### Etapa 2: Limpieza y Validación (`code/cleaning.py`)
- **Qué hace:**
  1. Descarta filas de cabecera repetida dentro de los datos y filas de resumen (`TOTAL` o vacías) en `bronze_costos`, registrando cada caso en el log de validaciones.
  2. Estandariza fechas:
     - La fecha operacional admite `YYYY-MM-DD`, `DD/MM/YYYY`, `DD-MM-YY` y abreviaturas de meses en español. El mes `YYYY-MM` se deriva de esta fecha; el campo `mes` original sólo se conserva para auditoría.
     - Fechas de las series de precios (formato ISO 8601 UTC de la API) se convierten a `YYYY-MM-DD`.
  3. Convierte formatos numéricos:
     - Costos en CLP: elimina símbolos de moneda (`$`), puntos separadores de miles y maneja comas decimales (incluyendo terminaciones `,00`).
     - Lotes: aplica heurística dependiente del campo para resolver la ambigüedad entre puntos como miles en toneladas y puntos decimales en leyes o recuperaciones.
  4. Ejecuta validaciones de campo post-conversión: verifica que los valores numéricos no sean nulos, no sean negativos y se encuentren dentro de los rangos plausibles (`VALID_RANGES` en `business_rules.py`). Valida que los porcentajes estén en escala 0–100.
  5. Detecta los `lote_id` repetidos y registra alertas (las filas se preservan en silver para revisión).
  6. Normaliza laboratorio, incluida la coma decimal, sin reemplazar `ley_lab_prom` por el promedio de muestras.
  7. Comprueba que las series de precios tengan al menos una cotización por mes; Gold usa el último día hábil disponible.
- **Qué lee:**
  - `bronze/bronze_costos.parquet`
  - `bronze/bronze_lotes.parquet`
  - `bronze/bronze_leyes_laboratorio.parquet`
  - `bronze/bronze_precio_cobre.parquet`
  - `bronze/bronze_tipo_cambio.parquet`
- **Qué escribe:**
  - `silver/silver_costos.parquet`
  - `silver/silver_lotes.parquet`
  - `silver/silver_laboratorio.parquet`
  - `silver/silver_precio_cobre.parquet`
  - `silver/silver_tipo_cambio.parquet`

### Etapa 3: Cálculo de Métricas (`code/metrics.py`)
- **Qué hace:**
  1. Lee los cinco datasets limpios desde `silver/`.
  2. Calcula el cobre fino primero a nivel de cada lote individual en toneladas métricas, suma mensualmente a nivel empresa y convierte la suma a libras una sola vez.
  3. Extrae la cotización del último día hábil disponible de cada mes tanto para el precio del cobre como para el tipo de cambio USD/CLP.
  4. Suma los costos operacionales de todas las plantas por mes. Si falta una planta o algún componente, el costo total del mes completo queda como `NaN` para no distorsionar el cálculo.
  5. Une las series mediante un `LEFT JOIN` por mes y calcula: ingresos en USD, costos totales en USD, cash cost por libra y utilidades operacionales en USD.
  6. Valida la presencia de los 12 meses de 2025 y aplica verificaciones de sanidad numérica (no-negatividad y rangos históricos).
- **Qué lee:**
  - `silver/silver_lotes.parquet`
  - `silver/silver_costos.parquet`
  - `silver/silver_laboratorio.parquet`
  - `silver/silver_precio_cobre.parquet`
  - `silver/silver_tipo_cambio.parquet`
- **Qué escribe:**
  - `gold/gold_metricas.parquet`
  - `gold/gold_sensibilidad_laboratorio.parquet`
  - `gold/gold_sensibilidad_lotes_repetidos.parquet`
  - `gold/revision_lotes_2026_laboratorio.parquet`

### Finalización (`main.py` y `code/validation_log.py`)
- Al concluir las tres etapas (o al ocurrir una interrupción técnica), `validation_log.write_log()` persiste todas las fallas acumuladas en `validation_log.parquet` y `validation_log.print_summary()` imprime un informe condensado en consola.

---

## 7. Reglas de negocio y fórmulas de la tabla Gold

Las reglas analíticas están centralizadas en `code/business_rules.py`:

1. **Grano analítico:** Una fila por mes del año 2025 a nivel empresa consolidada. Planta-mes está disponible en el origen de costos y lote-planta-rajo en Silver, pero el resultado destinado a gerencia se mantiene en empresa-mes.
2. **Fórmula de Cobre Fino (REGLA 2):**
   - A nivel de lote:
     $$\text{cobre\_fino\_lote\_t} = \text{toneladas\_tratadas} \times \left(\frac{\text{ley\_pct}}{100}\right) \times \left(\frac{\text{recuperacion\_pct}}{100}\right)$$
   - A nivel mensual de empresa:
     $$\text{cobre\_fino\_mes\_t} = \sum \text{cobre\_fino\_lote\_t}$$
     $$\text{cobre\_fino\_lb} = \text{cobre\_fino\_mes\_t} \times 2204.62262$$
   - *Razón:* Al calcular el cobre fino primero por lote antes de agrupar, la ponderación por ley y tonelaje surge de forma natural y matemáticamente exacta, sin requerir una columna de ley ponderada artificial.
3. **Precio del cobre y Tipo de Cambio de cierre (REGLA 1 y REGLA 4):**
   - Se selecciona el valor de la fecha más reciente (último día hábil registrado en la serie) de cada mes. Este valor único se aplica a toda la producción del mes.
4. **Costos Totales en USD (REGLA 3):**
   - Suma mensual en moneda local:
     $$\text{costos\_totales\_clp} = \sum (\text{energia\_clp} + \text{insumos\_clp} + \text{mano\_obra\_clp})$$
   - Conversión a USD con el dólar de cierre:
     $$\text{costos\_totales\_usd} = \frac{\text{costos\_totales\_clp}}{\text{tipo\_cambio\_clp\_usd}}$$
   - *Tratamiento de costos faltantes:* Deben estar las cuatro plantas y sus tres componentes. Si falta una planta o un componente (por ejemplo, `N/A` en insumos de Cerro Norte en junio), **no se imputa con cero** y el mes queda sin métricas económicas dependientes.
5. **Valorización de referencia (`ingresos_usd`):**
   $$\text{ingresos\_usd} = \text{cobre\_fino\_lb} \times \text{precio\_cobre\_usd\_lb}$$
   Representa una valorización de la producción al precio de mercado de referencia; no corresponde a ventas observadas.
6. **Cash Cost (USD/lb):**
   $$\text{cash\_cost\_usd\_lb} = \frac{\text{costos\_totales\_usd}}{\text{cobre\_fino\_lb}}$$
   Representa el costo operacional unitario por libra producida. Si los costos totales son `NaN`, el cash cost queda como `NaN`.
7. **Utilidades Operacionales (USD):**
   $$\text{utilidades\_usd} = \text{ingresos\_usd} - \text{costos\_totales\_usd}$$
   Es un margen directo de referencia valorizado a precio de mercado; no representa utilidad contable ni un precio de venta efectivamente realizado.

### Esquema final de `gold/gold_metricas.parquet` (7 columnas)
| Columna | Tipo | Unidad | Descripción |
|---|---|---|---|
| `mes` | string | `YYYY-MM` | Mes del año 2025 (`2025-01` a `2025-12`) |
| `cobre_fino_lb` | float64 | Libras (lb) | Producción total de cobre fino en libras |
| `precio_cobre_usd_lb` | float64 | USD/lb | Precio del cobre al cierre del mes |
| `ingresos_usd` | float64 | USD | Valorización de referencia a precio de mercado; no corresponde a ventas observadas |
| `costos_totales_usd` | float64 | USD | Costos operacionales totales en USD (`NaN` en marzo, junio, agosto y noviembre) |
| `cash_cost_usd_lb` | float64 | USD/lb | Costo operacional por libra producida (`NaN` en esos cuatro meses) |
| `utilidades_usd` | float64 | USD | Margen directo de referencia (`NaN` en esos cuatro meses) |

---

## 8. Lectura del log de validaciones y resumen en pantalla

El pipeline está diseñado bajo el principio de que **las alertas de calidad de datos no detienen la ejecución**, sino que se capturan de forma no invasiva en un registro unificado para su posterior auditoría. Un mismo registro puede generar más de un evento, por lo que el total del log no equivale al número de filas excluidas ni indica que la ejecución haya fallado.

### Resumen en pantalla
Al terminar de ejecutar `main.py`, se despliega en la terminal un resumen estructurado:
- Total global de fallas registradas.
- Agrupación por etapa (`cleaning`, `metrics`).
- Para cada etapa, el conteo total de anomalías y hasta 5 ejemplos con su dataset, identificador de fila fuente (`ref`), columna observada, valor original recibido y la regla infringida.

### Inspección del archivo `validation_log.parquet`
El registro completo se almacena en la raíz del proyecto en `validation_log.parquet`. Puede consultarse directamente mediante Python y pandas:

```python
import pandas as pd

# Cargar el log de validaciones
df_log = pd.read_parquet("validation_log.parquet")

# Ver la estructura y las primeras filas
print(df_log.head(10))

# Ver el conteo de fallas agrupadas por etapa y dataset
print(df_log.groupby(["stage", "dataset"]).size())

# Filtrar fallas ocurridas en la etapa de métricas
fallas_metricas = df_log[df_log["stage"] == "metrics"]
print(fallas_metricas[["source_ref", "field", "rule"]])

# Filtrar descartes de cabeceras o filas de totales en costos
descartes_costos = df_log[df_log["rule"].str.contains("se elimina")]
print(descartes_costos[["source_ref", "field", "value", "rule"]])
```

Las columnas de `validation_log.parquet` son:
- `stage`: Nombre de la etapa que reportó la falla (`cleaning` o `metrics`).
- `dataset`: Dataset de origen (ej. `bronze_costos`, `silver_costos`, `silver_precio_cobre`, `gold_metricas`).
- `source_ref`: Referencia trazable a la fila de origen (número de línea en el archivo original o identificador de mes).
- `field`: Nombre del campo o columna involucrada.
- `value`: Valor original recibido tal como estaba en crudo.
- `rule`: Descripción detallada de la regla violada o decisión aplicada.

---

## 9. Repetición de la corrida de forma 100% offline

Una vez completada la primera ejecución, los archivos JSON con las respuestas de la API quedan almacenados permanentemente en:
- `raw/precio_cobre_2025.json`
- `raw/tipo_cambio_2025.json`

La función `fetch_or_load_api_json()` en `code/extraction.py` comprueba primero si el archivo de destino existe en el disco antes de abrir cualquier conexión de red. Si el archivo existe, lo carga directamente de disco e imprime:
```text
[extraction] Reutilizando respuesta existente de API desde precio_cobre_2025.json
[extraction] Reutilizando respuesta existente de API desde tipo_cambio_2025.json
```

Por lo tanto, para repetir la ejecución completa sin acceso a Internet:
1. Asegúrese de que los archivos JSON existen en `raw/` (se encuentran versionados en el repositorio o creados tras la primera corrida).
2. Desactive su conexión de red (opcional).
3. Ejecute normalmente:
   ```bash
   python main.py
   ```
El pipeline se completará de punta a punta sin realizar ninguna solicitud HTTP.

---

## 10. Problemas comunes y soluciones

A continuación se detallan las incidencias técnicas y operacionales reales que pueden presentarse durante la ejecución:

1. **Error `ModuleNotFoundError: No module named 'pandas'` o `pyarrow`:**
   - *Causa:* El comando `python main.py` se ejecutó fuera del entorno virtual donde se instalaron las dependencias, o no se ejecutó el paso de instalación.
   - *Solución:* Verifique que el entorno virtual esté activo (el prompt de la terminal debe mostrar `(.venv)`) y vuelva a ejecutar `pip install -r requirements.txt`.

2. **Falla de conexión en la primera ejecución (`urllib.error.URLError` o `TimeoutError`):**
   - *Causa:* La máquina no tiene acceso a Internet o la API de `mindicador.cl` no responde, y los archivos `precio_cobre_2025.json` y `tipo_cambio_2025.json` aún no han sido descargados en `raw/`.
   - *Solución:* Asegúrese de disponer de conexión a Internet durante la primera corrida, o verifique que los dos archivos JSON ya se encuentren en la carpeta `raw/`.

3. **Error `FileNotFoundError` al iniciar la extracción:**
   - *Causa:* Se eliminó o modificó la ubicación de alguno de los archivos originales en `raw/` (`origen_mina.sqlite`, `origen_costos_planta.csv`, `origen_leyes_laboratorio.json`).
   - *Solución:* Asegúrese de que la carpeta `raw/` contenga íntegros los tres archivos originales entregados.

4. **Presencia de valores `NaN` en marzo, junio, agosto y noviembre:**
   - *Causa:* No constituye un error del código ni del entorno. Falta Loa en marzo y agosto, falta Salado en noviembre y Cerro Norte registra `N/A` en insumos de junio. El pipeline no suma parcialmente ni imputa ceros. Las alertas quedan documentadas en `validation_log.parquet`.

---

## 11. Conciliación E1 y análisis de sensibilidad

La corrida vigente conserva los 14.598 lotes originales. La fecha normalizada identifica 86 registros fechados en 2026, que se excluyen sin corregir ni reintegrar. Entre los lotes de 2025 se excluyen otros 272 por las reglas `0 < ley_pct < 5`, `50 <= recuperacion_pct <= 100` y `toneladas_tratadas >= 100`. El resultado es 358 exclusiones únicas (2,45 %) y 14.240 lotes elegibles. Los 144 identificadores `lote_id` repetidos se mantienen; su eliminación se documenta sólo como sensibilidad separada.

De los 879 análisis de laboratorio, 843 se vinculan a lotes elegibles de 2025 (aproximadamente 5,9 %). La alternativa mensual calcula `f = Σ(T × ley_lab_prom) / Σ(T × ley_operacional)` y aplica ese factor al cobre fino principal, sin cambiar la definición oficial de Gold.

Los 86 lotes fechados en 2026 se conservan para revisión contra laboratorio en un resultado separado. Esa revisión no corrige sus fechas ni los reincorpora automáticamente al cálculo principal.

Los resultados conciliados son: cash cost enero `1,845768 USD/lb`, diciembre `3,408989 USD/lb`, variación `+84,692 %`, cash cost ponderado de los ocho meses completos `2,340304 USD/lb`, margen de referencia `2,233095 USD/lb` en S1 y `2,125960 USD/lb` en S2, efecto precio `+0,598223 USD/lb` y efecto costo `-0,705358 USD/lb`.

Las pruebas se ejecutan con:

```bash
python -m unittest discover -s tests -v
```

La ejecución revisada terminó satisfactoriamente con 14 pruebas aprobadas.
