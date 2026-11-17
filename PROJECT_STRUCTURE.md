# PROJECT_STRUCTURE.md

Mapa de navegación del repositorio CuprumAndes. Actualizar cuando se agregue,
mueva o elimine un archivo relevante.

---

## Raíz del proyecto

```
/
├── main.py                    # Punto de entrada para reproducir E1
├── run_e2.py                  # Cadena E2, controles y escritura de Gold
├── requirements.txt           # Dependencias de terceros con versiones exactas
├── PROJECT_STRUCTURE.md       # Este archivo — mapa de navegación del repositorio
├── README.md                  # Instrucciones de instalación y ejecución para terceros
├── README_E2_B.md             # Operación, evidencia y límites específicos de E2-B
├── reporte_calidad.csv        # Resultado de la última corrida E2 (generado)
├── Enunciado.md               # Enunciado académico original (referencia, no modificar)
├── .gitignore
│
├── raw/                       # Fuentes originales — NO MODIFICAR
│   ├── origen_mina.sqlite     # Base operacional: lotes de producción
│   ├── origen_costos_planta.csv  # Costos de planta (energía, insumos, mano de obra)
│   ├── origen_leyes_laboratorio.json  # Leyes de laboratorio para sensibilidad separada
│   └── [precio_cobre_2025.json]   # Respuesta API cobre — escrito por extraction.py
│   └── [tipo_cambio_2025.json]    # Respuesta API dólar — escrito por extraction.py
│
├── BRONZE_SCHEMA.md           # Esquema, anomalías y trazabilidad de la capa bronze
├── SILVER_SCHEMA.md           # Esquema, tipos, formatos canónicos y notas de la capa silver
├── GOLD_SCHEMA.md             # Esquema de la capa gold: columnas, fórmulas y comportamiento ante datos faltantes
├── docs/
│   └── ARQUITECTURA_E2_B.md   # Arquitectura, linaje, calidad y contrato del KPI
│
├── bronze/                    # Datos extraídos de raw/, sin limpiar, Parquet (todos los valores como texto)
│   └── [generado por extraction.py]
│
├── silver/                    # Datos limpios y tipados, Parquet
│   ├── silver_laboratorio.parquet  # Laboratorio normalizado y vinculable por lote_id
│   └── [otros generados por cleaning.py]
│
├── gold/
│   ├── gold_metricas.parquet  # Legado E1 — 12 filas, 7 columnas
│   ├── [sensibilidades y revisión de E1]
│   └── e2/                   # Esquema estrella vigente
│       ├── dim_planta.parquet
│       ├── dim_tiempo.parquet
│       └── fact_planta_mes.parquet

└── tests/
    ├── test_pipeline.py       # Pipeline base, transformación y conciliación
    └── test_e2.py             # Modelo, calidad, semántica y esquema SQLite real
```

---

## Módulos de código (`code/`)

| Archivo             | Propósito                                                                 | Etapa       |
|---------------------|---------------------------------------------------------------------------|-------------|
| `config.py`         | Rutas de las cuatro capas, log de validaciones y constantes analíticas    | Compartido  |
| `business_rules.py` | Única fuente de verdad para reglas analíticas y rangos de validación      | Compartido  |
| `validation_log.py` | Registro de fallas de validación: acumulación, escritura y resumen        | Compartido  |
| `extraction.py`     | Etapa 1 — Extrae fuentes a bronze/ y guarda respuesta de la API en raw/   | Extracción  |
| `cleaning.py`       | Etapa 2 — Estandariza formatos, valida campos, elimina cabeceras/TOTAL, detecta duplicados y faltantes; produce silver/ | Limpieza    |
| `metrics.py`        | Etapa 3 — Calcula Gold principal y sensibilidades separadas desde silver/ | Métricas    |
| `e2_model.py`       | Construye dimensiones y hecho planta-mes de 48 filas                      | Modelo E2   |
| `e2_semantic.py`    | Calcula el KPI y su historia a partir del modelo estrella                 | Semántica   |
| `e2_quality.py`     | Ejecuta controles PASA/AVISA/BLOQUEA antes de publicar                   | Calidad E2  |

---

## Flujo de datos

```
raw/  →  extraction.py  →  bronze/
                        →  raw/ (respuesta API)
bronze/  →  cleaning.py  →  silver/
silver/  →  metrics.py   →  gold/
silver/ + catálogos → e2_model.py → modelo estrella
modelo estrella → e2_semantic.py → KPI + historia mensual
modelo + referencia E1 → e2_quality.py → reporte_calidad.csv
modelo aprobado → gold/e2/
```

Las etapas E1 se orquestan desde `main.py`. `run_e2.py` reutiliza extracción
y limpieza, valida contra la referencia histórica y actualiza los tres Parquet
de E2 si pasan los controles bloqueantes.
El modelo E2 usa Silver para las medidas y SQLite RAW para los catálogos de
planta y región. Solo la extracción accede a la API y reutiliza los snapshots
locales cuando existen.

---

## Archivos generados en tiempo de ejecución

| Archivo                   | Escrito por          | Descripción                                      |
|---------------------------|----------------------|--------------------------------------------------|
| `raw/precio_cobre_2025.json` | `extraction.py`   | Respuesta JSON de la API de precios del cobre    |
| `raw/tipo_cambio_2025.json`  | `extraction.py`   | Respuesta JSON de la API del tipo de cambio      |
| `bronze/*.parquet`           | `extraction.py`   | Datos extraídos sin limpiar                      |
| `silver/*.parquet`           | `cleaning.py`     | Datos limpios y tipados                          |
| `gold/*.parquet`             | `metrics.py`      | Métrica principal, sensibilidades y revisión 2026|
| `gold/e2/*.parquet`          | `run_e2.py`       | Hecho planta-mes y dos dimensiones de la última corrida aprobada |
| `reporte_calidad.csv`        | `run_e2.py`       | Resultado de los controles de la última corrida |
| `validation_log.parquet`     | `validation_log.py` (llamado por `main.py`) | Log tabular de fallas de validación |

---

## Dependencias entre módulos

```
main.py
  └── extraction.py   →  config.py
  └── cleaning.py     →  config.py, business_rules.py, validation_log.py
  └── metrics.py      →  config.py, business_rules.py, validation_log.py
  └── validation_log.py → config.py

run_e2.py
  └── extraction.py, cleaning.py, validation_log.py
  └── e2_model.py → config.py
  └── e2_quality.py → config.py, e2_semantic.py
```

`business_rules.py` y `config.py` no importan otros módulos del proyecto.
`validation_log.py` solo importa `config.py`.

---

## Convenciones

- Rutas: siempre relativas a `PROJECT_ROOT` (definido en `config.py`).
- Idioma de comentarios: español. Idioma de identificadores: inglés.
- raw/ nunca se modifica desde el código (excepto agregar respuestas de la API).
- E1 registra alertas de transformación en `validation_log.parquet`. E2
  registra los controles de su corrida en `reporte_calidad.csv`.
