# PROJECT_STRUCTURE.md

Mapa de navegación del repositorio CuprumAndes. Actualizar cuando se agregue,
mueva o elimine un archivo relevante.

---

## Raíz del proyecto

```
/
├── main.py                    # Punto de entrada único del pipeline
├── requirements.txt           # Dependencias de terceros con versiones exactas
├── PROJECT_STRUCTURE.md       # Este archivo — mapa de navegación del repositorio
├── README.md                  # Instrucciones de instalación y ejecución para terceros
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
│
├── bronze/                    # Datos extraídos de raw/, sin limpiar, Parquet (todos los valores como texto)
│   └── [generado por extraction.py]
│
├── silver/                    # Datos limpios y tipados, Parquet
│   ├── silver_laboratorio.parquet  # Laboratorio normalizado y vinculable por lote_id
│   └── [otros generados por cleaning.py]
│
├── gold/                      # Métricas mensuales a nivel empresa, Parquet
    ├── gold_metricas.parquet  # [generado por metrics.py] — 12 filas, 7 columnas
    ├── gold_sensibilidad_laboratorio.parquet
    ├── gold_sensibilidad_lotes_repetidos.parquet
    └── revision_lotes_2026_laboratorio.parquet

└── tests/
    └── test_pipeline.py       # Pruebas unitarias y controles de conciliación
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

---

## Flujo de datos

```
raw/  →  extraction.py  →  bronze/
                        →  raw/ (respuesta API)
bronze/  →  cleaning.py  →  silver/
silver/  →  metrics.py   →  gold/
```

Las etapas son estrictamente secuenciales y se orquestan desde `main.py`.
Cada etapa lee solo la capa anterior; nunca salta capas ni hace llamadas de red
(excepto extraction.py, que es la única con acceso a la API).

---

## Archivos generados en tiempo de ejecución

| Archivo                   | Escrito por          | Descripción                                      |
|---------------------------|----------------------|--------------------------------------------------|
| `raw/precio_cobre_2025.json` | `extraction.py`   | Respuesta JSON de la API de precios del cobre    |
| `raw/tipo_cambio_2025.json`  | `extraction.py`   | Respuesta JSON de la API del tipo de cambio      |
| `bronze/*.parquet`           | `extraction.py`   | Datos extraídos sin limpiar                      |
| `silver/*.parquet`           | `cleaning.py`     | Datos limpios y tipados                          |
| `gold/*.parquet`             | `metrics.py`      | Métrica principal, sensibilidades y revisión 2026|
| `validation_log.parquet`     | `validation_log.py` (llamado por `main.py`) | Log tabular de fallas de validación |

---

## Dependencias entre módulos

```
main.py
  └── extraction.py   →  config.py
  └── cleaning.py     →  config.py, business_rules.py, validation_log.py
  └── metrics.py      →  config.py, business_rules.py, validation_log.py
  └── validation_log.py → config.py
```

`business_rules.py` y `config.py` no importan otros módulos del proyecto.
`validation_log.py` solo importa `config.py`.

---

## Convenciones

- Rutas: siempre relativas a `PROJECT_ROOT` (definido en `config.py`).
- Idioma de comentarios: español. Idioma de identificadores: inglés.
- raw/ nunca se modifica desde el código (excepto agregar respuestas de la API).
- Toda falla de validación va a `validation_log.register_failure()`; nunca lanza excepción.
