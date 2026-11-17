# Integración del paquete E2-B en Proyecto-TIN-G6

## Alcance

La sección B implementa el esquema estrella planta-mes, la conciliación con E1
y los controles de cada corrida. Lee la columna `regiones.region` del SQLite y
normaliza `ley_lab_prom` como decimal porcentual. El prototipo y el caso de
negocio corresponden a las secciones D y E.

## Reproducción

Desde la raíz del repositorio con Python 3.12:

```bash
python -m pip install -r requirements.txt
python -m unittest discover -s tests -v
python run_e2.py
```

Al terminar con código 0, `gold/e2/` contiene tres Parquet, `fact_planta_mes`,
`dim_planta` y `dim_tiempo`. `reporte_calidad.csv` queda en la raíz con el
resultado de cada control. Ante un bloqueo, se actualiza el reporte de la
corrida fallida y se conserva la última Gold válida.

E2 contrasta el KPI con la referencia histórica `gold/gold_metricas.parquet`
de E1. Las salidas históricas conservan sus rutas durante la migración.
`validation_log.parquet` sigue siendo el log detallado de `main.py`.
El CSV de E2 registra el resultado de los controles de publicación, con
estados `PASA`, `AVISA` y `BLOQUEA`.
El reporte describe la última ejecución, que puede ser posterior a la corrida
que dejó la Gold vigente. Ambos resultados se regeneran en local.

### Consumir el indicador y su historia

El informe o prototipo puede leer los tres Parquet y usar las funciones existentes:

```python
import sys
from pathlib import Path
import pandas as pd

sys.path.insert(0, str(Path("code").resolve()))
from e2_semantic import costo_directo_planta_por_libra, historia_costo_empresa_mes

gold = Path("gold/e2")
hecho = pd.read_parquet(gold / "fact_planta_mes.parquet")
tiempo = pd.read_parquet(gold / "dim_tiempo.parquet")
plantas = pd.read_parquet(gold / "dim_planta.parquet")
kpi = costo_directo_planta_por_libra(hecho)
historia = historia_costo_empresa_mes(hecho, tiempo, plantas)
```

La opción `definicion` admite `operacional`, `laboratorio` y `simple`.
Para el detalle por planta, el KPI admite `nivel="planta_mes"`.
Las tres medidas de cobre fino ya están en el hecho. El KPI y su historia
se calculan al consumirlos.

## Archivos

- `run_e2.py`: reconstruye la cadena, genera el reporte y actualiza Gold E2 después de validar.
- `code/e2_model.py`: dimensiones 4/12, hecho planta-mes 48 filas, ley operacional/laboratorio/simple, costos sin imputación.
- `code/e2_semantic.py`: fórmula única del KPI y cálculo mensual para contar el número y su historia.
- `code/e2_quality.py`: pruebas de las cuatro fuentes, integridad del esquema, conciliación, política PASA/AVISA/BLOQUEA.
- `tests/test_e2.py`: siete pruebas, incluyendo duplicados de costos, discrepancia de métrica, catálogo SQLite real y contrato narrativo.
- `docs/ARQUITECTURA_E2_B.md`: contenido completo para el apartado B, tabla de linaje y SLA.

## Estado real de verificación

La corrida local del 17-11-2026 terminó con
código 0 en 1,584 s, con las fuentes del proyecto y los snapshots de API
disponibles. Produjo 4 plantas, 12 meses y 48 filas del hecho. Los 16 controles
`PASA` incluyen la conciliación de los ocho meses empresariales completos con E1.
Los dos `AVISA` corresponden a 44 de 48 celdas con costo completo y al umbral de
completitud propuesto de 95%. La suite completa aprobó 22 pruebas.

La corrección de laboratorio evitó interpretar 84 valores con punto decimal como miles (por ejemplo, `1.116` como `1116`). Después de reconstruir Silver, los 843 pares vinculables quedaron dentro del rango `(0,5)` y la Gold principal de E1 mantuvo sus cifras.

## Notas y límites operativos

- La API para 2025 se consulta únicamente si el snapshot RAW correspondiente no existe; un snapshot presente se reutiliza, no se vuelve a validar con una llamada en vivo.
- El catálogo de plantas se lee de `raw/origen_mina.sqlite`. La asignación de Illapel a Coquimbo es una decisión documentada del equipo ante `region_id=9` inexistente; otras referencias huérfanas bloquean la corrida.
- El modelo retiene los lotes con `lote_id` repetido cuando tienen el indicador `es_elegible` verdadero, igual que E1; no se atribuye unicidad artificial al identificador.
- Los controles se componen de validaciones de negocio con casos advertibles (por ejemplo 44/48 completa) y fallas estructurales bloqueantes.
- La operación prevista es local, con una corrida por vez. Consumir los Parquet después de que termine la ejecución.
