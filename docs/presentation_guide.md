# Guía de presentación — GlobalRetail ETL

Este documento es el material de apoyo para defender el proyecto. Cubre:

1. Las **mejoras introducidas** y cómo justificarlas si te preguntan.
2. Los **insights de negocio** que cierran el círculo "datos crudos → decisión".
3. Una **guía concreta para mejorar el dashboard Power BI** en 30 minutos.
4. **Preguntas frecuentes** que es muy probable que te hagan, con respuesta preparada.

---

## 1. Mejoras introducidas y cómo justificarlas

Cada mejora se introdujo respondiendo a una pregunta concreta que un ingeniero de datos senior haría al revisar el proyecto. Si en la presentación alguien las plantea, esta es la respuesta corta.

### 1.1. Lectura del CSV en chunks (`extract.py`)

| | |
|---|---|
| **Qué cambia** | `pd.read_csv(..., chunksize=100_000)` en lugar de carga en memoria de las 541k filas. |
| **Por qué** | El consumo de memoria pico ya no depende del tamaño del fichero fuente. Para un CSV de 100GB, el pipeline sigue funcionando con la misma RAM. |
| **Justificación si preguntan** | "El watermark filtra después de leer; con chunks, podemos descartar bloques enteros sin materializar la fila completa. Es el primer paso hacia una extracción verdaderamente incremental — el siguiente sería que la fuente entregue solo deltas vía CDC (Debezium, Kafka Connect)." |

### 1.2. Tipos de cambio históricos por fecha (`extract.py` + `transform.py`)

| | |
|---|---|
| **Qué cambia** | `fetch_fx_rates_for_dates` consulta Frankfurter `/v1/{YYYY-MM-DD}` para cada día único de facturación. `apply_fx_historical` aplica el rate del día de la transacción, no el rate de hoy. |
| **Por qué** | Los datos son de 2010-2011. Aplicar el tipo de cambio de 2026 corrompe el `revenue_eur`. Un analista financiero rechazaría inmediatamente esos números. |
| **Justificación si preguntan** | "El revenue en EUR debe reflejar el valor económico real del momento de la venta. Usamos forward-fill para fines de semana y festivos (Frankfurter no publica rates esos días), exactamente como hace el Banco Central Europeo al reportar series temporales." |

### 1.3. Validación del schema de las APIs (`extract.py`)

| | |
|---|---|
| **Qué cambia** | Asserts explícitos: payload debe ser lista, REST Countries debe devolver ≥100 países, FX rate debe estar en `[0.5, 2.5]`. |
| **Por qué** | Si una API cambia silenciosamente (renombra un campo, devuelve `[]` durante un outage), sin validación el pipeline se ejecuta "verde" pero carga datos corruptos o vacíos. **Fail loud, fail early.** |
| **Justificación si preguntan** | "Hay dos formas de fallar: ruidosamente o silenciosamente. La segunda es mucho peor porque corrompe datos durante semanas antes de que alguien lo detecte. Las cotas (`MIN_COUNTRIES_EXPECTED`, `MIN_FX_RATE`) son de orden de magnitud, no precisas — solo deben capturar fallos catastróficos." |

### 1.4. Business key con hash de fila (`transform.py` + `load.py`)

| | |
|---|---|
| **Qué cambia** | El índice único en MongoDB pasó de `(invoice_no, stock_code, customer_id)` a `(invoice_no, stock_code, customer_id, row_hash)`. El hash es SHA1 de los campos value-bearing. |
| **Por qué** | El dataset Online Retail tiene casos en los que la misma factura tiene la misma línea de producto dos veces (ajustes, correcciones manuales). Con la clave anterior, el segundo upsert sobreescribía silenciosamente el primero, perdiendo una transacción. |
| **Justificación si preguntan** | "Una clave de negocio debe ser por construcción única. Si la fuente puede emitir filas con el mismo triple (factura, producto, cliente) pero distinta cantidad o precio, el hash es la forma matemáticamente correcta de distinguirlas. Si dos filas son *literalmente* idénticas, el hash coincide y el upsert es idempotente, que es el comportamiento que queremos." |

### 1.5. Reporte de calidad de datos (`transform.py`)

| | |
|---|---|
| **Qué cambia** | `clean_sales` ahora devuelve `(df, counters)`. `build_quality_report` agrega un dict con métricas: filas dropped por regla, % de retención, países sin match, totales de revenue. Se loguea como JSON estructurado y viaja por XCom. |
| **Por qué** | Un pipeline que dropa el 28% de las filas de entrada sin reportarlo es una caja negra. Estas métricas son la base de un dashboard de observabilidad y de las alertas de drift. |
| **Justificación si preguntan** | "Sin estas métricas, si mañana las APIs devuelven datos de peor calidad y el pipeline empieza a dropar el 60% en lugar del 28%, no nos enteraríamos hasta que el equipo de negocio se queje. Con el reporte, una alerta sobre `kept_pct < 65` lo detecta automáticamente." |

### 1.6. `on_failure_callback` y limpieza de staging (DAG)

| | |
|---|---|
| **Qué cambia** | Callback que emite un log JSON estructurado en cualquier fallo de tarea. Nueva tarea `cleanup_staging` con `trigger_rule='all_done'` que elimina Parquet de más de N días. |
| **Por qué (callback)** | Por defecto Airflow falla en silencio: la UI muestra rojo, pero nadie la mira. Un log estructurado es directamente consumible por Loki/Datadog/PagerDuty para alertar a un humano. |
| **Por qué (cleanup)** | Sin retención, el directorio de staging crece sin límite (3 archivos × 365 días = >1000 archivos/año). El `trigger_rule='all_done'` garantiza que la limpieza corre incluso si una tarea anterior falla — los archivos de runs fallidos son útiles para debug pero no para siempre. |

### 1.7. Cobertura de tests ampliada

| | |
|---|---|
| **Qué cambia** | Antes: 7 tests, todos en `transform.py`. Ahora: ~25 tests cubriendo `extract` (CSV chunks, watermark, validación de APIs, FX) y `load` (operaciones de upsert, contrato del watermark). |
| **Justificación si preguntan** | "Los tests de transform validan la lógica pura — pero la mayor parte de los fallos en producción están en los bordes (lectura, escritura, llamadas a red). Mockear estos bordes nos permite probar la lógica sin levantar Mongo ni hacer HTTP, y a la vez documenta el comportamiento esperado." |

---

## 2. Insights de negocio (cerrar el círculo)

Estos números están calculados con `scripts/compute_insights.py` sobre el CSV real, aplicando las mismas reglas de limpieza que el ETL. **Inclúyelos al final de la presentación**: demuestran que el pipeline no es un ejercicio técnico aislado, sino que produce información sobre la que se puede actuar.

### Headline
- **€10.4M** revenue total, **4.338** clientes únicos, **18.532** facturas
- Ticket medio: **€563**
- Periodo: 1 dic 2010 → 9 dic 2011

### Los 6 insights

1. **Concentración en UK (82%)** → riesgo país existencial. La internacionalización no es crecimiento, es supervivencia.
2. **Pico estacional Nov-Dic** → noviembre 2011 fue 1.9× la mediana mensual. El stock y el marketing deben planificarse para Q4.
3. **Concentración en clientes top** → el 1% de los clientes (43 cuentas) genera el 31.8% del revenue. Cada cuenta top vale invertir en account management dedicado.
4. **34% de clientes son one-shot** → la verdadera palanca de crecimiento es la retención, no la adquisición. Es matemática: convertir un comprador en recurrente vale más que conseguir uno nuevo.
5. **Top productos**: el `PAPER CRAFT, LITTLE BIRDIE` (cód. 23843) genera €197k él solo. Concentrar surtido en SKUs probados.
6. **Tue-Thu son los días fuertes** → calendario óptimo para campañas de email/anuncios.

El detalle completo está en [`business_insights.md`](docs/business_insights.md).

---

## 3. Cómo mejorar el dashboard Power BI en 30 minutos

El `.pbix` actual tiene 3 visualizaciones básicas. Para una presentación, añade lo siguiente:

### 3.1. Tarjetas KPI en la parte superior (5 min)

Añade tarjetas (`Card` visual) con:
- **Revenue total**: `SUM(fact_sales[revenue_eur])`
- **Clientes únicos**: `DISTINCTCOUNT(fact_sales[customer_id])`
- **Facturas**: `DISTINCTCOUNT(fact_sales[invoice_no])`
- **Ticket medio**: `[Revenue total] / [Facturas]`

### 3.2. Slicer de región (5 min)

Inserta un visual `Slicer` con `fact_sales[region]`. Configúralo como botones horizontales en la cabecera. Al hacer clic, **todos** los gráficos se filtran. Es el cambio que más impresiona porque convierte un dashboard estático en una herramienta exploratoria.

### 3.3. Tabla "Top 10 clientes por revenue" (5 min)

Visual `Table` con columnas:
- `customer_id`
- `SUM(revenue_eur)` ordenado descendente
- `DISTINCTCOUNT(invoice_no)` (nº de pedidos)

Filtra a Top N = 10 con el panel de filtros.

### 3.4. Mejora del gráfico de tendencia (5 min)

El gráfico de líneas actual: añádele una segunda serie con `[Revenue total]` mes a mes del año anterior (`SAMEPERIODLASTYEAR`) — aunque el dataset solo cubre un año, con DAX puedes mostrar la línea de tendencia (`LINEAR.TREND`) o una banda de medias móviles a 3 meses. Demuestra control de DAX.

### 3.5. Página separada "Insights" (10 min)

Crea una segunda página del dashboard con:
- Una tarjeta grande mostrando: **"82% del revenue viene de UK"**
- Un mapa coroplético del mundo con `revenue_eur` por país
- Una tabla con las métricas de los 6 insights del documento

Es la página que te asegura puntos en la sección de "valor de negocio" de la rúbrica.

---

## 4. Preguntas frecuentes preparadas

### "¿Qué pasa si MongoDB está caído cuando se ejecuta el DAG?"
Las tareas tienen 2 reintentos con backoff exponencial (2 min, 4 min, máximo 15 min). Si los 2 reintentos fallan, el DAG queda en estado `failed`, se dispara `on_failure_callback` que emite un log JSON estructurado para alertar al on-call. Crucialmente: el watermark **no avanza** hasta que la carga es exitosa, así que la siguiente ejecución reprocesa exactamente la misma ventana sin perder datos.

### "¿Por qué MongoDB y no PostgreSQL u otro warehouse?"
MongoDB Atlas tiene un free tier (M0, 512MB) que cubre las necesidades del capstone sin coste. Su modelo flexible facilita iterar sobre el schema durante desarrollo. **Para producción real con análisis pesado**, lo correcto sería un warehouse columnar (BigQuery, Snowflake, Redshift) — MongoDB es un compromiso pragmático aquí, no la elección óptima.

### "¿Cómo se asegura que no hay duplicados?"
Tres capas: (1) la business key con `row_hash` garantiza identidad única por contenido; (2) el índice único compuesto en MongoDB lo enforza a nivel de almacenamiento; (3) `bulk_write(UpdateOne, upsert=True)` hace que re-ejecuciones sean idempotentes — actualizan en sitio, no insertan copias.

### "¿Y si se quiere reprocesar todo desde cero?"
Borrar el documento `_pipeline_metadata/last_watermark` resetea el watermark; la siguiente ejecución hace full load. Como las cargas son upserts, **incluso un full load sobre datos ya cargados es seguro** — el resultado final es el mismo. Esto es la propiedad de idempotencia llevada al extremo.

### "¿Por qué FX por fecha y no en tiempo real?"
Una venta de enero 2011 tiene un valor económico que depende del tipo de cambio de enero 2011, no del actual. Es contabilidad básica. Para series temporales históricas es la única respuesta correcta. Para revenue *del día de hoy* sí usaríamos rate spot.

### "¿Qué hace que esto sea 'idempotente'?"
Una operación es idempotente si aplicarla N veces produce el mismo resultado que aplicarla una vez. Aquí: (1) el watermark filtra siempre el mismo conjunto si no hay datos nuevos, (2) los upserts actualizan en sitio sin duplicar, (3) el watermark solo avanza tras éxito completo. Resultado: el pipeline se puede re-ejecutar tantas veces como se quiera sin corromper datos.

### "¿Y si el CSV creciera a 100GB?"
La lectura en chunks (100k filas por chunk) ya garantiza que la memoria no escala con el tamaño de la fuente. El siguiente paso sería sustituir el CSV por una fuente con CDC nativo (Debezium leyendo el binlog de la base operacional, o S3 con particiones por fecha), de forma que el extract solo descargase los deltas, no la fuente completa.

### "¿Qué métricas observan en producción?"
El reporte de calidad (`build_quality_report`) emite por XCom y por log: `kept_pct`, `rows_with_unknown_region`, `revenue_eur_total`. Sobre esos campos definiríamos alertas: por ejemplo, "alerta si `kept_pct < 65%` durante 3 ejecuciones consecutivas" o "alerta si `revenue_eur_total` desvía >40% del valor esperado para ese día de la semana".

### "¿Tests? ¿Cobertura?"
~25 tests unitarios cubriendo las tres etapas. Transform: lógica pura de limpieza, normalización, FX y business key (incluyendo el caso de duplicados legítimos). Extract: lectura por chunks, filtro por watermark, validación de schemas de APIs, sanity bounds del FX. Load: construcción de operaciones de upsert, contrato del watermark UTC. Lo que **no** cubrimos son tests de integración end-to-end con MongoDB real — eso requeriría docker-compose en CI.

---

## 5. Estructura sugerida para la presentación (10-12 min)

| Min | Sección | Mensaje principal |
|---|---|---|
| 0-1 | Contexto de negocio | "GlobalRetail necesita consolidar ventas, geografía y FX para análisis." |
| 1-3 | Arquitectura | Diagrama: CSV+APIs → Airflow (E/T/L) → MongoDB → Power BI |
| 3-5 | Decisiones técnicas clave | Idempotencia, watermark, chunks, FX histórico, validación de schemas |
| 5-7 | Calidad y observabilidad | Tests, reporte de calidad, alertas, cleanup |
| 7-9 | **Demo del dashboard** | Mostrar interactividad (slicer de región) y los KPIs |
| 9-11 | **Insights de negocio** | Los 6 insights — esto es lo que justifica el proyecto |
| 11-12 | Cierre y siguientes pasos | "En producción: CDC, alertas reales, warehouse columnar" |

**Las dos secciones más importantes son las dos últimas**: el tribunal recordará el dashboard y los insights mucho más que la arquitectura. La arquitectura demuestra competencia técnica; los insights demuestran que entiendes para qué sirve.
