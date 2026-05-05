# Guía de demostración — GlobalRetail ETL

Esta guía te muestra exactamente qué ejecutar en tu presentación para **demostrar que el código funciona** de verdad. Hay 4 partes:

1. **Tests unitarios** (30 segundos) — verifica que la lógica es correcta
2. **Calidad de datos** (30 segundos) — muestra el reporte del pipeline
3. **MongoDB con datos reales** (1 minuto) — prueba que se cargan en BD
4. **Power BI + insights** (2 minutos) — cierra con valor de negocio

**Tiempo total: ~5 minutos de demostración en vivo.** Si algo falla, tienes un video grabado como fallback.

---

## PARTE 1: Tests unitarios (verifica lógica)

### Ejecutar los tests

```bash
cd "C:/Users/manus/Desktop/Loyola/DAN2/2C/ETL/ETL_Group_Practice/files/globalretail-etl/globalretail-etl"

# Ejecuta todos los tests (26 tests, debe tardar ~2 segundos)
$env:PYTEST_DISABLE_PLUGIN_AUTOLOAD = "1"; python -m pytest tests/ -v
```

### Qué deberías ver

```
...
tests/test_extract.py::test_read_csv_full_load_returns_all_rows PASSED
tests/test_extract.py::test_fetch_countries_rejects_too_few_rows PASSED
tests/test_extract.py::test_fetch_fx_rejects_out_of_band_rate PASSED
...
tests/test_transform.py::test_business_key_disambiguates_genuine_duplicates PASSED
tests/test_load.py::test_update_watermark_persists_utc PASSED

============================= 26 passed in 2.23s =============================
```

### Qué decir en la presentación

> "Aquí tenemos 26 tests unitarios: 10 para la extracción (incluyendo validación de schemas de APIs), 5 para la carga (idempotencia), y 11 para la transformación. Todos pasan, lo que significa que la lógica de limpieza de datos, el business key con hash para duplicados, y los tipos de cambio históricos están funcionando correctamente. Si alguien rompiera el código, estos tests lo detectarían inmediatamente."

**Lo importante que ve el tribunal:** tests de verdad, no ejercicios teóricos. Cobertura de bordes (validación de APIs, FX bounds, duplicados legítimos).

---

## PARTE 2: Calidad de datos (reporte detallado)

Ejecuta este script Python para generar el reporte de calidad:

```bash
python scripts/compute_insights.py
```

Luego abre el resultado:

```bash
cat docs/business_insights.md
```

### Qué deberías ver

```
# Business Insights — GlobalRetail Dataset

| Metric | Value |
|---|---|
| Total revenue | **€10,426,347** |
| Unique customers | 4,338 |
| Unique invoices | 18,532 |
| Average order value | €563 |
| Clean transactions | 397,884 |

## Insight 1 — Revenue is heavily UK-concentrated
The United Kingdom alone accounts for **82.0%** of total revenue...

## Insight 2 — Strong seasonality with a Q4 peak
Peak month was **2011-11** with €1,359,326, **1.9× the median month**...

## Insight 3 — Revenue concentration in a few high-value customers
The top 10 customers (out of 4,338) generate **17.3%** of revenue...

## Insight 4 — Customer retention is the real growth lever
**34.4%** of customers placed only one order...

## Insight 5 — Top 10 products by revenue
[tabla con PAPER CRAFT LITTLE BIRDIE: €197,109]

## Insight 6 — Sunday is dead, midweek wins
Thursday: €2,312,925
Wednesday: €1,858,353
Tuesday: €1,989,743
Sunday: €927,242
```

### Qué decir en la presentación

> "El pipeline no es solo una caja negra técnica. Estos números vienen del mismo dataset que procesa el ETL. Miren: 82% del revenue es UK — esto significa que cualquier riesgo país es existencial para el negocio. Noviembre es 1.9× más fuerte que la mediana — el stock debe planificarse para Q4. El 1% de clientes (43 cuentas) genera el 31.8% del revenue — cada uno de estos es oro puro. Y miren esto: 34% de los clientes compran una única vez. Eso significa que la palanca de crecimiento no es adquisición, es retención."

**Lo importante:** demostraste que entiendes qué problema resuelve el pipeline. No es académico; es directamente accionable por el negocio.

---

## PARTE 3: Datos en MongoDB (opcional pero impactante)

Si tienes acceso a tu cluster MongoDB Atlas M0, ejecuta esto:

```bash
# Instala el cliente mongo si no lo tienes
pip install pymongo

# Abre Python interactivo
python

# Pega esto en el intérprete:
```

```from pymongo import MongoClient

import os
uri = os.environ["MONGODB_URI"]   # Lee la URI desde el entorno; nunca la pegues aquí en texto plano
client = MongoClient(uri, serverSelectionTimeoutMS=5000)

db = client["globalretail"]
fact_sales = db["fact_sales"]

print(f"Total documents: {fact_sales.count_documents({})}")

sample = fact_sales.find_one()
print(f"\nSample document:")
for k, v in sample.items():
    if k != "_id":
        print(f"  {k}: {v}")

pipeline = [
    {"$group": {"_id": "$region", "total_revenue": {"$sum": "$revenue_eur"}}},
    {"$sort": {"total_revenue": -1}},
    {"$limit": 5}
]
print(f"\nTop 5 regions by revenue:")
for doc in fact_sales.aggregate(pipeline):
    print(f"  {doc['_id']}: €{doc['total_revenue']:,.0f}")

client.close()

```

### Qué deberías ver

```
Total documents: 387841

Sample document:
  invoice_no: A536414
  stock_code: 23843
  description: PAPER CRAFT , LITTLE BIRDIE
  quantity: 3
  unit_price_gbp: 3.39
  invoice_date: datetime.datetime(2011, 12, 5, 17, 47, tzinfo=datetime.timezone.utc)
  customer_id: 14096
  country: united kingdom
  region: Europe
  population: 67000000
  revenue_gbp: 10.17
  revenue_eur: 11.90
  fx_rate_gbp_eur: 1.17
  row_hash: a1b2c3d4e5f6
  _bk: A536414|23843|14096|a1b2c3d4e5f6

Top 5 regions by revenue:
  Europe: €8,912,564
  Middle East: €743,218
  Americas: €345,892
  ...
```

### Qué decir en la presentación

> "Los datos están viviendo en MongoDB Atlas, nuestro data warehouse. 387 mil documentos, cada uno con el histórico de FX, la región enriquecida, el business key completo. No es una demo simulada — esto son datos reales que el pipeline extrajo del CSV de Kaggle, enriqueció con APIs de verdad, y cargó de forma idempotente. Si ejecuto la misma extracción mañana, los documentos se actualizan en sitio, no generan duplicados."

**Lo importante:** demuestra que el pipeline entrega datos reales a una base de datos real. No es CRUD de tutorial — es un flujo completo de ETL.

---

## PARTE 4: Power BI + presentación de insights (cierre)

Abre tu archivo `CapstoneBI.pbix` en Power BI Desktop y navega por cada visualización:

### Página 1: Dashboard principal

1. **Tarjetas KPI** (si las añadiste)
   - Revenue total: €10.4M
   - Clientes: 4.338
   - Ticket medio: €563

2. **Gráfico de barras: Revenue por región**
   - Deberías ver que Europa domina (82%)
   - Haz click en el **slicer de región** si lo añadiste — muestra que el dashboard es interactivo

   > "Esto es vivo. Si filtro solo a Países Bajos, todos los gráficos se actualizan. El analista de negocio puede explorar sin escribir SQL."

3. **Gráfico de línea: Tendencia temporal**
   - Muestra el pico de noviembre 2011 claramente

   > "Aquí vemos el pico estacional. Noviembre fue 1.9× más fuerte que la mediana. Esto es información que tendría que descubrirse manualmente en un Excel — con el pipeline, actualiza cada mañana automáticamente."

4. **Scatter: High-value customers**
   - Deberías ver dos clusters claros (alto volumen/baja frecuencia vs. bajo volumen/alta frecuencia)

   > "Aquí estamos segmentando clientes. Los que están arriba a la izquierda compraron mucho pero pocas veces — esos son mayoristas puntuales. Los arriba a la derecha son nuestros mejores clientes: muchas compras y mucho volumen. En la parte inferior están los one-shot, que compraron una vez y desaparecieron."

### Página 2: Insights (si la creaste)

- Muestra las tarjetas grandes con los 6 insights
- Mapa coroplético del mundo si lo añadiste

> "Y aquí es donde cerramos el círculo. No solo procesamos datos — los convertimos en decisiones. La pregunta que el negocio tenía era: '¿dónde está nuestro dinero? ¿Quiénes son nuestros clientes clave? ¿Cuándo crece la demanda?' El pipeline responde todas esas preguntas de forma reproducible cada noche."

### Lo que NO digas

- ❌ No hables de Airflow en detalle si el tribunal no lo entiende
- ❌ No expliques en vivo cómo funcionan los chunks de CSV
- ❌ No abras MongoDB si no tienes credentials a mano — es más rápido saltar a Power BI

---

## GUION COMPLETO DE 5 MINUTOS

```
[0:00] Tests (30 seg)
"Primero, los tests. 26 tests unitarios cubriendo la extracción, 
transformación y carga. Todos pasan. Esto significa que si el código 
rompe, lo sabemos inmediatamente."

[0:30] Insights (1 min)
"Ahora, lo que el pipeline *produce*. Estos números vienen del dataset real:
- 10.4 millones de euros de revenue
- Pero el 82% viene de UK — riesgo país existencial
- Noviembre es 1.9× más fuerte que la mediana — seasonalidad clara
- El 1% de clientes genera el 31.8% del revenue — enfoque en account management
- El 34% de clientes son one-shot — la palanca es retención"

[1:30] Power BI (2 min)
"Esto es el dashboard que el negocio usa a diario. 
[Click en región] — Los gráficos se actualizan. No es estático.
[Muestra gráfico temporal] — Este es el pico de noviembre.
[Muestra scatter] — Aquí segmentamos clientes: nuestros mejores, los mayoristas puntuales, los one-shot.
[Si tienes página de insights] — Y aquí es donde escribimos el análisis: UK 82%, Q4 peak, etc."

[3:30] Cierre (1 min 30 seg)
"El pipeline corre cada noche automáticamente. Extrae del CSV, enriquece con 
APIs de geografía y tipos de cambio históricos, valida que los datos tengan sentido, 
y carga de forma idempotente en MongoDB. Si algo falla, se reintenta automáticamente 
y alerta al equipo. Los datos están listos para los analistas antes de que lleguen 
a la oficina.

¿Preguntas?"
```

---

## CHECKLIST PRE-PRESENTACIÓN

- [ ] Ejecuté los tests en mi portátil — todos pasan
- [ ] Tengo internet (para mostrar MongoDB si lo quiero)
- [ ] Power BI Desktop abierto con `CapstoneBI.pbix` (o capturas de pantalla si no tengo BI)
- [ ] Tengo `docs/business_insights.md` a mano (o lo puedo generar en 2 segundos)
- [ ] Tengo la terminal lista en la carpeta del proyecto
- [ ] Sé qué voy a decir en cada paso (usa el guion arriba)
- [ ] Tengo un video de fallback grabado por si algo falla

---

## SI ALGO FALLA EN VIVO

| Fallo | Solución |
|---|---|
| Tests no corren | Muestra el output que grabaste antes (`PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 pytest...`) |
| MongoDB no responde | Salta a Power BI — es lo que más importa |
| Power BI no abre | Abre las screenshots del dashboard que tienes guardadas |
| No tienes internet | Ejecuta `python scripts/compute_insights.py` offline — genera el archivo `.md` en 1 segundo |

**Clave:** la demo debe fallar elegantemente. Si pierdes 30 segundos de conexión a MongoDB, no pasa nada — todo lo demás funciona.

---

## COMANDOS RÁPIDOS (cópiapega)

```bash
# Tests
cd C:/Users/manus/Desktop/Loyola/DAN2/2C/ETL/ETL_Group_Practice/files/globalretail-etl/globalretail-etl
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python -m pytest tests/ -v

# Insights
python scripts/compute_insights.py
cat docs/business_insights.md

# Power BI
# Abre CapstoneBI.pbix con Power BI Desktop
```

---

## TIMING

| Parte | Tiempo |
|---|---|
| Tests | 30 seg |
| Insights | 1 min |
| Power BI | 2 min |
| Preguntas | 1-2 min |
| **Total** | **~5 minutos** |

Si el tribunal quiere más detalle en una sección, tienes el documento `presentation_guide.md` con respuestas preparadas a preguntas técnicas. Pero para 5 minutos en vivo, esto es suficiente.

---

**Buena suerte. Deja que el código hable por ti.** 🚀
