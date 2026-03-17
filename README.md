# Healthcare Data Engineering Pipeline
**Prueba Técnica — Data Engineer Intern | Invers AI**

Pipeline completo de ingeniería de datos para un dataset hospitalario, cubriendo desde la exploración y limpieza hasta la automatización con un Bot de Telegram.

---

## Estructura del Repositorio

```
prueba-tecnica-invers/
├── data/
│   └──                              #archivo csv descargado de telegram
├── diagrams/
│   └── DiagramaBD.drawio.png        # Diagrama lógico del Esquema Estrella
├── src/
│   ├── 01_exploracion.ipynb         # Fase 1 — Exploración y diagnóstico
│   ├── 02_limpieza.py               # Fase 2 — Limpieza y transformación
│   ├── 03_modelado.py               # Fase 3 — Orquestador del modelo Estrella
│   ├── 04_run_pipeline.py           # Fase 3 — Ejecución del pipeline
│   ├── bot.py                       # Fase 5 — Chatbot de Telegram (Orquestador)
│   ├── database.py                  # Capa de Acceso a Datos (DAO)
│   └── sql/                         # Scripts de Base de Datos
│       ├── schema.sql               # DDL del Modelo Estrella
│       └── etl_insert.sql           # Transformación de Staging a modelo Estrella
├── .env                             # Variables de entorno
├── requirements.txt
└── README.md
```

---

## Instalación y Configuración

**1. Instalar dependencias:**
```bash
pip install -r requirements.txt
```

**2. Configurar `.env`:**
```env
DATABASE_URL=postgresql://usuario:password@localhost:5432/health
TELEGRAM_TOKEN=tu_token_de_telegram
```



---

## Fase 1 — Exploración y Diagnóstico

> Script: `src/01_exploracion.ipynb`

Se realizó un análisis exploratorio del dataset crudo para identificar problemas de calidad antes de aplicar cualquier transformación. Los hallazgos documentados fueron:

| Problema detectado | Columna |
|---|---|
| Registros duplicados | Todas |
| Prefijos y sufijos en nombres (`Dr.`, `Mr.`, `PhD`) | `Name`, `Doctor` |
| Montos de facturación negativos | `Billing Amount` |
| Comas y ruido en nombres de hospitales | `Hospital` |
| Columnas de fecha almacenadas como cadena de texto | `Date of Admission`, `Discharge Date` |

---

## Fase 2 — Limpieza y Transformación

> Script: `src/02_limpieza.py`

Cada transformación fue aplicada con una justificación técnica y de negocio explícita.

### 1. Estandarización de nombres de pacientes y doctores (`clean_names`)
Para evitar duplicidades en los análisis y las visualizaciones, se estandarizaron los nombres de pacientes y doctores. Esto se logró eliminando prefijos y sufijos como `"Dr."`, `"Mr."`, `"PhD"`, entre otros, y aplicando formato Title Case. Esta limpieza asegura que variaciones en la escritura no generen múltiples registros de la misma persona y permite una correcta agrupación en los reportes y dashboards.

### 2. Eliminación de duplicados (`remove_duplicates`)
Se eliminaron registros completamente duplicados, conservando únicamente la primera ocurrencia. Esto es fundamental para que las métricas de volumen, facturación y análisis estadístico no se vean infladas artificialmente por datos repetidos.

### 3. Corrección de montos de facturación negativos (`handle_negative_billing`)
Al revisar la columna de facturación, se encontraron montos negativos que no correspondían a reembolsos reales, sino a errores de digitación (guiones accidentales). Para corregir esto, se aplicó la función de valor absoluto, garantizando que todos los montos sean positivos y reflejen correctamente los ingresos.

### 4. Normalización de nombres de hospitales (`clean_hospital_names`)
Se limpiaron los nombres de los hospitales eliminando comas y palabras que generaban ruido, como `"and"`, así como espacios extras. Esta normalización permite que los ingresos se agrupen correctamente por institución, evitando la creación de categorías separadas por errores de puntuación o formato.

### 5. Conversión de tipos de datos (`cast_data_types`)
Las columnas de fechas de admisión y egreso se transformaron al tipo `datetime`. Esta conversión fue necesaria para calcular correctamente métricas de tiempo, como la estancia promedio de los pacientes (Q3) y el análisis de estacionalidad por mes (Q1).

### 6. Carga a Staging
El DataFrame limpio se carga a la tabla `stg_healthcare` en PostgreSQL como área de trabajo intermedia. Esta separación permite re-ejecutar el proceso ELT al Modelo Estrella de forma idempotente sin necesidad de reprocesar el CSV original.

---

## Fase 3 — Modelado y Carga en Base de Datos

> Scripts: `src/schema.sql`, `src/etl_insert.sql`, `src/03_modelado.py`

### Diseño del Modelo de Datos: Esquema Estrella

Para el almacenamiento de los datos se decidió implementar un esquema estrella, debido a que este tipo de modelado permite organizar la información de forma eficiente para el análisis. Esto ayuda a reducir la redundancia de datos, ya que evita la repetición innecesaria de información, y facilita el mantenimiento y la escalabilidad.

El esquema se compone de una tabla de hechos central (`fact_admission`) que contiene las métricas principales del negocio, y varias tablas de dimensiones que describen el contexto de cada registro.

La tabla de hechos `fact_admission` representa cada evento de admisión hospitalaria dentro del sistema. En ella se almacenan las métricas cuantitativas más relevantes, como el monto de facturación (`billing_amount`), así como atributos operativos relacionados con la admisión: el número de habitación, el tipo de admisión y los resultados de pruebas.

Por otro lado, las tablas de dimensiones almacenan información descriptiva que permite contextualizar cada admisión hospitalaria. La dimensión `dim_patient` contiene los datos demográficos básicos de los pacientes, como nombre, edad, género y tipo de sangre. La dimensión `dim_doctor` registra los médicos responsables de la atención, mientras que `dim_hospital` identifica los centros de salud donde se realizan las admisiones.

La tabla `dim_insurance` permite analizar la facturación según el proveedor de seguros médicos, mientras que `dim_condition` describe la condición o diagnóstico principal del paciente. La dimensión `dim_medication` almacena los medicamentos asociados al tratamiento.

Finalmente, se implementó una dimensión temporal `dim_date`, que permite analizar los eventos a lo largo del tiempo. Esta tabla incluye atributos como año, mes, día y trimestre, lo cual facilita responder preguntas de negocio relacionadas con tendencias temporales, estacionalidad y volumen de admisiones por periodo.

### Diagrama Lógico

![Diagrama Lógico — Modelo Estrella](diagrams/DiagramaBD.drawio.png)



---

## Fase 4 — Visualización y Dashboard



### Q1 — Volumen total de registros por mes. ¿Hay estacionalidad?

Se observa un flujo operativo constante con un promedio aproximado de 914 admisiones mensuales. El volumen de registros por mes se mantiene relativamente estable entre aproximadamente 850 y 1000 admisiones.

Se observan algunos picos en los meses de julio y agosto, donde en algunos años se alcanzan valores cercanos o superiores a 1000 registros. Febrero suele presentar valores ligeramente más bajos, como en 2022 con 772 admisiones.

En general, no se observa una estacionalidad marcada, ya que el número de admisiones se mantiene bastante constante a lo largo del año sin picos recurrentes en meses específicos.

### Q2 — Top 10 pacientes por mayor valor generado

Los pacientes que generaron el mayor valor de facturación están encabezados por Michael Smith, con un total aproximado de 674,854.62 en facturación acumulada a partir de 25 admisiones. Le siguen Robert Smith con 562,973.82 (22 admisiones) y Michael Williams con 558,963.43 (25 admisiones).

En general, los pacientes del top 10 presentan entre 14 y 25 admisiones, lo que indica que el mayor valor generado está asociado principalmente a pacientes con múltiples ingresos hospitalarios, incrementando el monto total de facturación acumulado.

### Q3 — Tiempo promedio entre admisión y alta hospitalaria

El tiempo promedio entre los eventos de admisión y alta hospitalaria es de aproximadamente **15.5 días**. Esto significa que, en promedio, los pacientes permanecen hospitalizados poco más de dos semanas antes de recibir el alta médica.

### Q4 — Porcentaje de registros con resultado anormal

El **33.54%** de los registros presentan resultados anormales en las pruebas médicas. Esto indica que aproximadamente 1 de cada 3 admisiones hospitalarias está asociada a algún resultado clínico fuera de los parámetros normales.

Este indicador es relevante para monitorear la proporción de casos que requieren mayor atención médica, ya que los resultados anormales pueden estar asociados a condiciones de salud más complejas o que requieren atención especializada.

### Q5 — ¿Qué grupos de edad y género generan mayor facturación hospitalaria?

Los resultados muestran que los grupos de edad entre 20 y 70 años concentran la mayor parte de la facturación total, tanto en pacientes masculinos como femeninos. Dentro de este rango, las edades cercanas a los 50 y 60 años presentan los niveles más altos de facturación.

En contraste, los pacientes más jóvenes (menores de 20 años) y los de mayor edad (alrededor de 80 años) presentan una menor facturación total.

Asimismo, no se observan diferencias significativas entre hombres y mujeres, ya que la distribución de la facturación por género es equilibrada en todos los grupos de edad.

**Dashboard publicado (Power BI):**
[Ver dashboard en Power BI](https://app.powerbi.com/view?r=eyJrIjoiMjgxMTEzOWQtY2E1ZC00MGY5LTliZTMtYjllMDIwMTlmNDdjIiwidCI6IjY4MmE0ZTZhLWE3N2YtNDk1OC1hM2FjLTllMjY2ZDE4YWEzNyIsImMiOjR9)

---


## Fase 5 — Pipeline y Orquestación (Opcional)

> Script: `src/bot.py` (python-telegram-bot)

Se implementó un Bot de Telegram que automatiza el flujo de datos de extremo a extremo mediante un único trigger: el envío de un archivo CSV al chat.

### Flujo automatizado

```
Usuario envía CSV → Validación de archivo → Pipeline → Reporte ejecutivo automático
```

### Funcionalidades implementadas

| Función | Descripción |
|---|---|
| Ingesta por CSV | El bot recibe, valida la estructura y dispara el pipeline ELT completo |
| Reporte ejecutivo automático | Al finalizar el pipeline, envía KPIs clave (registros totales, facturación, tasa de alertas) junto con un gráfico |
| Menú de reportes analíticos | Acceso bajo demanda a reportes Q1 (estacionalidad), Q2 (medicamentos) y Q5 (seguros) mediante botones interactivos |

**Para ejecutar el bot:**
```bash
python src/bot.py
```

---

## Stack Tecnológico

| Capa | Tecnología |
|---|---|
| Lenguaje | Python 3.12 |
| Almacenamiento | PostgreSQL 16 |
| ETL / ORM | SQLAlchemy + SQL puro |
| Visualización | Streamlit + Plotly |
| Automatización | python-telegram-bot |
| Gestión de entorno | python-dotenv |

---

