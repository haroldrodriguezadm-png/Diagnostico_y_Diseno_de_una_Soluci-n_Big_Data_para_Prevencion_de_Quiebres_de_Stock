# RAJA MARKET — Diagnóstico y Diseño de una Solución Big Data para Prevención de Quiebres de Stock

Diagnóstico y diseño end-to-end de una arquitectura Big Data para una cadena de supermercados, orientada a **predecir y prevenir quiebres de stock durante eventos de alto tráfico**, incluyendo simulación de datos, pipeline en Apache Spark, modelo predictivo de demanda y evaluación de valor estratégico para el negocio.

A diferencia de un notebook de modelado puntual, este proyecto aborda el problema desde el diseño de la solución: qué datos existen, cómo se ingieren, dónde se almacenan, cómo se procesan (batch y streaming) y qué modelo predictivo se construye sobre esa arquitectura — cerrando con el impacto de negocio esperado.

## 🎯 Problema

RAJA MARKET (caso de negocio ficticio, 450 tiendas / 35.000 productos) enfrenta quiebres de stock recurrentes, especialmente durante eventos de alto tráfico (por ejemplo, fechas de alta demanda), donde la falta de anticipación genera ventas perdidas y reclamos de clientes. El desafío es diseñar una solución de datos capaz de **anticipar la demanda por producto y tienda**, y a partir de ella, **detectar con antelación qué productos están en riesgo de quedarse sin stock**.

## 🛠️ Metodología

### 1. Caracterización del problema con las 5V del Big Data
Volumen (~2 TB/día simulados, 8M tickets diarios), Velocidad (streaming desde ventas y app, batch desde inventario), Variedad (datos estructurados, semiestructurados y no estructurados), Veracidad (registros incompletos y duplicados estimados) y Valor (anticipar demanda, reducir ventas perdidas) — como marco para justificar por qué el problema requiere un enfoque Big Data y no una hoja de cálculo.

### 2. Diseño de arquitectura
- **Ingesta:** Apache Kafka (eventos en near real-time), APIs (clima), CSV/Parquet (cargas batch), ETL/ELT
- **Almacenamiento:** Data Lake por capas (`raw` / `cleaned` / `curated`) en formato Parquet
- **Procesamiento:** Apache Spark como motor central — DataFrames, Spark SQL, MLlib y Structured Streaming
- **Arquitectura híbrida Batch + Streaming**, justificada explícitamente: Batch para analizar y planificar (históricos, KPIs, entrenamiento de modelos), Streaming para detectar y reaccionar (alertas en tiempo casi real)

### 3. Simulación y pipeline en Spark
Con la arquitectura definida, se construye un dataset simulado y realista (ventas, inventario, navegación en app, logística y clima) directamente en Spark DataFrames:
- Generación de 5 fuentes de datos relacionadas (productos, ventas, inventario, app, logística), incluyendo un evento de alta demanda simulado (3 días con multiplicador de demanda)
- Limpieza: eliminación de duplicados y filtrado de valores inválidos por fuente
- Integración de las 5 fuentes mediante `join` (ventas + productos + inventario + clima)
- Feature engineering con `Window`/`lag()` para calcular la demanda del día anterior por producto y tienda

### 4. Modelo predictivo de demanda
`RandomForestRegressor` de Spark MLlib, encapsulado en un `Pipeline` (`StringIndexer` + `VectorAssembler` + modelo), con las siguientes decisiones deliberadas:
- **División temporal (no aleatoria):** entrenamiento con los días 1–23, prueba con los días 24–30, para simular cómo se usaría el modelo en producción y evitar que el modelo "vea" datos futuros durante el entrenamiento
- El stock del inventario se generó de forma **independiente** de la demanda real del mismo día, evitando fuga de información entre la variable objetivo y los predictores

A partir de la demanda predicha se calcula un **stock proyectado** (stock actual − demanda predicha) y se clasifica cada producto/tienda en riesgo `ALTO` o `BAJO` según si ese stock proyectado cae bajo el stock mínimo, generando una acción recomendada (`REPOSICIONAR/REDISTRIBUIR` o `MANTENER`).

### 5. Visualización y KPIs de negocio
4 gráficos generados con Matplotlib (demanda diaria con eventos destacados, comparación normal vs. evento, ventas por categoría, productos por nivel de riesgo) y definición de 7 KPIs de negocio (tasa de quiebre, precisión del pronóstico, tiempo de detección, ventas perdidas, tiempo de reposición, redistribución oportuna, reclamos), con una tabla de "situación actual simulada" vs. "meta propuesta" para cada uno.

## 📊 Resultados

**Datos simulados:** 100 productos, 30 tiendas, 30 días (3 de ellos de evento de alta demanda) → 40.500 registros de ventas, 90.000 de inventario, 3.000 de navegación en app, 5.000 logísticos.

**Validación de la hipótesis de negocio:** la demanda promedio diaria durante eventos de alto tráfico resultó efectivamente superior a la de un día normal, confirmando el comportamiento esperado antes de entrenar el modelo sobre esos datos.

**Modelo de demanda (Random Forest, validación temporal días 24–30):**

| Métrica | Valor |
|---|---|
| RMSE | 1.7839 |
| MAE | 1.4255 |

**Detección de riesgo de quiebre:** 83% de los productos evaluados presentó al menos un evento de riesgo `ALTO` durante el período simulado, lo que en el diseño de la solución se traduce en una acción concreta de reposición o redistribución.

## 🚀 Cómo ejecutarlo

```bash
# Clonar el repositorio
git clone <https://github.com/haroldrodriguezadm-png/Diagnostico_y_Diseno_de_una_Soluci-n_Big_Data_para_Prevencion_de_Quiebres_de_Stock/blob/main/README_Diagnostico_Diseno_BigData.md>
cd <Diagnostico_y_Diseno_de_una_Soluci-n_Big_Data_para_Prevencion_de_Quiebres_de_Stock>

# Instalar dependencias
pip install pyspark pandas numpy matplotlib

# Ejecutar el notebook
jupyter notebook "Diagnóstico y diseño de una solución inicial con enfoque Big Data.ipynb"
```

Este notebook **no requiere un archivo de datos externo**: genera su propio dataset simulado (`np.random.seed(42)`) directamente en la ejecución, por lo que corre de extremo a extremo sin dependencias de datos adicionales.

**Requisitos:** Python 3.8+, Java 8/11 (requerido por Spark), PySpark 4.x

## 📁 Estructura del repositorio

```
├── Diagnóstico y diseño de una solución inicial con enfoque Big Data.ipynb   # Notebook principal
└── README.md
```

## 🔭 Limitaciones y próximos pasos

- Los datos son enteramente **simulados**; la arquitectura y el modelo están diseñados para datos reales de producción, pero las métricas (RMSE, tasa de riesgo) deben leerse como validación del enfoque, no como desempeño real de negocio.
- La tasa de riesgo de 83% es alta porque el umbral de stock mínimo y la magnitud del evento simulado se definieron de forma conservadora para probar que el sistema de alertas reacciona correctamente; en un caso real conviene calibrar ese umbral con datos históricos.
- Próximos pasos naturales: implementar el componente de Structured Streaming (hoy descrito a nivel de diseño, no de código), conectar el Data Lake con un almacenamiento real (S3/HDFS), y exponer los KPIs en un dashboard (Power BI/Tableau) en lugar de gráficos estáticos.

## 👤 Autor

Harold Rodríguez B. — [LinkedIn](https://www.linkedin.com/in/harold-rodriguez-boisset/) 
