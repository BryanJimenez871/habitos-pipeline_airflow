# Pipeline PostgreSQL → BigQuery con Apache Airflow

Pipeline de datos automatizado que extrae registros desde una base de datos PostgreSQL local y los carga en Google BigQuery en formato Parquet, orquestado con Apache Airflow 3.1.7.

Este proyecto es una extensión del proyecto [Hábitos Personales](https://github.com/BryanJimenez871/habitos_personales_fastAPI), y ahora, aparte de almacenarlo de forma local, se almacena en la nube. Además es para aprender a analizar los datos e ir estudiando la información que pueden entregar, junto a observaciones de gráficos


## Flujo del pipeline

```
PostgreSQL local
      │
      │  SELECT * FROM tabla
      ▼
  Archivo .parquet (temporal)
      │
      │  load_table_from_file()
      ▼
Google BigQuery
```

1. **extract_postgres** — consulta la tabla en PostgreSQL y guarda un archivo `.parquet` temporal
2. **load_bigquery** — carga el archivo directamente a BigQuery y lo elimina al finalizar