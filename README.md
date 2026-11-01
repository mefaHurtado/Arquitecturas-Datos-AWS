# Arquitecturas de Datos AWS - Universidad EIA

Proyecto académico de Ingeniería de Datos, periodo 2026-2.

## Data Lake

Pipeline de noticias RSS de El Tiempo con arquitectura
Bronze, Silver y Gold.

Tecnologías:
- Python y Parquet
- Amazon S3
- AWS Glue y Athena
- Apache Airflow con Docker
- Pruebas automatizadas

La implementación local cuenta con procesamiento,
validaciones, consultas y pruebas de recuperación.

El enriquecimiento real de las 60 noticias mediante
Amazon Comprehend y el despliegue en EC2 siguen pendientes
de autorización para utilizar servicios AWS.

Ver documentacion/ESTADO_FINAL_DATA_LAKE.md.

## Data Warehouse

Pendiente de desarrollo.

## Seguridad

No se incluyen credenciales, archivos .env,
claves privadas ni datos temporales.

## Entorno local

Ubuntu WSL y Docker Desktop.
