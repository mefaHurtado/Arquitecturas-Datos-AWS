# Estado final del Data Lake

## Arquitectura
Proyecto de noticias RSS de El Tiempo con capas
Bronze, Silver y Gold.

## Componentes desarrollados

- Extracción de tres fuentes RSS.
- Almacenamiento original Bronze en Amazon S3.
- Limpieza, validación y deduplicación Silver.
- Detección de noticias nuevas, repetidas y actualizadas.
- Archivos Parquet particionados.
- Indicadores Gold por fecha y sección.
- Tablas y consultas de prueba en Glue y Athena.
- Automatización local con Apache Airflow.
- Recuperación ante fallos e idempotencia.
- Pruebas automatizadas de calidad.
- Demostración de entidades y frases clave.

## Resultados verificados

- 60 noticias vigentes en Silver.
- 17 filas agregadas en Gold.
- 60 conteos en Gold.
- 4 pruebas automatizadas exitosas.
- Demostración NLP: 1 noticia, 2 entidades y 5 frases.

## Pendientes del enunciado original

- Enriquecer las 60 noticias reales con Amazon Comprehend.
- Completar indicadores por fecha y sección usando
  enriquecimiento real.
- Publicar y consultar las tablas enriquecidas en AWS.
- Ejecutar Apache Airflow en una instancia Amazon EC2.
- Completar la validación final en infraestructura AWS.

## Restricción de costos

Se desarrolló una alternativa local con Docker y
Ubuntu WSL para evitar nuevos cargos en AWS.

La implementación local no sustituye los servicios
AWS exigidos por el enunciado. Se requiere autorización
del profesor o aceptación de la alternativa local.

## Evidencias

Las capturas se organizan en evidencias/data-lake.

## Seguridad

No se deben publicar credenciales AWS, archivos .env,
claves privadas ni contraseñas en GitHub.
