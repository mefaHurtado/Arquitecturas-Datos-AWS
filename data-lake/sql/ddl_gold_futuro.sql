-- PLANTILLAS para publicación futura (NO ejecutadas ni verificadas en AWS).
-- Reemplazar TU_BUCKET y SNAPSHOT_ID por recursos reales,
-- y utilizar el snapshot correspondiente a cada conjunto de datos.
-- IMPORTANTE: cada LOCATION apunta a UNA tabla, nunca a un ancestro común.

CREATE EXTERNAL TABLE IF NOT EXISTS eia_datalake_stefany.entities_ranking (
  entity_type string,
  entity_text string,
  total_noticias bigint
)
STORED AS PARQUET
LOCATION 's3://TU_BUCKET/gold/entities_ranking/snapshot_id=SNAPSHOT_ID/';

CREATE EXTERNAL TABLE IF NOT EXISTS eia_datalake_stefany.entity_trend_by_day (
  entity_type string,
  entity_text string,
  total_noticias bigint
)
PARTITIONED BY (publication_date string)
STORED AS PARQUET
LOCATION 's3://TU_BUCKET/gold/entity_trend_by_day/snapshot_id=SNAPSHOT_ID/';

CREATE EXTERNAL TABLE IF NOT EXISTS eia_datalake_stefany.phrases_by_section (
  medio string,
  seccion string,
  phrase_text string,
  total_noticias bigint
)
STORED AS PARQUET
LOCATION 's3://TU_BUCKET/gold/phrases_by_section/snapshot_id=SNAPSHOT_ID/';

-- Si se publica una nueva fecha bajo entity_trend_by_day:
-- MSCK REPAIR TABLE eia_datalake_stefany.entity_trend_by_day;
-- MSCK agrega particiones; NO elimina particiones históricas obsoletas.
-- Para cambiar el snapshot, actualizar LOCATION/metadatos de forma controlada.
