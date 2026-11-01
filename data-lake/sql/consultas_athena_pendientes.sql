-- PREPARACIÓN: NO EJECUTAR hasta publicar Parquet en S3, crear tablas y
-- actualizar particiones en AWS Glue. Este archivo no genera cargos por sí mismo.
-- Base esperada: eia_datalake_stefany.
-- Las tablas entities_ranking, entity_trend_by_day y phrases_by_section
-- NO EXISTEN en Glue hasta su despliegue autorizado.

-- 1. Noticias por día, medio y sección: ya puede usar la tabla existente.
SELECT publication_date, medio, seccion, total_noticias
FROM eia_datalake_stefany.news_by_day
ORDER BY publication_date, seccion;

-- 2. Organizaciones que aparecen en más noticias.
SELECT entity_text AS organizacion, total_noticias
FROM eia_datalake_stefany.entities_ranking
WHERE entity_type = 'ORGANIZATION'
ORDER BY total_noticias DESC, organizacion
LIMIT 10;

-- 3. Cambio de presencia de entidad entre fechas.
-- Sustituir el valor del texto por una entidad existente en los datos.
SELECT publication_date, entity_type, entity_text, total_noticias
FROM eia_datalake_stefany.entity_trend_by_day
WHERE lower(entity_text) = lower('ENTIDAD_A_CONSULTAR')
ORDER BY publication_date;

-- 4. Frases clave por sección.
SELECT seccion, phrase_text, total_noticias
FROM eia_datalake_stefany.phrases_by_section
ORDER BY total_noticias DESC, seccion
LIMIT 30;

-- 5. Conciliación del Gold de noticias con la tabla Silver.
-- Cuenta una noticia en cada sección a la que pertenece (sin duplicados de la misma fuente).
WITH silver_por_seccion AS (
    SELECT publication_date, 'Colombia' AS seccion, count(DISTINCT news_id) AS noticias
    FROM eia_datalake_stefany.news
    WHERE contains(split(fuentes, ','), 'eltiempo_colombia')
    GROUP BY publication_date
    UNION ALL
    SELECT publication_date, 'Economía' AS seccion, count(DISTINCT news_id) AS noticias
    FROM eia_datalake_stefany.news
    WHERE contains(split(fuentes, ','), 'eltiempo_economia')
    GROUP BY publication_date
    UNION ALL
    SELECT publication_date, 'Tecnología' AS seccion, count(DISTINCT news_id) AS noticias
    FROM eia_datalake_stefany.news
    WHERE contains(split(fuentes, ','), 'eltiempo_tecnologia')
    GROUP BY publication_date
)
SELECT g.publication_date, g.seccion, g.total_noticias,
       s.noticias AS silver_noticias,
       g.total_noticias - coalesce(s.noticias, 0) AS diferencia
FROM eia_datalake_stefany.news_by_day AS g
LEFT JOIN silver_por_seccion AS s
ON g.publication_date = s.publication_date AND g.seccion = s.seccion
ORDER BY g.publication_date, g.seccion;

-- 6. Evidencia de partition pruning: revisar los bytes realmente escaneados
-- en el historial de ejecución Athena; no inferir el ahorro solo del SQL.
SELECT * FROM eia_datalake_stefany.news_by_day;
SELECT * FROM eia_datalake_stefany.news_by_day
WHERE publication_date = '2026-11-01';
