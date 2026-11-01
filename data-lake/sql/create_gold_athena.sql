CREATE EXTERNAL TABLE IF NOT EXISTS
eia_datalake_stefany.news_by_day (
    medio STRING,
    seccion STRING,
    total_noticias BIGINT
)
PARTITIONED BY (
    publication_date STRING
)
STORED AS PARQUET
LOCATION 's3://eia-datalake-stefany-38c322fdd8/gold/news_by_day/snapshot_id=c6aef2e790702a3c36b1/';
