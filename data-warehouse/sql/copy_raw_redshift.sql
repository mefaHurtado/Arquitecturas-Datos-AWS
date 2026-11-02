-- Carga inicial Data Warehouse EIA
-- Requiere rol IAM predeterminado en Redshift

COPY raw.mysql_customers
FROM 's3://stefany-redshift-taller-2026/data-warehouse/redshift-manifests/batch_id=20261102T013140Z-c5e61618/mysql_customers.manifest'
IAM_ROLE default
MANIFEST
FORMAT AS CSV
IGNOREHEADER 1
EMPTYASNULL
BLANKSASNULL
DATEFORMAT 'auto'
TIMEFORMAT 'auto';


COPY raw.mysql_products
FROM 's3://stefany-redshift-taller-2026/data-warehouse/redshift-manifests/batch_id=20261102T013140Z-c5e61618/mysql_products.manifest'
IAM_ROLE default
MANIFEST
FORMAT AS CSV
IGNOREHEADER 1
EMPTYASNULL
BLANKSASNULL
DATEFORMAT 'auto'
TIMEFORMAT 'auto';


COPY raw.mysql_orders
FROM 's3://stefany-redshift-taller-2026/data-warehouse/redshift-manifests/batch_id=20261102T013140Z-c5e61618/mysql_orders.manifest'
IAM_ROLE default
MANIFEST
FORMAT AS CSV
IGNOREHEADER 1
EMPTYASNULL
BLANKSASNULL
DATEFORMAT 'auto'
TIMEFORMAT 'auto';


COPY raw.mysql_order_items
FROM 's3://stefany-redshift-taller-2026/data-warehouse/redshift-manifests/batch_id=20261102T013140Z-c5e61618/mysql_order_items.manifest'
IAM_ROLE default
MANIFEST
FORMAT AS CSV
IGNOREHEADER 1
EMPTYASNULL
BLANKSASNULL
DATEFORMAT 'auto'
TIMEFORMAT 'auto';


COPY raw.mongo_reviews
FROM 's3://stefany-redshift-taller-2026/data-warehouse/redshift-manifests/batch_id=20261102T013140Z-c5e61618/mongo_reviews.manifest'
IAM_ROLE default
MANIFEST
FORMAT AS CSV
IGNOREHEADER 1
EMPTYASNULL
BLANKSASNULL
DATEFORMAT 'auto'
TIMEFORMAT 'auto';


COPY raw.mongo_navigation_events
FROM 's3://stefany-redshift-taller-2026/data-warehouse/redshift-manifests/batch_id=20261102T013140Z-c5e61618/mongo_navigation_events.manifest'
IAM_ROLE default
MANIFEST
FORMAT AS CSV
IGNOREHEADER 1
EMPTYASNULL
BLANKSASNULL
DATEFORMAT 'auto'
TIMEFORMAT 'auto';
