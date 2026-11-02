#!/usr/bin/env bash
set -euo pipefail

MODO="${1:---check}"
RESPALDO="${RDS_MYSQL_DUMP:-$HOME/dw-migracion-local/mysql/ecommerce.sql}"
BASE_DATOS="ecommerce"

echo "=== PREPARACION RESTAURACION MYSQL RDS ==="

if [ ! -s "$RESPALDO" ]; then
    echo "ERROR: No se encontro el respaldo MySQL"
    exit 1
fi

python3 - "$RESPALDO" <<'PY'
from pathlib import Path
import re
import sys

archivo = Path(sys.argv[1])
sql = archivo.read_text(encoding="utf-8")

tablas = re.findall(
    r"(?i)\bCREATE\s+TABLE\s+`([^`]+)`",
    sql
)

esperadas = {
    "customers",
    "products",
    "orders",
    "order_items"
}

if set(tablas) != esperadas or len(tablas) != 4:
    raise SystemExit("ERROR: tablas inesperadas")

peligrosos = [
    r"(?im)^\s*DROP\s+DATABASE\b",
    r"(?im)^\s*CREATE\s+DATABASE\b",
    r"(?im)^\s*USE\s+",
    r"(?i)\bGTID_PURGED\b",
    r"(?i)\bDEFINER\s*=",
    r"(?im)^\s*CREATE\s+USER\b",
    r"(?im)^\s*GRANT\s+"
]

if any(re.search(p, sql) for p in peligrosos):
    raise SystemExit(
        "REVISAR: el respaldo contiene "
        "instrucciones que requieren adaptacion"
    )

print("Respaldo MySQL: CORRECTO")
print("Tablas encontradas:", len(tablas))
print("Tablas:", ", ".join(tablas))
print("Registros esperados: 446")
PY

if [ "$MODO" = "--check" ]; then
    echo ""
    echo "Destino previsto: Amazon RDS MySQL"
    echo "Version objetivo: 8.4.11"
    echo "Base de datos: ecommerce"
    echo "Conexion TLS: VERIFY_IDENTITY"
    echo "Validacion previa de base vacia: SI"
    echo ""
    echo "PLAN DE RESTAURACION: PREPARADO"
    echo "NO SE ENVIARON DATOS A AWS"
    exit 0
fi

if [ "$MODO" != "--execute" ]; then
    echo "Usa --check o --execute"
    exit 1
fi

: "${RDS_MYSQL_HOST:?Falta RDS_MYSQL_HOST}"
: "${RDS_MYSQL_USER:?Falta RDS_MYSQL_USER}"
: "${RDS_CA_FILE:?Falta RDS_CA_FILE}"

if [ ! -f "$RDS_CA_FILE" ]; then
    echo "ERROR: no se encontro el certificado TLS"
    exit 1
fi

if ! command -v mysql >/dev/null 2>&1; then
    echo "ERROR: falta instalar el cliente MySQL"
    exit 1
fi

read -rsp "Contrasena de RDS MySQL: " CLAVE
echo ""

mysql_rds() {
    MYSQL_PWD="$CLAVE" mysql \
        --host="$RDS_MYSQL_HOST" \
        --port=3306 \
        --user="$RDS_MYSQL_USER" \
        --ssl-mode=VERIFY_IDENTITY \
        --ssl-ca="$RDS_CA_FILE" \
        --batch \
        --skip-column-names \
        "$@"
}

EXISTE=$(mysql_rds -e "
    SELECT COUNT(*)
    FROM information_schema.schemata
    WHERE schema_name = 'ecommerce';
")

if [ "$EXISTE" != "0" ]; then
    echo "La base ecommerce ya existe en RDS."
    echo "No se sobrescribieron datos."
    exit 1
fi

echo ""
echo "La restauracion creara una base nueva en RDS."
read -rp "Escribe RESTAURAR para continuar: " CONFIRMACION

if [ "$CONFIRMACION" != "RESTAURAR" ]; then
    echo "Operacion cancelada"
    exit 1
fi

mysql_rds -e "
    CREATE DATABASE ecommerce
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_0900_ai_ci;
"

mysql_rds --database="$BASE_DATOS" < "$RESPALDO"

echo "=== REGISTROS RESTAURADOS ==="

mysql_rds -e "
    SELECT 'customers', COUNT(*) FROM ecommerce.customers
    UNION ALL
    SELECT 'products', COUNT(*) FROM ecommerce.products
    UNION ALL
    SELECT 'orders', COUNT(*) FROM ecommerce.orders
    UNION ALL
    SELECT 'order_items', COUNT(*) FROM ecommerce.order_items;
"

RESULTADO=$(mysql_rds -e "
    SELECT
        (SELECT COUNT(*) FROM ecommerce.customers) = 54
        AND (SELECT COUNT(*) FROM ecommerce.products) = 27
        AND (SELECT COUNT(*) FROM ecommerce.orders) = 106
        AND (SELECT COUNT(*) FROM ecommerce.order_items) = 259;
")

if [ "$RESULTADO" != "1" ]; then
    echo "ERROR: cantidades inesperadas en RDS"
    exit 1
fi

echo "RESTAURACION MYSQL RDS: EXITOSA"
echo "Total de registros verificados: 446"

unset CLAVE
