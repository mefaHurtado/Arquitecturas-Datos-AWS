#!/usr/bin/env bash
set -euo pipefail

DUMP="$HOME/dw-migracion-local/mysql/ecommerce.sql"
TEMP_DB="dw_restore_check_20261102"

# Verificar que el respaldo no cambie de base de datos.
python3 - "$DUMP" <<'PY'
from pathlib import Path
import re
import sys

archivo = Path(sys.argv[1])
texto = archivo.read_text(encoding="utf-8")

if re.search(
    r"(?im)^\s*(?:USE\s+|(?:CREATE|DROP|ALTER)\s+DATABASE\b)",
    texto
):
    raise SystemExit(
        "REVISAR: el respaldo contiene instrucciones "
        "para cambiar o modificar bases de datos."
    )

tablas = re.findall(r"CREATE TABLE `([^`]+)`", texto)

if sorted(tablas) != sorted([
    "customers", "products", "orders", "order_items"
]):
    raise SystemExit("REVISAR: tablas inesperadas")

print("RESPALDO MYSQL: VALIDADO")
PY

mysql_local() {
    docker exec de-dw-mysql-1 sh -c '
        MYSQL_PWD="${MYSQL_ROOT_PASSWORD:?}" \
        exec mysql -uroot -N -B "$@"
    ' _ "$@"
}

EXISTE=$(mysql_local -e "
    SELECT COUNT(*)
    FROM information_schema.SCHEMATA
    WHERE SCHEMA_NAME = '$TEMP_DB';
")

if [ "$EXISTE" != "0" ]; then
    echo "La base temporal ya existe. No se sobrescribieron datos."
    exit 1
fi

echo "=== CREANDO BASE TEMPORAL ==="

mysql_local -e "CREATE DATABASE $TEMP_DB;"

echo "=== RESTAURANDO RESPALDO MYSQL ==="

docker exec -i de-dw-mysql-1 sh -c '
    MYSQL_PWD="${MYSQL_ROOT_PASSWORD:?}" \
    exec mysql -uroot --database=dw_restore_check_20261102
' < "$DUMP"

echo "=== REGISTROS RESTAURADOS ==="

mysql_local -e "
    SELECT 'customers', COUNT(*) FROM $TEMP_DB.customers
    UNION ALL
    SELECT 'products', COUNT(*) FROM $TEMP_DB.products
    UNION ALL
    SELECT 'orders', COUNT(*) FROM $TEMP_DB.orders
    UNION ALL
    SELECT 'order_items', COUNT(*) FROM $TEMP_DB.order_items;
"

RESULTADO=$(mysql_local -e "
    SELECT CASE WHEN
        (SELECT COUNT(*) FROM $TEMP_DB.customers) = 54
        AND (SELECT COUNT(*) FROM $TEMP_DB.products) = 27
        AND (SELECT COUNT(*) FROM $TEMP_DB.orders) = 106
        AND (SELECT COUNT(*) FROM $TEMP_DB.order_items) = 259
    THEN 'EXITOSA'
    ELSE 'REVISAR'
    END;
")

echo ""
echo "=== RESULTADO FINAL ==="

if [ "$RESULTADO" = "EXITOSA" ]; then
    echo "Tablas restauradas: 4"
    echo "Registros restaurados: 446"
    echo "Base original ecommerce: SIN MODIFICAR"
    echo "RESTAURACION MYSQL: EXITOSA"
    echo "No se enviaron datos a AWS."
else
    echo "RESTAURACION MYSQL: REVISAR"
    exit 1
fi
