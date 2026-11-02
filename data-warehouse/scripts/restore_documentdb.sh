#!/usr/bin/env bash
set -euo pipefail

ARCHIVO="${DOCDB_ARCHIVE:-$HOME/dw-migracion-local/mongodb/ecommerce.archive.gz}"
MODO="${1:---check}"

if [ ! -s "$ARCHIVO" ]; then
    echo "ERROR: no se encontro el respaldo MongoDB"
    exit 1
fi

if [ "$MODO" = "--check" ]; then
    echo "=== PREPARACION DOCUMENTDB ==="

    gzip -t "$ARCHIVO"

    echo "Respaldo comprimido: CORRECTO"
    echo "Base de origen: ecommerce"
    echo "Colecciones esperadas: navigation_events, reviews"
    echo "Documentos esperados: 666"
    echo "Restaurar validadores originales: NO"
    echo "Restaurar indices originales: NO"
    echo "Conexion TLS: OBLIGATORIA"
    echo ""
    echo "PLAN DE RESTAURACION: PREPARADO"
    echo "NO SE ENVIARON DATOS A AWS"
    exit 0
fi

if [ "$MODO" != "--execute" ]; then
    echo "Usa --check o --execute"
    exit 1
fi

: "${DOCDB_HOST:?Falta DOCDB_HOST}"
: "${DOCDB_USER:?Falta DOCDB_USER}"
: "${DOCDB_CA_FILE:?Falta DOCDB_CA_FILE}"

if [ ! -f "$DOCDB_CA_FILE" ]; then
    echo "ERROR: falta el certificado TLS"
    exit 1
fi

if ! command -v mongorestore >/dev/null 2>&1; then
    echo "ERROR: instalar MongoDB Database Tools"
    exit 1
fi

VERSION=$(mongorestore --version | head -1 | awk '{print $3}')

python3 - "$VERSION" <<'PY'
import sys
version = tuple(map(int, sys.argv[1].split(".")[:3]))
if version > (100, 11, 0):
    raise SystemExit(
        "ERROR: DocumentDB requiere MongoDB Database Tools "
        "version 100.11.0 o anterior"
    )
PY

echo "=== RESTAURACION REAL DOCUMENTDB ==="
echo "Destino: $DOCDB_HOST"
echo "No se restauraran validadores ni indices originales."

read -r -p "Escribe RESTAURAR para continuar: " CONFIRMACION

if [ "$CONFIRMACION" != "RESTAURAR" ]; then
    echo "Operacion cancelada"
    exit 1
fi

mongorestore \
    --host "$DOCDB_HOST" \
    --port 27017 \
    --username "$DOCDB_USER" \
    --password \
    --authenticationDatabase admin \
    --tls \
    --tlsCAFile "$DOCDB_CA_FILE" \
    --archive="$ARCHIVO" \
    --gzip \
    --nsInclude="ecommerce.*" \
    --noOptionsRestore \
    --noIndexRestore \
    --stopOnError

echo "Restauracion finalizada. Verificar cantidades en destino."
