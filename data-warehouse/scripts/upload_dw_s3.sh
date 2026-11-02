#!/usr/bin/env bash
set -euo pipefail

BUCKET="stefany-redshift-taller-2026"
LOTE="20261102T013140Z-c5e61618"
BASE="$HOME/dw-migracion-local/s3-preparado"
MODO="${1:---dry-run}"

if [[ "$MODO" != "--dry-run" && "$MODO" != "--execute" ]]; then
    echo "Modo invalido"
    exit 1
fi

mapfile -d '' ARCHIVOS < <(
    find "$BASE/raw" -type f \
      -path "*/batch_id=$LOTE/*" -print0 | sort -z
)

if [[ ${#ARCHIVOS[@]} -ne 12 ]]; then
    echo "ERROR: se esperaban 12 archivos"
    exit 1
fi

echo "=== CARGA DATA WAREHOUSE S3 ==="
echo "Bucket: $BUCKET"
echo "Archivos: ${#ARCHIVOS[@]}"
echo "Modo: $MODO"

if [[ "$MODO" == "--execute" ]]; then
    echo "Esta operacion puede generar cargos AWS."
    read -r -p "Escribe SUBIR para autorizar: " RESPUESTA
    if [[ "$RESPUESTA" != "SUBIR" ]]; then
        echo "Carga cancelada"
        exit 0
    fi
fi

for archivo in "${ARCHIVOS[@]}"; do
    RELATIVO="${archivo#"$BASE"/}"
    CLAVE="data-warehouse/$RELATIVO"

    case "$archivo" in
        *.csv) TIPO="text/csv" ;;
        *.jsonl) TIPO="application/x-ndjson" ;;
        *.json) TIPO="application/json" ;;
        *) echo "Formato no admitido"; exit 1 ;;
    esac

    echo "$CLAVE"

    if [[ "$MODO" == "--execute" ]]; then
        aws s3api put-object \
          --bucket "$BUCKET" \
          --key "$CLAVE" \
          --body "$archivo" \
          --content-type "$TIPO" \
          --server-side-encryption AES256 \
          --if-none-match "*" \
          --region us-east-1 \
          --output json
    fi
done

echo "=== PROCESO COMPLETADO ==="
