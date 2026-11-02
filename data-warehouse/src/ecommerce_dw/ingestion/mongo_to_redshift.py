import csv
import os
from pathlib import Path
from datetime import datetime, timezone
from bson import json_util

os.umask(0o077)

LOTE = "20261102T013140Z-c5e61618"

base = Path.home() / "dw-migracion-local"
entrada = base / "extracciones-reales/mongodb"
salida = base / "redshift-preparado" / LOTE
salida.mkdir(parents=True, exist_ok=True)

def obtener(documento, campo):
    valor = documento.get(campo)
    return "" if valor is None else str(valor)

def identificador(documento, campo):
    valor = documento.get(campo + "_id")

    if valor is None:
        anidado = documento.get(campo, {})
        if isinstance(anidado, dict):
            valor = anidado.get(
                campo + "_id", anidado.get("id")
            )

    return "" if valor is None else str(valor)

def fecha_texto(valor):
    if hasattr(valor, "isoformat"):
        return valor.isoformat()
    return "" if valor is None else str(valor)

colecciones = {
    "reviews": [
        "review_id", "customer_id", "product_id",
        "rating", "reviewed_at", "document_json",
        "_loaded_at", "_source", "_batch_id"
    ],
    "navigation_events": [
        "event_id", "event_type", "occurred_at",
        "customer_id", "product_id", "sales_channel",
        "document_json", "_loaded_at", "_source",
        "_batch_id"
    ]
}

total = 0

print("=== PREPARACION MONGODB PARA REDSHIFT ===")

for nombre, columnas in colecciones.items():
    origen = entrada / f"mongodb_{nombre}.jsonl"
    destino = salida / f"mongo_{nombre}.csv"
    filas = []

    with origen.open(encoding="utf-8") as archivo:
        for linea in archivo:
            if not linea.strip():
                continue

            documento = json_util.loads(linea)
            ahora = datetime.now(timezone.utc).isoformat()

            fila = {
                "customer_id": identificador(documento, "customer"),
                "product_id": identificador(documento, "product"),
                "document_json": json_util.dumps(
                    documento,
                    json_options=json_util.RELAXED_JSON_OPTIONS,
                    ensure_ascii=False
                ),
                "_loaded_at": ahora,
                "_source": "mongodb",
                "_batch_id": LOTE
            }

            if nombre == "reviews":
                fila.update({
                    "review_id": obtener(documento, "review_id"),
                    "rating": obtener(documento, "rating"),
                    "reviewed_at": fecha_texto(
                        documento.get("reviewed_at")
                    )
                })
            else:
                sesion = documento.get("session") or {}
                canal = documento.get("sales_channel")
                if canal is None:
                    dispositivo = documento.get("device")
                    if isinstance(dispositivo, dict):
                        canal = dispositivo.get("sales_channel")
                if canal is None and isinstance(sesion, dict):
                    canal = sesion.get("sales_channel")

                fila.update({
                    "event_id": obtener(documento, "event_id"),
                    "event_type": obtener(documento, "event_type"),
                    "occurred_at": fecha_texto(
                        documento.get("occurred_at")
                    ),
                    "sales_channel": "" if canal is None else str(canal)
                })

            filas.append(fila)

    with destino.open("w", newline="", encoding="utf-8") as f:
        escritor = csv.DictWriter(f, fieldnames=columnas)
        escritor.writeheader()
        escritor.writerows(filas)

    destino.chmod(0o600)

    obligatorios = ["review_id"] if nombre == "reviews" else ["event_id"]
    incompletos = sum(
        any(not fila[campo] for campo in obligatorios)
        for fila in filas
    )

    print(f"{nombre}: {len(filas)} registros")
    print(f"Identificadores obligatorios vacios: {incompletos}")
    total += len(filas)

    esperado = 158 if nombre == "reviews" else 508
    if len(filas) != esperado or incompletos:
        raise SystemExit("Se requiere revisar esta coleccion")

print("============================")
print("TOTAL:", total)
print("Archivos preparados:", len(colecciones))
print("No se enviaron datos a AWS")
