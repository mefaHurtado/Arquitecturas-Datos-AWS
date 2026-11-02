import csv
import hashlib
import json
import sqlite3
from pathlib import Path

LOTE = "20261102T013140Z-c5e61618"
base = (
    Path.home() / "dw-migracion-local"
    / "redshift-carga" / LOTE / "datos"
)

entidades = {
    "mysql_customers": "customer_id",
    "mysql_products": "product_id",
    "mysql_orders": "order_id",
    "mysql_order_items": "order_item_id",
    "mongo_reviews": "review_id",
    "mongo_navigation_events": "event_id"
}

db = sqlite3.connect(":memory:")

db.executescript("""
CREATE TABLE registros (
    entidad TEXT NOT NULL,
    registro_id TEXT NOT NULL,
    lote TEXT NOT NULL,
    PRIMARY KEY (entidad, registro_id)
);

CREATE TABLE lotes (
    lote TEXT PRIMARY KEY,
    huella TEXT NOT NULL
);
""")

def contar():
    return db.execute(
        "SELECT COUNT(*) FROM registros"
    ).fetchone()[0]

def huella(registros):
    contenido = json.dumps(registros, sort_keys=True)
    return hashlib.sha256(
        contenido.encode("utf-8")
    ).hexdigest()

iniciales = []

for entidad, clave in entidades.items():
    with (base / f"{entidad}.csv").open(
        encoding="utf-8", newline=""
    ) as archivo:
        filas = list(csv.DictReader(archivo))

    iniciales.extend(
        (entidad, fila[clave])
        for fila in filas
    )

with db:
    db.executemany(
        "INSERT INTO registros VALUES (?, ?, ?)",
        [(e, identificador, LOTE) for e, identificador in iniciales]
    )
    db.execute(
        "INSERT INTO lotes VALUES (?, ?)",
        (LOTE, huella(iniciales))
    )

print("=== ESTADO INICIAL ===")
print("Registros:", contar())

incremental = [
    ("mysql_customers", "910001"),
    ("mysql_customers", "910002"),
    ("mysql_customers", "910003")
]

LOTE_NUEVO = "lote_recuperacion_prueba"

def procesar(registros, provocar_fallo=False):
    hash_nuevo = huella(registros)

    anterior = db.execute(
        "SELECT huella FROM lotes WHERE lote = ?",
        (LOTE_NUEVO,)
    ).fetchone()

    if anterior:
        if anterior[0] != hash_nuevo:
            raise ValueError("Lote repetido con contenido diferente")
        return "OMITIDO"

    db.execute("BEGIN")

    try:
        for indice, (entidad, identificador) in enumerate(registros):
            db.execute(
                "INSERT INTO registros VALUES (?, ?, ?)",
                (entidad, identificador, LOTE_NUEVO)
            )

            if provocar_fallo and indice == 1:
                raise RuntimeError("FALLO SIMULADO DURANTE LA CARGA")

        db.execute(
            "INSERT INTO lotes VALUES (?, ?)",
            (LOTE_NUEVO, hash_nuevo)
        )
        db.commit()
        return "PROCESADO"

    except Exception:
        db.rollback()
        raise

print("\n=== SIMULACION DE FALLO ===")

try:
    procesar(incremental, provocar_fallo=True)
    raise AssertionError("No se produjo el fallo esperado")
except RuntimeError as error:
    print("Error detectado:", error)

despues_fallo = contar()

lote_incompleto = db.execute(
    "SELECT COUNT(*) FROM lotes WHERE lote = ?",
    (LOTE_NUEVO,)
).fetchone()[0]

print("Registros despues del fallo:", despues_fallo)
print("Lotes incompletos registrados:", lote_incompleto)
print("Rollback ejecutado correctamente")

assert despues_fallo == 1112
assert lote_incompleto == 0

print("\n=== RECUPERACION DE LA CARGA ===")

estado = procesar(incremental)
print("Estado:", estado)
print("Registros despues de recuperar:", contar())

assert contar() == 1115

print("\n=== REPETICION DESPUES DE RECUPERAR ===")

estado = procesar(incremental)
print("Estado:", estado)
print("Total final:", contar())

assert estado == "OMITIDO"
assert contar() == 1115

print("\n=== RESULTADO FINAL ===")
print("Registros iniciales: 1112")
print("Registros despues del fallo:", despues_fallo)
print("Registros despues de recuperar:", contar())
print("Registros duplicados: 0")
print("Rollback: EXITOSO")
print("Reintento: EXITOSO")
print("PRUEBA DE RECUPERACION: EXITOSA")
print("Simulacion local. No se enviaron datos a AWS.")
