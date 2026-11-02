import csv
import hashlib
import io
import sqlite3
from pathlib import Path

LOTE = "20261102T013140Z-c5e61618"

carpeta = (
    Path.home()
    / "dw-migracion-local/redshift-carga"
    / LOTE
    / "datos"
)

entidades = {
    "mysql_customers": "customer_id",
    "mysql_products": "product_id",
    "mysql_orders": "order_id",
    "mysql_order_items": "order_item_id",
    "mongo_reviews": "review_id",
    "mongo_navigation_events": "event_id",
}

conexion = sqlite3.connect(":memory:")

conexion.executescript("""
CREATE TABLE lotes (
    entidad TEXT NOT NULL,
    lote TEXT NOT NULL,
    hash_archivo TEXT NOT NULL,
    cantidad INTEGER NOT NULL,
    PRIMARY KEY (entidad, lote)
);

CREATE TABLE registros (
    entidad TEXT NOT NULL,
    lote TEXT NOT NULL,
    registro_id TEXT NOT NULL,
    PRIMARY KEY (entidad, lote, registro_id)
);
""")

def cargar(entidad, contenido):
    clave = entidades[entidad]
    huella = hashlib.sha256(contenido).hexdigest()

    lector = csv.DictReader(
        io.StringIO(contenido.decode("utf-8-sig"))
    )
    filas = list(lector)

    if clave not in (lector.fieldnames or []):
        raise ValueError(f"Falta columna {clave}")

    identificadores = [
        str(fila.get(clave) or "").strip()
        for fila in filas
    ]

    if not all(identificadores):
        raise ValueError("Hay identificadores vacios")

    if len(set(identificadores)) != len(identificadores):
        raise ValueError("Hay identificadores duplicados")

    with conexion:
        anterior = conexion.execute(
            """SELECT hash_archivo, cantidad
               FROM lotes
               WHERE entidad = ? AND lote = ?""",
            (entidad, LOTE)
        ).fetchone()

        if anterior:
            if anterior != (huella, len(filas)):
                raise ValueError(
                    "LOTE REPETIDO CON CONTENIDO DIFERENTE"
                )
            return "OMITIDO", len(filas)

        conexion.executemany(
            """INSERT INTO registros
               (entidad, lote, registro_id)
               VALUES (?, ?, ?)""",
            [
                (entidad, LOTE, identificador)
                for identificador in identificadores
            ]
        )

        conexion.execute(
            """INSERT INTO lotes
               (entidad, lote, hash_archivo, cantidad)
               VALUES (?, ?, ?, ?)""",
            (entidad, LOTE, huella, len(filas))
        )

    return "CARGADO", len(filas)


archivos = {}

for entidad in entidades:
    archivos[entidad] = (
        carpeta / f"{entidad}.csv"
    ).read_bytes()


def contar():
    return conexion.execute(
        "SELECT COUNT(*) FROM registros"
    ).fetchone()[0]


print("=== PRIMERA CARGA ===")

for entidad, contenido in archivos.items():
    estado, cantidad = cargar(entidad, contenido)
    print(entidad, "|", estado, "|", cantidad)

primera = contar()
print("TOTAL:", primera)


print("\n=== SEGUNDA CARGA DEL MISMO LOTE ===")

for entidad, contenido in archivos.items():
    estado, cantidad = cargar(entidad, contenido)
    print(entidad, "|", estado, "|", cantidad)

segunda = contar()
print("TOTAL:", segunda)


print("\n=== PRUEBA DE ARCHIVO MODIFICADO ===")

rechazo_correcto = False

try:
    cargar(
        "mysql_customers",
        archivos["mysql_customers"] + b"\n"
    )
except ValueError as error:
    print("MODIFICACION DETECTADA:", error)
    rechazo_correcto = True


print("\n=== RESULTADO FINAL ===")

print("Registros primera carga:", primera)
print("Registros segunda carga:", segunda)
print("Duplicados agregados:", segunda - primera)
print("Archivo modificado rechazado:", rechazo_correcto)

if primera == segunda == 1112 and rechazo_correcto:
    print("PRUEBA DE IDEMPOTENCIA: EXITOSA")
else:
    raise SystemExit("PRUEBA DE IDEMPOTENCIA: FALLIDA")

print("Simulacion local. No se enviaron datos a AWS.")
