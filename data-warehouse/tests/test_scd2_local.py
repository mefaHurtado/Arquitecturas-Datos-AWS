import csv
import sqlite3
import hashlib
from pathlib import Path
from datetime import datetime, timedelta
from decimal import Decimal

LOTE = "20261102T013140Z-c5e61618"
base = Path.home() / "dw-migracion-local/redshift-preparado" / LOTE

clientes_csv = base / "mysql_customers.csv"
productos_csv = base / "mysql_products.csv"

archivos = [clientes_csv, productos_csv]
hashes = {
    str(p): hashlib.sha256(p.read_bytes()).hexdigest()
    for p in archivos
}

def leer(ruta):
    with ruta.open(encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))

clientes = leer(clientes_csv)
productos = leer(productos_csv)

cliente = next(
    r for r in clientes
    if r.get("customer_id")
    and r.get("name")
    and "@" in (r.get("email") or "")
    and r.get("updated_at")
)

producto = next(
    r for r in productos
    if r.get("product_id")
    and r.get("category")
    and r.get("price")
    and Decimal(r["price"]) >= 0
    and r.get("updated_at")
)

def fecha(texto):
    return datetime.fromisoformat(
        texto.strip().replace("Z", "+00:00")
    ).strftime("%Y-%m-%d %H:%M:%S")

def siguiente_fecha(texto):
    origen = datetime.fromisoformat(
        texto.strip().replace("Z", "+00:00")
    )
    return (origen + timedelta(days=1)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )

db = sqlite3.connect(":memory:")

db.execute("""
CREATE TABLE historial (
    entidad TEXT NOT NULL,
    registro_id TEXT NOT NULL,
    valor TEXT NOT NULL,
    dbt_valid_from TEXT NOT NULL,
    dbt_valid_to TEXT,
    PRIMARY KEY (entidad, registro_id, dbt_valid_from)
)
""")

def snapshot(entidad, registro_id, valor, actualizado):
    actual = db.execute("""
        SELECT dbt_valid_from
        FROM historial
        WHERE entidad = ?
          AND registro_id = ?
          AND dbt_valid_to IS NULL
    """, (entidad, registro_id)).fetchone()

    if actual is None:
        db.execute("""
            INSERT INTO historial VALUES (?, ?, ?, ?, NULL)
        """, (entidad, registro_id, valor, actualizado))
        return "VERSION INICIAL"

    if actualizado > actual[0]:
        db.execute("""
            UPDATE historial
            SET dbt_valid_to = ?
            WHERE entidad = ?
              AND registro_id = ?
              AND dbt_valid_to IS NULL
        """, (actualizado, entidad, registro_id))

        db.execute("""
            INSERT INTO historial VALUES (?, ?, ?, ?, NULL)
        """, (entidad, registro_id, valor, actualizado))

        return "NUEVA VERSION"

    return "SIN CAMBIOS"

id_cliente = cliente["customer_id"]
id_producto = producto["product_id"]

ciudad_original = cliente.get("city") or ""
precio_original = Decimal(producto["price"])

fecha_cliente = fecha(cliente["updated_at"])
fecha_producto = fecha(producto["updated_at"])

print("=== SNAPSHOT INICIAL ===")

print("Cliente:", snapshot(
    "cliente", id_cliente, ciudad_original, fecha_cliente
))

print("Producto:", snapshot(
    "producto", id_producto,
    str(precio_original), fecha_producto
))

ciudad_nueva = "Ciudad de prueba SCD2"
if ciudad_original == ciudad_nueva:
    ciudad_nueva = "Otra ciudad de prueba SCD2"

precio_nuevo = precio_original + Decimal("1000.00")

fecha_cliente_nueva = siguiente_fecha(cliente["updated_at"])
fecha_producto_nueva = siguiente_fecha(producto["updated_at"])

print("\n=== SNAPSHOT DESPUES DE CAMBIOS ===")

print("Cliente:", snapshot(
    "cliente", id_cliente,
    ciudad_nueva, fecha_cliente_nueva
))

print("Producto:", snapshot(
    "producto", id_producto,
    str(precio_nuevo), fecha_producto_nueva
))

print("\n=== REPETIR SNAPSHOT SIN CAMBIOS ===")

print("Cliente:", snapshot(
    "cliente", id_cliente,
    ciudad_nueva, fecha_cliente_nueva
))

print("Producto:", snapshot(
    "producto", id_producto,
    str(precio_nuevo), fecha_producto_nueva
))

print("\n=== HISTORIAL SCD TIPO 2 ===")

for entidad, registro_id in [
    ("cliente", id_cliente),
    ("producto", id_producto)
]:
    filas = db.execute("""
        SELECT valor, dbt_valid_from, dbt_valid_to
        FROM historial
        WHERE entidad = ? AND registro_id = ?
        ORDER BY dbt_valid_from
    """, (entidad, registro_id)).fetchall()

    print(f"\n{entidad.upper()} ID: {registro_id}")

    for valor, desde, hasta in filas:
        estado = "ACTUAL" if hasta is None else "HISTORICO"
        print(
            f"{estado} | Valor: {valor} | "
            f"Desde: {desde} | "
            f"Hasta: {hasta or 'VIGENTE'}"
        )

    assert len(filas) == 2
    assert filas[0][2] == filas[1][1]
    assert filas[1][2] is None

total = db.execute(
    "SELECT COUNT(*) FROM historial"
).fetchone()[0]

actuales = db.execute("""
    SELECT COUNT(*) FROM historial
    WHERE dbt_valid_to IS NULL
""").fetchone()[0]

historicos = total - actuales

fuentes_intactas = all(
    hashlib.sha256(p.read_bytes()).hexdigest()
    == hashes[str(p)]
    for p in archivos
)

assert (total, actuales, historicos) == (4, 2, 2)
assert fuentes_intactas

print("\n=== RESULTADO FINAL ===")
print("Versiones historicas:", historicos)
print("Versiones actuales:", actuales)
print("Total versiones:", total)
print("Versiones duplicadas al repetir:", 0)
print("Archivos originales intactos:", fuentes_intactas)
print("PRUEBA SCD TIPO 2: EXITOSA")
print("Simulacion local. No se enviaron datos a AWS.")
