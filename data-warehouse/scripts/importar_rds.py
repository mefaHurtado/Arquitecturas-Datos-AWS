import json
import os
import subprocess
from pathlib import Path

REGION = "us-east-1"
CARPETA = Path.home() / "dw-restauracion"
RESPALDO = CARPETA / "ecommerce.sql"
CERTIFICADO = CARPETA / "global-bundle.pem"

def aws(*argumentos):
    resultado = subprocess.run(
        ["aws", *argumentos, "--region", REGION, "--output", "json"],
        capture_output=True, text=True, check=True
    )
    return json.loads(resultado.stdout)

print("=== RESTAURACION MYSQL EN AWS RDS ===", flush=True)

if not RESPALDO.is_file() or not CERTIFICADO.is_file():
    raise SystemExit("ERROR: faltan el respaldo o el certificado en EC2")

rds = aws(
    "rds", "describe-db-instances",
    "--db-instance-identifier", "stefany-dw-mysql"
)["DBInstances"][0]

if rds["DBInstanceStatus"] != "available":
    raise SystemExit("ERROR: RDS no esta disponible")

secreto = aws(
    "secretsmanager", "get-secret-value",
    "--secret-id", rds["MasterUserSecret"]["SecretArn"]
)

credenciales = json.loads(secreto["SecretString"])

entorno = os.environ.copy()
entorno["MYSQL_PWD"] = credenciales["password"]

mysql = [
    "mysql",
    "--host=" + rds["Endpoint"]["Address"],
    "--port=3306",
    "--user=" + credenciales["username"],
    "--ssl-ca=" + str(CERTIFICADO),
    "--ssl-verify-server-cert",
    "--connect-timeout=10",
    "--batch",
    "--skip-column-names"
]

def consultar(sql):
    resultado = subprocess.run(
        mysql + ["-e", sql],
        env=entorno,
        capture_output=True,
        text=True,
        check=True
    )
    return resultado.stdout.strip()

consultar("SELECT 1")
print("CONEXION CIFRADA CON RDS: EXITOSA", flush=True)

existe = consultar(
    "SELECT COUNT(*) FROM information_schema.schemata "
    "WHERE schema_name = 'ecommerce'"
)

if existe != "0":
    raise SystemExit(
        "La base ecommerce ya existe. "
        "No se modificaron sus datos."
    )

consultar("CREATE DATABASE ecommerce CHARACTER SET utf8mb4")

print("Importando respaldo MySQL...", flush=True)

with RESPALDO.open("rb") as archivo:
    subprocess.run(
        mysql + ["--database=ecommerce"],
        stdin=archivo,
        env=entorno,
        check=True
    )

print("\n=== TABLAS RESTAURADAS EN RDS ===")

tablas = {
    "customers": 54,
    "products": 27,
    "orders": 106,
    "order_items": 259
}

total = 0

for nombre, esperado in tablas.items():
    cantidad = int(
        consultar(f"SELECT COUNT(*) FROM ecommerce.{nombre}")
    )
    print(f"{nombre}: {cantidad}")

    if cantidad != esperado:
        raise SystemExit(
            f"ERROR: {nombre} tiene una cantidad inesperada"
        )

    total += cantidad

print("\n=== RESULTADO FINAL ===")
print("Tablas:", len(tablas))
print("Registros:", total)
print("RESTAURACION RDS MYSQL: EXITOSA")
