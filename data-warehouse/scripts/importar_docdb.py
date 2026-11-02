import json
import os
import subprocess
import tempfile
from pathlib import Path

REGION = "us-east-1"
BASE = Path.home() / "dw-restauracion"
ARCHIVO = BASE / "ecommerce.archive.gz"
CERTIFICADO = BASE / "global-bundle.pem"

def aws(*args):
    resultado = subprocess.check_output(
        ["aws", *args, "--region", REGION, "--output", "json"],
        text=True
    )
    return json.loads(resultado)

if not ARCHIVO.is_file() or not CERTIFICADO.is_file():
    raise SystemExit("Falta respaldo o certificado TLS")

print("=== RESTAURACION DOCUMENTDB ===", flush=True)

cluster = aws(
    "docdb", "describe-db-clusters",
    "--db-cluster-identifier", "stefany-dw-documentdb"
)["DBClusters"][0]

if cluster["Status"] != "available":
    raise SystemExit("DocumentDB no esta disponible")

secreto = aws(
    "secretsmanager", "get-secret-value",
    "--secret-id", cluster["MasterUserSecret"]["SecretArn"]
)

cred = json.loads(secreto["SecretString"])

herramientas = list(BASE.rglob("mongorestore"))
if not herramientas:
    raise SystemExit("No se encontro mongorestore")

mongorestore = herramientas[0]
mongoexport = mongorestore.with_name("mongoexport")

fd, ruta = tempfile.mkstemp(
    prefix=".docdb-",
    suffix=".yaml",
    dir=BASE
)

try:
    with os.fdopen(fd, "w") as f:
        f.write("password: " + json.dumps(cred["password"]) + "\n")

    comunes = [
        "--config=" + ruta,
        "--host=" + cluster["Endpoint"],
        "--port=27017",
        "--username=" + cred["username"],
        "--authenticationDatabase=admin",
        "--tls",
        "--tlsCAFile=" + str(CERTIFICADO)
    ]

    print("Iniciando restauracion cifrada...", flush=True)

    comando = [
        str(mongorestore),
        *comunes,
        "--archive=" + str(ARCHIVO),
        "--gzip",
        "--nsInclude=ecommerce.*",
        "--noOptionsRestore",
        "--noIndexRestore",
        "--stopOnError"
    ]

    resultado = subprocess.run(
        comando,
        capture_output=True,
        text=True
    )

    if resultado.returncode:
        print(resultado.stderr[-3000:])
        raise SystemExit(
            "RESTAURACION FALLIDA. No repetir antes de revisar."
        )

    print(resultado.stderr[-1500:])

    print("\n=== DOCUMENTOS RESTAURADOS ===")

    total = 0

    for nombre, esperado in [
        ("navigation_events", 508),
        ("reviews", 158)
    ]:
        comando = [
            str(mongoexport),
            *comunes,
            "--db=ecommerce",
            "--collection=" + nombre,
            "--type=json",
            "--quiet"
        ]

        resultado = subprocess.run(
            comando,
            capture_output=True,
            text=True
        )

        if resultado.returncode:
            raise SystemExit(
                f"Error verificando {nombre}: "
                + resultado.stderr[-1000:]
            )

        cantidad = len([
            linea for linea in resultado.stdout.splitlines()
            if linea.strip()
        ])

        print(f"{nombre}: {cantidad}")

        if cantidad != esperado:
            raise SystemExit(
                f"Cantidad inesperada en {nombre}"
            )

        total += cantidad

    print("\n=== RESULTADO FINAL ===")
    print("Colecciones: 2")
    print("Documentos:", total)
    print("RESTAURACION DOCUMENTDB: EXITOSA")

finally:
    os.unlink(ruta)
