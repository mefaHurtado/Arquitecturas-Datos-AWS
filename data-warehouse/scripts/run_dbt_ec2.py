import json
import os
import subprocess
import sys
from pathlib import Path

REGION = "us-east-1"

def aws(*args):
    r = subprocess.run(
        ["aws", *args, "--region", REGION, "--output", "json"],
        capture_output=True, text=True, check=True
    )
    return json.loads(r.stdout)

namespace = aws(
    "redshift-serverless", "get-namespace",
    "--namespace-name", "stefany-dw-ns"
)["namespace"]

workgroup = aws(
    "redshift-serverless", "get-workgroup",
    "--workgroup-name", "stefany-dw-wg"
)["workgroup"]

print("Estado reportado por Redshift:", workgroup["status"], flush=True)

secreto = aws(
    "secretsmanager", "get-secret-value",
    "--secret-id", namespace["adminPasswordSecretArn"]
)

credenciales = json.loads(secreto["SecretString"])

entorno = os.environ.copy()
entorno["DW_REDSHIFT_HOST"] = workgroup["endpoint"]["address"]
entorno["DW_REDSHIFT_USER"] = credenciales["username"]
entorno["DW_REDSHIFT_DATABASE"] = "ecommerce"
entorno["DBT_ENV_SECRET_REDSHIFT_PASSWORD"] = credenciales["password"]

carpeta = Path.home() / ".dbt"
carpeta.mkdir(mode=0o700, exist_ok=True)

perfil = carpeta / "profiles.yml"

perfil.write_text("""
ecommerce_dw:
  target: aws
  outputs:
    aws:
      type: redshift
      host: "{{ env_var('DW_REDSHIFT_HOST') }}"
      port: 5439
      user: "{{ env_var('DW_REDSHIFT_USER') }}"
      password: "{{ env_var('DBT_ENV_SECRET_REDSHIFT_PASSWORD') }}"
      dbname: "{{ env_var('DW_REDSHIFT_DATABASE') }}"
      schema: dbt_dev
      threads: 2
      sslmode: require
""".lstrip())

perfil.chmod(0o600)

print("=== DBT CONECTADO A AWS ===", flush=True)
print("Redshift:", workgroup["workgroupName"], flush=True)
print("Base de datos: ecommerce", flush=True)
print("Credenciales: protegidas", flush=True)

comando = [
    str(Path.home() / "dw-dbt-venv/bin/dbt"),
    *sys.argv[1:],
    "--profiles-dir",
    str(carpeta)
]

resultado = subprocess.run(comando, env=entorno)
sys.exit(resultado.returncode)
