
import json
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from uuid import uuid4
import xml.etree.ElementTree as ET

import requests
import feedparser


# 1. Localizar nuestro proyecto
BASE_DIR = Path(__file__).resolve().parents[1]
CONFIG_PATH = BASE_DIR / "config" / "sources.json"
BRONZE_DIR = BASE_DIR / "local" / "bronze"


# 2. Leer las fuentes RSS configuradas
with open(CONFIG_PATH, "r", encoding="utf-8") as archivo:
    configuracion = json.load(archivo)

fuentes = configuracion["feeds"]


# 3. Crear un identificador único de ejecución
inicio = datetime.now(timezone.utc)

fecha = inicio.strftime("%Y-%m-%d")

run_id = (
    inicio.strftime("%Y%m%dT%H%M%S%fZ")
    + "-"
    + uuid4().hex[:8]
)

print(f"\nIniciando extracción: {run_id}\n")


# 4. Consultar cada fuente
for fuente in fuentes:

    source_id = fuente["source_id"]
    url = fuente["url"]

    ruta_lote = (
        BRONZE_DIR
        / f"source={source_id}"
        / f"ingestion_date={fecha}"
        / f"run_id={run_id}"
    )

    ruta_lote.mkdir(parents=True, exist_ok=False)

    metadata = {
        "run_id": run_id,
        "medio": fuente["medio"],
        "seccion": fuente["seccion"],
        "source_id": source_id,
        "url": url,
        "extracted_at_utc": datetime.now(
            timezone.utc
        ).isoformat(),
        "http_status": None,
        "sha256": None,
        "record_count": 0,
        "original_path": None,
        "status": "failed",
        "error": None
    }

    print(f"Consultando: {source_id}")

    try:
        # 5. Realizar la petición HTTP
        respuesta = requests.get(
            url,
            timeout=(5, 20),
            headers={
                "User-Agent": "AcademicRSSLab/1.0"
            }
        )

        metadata["http_status"] = respuesta.status_code

        respuesta.raise_for_status()

        contenido = respuesta.content

        if not contenido.strip():
            raise ValueError("Respuesta vacía")

        # 6. Validar que sea XML correcto
        ET.fromstring(contenido)

        # 7. Interpretar el feed
        feed = feedparser.parse(contenido)

        if not feed.version:
            raise ValueError("XML sin formato RSS/Atom válido")

        cantidad = len(feed.entries)

        # 8. Calcular el hash del contenido
        hash_xml = hashlib.sha256(
            contenido
        ).hexdigest()

        # 9. Guardar exactamente el XML recibido
        ruta_xml = ruta_lote / "response.xml"

        with open(ruta_xml, "xb") as archivo:
            archivo.write(contenido)

        metadata["sha256"] = hash_xml
        metadata["record_count"] = cantidad
        metadata["original_path"] = str(ruta_xml)

        if cantidad == 0:
            metadata["status"] = "empty"
        else:
            metadata["status"] = "success"

        print(f"Noticias encontradas: {cantidad}")
        print(f"Estado: {metadata['status']}\n")

    except (
        requests.RequestException,
        ET.ParseError,
        ValueError
    ) as error:

        metadata["error"] = str(error)

        print(f"Error: {error}\n")

    # 10. Guardar los metadatos del lote
    ruta_metadata = ruta_lote / "manifest.json"

    with open(
        ruta_metadata,
        "x",
        encoding="utf-8"
    ) as archivo:

        json.dump(
            metadata,
            archivo,
            indent=4,
            ensure_ascii=False
        )


print("Extracción finalizada.")
print(f"Identificador de ejecución: {run_id}")


# Enviar a S3 los archivos del lote actual
import subprocess
import sys

print("\nIniciando carga a Amazon S3...")

subprocess.run(
    [
        sys.executable,
        str(BASE_DIR / "src" / "upload_bronze.py"),
        "--run-id",
        run_id
    ],
    check=True
)

print("Proceso de extracción y carga finalizado.")
