
import os
import hashlib
import xml.etree.ElementTree as ET
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

import boto3
import pandas as pd
import feedparser
from bs4 import BeautifulSoup


BASE = Path(__file__).resolve().parents[1]
BUCKET = os.environ["DATALAKE_BUCKET"]

RUN_ID = "20261101T203934514019Z-469143e7"
FUENTE = "eltiempo_colombia"

def limpiar(texto):
    return BeautifulSoup(
        str(texto or ""), "html.parser"
    ).get_text(" ", strip=True)

def identificador(enlace):
    p = urlsplit(enlace.strip())
    url = urlunsplit((
        p.scheme.lower(),
        p.netloc.lower(),
        p.path or "/",
        p.query,
        ""
    ))
    return hashlib.sha256(url.encode()).hexdigest()

def hash_texto(noticia):
    texto = (
        limpiar(noticia.get("title", ""))
        + "|"
        + limpiar(noticia.get("summary", ""))
    )
    return hashlib.sha256(texto.encode()).hexdigest()


# 1. Leer el lote ya procesado
ruta_parquet = (
    BASE / "local" / "silver_batches"
    / f"run_id={RUN_ID}" / "news.parquet"
)

anteriores = pd.read_parquet(ruta_parquet)
anteriores = anteriores.set_index("news_id")


# 2. Recuperar el RSS original desde S3
fecha = f"{RUN_ID[:4]}-{RUN_ID[4:6]}-{RUN_ID[6:8]}"

key = (
    f"bronze/source={FUENTE}/"
    f"ingestion_date={fecha}/"
    f"run_id={RUN_ID}/response.xml"
)

s3 = boto3.client("s3")
xml_original = s3.get_object(
    Bucket=BUCKET, Key=key
)["Body"].read()

feed_original = feedparser.parse(xml_original)
raiz = ET.fromstring(xml_original)

# 3. Elegir una noticia válida del lote anterior
seleccion = None

for item in raiz.findall("./channel/item"):
    enlace = (item.findtext("link") or "").strip()
    noticia_id = identificador(enlace)

    if noticia_id not in anteriores.index:
        continue

    entrada_original = next(
        (
            e for e in feed_original.entries
            if identificador(e.get("link", "")) == noticia_id
        ),
        None
    )

    if entrada_original is None:
        continue

    hash_anterior = anteriores.loc[
        noticia_id, "text_hash"
    ]

    if hash_texto(entrada_original) != hash_anterior:
        continue

    titulo = item.find("title")
    if titulo is None:
        continue

    seleccion = (item, noticia_id, hash_anterior)
    break

if seleccion is None:
    raise SystemExit(
        "No se encontró una noticia comparable. "
        "Revisa el lote y el Parquet original."
    )


# 4. Modificar únicamente la copia del título
item, noticia_id, hash_anterior = seleccion

titulo = item.find("title")
titulo_original = limpiar(titulo.text)

titulo.clear()
titulo.text = (
    titulo_original
    + " [PRUEBA ACADÉMICA DE ACTUALIZACIÓN]"
)

xml_modificado = ET.tostring(
    raiz,
    encoding="utf-8",
    xml_declaration=True
)

# 5. Guardar la copia local del RSS
carpeta = BASE / "local" / "tests" / "cambio_noticia"
carpeta.mkdir(parents=True, exist_ok=True)

archivo = carpeta / "rss_modificado.xml"
archivo.write_bytes(xml_modificado)


# 6. Leer nuevamente la copia modificada
feed_nuevo = feedparser.parse(xml_modificado)

entrada_nueva = next(
    e for e in feed_nuevo.entries
    if identificador(e.get("link", "")) == noticia_id
)

hash_nuevo = hash_texto(entrada_nueva)
id_nuevo = identificador(entrada_nueva["link"])

assert id_nuevo == noticia_id
assert hash_nuevo != hash_anterior

estado = "actualizada"

print("\n===== PRUEBA DE CAMBIO CONTROLADO =====")
print("Fuente:", FUENTE)
print("ID original:", noticia_id[:20])
print("ID nuevo:", id_nuevo[:20])
print("Identificador conservado:", id_nuevo == noticia_id)
print("Hash anterior:", hash_anterior[:20])
print("Hash nuevo:", hash_nuevo[:20])
print("Contenido modificado:", hash_nuevo != hash_anterior)
print("Clasificación:", estado)
print("XML de prueba:", archivo)
print("\nPRUEBA EXITOSA")
