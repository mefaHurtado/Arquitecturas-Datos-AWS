
import os
import json
import hashlib
from pathlib import Path

import boto3
import pandas as pd
from botocore.exceptions import ClientError


BASE = Path(__file__).resolve().parents[1]
GOLD = BASE / "local/gold/news_by_day"

BUCKET = os.environ["DATALAKE_BUCKET"]
REGION = os.environ.get("AWS_REGION", "us-east-1")

s3 = boto3.client("s3", region_name=REGION)


def guardar(key, contenido, tipo):
    try:
        s3.put_object(
            Bucket=BUCKET,
            Key=key,
            Body=contenido,
            ContentType=tipo,
            IfNoneMatch="*"
        )
        print("SUBIDO:", key)

    except ClientError as error:
        codigo = error.response["Error"]["Code"]

        if codigo not in ("412", "PreconditionFailed"):
            raise

        existente = s3.get_object(
            Bucket=BUCKET,
            Key=key
        )["Body"].read()

        if existente != contenido:
            raise ValueError(
                f"Conflicto de contenido: {key}"
            )

        print("YA EXISTE:", key)


def publicar():
    archivos = sorted(GOLD.rglob("*.parquet"))

    if not archivos:
        raise ValueError("No hay archivos Gold para publicar")

    # Validar el contenido
    df = pd.read_parquet(GOLD)

    if df.empty:
        raise ValueError("Gold está vacío")

    if df["publication_date"].isna().any():
        raise ValueError("Existen fechas vacías")

    if (df["total_noticias"] <= 0).any():
        raise ValueError("Hay conteos inválidos")

    # Identificar el contenido de manera reproducible
    huella = hashlib.sha256()
    objetos = []

    for archivo in archivos:
        ruta = archivo.relative_to(GOLD).as_posix()
        contenido = archivo.read_bytes()

        sha = hashlib.sha256(contenido).hexdigest()

        huella.update(ruta.encode("utf-8"))
        huella.update(sha.encode("utf-8"))

        objetos.append((ruta, contenido, sha))

    snapshot_id = huella.hexdigest()[:20]

    prefijo = (
        f"gold/news_by_day/"
        f"snapshot_id={snapshot_id}"
    )

    manifiesto_archivos = []

    # Subir cada partición
    for ruta, contenido, sha in objetos:
        key = f"{prefijo}/{ruta}"

        guardar(
            key,
            contenido,
            "application/octet-stream"
        )

        manifiesto_archivos.append({
            "key": key,
            "sha256": sha
        })

    # Publicar el manifiesto solo al completar los archivos
    ready_key = f"{prefijo}/_READY.json"

    manifiesto = {
        "snapshot_id": snapshot_id,
        "rows": len(df),
        "total_noticias": int(df["total_noticias"].sum()),
        "files": manifiesto_archivos
    }

    guardar(
        ready_key,
        json.dumps(
            manifiesto,
            sort_keys=True,
            indent=2
        ).encode("utf-8"),
        "application/json"
    )

    # Referencia a la versión vigente
    control_key = "gold/_control/current_news_by_day.json"

    control = {
        "snapshot_id": snapshot_id,
        "location": f"s3://{BUCKET}/{prefijo}/",
        "ready_key": ready_key
    }

    contenido_control = json.dumps(
        control,
        sort_keys=True,
        indent=2
    ).encode("utf-8")

    parametros = {}

    try:
        anterior = s3.get_object(
            Bucket=BUCKET,
            Key=control_key
        )

        if anterior["Body"].read() == contenido_control:
            print("Snapshot vigente sin cambios")
            print("GOLD PUBLICADO CORRECTAMENTE")
            return

        parametros["IfMatch"] = anterior["ETag"]

    except ClientError as error:
        codigo = error.response["Error"]["Code"]

        if codigo not in ("NoSuchKey", "404"):
            raise

        parametros["IfNoneMatch"] = "*"

    s3.put_object(
        Bucket=BUCKET,
        Key=control_key,
        Body=contenido_control,
        ContentType="application/json",
        **parametros
    )

    print("\n===== GOLD PUBLICADO =====")
    print("Snapshot:", snapshot_id)
    print("Archivos:", len(objetos))
    print("Registros agregados:", len(df))
    print("Noticias contabilizadas:", manifiesto["total_noticias"])
    print("Ubicación:", control["location"])
    print("GOLD PUBLICADO CORRECTAMENTE")


if __name__ == "__main__":
    publicar()
