
import os
import io
import json
import hashlib
from pathlib import Path

import boto3
import pandas as pd
from botocore.exceptions import ClientError


BASE_DIR = Path(__file__).resolve().parents[1]
ARCHIVO = BASE_DIR / "local/silver/news_current.parquet"
LOTES = BASE_DIR / "local/silver_batches"

BUCKET = os.environ["DATALAKE_BUCKET"]
REGION = os.environ.get("AWS_REGION", "us-east-1")

s3 = boto3.client("s3", region_name=REGION)


def guardar_sin_sobrescribir(key, contenido, tipo):
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

        if codigo not in ("PreconditionFailed", "412"):
            raise

        existente = s3.get_object(
            Bucket=BUCKET, Key=key
        )["Body"].read()

        if existente != contenido:
            raise ValueError(
                f"Conflicto: el objeto cambió: {key}"
            )

        print("YA EXISTE, coincide:", key)


def publicar():
    if not ARCHIVO.exists():
        raise FileNotFoundError(ARCHIVO)

    # Identificar el último lote procesado
    ids = sorted(
        p.parent.name.replace("run_id=", "")
        for p in LOTES.glob("run_id=*/news.parquet")
    )

    if not ids:
        raise ValueError("No hay lotes Silver")

    snapshot_id = ids[-1]

    # Leer y validar noticias vigentes
    df = pd.read_parquet(ARCHIVO)

    if df.empty:
        raise ValueError("Silver no contiene noticias")

    if df["news_id"].isna().any():
        raise ValueError("Hay IDs vacíos")

    if df["news_id"].duplicated().any():
        raise ValueError("Hay noticias duplicadas")

    if df["publication_date"].isna().any():
        raise ValueError("Hay fechas vacías")

    pd.to_datetime(
        df["publication_date"],
        format="%Y-%m-%d",
        errors="raise"
    )

    df = df.sort_values("news_id")

    prefijo = f"silver/news/snapshot_id={snapshot_id}"
    archivos = []

    # Publicar un Parquet por fecha
    for fecha, grupo in df.groupby("publication_date"):

        key = (
            f"{prefijo}/"
            f"publication_date={fecha}/"
            "part-000.parquet"
        )

        # La fecha se recuperará de la partición
        datos = grupo.drop(columns=["publication_date"])

        memoria = io.BytesIO()
        datos.to_parquet(
            memoria,
            index=False,
            engine="pyarrow"
        )

        contenido = memoria.getvalue()

        guardar_sin_sobrescribir(
            key, contenido, "application/octet-stream"
        )

        archivos.append({
            "key": key,
            "rows": len(grupo),
            "sha256": hashlib.sha256(
                contenido
            ).hexdigest()
        })

    # Registrar la publicación completa
    ready_key = f"{prefijo}/_READY.json"

    manifiesto = {
        "snapshot_id": snapshot_id,
        "total_news": len(df),
        "files": archivos
    }

    guardar_sin_sobrescribir(
        ready_key,
        json.dumps(
            manifiesto,
            sort_keys=True,
            indent=2
        ).encode("utf-8"),
        "application/json"
    )

    # Actualizar referencia al snapshot vigente
    control_key = "silver/_control/current_news.json"

    control = {
        "snapshot_id": snapshot_id,
        "location": f"s3://{BUCKET}/{prefijo}/",
        "total_news": len(df),
        "ready_key": ready_key
    }

    parametros = {}

    try:
        anterior = s3.get_object(
            Bucket=BUCKET, Key=control_key
        )

        previo = json.loads(anterior["Body"].read())

        if previo["snapshot_id"] == snapshot_id:
            if previo != control:
                raise ValueError(
                    "El snapshot actual no coincide"
                )
            print("Snapshot ya publicado")
            return

        if previo["snapshot_id"] > snapshot_id:
            raise ValueError(
                "No se puede publicar un lote anterior"
            )

        parametros["IfMatch"] = anterior["ETag"]

    except ClientError as error:
        codigo = error.response["Error"]["Code"]

        if codigo not in ("NoSuchKey", "404"):
            raise

        parametros["IfNoneMatch"] = "*"

    s3.put_object(
        Bucket=BUCKET,
        Key=control_key,
        Body=json.dumps(
            control, indent=2
        ).encode("utf-8"),
        ContentType="application/json",
        **parametros
    )

    print("\n===== SILVER PUBLICADO =====")
    print("Snapshot:", snapshot_id)
    print("Noticias:", len(df))
    print("Particiones:", len(archivos))
    print("Ubicación:", control["location"])


if __name__ == "__main__":
    publicar()
