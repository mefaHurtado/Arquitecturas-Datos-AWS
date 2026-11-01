
import hashlib
import json
import shutil
import subprocess
import sys
import xml.etree.ElementTree as ET

from datetime import datetime, timedelta, timezone
from pathlib import Path
from airflow.sdk import Param, dag, get_current_context, task


PROYECTO = Path("/opt/airflow/datalake-project")
BRONZE = PROYECTO / "local/bronze"
BATCHES = PROYECTO / "local/silver_batches"
PRUEBAS = PROYECTO / "local/airflow_pipeline"

FUENTES = (
    "eltiempo_colombia",
    "eltiempo_economia",
    "eltiempo_tecnologia"
)

COLUMNAS = {
    "news_id", "text_hash",
    "publication_date", "fuentes"
}


def sha256_archivo(ruta):
    digest = hashlib.sha256()

    with ruta.open("rb") as archivo:
        for bloque in iter(
            lambda: archivo.read(1024 * 1024), b""
        ):
            digest.update(bloque)

    return digest.hexdigest()


@dag(
    dag_id="eia_datalake_pipeline_local",
    start_date=datetime(
        2026, 11, 1, tzinfo=timezone.utc
    ),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    params={
        "source_run_id": Param("", type="string")
    },
    tags=["EIA", "Data Lake", "Sin AWS"]
)
def pipeline():

    # 1. Validar Bronze
    @task(retries=0)
    def validar_bronze():

        catalogo = {}

        for fuente in FUENTES:
            archivos = list(BRONZE.glob(
                f"source={fuente}/ingestion_date=*/"
                "run_id=*/manifest.json"
            ))

            catalogo[fuente] = {
                ruta.parent.name.removeprefix("run_id="): ruta
                for ruta in archivos
            }

        completos = set.intersection(
            *(set(d) for d in catalogo.values())
        )

        solicitado = get_current_context()[
            "params"
        ]["source_run_id"].strip()

        lote = solicitado or (
            sorted(completos)[-1] if completos else ""
        )

        if lote not in completos:
            raise ValueError(
                f"Lote Bronze inexistente: {lote!r}"
            )

        total = 0

        for fuente in FUENTES:
            ruta = catalogo[fuente][lote]

            manifiesto = json.loads(
                ruta.read_text(encoding="utf-8")
            )

            xml = ruta.parent / "response.xml"

            if manifiesto.get("status") != "success":
                raise ValueError(
                    f"Manifiesto no exitoso: {fuente}"
                )

            if sha256_archivo(xml) != manifiesto.get("sha256"):
                raise ValueError(
                    f"Hash incorrecto: {fuente}"
                )

            noticias = ET.parse(
                xml
            ).getroot().findall("./channel/item")

            if len(noticias) != manifiesto.get("record_count"):
                raise ValueError(
                    f"Conteo incorrecto: {fuente}"
                )

            total += len(noticias)

        print("Bronze validado:", lote)
        print("Registros:", total)

        return {
            "lote": lote,
            "bronze_recibidos": total
        }

    # 2. Preparar copias de Silver
    @task(retries=0)
    def preparar_silver(validacion):

        import pandas as pd

        entradas = sorted(
            BATCHES.glob("run_id=*/news.parquet")
        )

        if len(entradas) < 2:
            raise ValueError(
                "Se necesitan al menos dos lotes Silver"
            )

        ids = {
            ruta.parent.name.removeprefix("run_id=")
            for ruta in entradas
        }

        if validacion["lote"] not in ids:
            raise ValueError(
                "El lote Bronze aún no tiene Silver local"
            )

        scripts = [
            PROYECTO / "src/merge_silver.py",
            PROYECTO / "src/build_gold.py"
        ]

        huella = hashlib.sha256()

        for archivo in entradas + scripts:
            if not archivo.is_file():
                raise FileNotFoundError(archivo)

            huella.update(
                str(archivo.relative_to(PROYECTO)).encode()
            )
            huella.update(
                sha256_archivo(archivo).encode()
            )

        destino = PRUEBAS / (
            "snapshot_id=" + huella.hexdigest()[:16]
        )

        (destino / "src").mkdir(
            parents=True, exist_ok=True
        )

        for script in scripts:
            shutil.copy2(
                script,
                destino / "src" / script.name
            )

        for archivo in entradas:
            tabla = pd.read_parquet(archivo)

            if tabla.empty or not COLUMNAS.issubset(
                tabla.columns
            ):
                raise ValueError(
                    f"Silver inválido: {archivo}"
                )

            if (
                tabla["news_id"].isna().any()
                or tabla["news_id"].duplicated().any()
            ):
                raise ValueError(
                    f"IDs incorrectos: {archivo}"
                )

            subcarpeta = (
                destino / "local/silver_batches"
                / archivo.parent.name
            )

            subcarpeta.mkdir(
                parents=True, exist_ok=True
            )

            shutil.copy2(
                archivo,
                subcarpeta / "news.parquet"
            )

        print("Lotes Silver:", len(entradas))
        print("Carpeta de prueba:", destino)

        return {
            "carpeta": str(destino),
            **validacion
        }

    # 3. Consolidar Silver usando nuestro script
    @task(
        retries=1,
        retry_delay=timedelta(seconds=15)
    )
    def consolidar_silver(datos):

        carpeta = Path(datos["carpeta"])

        proceso = subprocess.run(
            [
                sys.executable,
                str(carpeta / "src/merge_silver.py")
            ],
            cwd=carpeta,
            capture_output=True,
            text=True,
            timeout=120
        )

        print(proceso.stdout)

        if proceso.returncode != 0:
            raise RuntimeError(
                proceso.stderr or proceso.stdout
            )

        resultado = (
            carpeta / "local/silver/news_current.parquet"
        )

        if not resultado.is_file():
            raise FileNotFoundError(resultado)

        print("CONSOLIDACIÓN SILVER EXITOSA")

        return datos

    # 4. Construir Gold usando nuestro script
    @task(
        retries=1,
        retry_delay=timedelta(seconds=15)
    )
    def construir_gold(datos):

        carpeta = Path(datos["carpeta"])

        proceso = subprocess.run(
            [
                sys.executable,
                str(carpeta / "src/build_gold.py")
            ],
            cwd=carpeta,
            capture_output=True,
            text=True,
            timeout=120
        )

        print(proceso.stdout)

        if proceso.returncode != 0:
            raise RuntimeError(
                proceso.stderr or proceso.stdout
            )

        print("CONSTRUCCIÓN GOLD EXITOSA")

        return datos

    # 5. Verificar y registrar resultados
    @task(retries=0)
    def validar_resultados(datos):

        import pandas as pd

        carpeta = Path(datos["carpeta"])

        silver = pd.read_parquet(
            carpeta / "local/silver/news_current.parquet"
        )

        gold = pd.read_parquet(
            carpeta / "local/gold/news_by_day"
        )

        if (
            silver.empty
            or gold.empty
            or silver["news_id"].duplicated().any()
        ):
            raise ValueError(
                "Error de calidad en Silver o Gold"
            )

        if (gold["total_noticias"] <= 0).any():
            raise ValueError(
                "Gold contiene conteos inválidos"
            )

        esperado = sum(
            len({
                fuente.strip()
                for fuente in str(fuentes).split(",")
                if fuente.strip()
            })
            for fuentes in silver["fuentes"]
        )

        observado = int(
            gold["total_noticias"].sum()
        )

        if esperado != observado:
            raise ValueError(
                f"No concilian Silver y Gold: "
                f"{esperado} != {observado}"
            )

        reporte = {
            "source_run_id": datos["lote"],
            "bronze_recibidos": datos["bronze_recibidos"],
            "silver_vigentes": int(len(silver)),
            "gold_conteos": observado,
            "gold_grupos": int(len(gold)),
            "snapshot": carpeta.name,
            "status": "success",
            "modo": "local_sin_aws"
        }

        temporal = carpeta / "reporte.json.tmp"
        salida = carpeta / "reporte.json"

        temporal.write_text(
            json.dumps(
                reporte,
                ensure_ascii=False,
                indent=2,
                sort_keys=True
            ),
            encoding="utf-8"
        )

        temporal.replace(salida)

        print("PIPELINE LOCAL VALIDADO")
        print(json.dumps(reporte, indent=2))

    # Dependencias de las cinco tareas
    lote = validar_bronze()
    silver = preparar_silver(lote)
    consolidado = consolidar_silver(silver)
    gold = construir_gold(consolidado)
    validar_resultados(gold)


pipeline()
