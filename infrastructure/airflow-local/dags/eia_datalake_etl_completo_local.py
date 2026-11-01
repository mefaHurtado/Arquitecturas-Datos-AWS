"""DAG educativo: Bronze XML local -> Silver Parquet -> Gold Parquet.

No realiza llamadas de red ni a servicios AWS. Trabaja en una carpeta
aislada para no modificar los archivos originales del proyecto.
"""

import hashlib
import json
import re
import shutil
import subprocess
import sys
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from airflow.sdk import dag, task

PROYECTO = Path('/opt/airflow/datalake-project')
BRONZE = PROYECTO / 'local/bronze'
SALIDA = PROYECTO / 'local/airflow_end_to_end'
FUENTES = ('eltiempo_colombia', 'eltiempo_economia', 'eltiempo_tecnologia')
SCRIPTS = ('merge_silver.py', 'build_gold.py')


class _TextoHTML(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.partes = []

    def handle_data(self, texto):
        self.partes.append(texto)


def limpiar_texto(valor):
    parser = _TextoHTML()
    parser.feed(valor or '')
    return ' '.join(' '.join(parser.partes).split())


def normalizar_url(url):
    try:
        partes = urlsplit((url or '').strip())
        if partes.scheme.lower() not in ('http', 'https') or not partes.hostname:
            return None
        # Quitar fragmentos y parametros de seguimiento conocidos.
        parametros = sorted(
            (clave, valor) for clave, valor in parse_qsl(
                partes.query, keep_blank_values=True
            )
            if not clave.lower().startswith('utm_')
            and clave.lower() not in ('fbclid', 'gclid')
        )
        ruta = partes.path.rstrip('/') or '/'
        return urlunsplit((
            partes.scheme.lower(), partes.netloc.lower(), ruta,
            urlencode(parametros), ''
        ))
    except ValueError:
        return None


def fecha_publicacion(valor):
    if not valor:
        return None
    try:
        fecha = parsedate_to_datetime(valor)
    except (ValueError, TypeError, IndexError):
        try:
            fecha = datetime.fromisoformat(valor.replace('Z', '+00:00'))
        except ValueError:
            return None
    if fecha.tzinfo is None:
        fecha = fecha.replace(tzinfo=timezone.utc)
    return fecha.astimezone(timezone.utc)


def sha256_archivo(ruta):
    return hashlib.sha256(ruta.read_bytes()).hexdigest()


def buscar_lotes():
    """Exige XML y manifiesto consistente en las tres fuentes de cada lote."""
    catalogo = {}
    for fuente in FUENTES:
        for manifiesto in BRONZE.glob(
            f'source={fuente}/ingestion_date=*/run_id=*/manifest.json'
        ):
            lote = manifiesto.parent.name.removeprefix('run_id=')
            catalogo.setdefault(lote, {})[fuente] = manifiesto

    completos = {
        lote: rutas for lote, rutas in catalogo.items()
        if set(rutas) == set(FUENTES)
    }
    if len(completos) < 2:
        raise ValueError('Se necesitan al menos dos lotes Bronze completos')

    total = 0
    for lote, rutas in sorted(completos.items()):
        for fuente, manifiesto in rutas.items():
            datos = json.loads(manifiesto.read_text(encoding='utf-8'))
            xml = manifiesto.parent / 'response.xml'
            if datos.get('status') != 'success':
                raise ValueError(f'Manifiesto no exitoso: {lote}/{fuente}')
            if not xml.is_file() or sha256_archivo(xml) != datos.get('sha256'):
                raise ValueError(f'Hash o XML incorrecto: {lote}/{fuente}')
            items = ET.parse(xml).getroot().findall('./channel/item')
            if len(items) != int(datos.get('record_count', -1)):
                raise ValueError(f'Conteo incorrecto: {lote}/{fuente}')
            total += len(items)
    return completos, total


def generar_silver_local(lotes, carpeta):
    import pandas as pd

    recibidos = 0
    rechazados_total = 0
    for lote, manifiestos in sorted(lotes.items()):
        noticias = {}
        rechazados = []
        recibidos_lote = 0
        for fuente, manifiesto in sorted(manifiestos.items()):
            xml = manifiesto.parent / 'response.xml'
            items = ET.parse(xml).getroot().findall('./channel/item')
            for posicion, item in enumerate(items, start=1):
                recibidos += 1
                recibidos_lote += 1
                titulo = limpiar_texto(item.findtext('title'))
                descripcion = limpiar_texto(
                    item.findtext('description')
                    or item.findtext('{*}encoded')
                )
                url = normalizar_url(item.findtext('link'))
                fecha_original = (
                    item.findtext('pubDate')
                    or item.findtext('published')
                    or item.findtext('{*}date')
                    or item.findtext('updated')
                    or ''
                )
                fecha = fecha_publicacion(fecha_original)
                if not titulo or not url or fecha is None:
                    rechazados.append({
                        'fuente': fuente, 'posicion': posicion,
                        'motivo': 'Falta titulo, URL valida o fecha valida'
                    })
                    continue
                noticia_id = hashlib.sha256(url.encode('utf-8')).hexdigest()[:12]
                texto_hash = hashlib.sha256(
                    (titulo + '\n' + descripcion).encode('utf-8')
                ).hexdigest()
                if noticia_id in noticias:
                    fuentes = set(noticias[noticia_id]['fuentes'].split(','))
                    fuentes.add(fuente)
                    noticias[noticia_id]['fuentes'] = ','.join(sorted(fuentes))
                else:
                    noticias[noticia_id] = {
                        'news_id': noticia_id,
                        'titulo': titulo,
                        'descripcion': descripcion,
                        'enlace': url,
                        'fecha_original': fecha_original,
                        'published_at_utc': fecha.isoformat(),
                        'publication_date': fecha.date().isoformat(),
                        'fuentes': fuente,
                        'text_hash': texto_hash,
                        'run_id': lote,
                        'bronze_local_path': str(xml.relative_to(PROYECTO)),
                    }
        if not noticias:
            raise ValueError(f'Lote sin noticias validas: {lote}')

        destino = carpeta / 'local/silver_batches' / f'run_id={lote}'
        destino.mkdir(parents=True, exist_ok=True)
        tabla = pd.DataFrame(sorted(noticias.values(), key=lambda x: x['news_id']))
        if tabla['news_id'].duplicated().any():
            raise ValueError(f'IDs duplicados en Silver: {lote}')
        tabla.to_parquet(destino / 'news.parquet', index=False)
        (destino / 'quarantine.json').write_text(
            json.dumps(rechazados, ensure_ascii=False, indent=2), encoding='utf-8'
        )
        rechazados_total += len(rechazados)
        print(f'Silver {lote}: {recibidos_lote} recibidas, '
              f'{len(tabla)} noticias unicas, {len(rechazados)} rechazadas')
    return {'recibidos': recibidos, 'rechazados': rechazados_total}


def ejecutar_script(carpeta, nombre):
    proceso = subprocess.run(
        [sys.executable, str(carpeta / 'src' / nombre)],
        cwd=carpeta, capture_output=True, text=True, timeout=180,
    )
    print(proceso.stdout)
    if proceso.returncode != 0:
        raise RuntimeError(f'{nombre} fallo:\n{proceso.stderr or proceso.stdout}')


@dag(
    dag_id='eia_datalake_etl_completo_local',
    start_date=datetime(2026, 11, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=['EIA', 'Bronze-Silver-Gold', 'Sin AWS'],
)
def pipeline():

    @task
    def validar_bronze():
        lotes, total = buscar_lotes()
        huella = hashlib.sha256()
        huella.update(Path(__file__).read_bytes())
        for script in SCRIPTS:
            ruta = PROYECTO / 'src' / script
            if not ruta.is_file():
                raise FileNotFoundError(ruta)
            huella.update(ruta.read_bytes())
        for lote, manifiestos in sorted(lotes.items()):
            for fuente, manifiesto in sorted(manifiestos.items()):
                huella.update(lote.encode())
                huella.update(fuente.encode())
                huella.update(manifiesto.read_bytes())
                huella.update((manifiesto.parent / 'response.xml').read_bytes())
        carpeta = SALIDA / ('snapshot_id=' + huella.hexdigest()[:16])
        print(f'Lotes completos: {len(lotes)}; entradas Bronze: {total}')
        print(f'Carpeta aislada: {carpeta}')
        return {'carpeta': str(carpeta), 'lotes': len(lotes), 'bronze': total}

    @task
    def procesar_silver(datos):
        lotes, _ = buscar_lotes()
        carpeta = Path(datos['carpeta'])
        (carpeta / 'src').mkdir(parents=True, exist_ok=True)
        for script in SCRIPTS:
            shutil.copy2(PROYECTO / 'src' / script, carpeta / 'src' / script)
        estadisticas = generar_silver_local(lotes, carpeta)
        print('PROCESAMIENTO BRONZE -> SILVER CORRECTO')
        return {**datos, **estadisticas}

    @task(retries=1, retry_delay=timedelta(seconds=15))
    def consolidar_silver(datos):
        ejecutar_script(Path(datos['carpeta']), 'merge_silver.py')
        return datos

    @task(retries=1, retry_delay=timedelta(seconds=15))
    def construir_gold(datos):
        ejecutar_script(Path(datos['carpeta']), 'build_gold.py')
        return datos

    @task
    def validar_resultados(datos):
        import pandas as pd
        carpeta = Path(datos['carpeta'])
        silver = pd.read_parquet(carpeta / 'local/silver/news_current.parquet')
        eventos = pd.read_parquet(carpeta / 'local/silver/news_events.parquet')
        gold = pd.read_parquet(carpeta / 'local/gold/news_by_day')
        if silver.empty or gold.empty or silver['news_id'].duplicated().any():
            raise ValueError('Silver o Gold vacio o con news_id duplicados')
        esperado = sum(
            len({f.strip() for f in str(valor).split(',') if f.strip()})
            for valor in silver['fuentes']
        )
        observado = int(gold['total_noticias'].sum())
        if esperado != observado:
            raise ValueError(f'Silver != Gold ({esperado} != {observado})')
        if len(eventos) < len(silver):
            raise ValueError('Faltan eventos incrementales')
        reporte = {
            'lotes_bronze': datos['lotes'],
            'registros_bronze': datos['bronze'],
            'registros_rechazados': datos['rechazados'],
            'eventos_silver': len(eventos),
            'noticias_vigentes_silver': len(silver),
            'conteos_gold': observado,
            'grupos_gold': len(gold),
            'snapshot': carpeta.name,
            'status': 'success',
            'modo': 'local_sin_aws',
        }
        temporal = carpeta / 'reporte.json.tmp'
        temporal.write_text(
            json.dumps(reporte, ensure_ascii=False, indent=2, sort_keys=True),
            encoding='utf-8'
        )
        temporal.replace(carpeta / 'reporte.json')
        print('PIPELINE COMPLETO CORRECTO')
        print(json.dumps(reporte, ensure_ascii=False, indent=2))

    datos = validar_bronze()
    silver = procesar_silver(datos)
    unido = consolidar_silver(silver)
    gold = construir_gold(unido)
    validar_resultados(gold)


pipeline()
