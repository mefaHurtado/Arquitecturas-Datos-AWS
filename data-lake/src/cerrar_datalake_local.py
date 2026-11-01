#!/usr/bin/env python3
"""Auditoría y Gold final offline: no importa boto3 ni realiza llamadas de red.

Uso:
 python src/cerrar_datalake_local.py --project .
 python src/cerrar_datalake_local.py --project . --entities ruta.parquet \
     --phrases ruta.parquet --status ruta.parquet --provenance aws_comprehend

No atribuye resultados ficticios a AWS Comprehend. Al faltar entradas
realmente enriquecidas genera un reporte PENDIENTE, no resultados fabricados.
"""
import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

FUENTES = {
    'eltiempo_colombia': 'Colombia',
    'eltiempo_economia': 'Economía',
    'eltiempo_tecnologia': 'Tecnología'
}
ENTITY_TYPES = {'PERSON', 'ORGANIZATION', 'LOCATION'}


def safe_text(v):
    return '' if pd.isna(v) else str(v).strip()


def load_news(project):
    candidates = []
    for rp in (project / 'local/airflow_end_to_end').glob('snapshot_id=*/reporte.json'):
        try:
            r = json.loads(rp.read_text(encoding='utf-8'))
            if r.get('status') == 'success':
                q = rp.parent / 'local/silver/news_current.parquet'
                if q.is_file():
                    candidates.append((rp.stat().st_mtime, q))
        except (OSError, ValueError):
            continue
    if candidates:
        return max(candidates, key=lambda x: x[0])[1]
    q = project / 'local/silver/news_current.parquet'
    if q.is_file():
        return q
    raise FileNotFoundError('No se encontró Silver: news_current.parquet válido')


def normalize_news(df):
    required = {'news_id', 'publication_date', 'fuentes', 'text_hash'}
    if not required.issubset(df.columns):
        raise ValueError('Columnas obligatorias faltantes en Silver: ' + str(sorted(required - set(df.columns))))
    if df.empty or df['news_id'].isna().any() or df['news_id'].duplicated().any():
        raise ValueError('Silver vacío, ID nulo o IDs duplicados')
    x = df.copy()
    x['news_id'] = x['news_id'].astype(str).str.strip()
    x['publication_date'] = x['publication_date'].astype(str).str.strip()
    if x['news_id'].eq('').any():
        raise ValueError('IDs de noticia vacíos')
    fechas = pd.to_datetime(x['publication_date'], format='%Y-%m-%d', errors='coerce')
    if fechas.isna().any():
        raise ValueError('Fechas inválidas en noticias vigentes')
    if x['text_hash'].isna().any():
        raise ValueError('Textos sin hash')
    x['text_hash'] = x['text_hash'].astype(str).str.strip()
    if x['text_hash'].eq('').any() or x['text_hash'].eq('nan').any():
        raise ValueError('Textos sin hash')
    return x


def sections_of_news(news):
    rows = []
    for row in news[['news_id', 'publication_date', 'fuentes']].itertuples(index=False):
        f = {part.strip() for part in str(row.fuentes).split(',') if part.strip()}
        unknown = f - set(FUENTES)
        if not f or unknown:
            raise ValueError(f'Fuentes ausentes o desconocidas en {row.news_id}: {unknown}')
        for fuente in sorted(f):
            rows.append({'news_id': row.news_id,
                         'publication_date': row.publication_date,
                         'medio': 'El Tiempo', 'seccion': FUENTES[fuente]})
    return pd.DataFrame(rows).drop_duplicates()


def read_enrichment(entities_path, phrases_path, status_path, news):
    if not (entities_path and phrases_path and status_path):
        if any((entities_path, phrases_path, status_path)):
            raise ValueError('Para usar enriquecimiento se necesitan --entities, --phrases y --status juntos')
        return None, None, None
    ent = pd.read_parquet(entities_path)
    fra = pd.read_parquet(phrases_path)
    est = pd.read_parquet(status_path)
    entities_cols = {'news_id', 'text_hash', 'entity_type', 'entity_text', 'score'}
    phrase_cols = {'news_id', 'text_hash', 'phrase_text', 'score'}
    status_cols = {'news_id', 'text_hash', 'enrichment_status', 'language', 'analyzed_at'}
    for kind, df, needed in [('entities', ent, entities_cols), ('phrases', fra, phrase_cols), ('status', est, status_cols)]:
        if not needed.issubset(df.columns):
            raise ValueError(f'Columnas faltantes en {kind}: {sorted(needed - set(df.columns))}')
    if est['news_id'].duplicated().any():
        raise ValueError('news_id duplicado en enrichment_status')
    known = news.set_index('news_id')['text_hash'].to_dict()
    for kind, df in [('entities', ent), ('phrases', fra), ('status', est)]:
        for row in df[['news_id', 'text_hash']].itertuples(index=False):
            if row.news_id not in known:
                raise ValueError(f'Fila huérfana de {kind}: {row.news_id}')
            if str(row.text_hash) != str(known[row.news_id]):
                raise ValueError(f'Hash desactualizado en {kind}: {row.news_id}')
    states = set(est['enrichment_status'].astype(str).str.lower())
    if not states.issubset({'success', 'pending', 'skipped', 'error'}):
        raise ValueError(f'Estado de enriquecimiento no reconocido: {states}')
    success = set(est.loc[est['enrichment_status'].str.lower().eq('success'), 'news_id'])
    for kind, df in [('entities', ent), ('phrases', fra)]:
        if not set(df.news_id).issubset(success):
            raise ValueError(f'{kind}: resultados sin estado success')
        if (pd.to_numeric(df.score, errors='coerce').isna() | ~pd.to_numeric(df.score, errors='coerce').between(0,1)).any():
            raise ValueError(f'{kind}: score fuera de [0,1]')
    if not ent.empty:
        ent = ent[ent['entity_type'].isin(ENTITY_TYPES)].copy()
    return ent, fra, est


def aggregate_gold(news, entities, phrases):
    detail = sections_of_news(news)
    by_day = (detail.groupby(['publication_date','medio','seccion'], as_index=False)
              .agg(total_noticias=('news_id','nunique')))
    if int(by_day.total_noticias.sum()) != len(detail):
        raise AssertionError('Conciliación Silver/Gold de noticias falló')

    ent_cols = ['entity_type', 'entity_text', 'total_noticias']
    trend_cols = ['publication_date', 'entity_type', 'entity_text', 'total_noticias']
    phrase_cols = ['medio','seccion','phrase_text','total_noticias']
    ranking = pd.DataFrame(columns=ent_cols)
    trend = pd.DataFrame(columns=trend_cols)
    by_section = pd.DataFrame(columns=phrase_cols)
    if entities is not None and not entities.empty:
        e = entities[['news_id','entity_type','entity_text']].copy()
        e['entity_text'] = e['entity_text'].map(safe_text)
        e = e[e.entity_text.ne('')].drop_duplicates()
        ranking = (e.groupby(['entity_type','entity_text'], as_index=False)
                   .agg(total_noticias=('news_id','nunique')))
        time = e.merge(news[['news_id','publication_date']], on='news_id',validate='many_to_one')
        trend = (time.groupby(['publication_date','entity_type','entity_text'], as_index=False)
                 .agg(total_noticias=('news_id','nunique')))
    if phrases is not None and not phrases.empty:
        p = phrases[['news_id','phrase_text']].copy()
        p['phrase_text'] = p['phrase_text'].map(safe_text)
        p = p[p.phrase_text.ne('')].drop_duplicates()
        p = p.merge(detail[['news_id','medio','seccion']], on='news_id', validate='many_to_many')
        by_section = (p.groupby(['medio','seccion','phrase_text'], as_index=False)
                      .agg(total_noticias=('news_id','nunique')))
    for table in (by_day, ranking, trend, by_section):
        table['total_noticias'] = table['total_noticias'].astype('int64')
    return by_day, ranking, trend, by_section


def write_partitioned(df, root, name, partition=None):
    target = root / name
    if partition and partition in df.columns and not df.empty:
        df.to_parquet(target, index=False, partition_cols=[partition], engine='pyarrow')
    else:
        target.mkdir(parents=True, exist_ok=True)
        df.to_parquet(target / 'part-00000.parquet', index=False, engine='pyarrow')


def main():
    parser = argparse.ArgumentParser(description='Cierre Data Lake sin AWS')
    parser.add_argument('--project', type=Path, default=Path.cwd())
    parser.add_argument('--entities', type=Path)
    parser.add_argument('--phrases', type=Path)
    parser.add_argument('--status', type=Path)
    parser.add_argument('--provenance', choices=['aws_comprehend','demo'], default=None)
    args = parser.parse_args()
    if any((args.entities,args.phrases,args.status)) and not args.provenance:
        parser.error('Declare --provenance aws_comprehend o demo al usar resultados NLP')
    project = args.project.expanduser().resolve()
    source = load_news(project)
    news = normalize_news(pd.read_parquet(source))
    ent, fra, status = read_enrichment(args.entities, args.phrases, args.status, news)
    byday, ranking, trend, phrases = aggregate_gold(news, ent, fra)
    key = hashlib.sha256(source.read_bytes()).hexdigest()[:16]
    # El hash del conjunto enriquecido identifica otro snapshot al variar resultados.
    for p in (args.entities,args.phrases,args.status):
        if p:
            key = hashlib.sha256((key + hashlib.sha256(p.read_bytes()).hexdigest()).encode()).hexdigest()[:16]
    root = project / 'local/cierre_datalake'
    root.mkdir(parents=True, exist_ok=True)
    folder = root / ('snapshot_id=' + key)
    # Escribir en temporal para evitar exponer resultados parciales.
    temp_folder = root / ('_tmp_snapshot_id=' + key)
    if folder.exists():
        existing = folder / 'reporte_cierre.json'
        outputs = ['news_by_day', 'entities_ranking', 'entity_trend_by_day', 'phrases_by_section']
        if not existing.is_file() or any(not (folder / name).is_dir() for name in outputs):
            raise RuntimeError('Snapshot previo incompleto. Revisar antes de reconstruir: ' + str(folder))
        print('SNAPSHOT YA EXISTENTE: se conserva sin añadir Parquet duplicados')
        print(existing.read_text(encoding='utf-8'))
        return 0
    if temp_folder.exists():
        import shutil
        shutil.rmtree(temp_folder)
    temp_folder.mkdir(parents=True, exist_ok=True)
    write_partitioned(byday, temp_folder, 'news_by_day', 'publication_date')
    write_partitioned(ranking, temp_folder, 'entities_ranking')
    write_partitioned(trend, temp_folder, 'entity_trend_by_day', 'publication_date')
    write_partitioned(phrases, temp_folder, 'phrases_by_section')
    # Comprobaciones sin mascarar datos de Comprehend incompletos
    pending = len(news) if status is None else len(news) - int(status.enrichment_status.astype(str).str.lower().eq('success').sum())
    report = {
        'resultado': 'VALIDADO_LOCAL_CON_PENDIENTES' if pending else 'VALIDADO_LOCAL',
        'fuente_silver': str(source.relative_to(project)),
        'noticias_vigentes': len(news),
        'filas_news_by_day': len(byday),
        'conteos_news_by_day': int(byday.total_noticias.sum()),
        'personas_organizaciones_lugares': len(ranking),
        'tendencias_entidades': len(trend),
        'frases_por_seccion': len(phrases),
        'noticias_sin_enriquecimiento_confirmado': pending,
        'origen_nlp': args.provenance or 'SIN_COMPREHEND',
        'cumple_comprehend_real': args.provenance == 'aws_comprehend' and pending == 0,
        'no_usa_aws': True,
        'snapshot': folder.name,
    }
    reportfile = temp_folder / 'reporte_cierre.json'
    reportfile.write_text(json.dumps(report,indent=2,ensure_ascii=False),encoding='utf-8')
    temp_folder.rename(folder)
    reportfile = folder / 'reporte_cierre.json'
    print(json.dumps(report,indent=2,ensure_ascii=False))
    print('Ruta reporte:',reportfile)
    if report['noticias_sin_enriquecimiento_confirmado']:
        print('ATENCIÓN: indicadores de entidades y frases SIN VALIDAR contra Comprehend real.',file=sys.stderr)
    return 0

if __name__ == '__main__':
    raise SystemExit(main())
