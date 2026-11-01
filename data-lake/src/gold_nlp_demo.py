#!/usr/bin/env python3
"""Valida una demostración de Comprehend y genera indicadores Gold SOLO de DEMO.

NO consulta AWS, NO modifica Gold real ni mezcla demo_001 con noticias reales.
Requiere pandas y pyarrow instalados en el entorno local.
"""
import argparse
import hashlib
import json
from pathlib import Path

import pandas as pd

ENTITY_TYPES = {"PERSON", "ORGANIZATION", "LOCATION"}
SECCIONES = {
    "eltiempo_colombia": "Colombia",
    "eltiempo_economia": "Economía",
    "eltiempo_tecnologia": "Tecnología",
}


def require_columns(df, cols, name):
    missing = set(cols) - set(df.columns)
    if missing:
        raise ValueError(f"{name}: columnas ausentes {sorted(missing)}")


def normalized_text(s):
    return s.astype("string").str.strip().str.casefold()


def aggregate(news, entities, phrases):
    require_columns(news, ["news_id", "text_hash", "enrichment_status"], "news")
    require_columns(entities, ["news_id", "text_hash", "entity_type", "entity_text", "confidence"], "entities")
    require_columns(phrases, ["news_id", "text_hash", "phrase", "confidence"], "phrases")

    if news.empty or news.news_id.isna().any() or news.news_id.duplicated().any():
        raise ValueError("news demo: vacía, ID faltante o noticias duplicadas")
    if news.news_id.astype(str).str.strip().eq("").any():
        raise ValueError("news demo: identificador vacío")

    status = news.enrichment_status.astype(str).str.lower()
    if not status.eq("completed").all():
        raise ValueError("news demo: no todas las filas tienen enriquecimiento completado")

    valid_hashes = news.set_index("news_id")["text_hash"].astype(str).to_dict()
    for name, df in (("entities", entities), ("phrases", phrases)):
        if not set(df.news_id).issubset(valid_hashes):
            raise ValueError(f"{name}: hay referencias a noticias inexistentes")
        if any(str(r.text_hash) != valid_hashes[r.news_id]
               for r in df[["news_id", "text_hash"]].itertuples(index=False)):
            raise ValueError(f"{name}: hash distinto al texto de la noticia")
        score = pd.to_numeric(df.confidence, errors="coerce")
        if score.isna().any() or not score.between(0, 1).all():
            raise ValueError(f"{name}: confianza fuera de [0,1]")

    e = entities[entities.entity_type.isin(ENTITY_TYPES)].copy()
    e["entity_text"] = normalized_text(e["entity_text"])
    e = e[e.entity_text.notna() & e.entity_text.ne("")]
    e = e.drop_duplicates(["news_id", "entity_type", "entity_text"])

    p = phrases.copy()
    p["phrase"] = normalized_text(p["phrase"])
    p = p[p.phrase.notna() & p.phrase.ne("")]
    p = p.drop_duplicates(["news_id", "phrase"])

    ent_rank = (e.groupby(["entity_type", "entity_text"], as_index=False)
                .agg(total_noticias=("news_id", "nunique"))
                .sort_values(["total_noticias", "entity_type", "entity_text"],
                             ascending=[False, True, True]).reset_index(drop=True))
    phrase_rank = (p.groupby(["phrase"], as_index=False)
                   .agg(total_noticias=("news_id", "nunique"))
                   .sort_values(["total_noticias", "phrase"],
                                ascending=[False, True]).reset_index(drop=True))

    # Estos dos indicadores requieren metadatos que el archivo demo no trae.
    trend_cols = ["publication_date", "entity_type", "entity_text", "total_noticias"]
    section_cols = ["seccion", "phrase", "total_noticias"]
    trend = pd.DataFrame({c: pd.Series(dtype="int64" if c == "total_noticias" else "string")
                          for c in trend_cols})
    by_section = pd.DataFrame({c: pd.Series(dtype="int64" if c == "total_noticias" else "string")
                               for c in section_cols})

    has_date = "publication_date" in news.columns and (
        not news.publication_date.isna().any()
        and pd.to_datetime(news.publication_date, format="%Y-%m-%d", errors="coerce").notna().all()
    )
    has_section = "fuentes" in news.columns and news.fuentes.notna().all()

    if has_date:
        m = e.merge(news[["news_id", "publication_date"]], on="news_id", validate="many_to_one")
        trend = (m.groupby(["publication_date", "entity_type", "entity_text"], as_index=False)
                 .agg(total_noticias=("news_id", "nunique")))
    if has_section:
        rows = []
        for r in news[["news_id", "fuentes"]].itertuples(index=False):
            keys = {part.strip() for part in str(r.fuentes).split(",") if part.strip()}
            if not keys or keys - set(SECCIONES):
                raise ValueError(f"Sección/fuente no reconocida para {r.news_id}")
            for key in keys:
                rows.append({"news_id": r.news_id, "seccion": SECCIONES[key]})
        detail = pd.DataFrame(rows).drop_duplicates()
        j = p.merge(detail, on="news_id", validate="many_to_many")
        by_section = (j.groupby(["seccion", "phrase"], as_index=False)
                      .agg(total_noticias=("news_id", "nunique")))

    return ent_rank, phrase_rank, trend, by_section, has_date, has_section


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", type=Path, default=Path.cwd())
    args = parser.parse_args()
    project = args.project.resolve()
    source = project / "local/comprehend_demo"
    files = [source / f for f in ("news.parquet", "news_entities.parquet", "news_key_phrases.parquet")]
    for f in files:
        if not f.is_file():
            parser.error(f"Falta archivo: {f}")
    news, entities, phrases = [pd.read_parquet(f) for f in files]
    ent, phr, trend, by_section, has_date, has_section = aggregate(news, entities, phrases)

    h = hashlib.sha256()
    for f in files:
        h.update(f.read_bytes())
    snap = "snapshot_id=" + h.hexdigest()[:16]
    root = project / "local/gold_nlp_demo" / snap
    root.mkdir(parents=True, exist_ok=True)
    outputs = {
        "entities_ranking.parquet": ent,
        "phrases_ranking.parquet": phr,
        "entity_trend_by_day.parquet": trend,
        "phrases_by_section.parquet": by_section,
    }
    for filename, df in outputs.items():
        df.to_parquet(root / filename, index=False)

    report = {
        "tipo": "DEMOSTRACION_NO_REAL",
        "noticias_demo": len(news),
        "entidades_distintas_demo": len(ent),
        "frases_distintas_demo": len(phr),
        "registros_tendencia_por_fecha": len(trend),
        "registros_frases_por_seccion": len(by_section),
        "fecha_disponible_en_demo": bool(has_date),
        "seccion_disponible_en_demo": bool(has_section),
        "comprehend_60_noticias_reales": "PENDIENTE",
        "aws_llamadas": 0,
        "snapshot": snap,
    }
    (root / "reporte.json").write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    print("\nENTIDADES DEMO:\n", ent.to_string(index=False))
    print("\nFRASES DEMO:\n", phr.to_string(index=False))
    print("\nSalida:", root)
    if not has_date or not has_section:
        print("NOTA: No se generaron métricas por fecha/sección sin datos fuente; NO se inventaron valores.")


if __name__ == "__main__":
    main()
