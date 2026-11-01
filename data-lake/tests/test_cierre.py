import sys
import unittest
from pathlib import Path
import pandas as pd
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from cerrar_datalake_local import normalize_news, aggregate_gold


def news_fixture():
    return pd.DataFrame([
        {'news_id':'n1','publication_date':'2026-11-01','fuentes':'eltiempo_colombia,eltiempo_economia','text_hash':'hash1'},
        {'news_id':'n2','publication_date':'2026-11-02','fuentes':'eltiempo_economia','text_hash':'hash2'},
    ])

class CierreTests(unittest.TestCase):
    def test_rechaza_news_id_duplicados(self):
        x = news_fixture()
        x.loc[1, 'news_id'] = 'n1'
        with self.assertRaises(ValueError):
            normalize_news(x)

    def test_news_by_day_no_dobla_fuentes(self):
        day, *_ = aggregate_gold(normalize_news(news_fixture()), None, None)
        self.assertEqual(int(day.total_noticias.sum()), 3)
        self.assertEqual(len(day),3)

    def test_entidades_cuenta_noticias_unicas(self):
        n = normalize_news(news_fixture())
        e = pd.DataFrame([
            {'news_id':'n1','entity_type':'ORGANIZATION','entity_text':'Empresa X'},
            {'news_id':'n1','entity_type':'ORGANIZATION','entity_text':'Empresa X'},
            {'news_id':'n2','entity_type':'ORGANIZATION','entity_text':'Empresa X'},
        ])
        p = pd.DataFrame([
            {'news_id':'n1','phrase_text':'economía'},
            {'news_id':'n1','phrase_text':'economía'},
            {'news_id':'n2','phrase_text':'economía'},
        ])
        _, ranking, trend, phrase = aggregate_gold(n,e,p)
        self.assertEqual(int(ranking.total_noticias.iloc[0]),2)
        self.assertEqual(int(trend.total_noticias.sum()),2)
        self.assertEqual(int(phrase.total_noticias.sum()),3)

    def test_rechaza_fecha_invalida(self):
        x = news_fixture()
        x.loc[0, 'publication_date'] = '2026-02-30'
        with self.assertRaises(ValueError):
            normalize_news(x)

if __name__ == '__main__':
    unittest.main()
