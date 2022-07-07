from elasticsearch import Elasticsearch, helpers
from elasticsearch_dsl import Search
import pandas as pd
import requests

requests.urllib3.disable_warnings()


def extract_vacinas_mes(client, inicio, fim, operation):
    s = Search(using=client, index='').source(['vacina_dataAplicacao', 'vacina_codigo', 'vacina_numDose', 'estabelecimento_municipio_codigo']). \
        filter('range', vacina_dataAplicacao={'gte': inicio, 'lt': fim}). \
        filter('terms', vacina_numDose=['1', '2', '7', '38', '37', '9', '8']). \
        filter('terms', status=['final'])
    series_agg = None
    res = client.search(body=s.to_dict(), scroll='3m', size=1000)
    old_scroll_id = res['_scroll_id']
    while len(res['hits']['hits']):
        print(len(res['hits']['hits']))
        df = pd.DataFrame([r['_source'] for r in res['hits']['hits']])
        df['vacina_dataAplicacao'] = pd.to_datetime(df['vacina_dataAplicacao'], infer_datetime_format=True)
        df['mes'] = df['vacina_dataAplicacao'].dt.to_period('M').dt.to_timestamp()
        series_i = df.groupby(['mes', 'estabelecimento_municipio_codigo', 'vacina_codigo', 'vacina_numDose']).size()
        if series_agg is None:
            series_agg = series_i
        else:
            series_agg = series_i.add(series_agg, fill_value=0)

        res = client.scroll(scroll_id=old_scroll_id, scroll='2m')
        old_scroll_id = res['_scroll_id']
    df_agg = series_agg.reset_index()
    print(f"uploading: inicio={inicio}, fim={fim}, operation={operation}")
    return df_agg


def load_vacinas(dfv, dataset, table, project_id):
    dfv.to_gbq(f"{dataset}.{table}", project_id, chunksize=40000, if_exists=operation)


if __name__ == "__main__":
    dataset = 'competenciasgestao'
    table = 'municipio_mes_vacina'
    project_id = 'idp-mba'

    es = Elasticsearch('https://imunizacao-es.saude.gov.br:443', http_auth=("imunizacao_public", "qlto5t&7r_@+#Tlstigi"), verify_certs=False)
    tasks = [('2022-01-01', '2022-02-01', 'append'), ('2022-02-01', '2022-03-01', 'append'), ('2022-03-01', '2022-04-01', 'append'),
             ('2022-04-01', '2022-05-01', 'append'), ('2022-05-01', '2022-06-01', 'append'), ('2022-06-01', '2022-07-01', 'append')]

    for inicio, fim, operation in tasks:
        dfv = extract_vacinas_mes(es, inicio, fim, operation)
        load_vacinas(dfv, dataset, table, project_id)



