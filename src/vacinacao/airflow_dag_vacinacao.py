# Esta DAG captura os dados do Elasticsearch do Datasus numa granularidade diaria, agrega os valores e armazena no bigquery
# Você pode usar o Sandbox do Bigquery. Basta informar o caminho de uma chave json da GCP na variável gcp_key
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import requests

requests.urllib3.disable_warnings()

schema = Variable.get("GBQ_SCHEMA")
table_vacinacao = Variable.get("GBQ_TABLE_VACINACAO")
gcp_key_info = Variable.get("GCP_KEY_INFO")
project_id = Variable.get("GBQ_PROJECT_ID")
gcp_key = "/opt/bitnami/airflow/dags/key/key.json"

default_args = {
    'owner': 'alexlopespereira',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}


def extract_vacinas_mes(**kwargs):
    from google.oauth2 import service_account
    from dateutil.relativedelta import relativedelta
    from elasticsearch import Elasticsearch
    from elasticsearch_dsl import Search
    import pandas as pd
    client = Elasticsearch('https://imunizacao-es.saude.gov.br:443', http_auth=("imunizacao_public", "qlto5t&7r_@+#Tlstigi"), verify_certs=False)
    inicio = kwargs['execution_date'] - relativedelta(days=1)
    fim = kwargs["execution_date"]
    schema = kwargs['schema']
    table = kwargs['table']
    project_id = kwargs['project_id']
    s = Search(using=client, index='').source(['vacina_dataAplicacao', 'vacina_codigo', 'vacina_numDose', 'estabelecimento_municipio_codigo']). \
        filter('range', vacina_dataAplicacao={'gte': inicio.strftime("%Y-%m-%d"), 'lt': fim.strftime("%Y-%m-%d")}). \
        filter('terms', vacina_numDose=['1', '2', '7', '38', '37', '9', '8']). \
        filter('terms', estabelecimento_municipio_codigo=['530010']). \
        filter('terms', status=['final'])
    series_agg = None
    print(s.to_dict())
    res = client.search(body=s.to_dict(), scroll='3m', size=5000)
    old_scroll_id = res['_scroll_id']
    while len(res['hits']['hits']):
        print(len(res['hits']['hits']))
        df = pd.DataFrame([r['_source'] for r in res['hits']['hits']])
        df['vacina_dataAplicacao'] = pd.to_datetime(df['vacina_dataAplicacao'], infer_datetime_format=True)
        # df['mes'] = df['vacina_dataAplicacao'].dt.to_period('M').dt.to_timestamp()
        series_i = df.groupby(['vacina_dataAplicacao', 'estabelecimento_municipio_codigo', 'vacina_codigo', 'vacina_numDose']).size()
        if series_agg is None:
            series_agg = series_i
        else:
            series_agg = series_i.add(series_agg, fill_value=0)

        res = client.scroll(scroll_id=old_scroll_id, scroll='2m')
        old_scroll_id = res['_scroll_id']
    df_agg = series_agg.reset_index().rename(columns={0: "qtd"})
    dag_run = kwargs['dag_run']
    if dag_run.get_previous_dagrun():
        operation = 'append'
    else:
        operation = 'replace'

    credentials = service_account.Credentials.from_service_account_file(gcp_key)
    df_agg.to_gbq(f"{schema}.{table}", project_id, chunksize=40000, if_exists=operation, credentials=credentials)


with DAG(
    dag_id='dag_vacinacao',
    default_args=default_args,
    start_date=datetime(2021, 11, 1), #TODO: Ajustar a data de início de acordo com seu interesse
    schedule_interval='@once', #TODO: Mudar para @daily quando estiver satisfeito com seu código
    catchup=True,
    tags=['vacinacao'],
) as dag:
    task1 = PythonOperator(
        task_id='task1',
        provide_context=True,
        python_callable=extract_vacinas_mes,
        op_kwargs={'schema': schema, 'table': table_vacinacao, 'project_id': project_id},
    )