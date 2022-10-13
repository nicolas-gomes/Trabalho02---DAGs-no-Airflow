import pandas as pd
from airflow.decorators import dag, task
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime


NOME_ARQUIVO = '/tmp/tabela_unica.csv'
default_args = {
    'owner': "Nicolas",
    "depends_on_past": False,
    'start_date': datetime(2022, 10, 13)
}


@dag(default_args=default_args, schedule_interval='@once', catchup=False, tags=['Titanic'])
def dag_2():

    @task
    def resultado():
        NOME_TABELA = '/tmp/resultados.csv'
        df = pd.read_csv(NOME_ARQUIVO, sep=';')
        res = df.agg({'PassengerId': 'mean', 'Fare': 'mean', 'SibSp_Parch': 'mean'}).reset_index()
        print(res)
        res.to_csv(NOME_TABELA, index=False, sep=';')
        return NOME_TABELA

    fim = DummyOperator(task_id="fim")
    resultado_fim = resultado() 
    resultado_fim >> fim


execucao = dag_2()
