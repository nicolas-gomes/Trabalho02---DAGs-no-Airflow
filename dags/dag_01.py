import pandas as pd
from airflow.decorators import dag, task
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

URL = "https://raw.githubusercontent.com/neylsoncrepalde/titanic_data_with_semicolon/main/titanic.csv"

default_args = {
    'owner': "Nicolas",
    "depends_on_past": False,
    'start_date': datetime(2022, 10, 13)
}


@dag(default_args=default_args, schedule_interval='@once', catchup=False, tags=['Titanic'])
def dag_01():

    @task
    def ingestao():
        NOME_DO_ARQUIVO = "/tmp/titanic.csv"
        df = pd.read_csv(URL, sep=';')
        df.to_csv(NOME_DO_ARQUIVO, index=False, sep=";")
        return NOME_DO_ARQUIVO

    @task
    def ind_passageiros_1(nome_do_arquivo):
        NOME_TABELA = "/tmp/passageiros_por_sexo_classe.csv"
        df = pd.read_csv(nome_do_arquivo, sep=";")
        res = df.groupby(['Sex', 'Pclass']).agg(
            {"PassengerId": "count"}).reset_index()
        print(res)
        res.to_csv(NOME_TABELA, index=False, sep=";")
        return NOME_TABELA

    @task
    def ind_passageiros_2(nome_do_arquivo):
        NOME_TABELA = "/tmp/preco_medio_por_sexo_classe.csv"
        df = pd.read_csv(nome_do_arquivo, sep=";")
        res = df.groupby(['Sex', 'Pclass']).agg(
            {'Fare': 'mean'}).reset_index()
        print(res)
        res.to_csv(NOME_TABELA, index=False, sep=";")
        return NOME_TABELA

    @task
    def ind_passageiros_3(nome_do_arquivo):
        NOME_TABELA = "/tmp/familiares_por_sexo_classe.csv"
        df = pd.read_csv(nome_do_arquivo, sep=";")
        df['SibSp_Parch'] = df['SibSp'] + df['Parch']
        res = df.groupby(['Sex', 'Pclass']).agg(
            {'SibSp_Parch': 'sum'}).reset_index()
        print(res)
        res.to_csv(NOME_TABELA, index=False, sep=";")
        return NOME_TABELA

    @task
    def ind_total(nome_do_arquivo_1, nome_do_arquivo_2, nome_do_arquivo_3):
        NOME_TABELA = "/tmp/tabela_unica.csv"
        df_01 = pd.read_csv(nome_do_arquivo_1, sep=';')
        df_02 = pd.read_csv(nome_do_arquivo_2, sep=';')
        df_03 = pd.read_csv(nome_do_arquivo_3, sep=';')
        res = df_01.merge(df_02, how='inner', on=['Sex', 'Pclass'])
        res = res.merge(df_03, how='inner', on=['Sex', 'Pclass'])
        print(res)
        res.to_csv(NOME_TABELA, index=False, sep=';')
        return NOME_TABELA

    ing = ingestao()
    ind_01 = ind_passageiros_1(ing)

    ind_02 = ind_passageiros_2(ing)

    ind_03 = ind_passageiros_3(ing)

    ind_total_final = ind_total(ind_01, ind_02, ind_03)

    triggerdag = TriggerDagRunOperator(
        task_id="dag_2",
        trigger_dag_id="dag_2"
    )

    fim = DummyOperator(task_id="fim")
    [ind_01, ind_02, ind_03] >> ind_total_final >> triggerdag >> fim


execucao = dag_01()
