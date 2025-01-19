from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import pandas as pd
import os
from datetime import datetime
from sqlalchemy import create_engine

# Путь к файлам из переменной Airflow
PATH = Variable.get("my_path")

# Формируем строку подключения
connection_string = "postgresql://postgres:postgres@host.docker.internal:5432/dwh"

# Функция импорта данных
def import_data(table_name):
    file_path = os.path.join(PATH,  f"{table_name}.csv")

    # Проверяем, существует ли файл
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Файл {file_path} не существует.")

    # Читаем CSV файл
    df = pd.read_csv(file_path, delimiter=",")
    engine = create_engine(connection_string)
    df.to_sql(table_name, engine, schema="rd", if_exists="append", index=False)

default_args = {
    "owner": "irina",
    "start_date": datetime(2024, 2, 25),
    "retries": 2,
}

# Создаем DAG
with DAG(
    "import_rd_data",
    default_args=default_args,
    description="Загрузка данных в rd",
    catchup=False,
    # template_searchpath=[PATH],
    schedule="0 0 * * *",
) as dag:
    start = DummyOperator(task_id="start")

    import_rd_product = PythonOperator(
        task_id="import_rd_product",
        python_callable=import_data,
        op_kwargs={"table_name": "product"},
    )

    import_rd_deal = PythonOperator(
        task_id="import_rd_deal",
        python_callable=import_data,
        op_kwargs={"table_name": "deal_info"},
    )

    end = DummyOperator(task_id="end")

    (start >> [import_rd_product, import_rd_deal] >> end)
