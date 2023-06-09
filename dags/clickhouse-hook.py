from airflow import DAG
from airflow_clickhouse_plugin.hooks.clickhouse_hook import ClickHouseHook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


def mysql_to_clickhouse():
    ch_hook = ClickHouseHook()
    ch_hook.run('SELECT count() FROM eventsgo')


with DAG(
        dag_id='clickhouse_hook',
        start_date=days_ago(2),
) as dag:
    dag >> PythonOperator(
        task_id='get_count_via_hook',
        python_callable=mysql_to_clickhouse,
    )