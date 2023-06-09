from airflow import DAG
from airflow_clickhouse_plugin.operators.clickhouse_operator import ClickHouseOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

with DAG(
        dag_id='update_count_dag',
        start_date=days_ago(2),
) as dag:
    ClickHouseOperator(
        task_id='update_count',
        database='bots',
        sql=(
            '''
                SELECT count() FROM eventsgo
                WHERE Timestamp BETWEEN
                    '{{ execution_date.start_of('month').to_date_string() }}'
                    AND '{{ execution_date.end_of('month').to_date_string() }}'
            ''',
            # result of the last query is pushed to XCom
        ),
        clickhouse_conn_id='clickhouse_test',
    ) >> PythonOperator(
        task_id='print_count',
        provide_context=True,
        python_callable=lambda task_instance, **_:
            # pulling XCom value and printing it
            print(task_instance.xcom_pull(task_ids='update_count')),
    )