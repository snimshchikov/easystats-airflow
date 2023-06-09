from airflow import DAG
from airflow.operators.email import EmailOperator
from airflow_clickhouse_plugin.sensors.clickhouse_sql_sensor import ClickHouseSqlSensor
from airflow.utils.dates import days_ago


with DAG(
        dag_id='listen_warnings',
        start_date=days_ago(2),
) as dag:
    ClickHouseSqlSensor(
        task_id='get_today_count',
        database='bots',
        sql="SELECT count() FROM eventsgo WHERE eventDate = '{{ ds }}'",
        success=lambda cnt: cnt > 10000,
    ) >> EmailOperator(
       task_id="send_email",
       to='snimshchikov.ilya@gmail.com',
       subject='Alert Mail',
       html_content=""" Mail Test """,
       dag=dag
    )