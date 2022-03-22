from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator, PythonVirtualenvOperator
from airflow.operators.bash import BashOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.utils.dates import days_ago
from airflow.utils import timezone
from datetime import datetime, timedelta
import pendulum


seoul_tz = pendulum.timezone("Asia/Seoul")
TAGS_FOR_DAG = ['test', 'wjbyun']
default_args = {
    'owner': 'wjbyun',
    'start_date': datetime(2022, 2, 16, 15, 0, 0, tzinfo=seoul_tz),
    'tags': TAGS_FOR_DAG,
    'provide_context': True,
    'email_on_failure': True,
    'email': ['noti-airflow-aaaafupqxi4ijpuosssxjnmqpe@widerplanet.slack.com'],
}

with DAG(
    dag_id='hello_wjbyun',
    description='test',
    schedule_interval='20 * * * *',
    catchup=False,
    tags=TAGS_FOR_DAG,
    default_args=default_args
) as dag:

    cmd = f"""
    echo "hello"
    ls -lAh ~/
    echo ""
    """

    test_op = BashOperator(
        task_id='test_task',
        bash_command=cmd,
        dag=dag,
    )

    test_op 
