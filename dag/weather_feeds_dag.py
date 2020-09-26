import datetime as dt
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

def notify_script1():
    print('Writing in file')
    with open('/path/to/your/repo/weather_feeds/log/notification.txt', 'a+', encoding='utf8') as f:
        now = dt.datetime.now()
        t = now.strftime("%Y-%m-%d %H:%M")
        f.write(str(t) + '\n')
        f.write(str('Weather Script 1 Data is Updated') + '\n')
    return 'Data is updated'

def notify_script2():
    print('Writing in file')
    with open('/path/to/your/repo/weather_feeds/log/notification.txt', 'a+', encoding='utf8') as f:
        now = dt.datetime.now()
        t = now.strftime("%Y-%m-%d %H:%M")
        f.write(str(t) + '\n')
        f.write(str('Weather Script 2 Data is Updated') + '\n')
    return 'Data is updated'

default_args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2020, 9, 19, 10, 00, 00),
    'concurrency': 1,
    'retries': 0
}

with DAG('weather_data_feeds_dag',
         default_args=default_args,
         schedule_interval='@hourly',
         ) as dag:

    opr_script1 = BashOperator(task_id='run_report1',
                             bash_command='/path/to/your/repo/weather_feeds/bin/run-script1.sh ')

    opr_notify1 = PythonOperator(task_id='notify_script1',
                             python_callable=notify_script1)

    opr_script2 = BashOperator(task_id='run_report2',
                             bash_command='/path/to/your/repo/weather_feeds/bin/run-script2.sh ')

    opr_notify2 = PythonOperator(task_id='notify_script2',
                             python_callable=notify_script2)

opr_script1 >> opr_notify1 >> opr_script2 >> opr_notify2
