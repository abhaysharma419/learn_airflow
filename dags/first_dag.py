try:

    from datetime import timedelta
    from airflow import DAG
    from airflow.operators.python_operator import PythonOperator
    from datetime import datetime

    print("All Dag modules are ok ......")
except Exception as e:
    print("Error  {} ".format(e))

def first_function_execute(**kwargs):
    name = kwargs.get("name","key not found!")
    print(f"Hello world {name}")
    return "Hello World!!!!" +name

with DAG(
    dag_id="first_dag",
    schedule_interval="@daily",
    default_args={
        "owner":"abhay",
        "retries":1,
        "retry_delay":timedelta(minutes=5),
        "start_date":datetime(2023,1,21),
    },
    catchup=False
) as f:
    first_function_execute=PythonOperator(
        task_id="first_function_execute",
        python_callable= first_function_execute,
        op_kwargs={"name":"abhay"}
    )