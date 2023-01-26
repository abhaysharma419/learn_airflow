try:

    from datetime import timedelta
    from airflow import DAG
    from airflow.operators.python_operator import PythonOperator
    from datetime import datetime

    print("All Dag modules are ok ......")
except Exception as e:
    print("Error  {} ".format(e))

def first_function_execute(**context):
    # name = kwargs.get("name","key not found!")
    context['ti'].xcom_push(key="newkey",value="first_function_execute data flow")
    # print(f"Hello world {name}")
    # return "Hello World!!!!" +name

def second_function_execute(**context):
    # name = kwargs.get("name","key not found!")
    var = context['ti'].xcom_pull(key="newkey")
    print(f"In second function now and got the data from first function as :{var}")
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
        provide_context=True,
        # op_kwargs={"name":"abhay"}
    )
    second_function_execute=PythonOperator(
        task_id="second_function_execute",
        python_callable= second_function_execute,
        provide_context=True,
        # op_kwargs={"name":"abhay"}
    )
first_function_execute >> second_function_execute