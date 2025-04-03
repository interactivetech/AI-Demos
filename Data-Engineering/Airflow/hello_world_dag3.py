from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
# Import BashOperator
from airflow.operators.bash import BashOperator
from airflow import DAG
from airflow.utils.dates import days_ago
# We don't strictly need k8s import if not using specific overrides
# from kubernetes.client import models as k8s

from airflow.models.param import Param
#v0.0.1

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "max_active_runs": 1,
    "retries": 0,
}

dag = DAG(
    "hello-world-dag3",
    default_args=default_args,
    schedule_interval=None,
    tags=["ezaf"],
    params={
        "spark_image_url": Param(
            "gcr.io/mapr-252711/apache-spark:3.5.1-en2",
            type=["null", "string"],
            description="Provide Python-Spark image url",
        ),
        "spark_image_version": Param(
            "3.5.1",
            type=["null", "string"],
            description="Provide Spark image Version",
        )
    },
    render_template_as_native_obj=False,
    access_control={"All": {"can_read", "can_edit", "can_delete"}},
)

# Task 1: KubernetesPodOperator that pushes "hi" to XCom
# Note the change in 'arguments'
push_task = KubernetesPodOperator(
    name="hello-world-dag2-pusher", # Good practice to have unique names
    dag=dag,
    image="debian",
    cmds=["bash", "-cx"],
    # Write the JSON string '"hi"' to the XCom file
    arguments=['echo "\\"hi\\"" > /airflow/xcom/return.json'],
    labels={"foo": "bar"},
    task_id="push_hello", # More descriptive task_id
    do_xcom_push=True,
    # Resources for the main container
    container_resources={
        "requests": {
            "memory": "512Mi",
            "cpu": "250m",
        },
        "limits": {
            "memory": "1Gi",
            "cpu": "1",
        },
    },
    # Using default resources for the XCom sidecar now
)

# Task 2: BashOperator that pulls the XCom value
pull_task = BashOperator(
    task_id="pull_hello",
    bash_command="echo 'The first task said: {{ task_instance.xcom_pull(task_ids=\'push_hello\') }}'",
    dag=dag,
)

# Define the task dependency
push_task >> pull_task

# k.dry_run() # dry_run is a method, not meant to be called directly here