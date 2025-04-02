# Data-Engineering/Airflow/hello_world_dag.py
#v0.0.4
from airflow import DAG
from airflow.models.param import Param
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)
# You might still use the sensor if you want to wait for the 'driver' pod
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import (
    SparkKubernetesSensor,
)
from airflow.utils.dates import days_ago

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
    # ----> New DAG ID <----
    "hello_world_dag",
    default_args=default_args,
    schedule_interval=None,
    tags=["ezaf", "kubernetes", "hello_world"], # Updated tags
    # Keep parameters if you want to use the same image vars
    params={
        "spark_image_url": Param(
            "gcr.io/mapr-252711/apache-spark:3.5.1-en2",
            type=["null", "string"],
            description="Provide Python-capable image url (using Spark image)",
        ),
        "spark_image_version": Param(
            "3.5.1",
            type=["null", "string"],
            description="Provide image Version tag",
        )
    },
    render_template_as_native_obj=True,
    access_control={"All": {"can_read", "can_edit", "can_delete"}},
)

submit_hello_world = SparkKubernetesOperator(
    task_id="submit_hello_world", # New task ID
    # ----> Point to the new YAML <----
    application_file="hello_world.yaml",
    # do_xcom_push=True, # Optional: get metadata
    delete_on_termination=False,
    dag=dag,
    enable_impersonation_from_ldap_user=True,
)

# # Optional: Monitor the 'SparkApplication' completion
# monitor_hello_world = SparkKubernetesSensor(
#     task_id="monitor_hello_world",
#     application_name="{{ task_instance.xcom_pull(task_ids='submit_hello_world')['metadata']['name'] }}",
#     dag=dag,
#     attach_log=True, # See the "Hello World!" output in Airflow logs
# )

# submit_hello_world >> monitor_hello_world # Define dependency