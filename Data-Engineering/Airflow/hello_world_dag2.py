from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.cncf.kubernetes.backcompat.pod import Resources

from airflow.models.param import Param
#v0.0.5

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
    "hello-world-dag2",
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

k = KubernetesPodOperator(
    name="hello-world-dag2",
    dag=dag,
    image="debian",
    cmds=["bash", "-cx"],
    arguments=["echo hi"],
    labels={"foo": "bar"},
    task_id="dry_run_demo",
    do_xcom_push=True,
    resources=Resources(
        request_memory="512Mi",
        request_cpu="250m",
        limit_memory="1Gi",
        limit_cpu="1"
    )
)

# k.dry_run()