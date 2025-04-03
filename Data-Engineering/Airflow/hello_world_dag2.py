from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.models.param import Param
#v0.0.6

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
    pod_override=k8s.V1Pod(
        spec=k8s.V1PodSpec(
            containers=[
                k8s.V1Container(
                    name="airflow-xcom-sidecar",
                    resources=k8s.V1ResourceRequirements(
                        requests={"memory": "16Mi", "cpu": "10m"},
                        limits={"memory": "64Mi", "cpu": "50m"},
                    ),
                )
            ]
        )
    )
)

# k.dry_run()