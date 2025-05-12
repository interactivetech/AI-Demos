from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

from airflow.models.param import Param

# DAG arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "max_active_runs": 1,
}

# Volume that mounts the MLflow service account token
MLFLOW_TOKEN_VOLUME = "mlflow-sa-token"

token_volume = k8s.V1Volume(
    name=MLFLOW_TOKEN_VOLUME,
    projected=k8s.V1ProjectedVolumeSource(
        sources=[
            k8s.V1VolumeProjection(
                service_account_token=k8s.V1ServiceAccountTokenProjection(
                    path="mlflow-token",
                    expiration_seconds=3600,
                    audience="mlflow",
                )
            )
        ]
    ),
)

token_volume_mount = k8s.V1VolumeMount(
    name=MLFLOW_TOKEN_VOLUME,
    mount_path="/var/run/secrets/mlflow",
    read_only=True,
)

dag = DAG(
    "debug_mlflow_token",
    default_args=default_args,
    schedule_interval=None,
    tags=["ezaf", "mlflow"],
    render_template_as_native_obj=False,
    access_control={"All": {"can_read", "can_edit", "can_delete"}}
)

# Task: Print token contents
print_mlflow_token = KubernetesPodOperator(
    task_id="print_mlflow_token",
    name="print_mlflow_token_task",
    dag=dag,
    image="python:3.10-slim",  # Lightweight base image
    cmds=["bash", "-c"],
    arguments=[
        "echo 'Token Path: /var/run/secrets/mlflow/mlflow-token' && "
        "if [ -f /var/run/secrets/mlflow/mlflow-token ]; then "
        "echo 'Token Found' && "
        "head -c 30 /var/run/secrets/mlflow/mlflow-token && echo '...'; "
        "else echo 'Token not found'; fi"
    ],
    labels={"debug": "mlflow-token"},
    do_xcom_push=False,
    volumes=[token_volume],
    volume_mounts=[token_volume_mount],
    container_resources={
        "requests": {"memory": "64Mi", "cpu": "50m"},
        "limits": {"memory": "128Mi", "cpu": "100m"},
    }
)

print_mlflow_token
