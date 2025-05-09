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

# Shared volume configuration
SHARED_VOLUME_NAME = "shared-volume"
PVC_NAME = "kubeflow-shared-pvc"
MOUNT_PATH = "/mounts/shared-volume/shared"
SCRIPT_PATH = f"{MOUNT_PATH}/AI-Demos/Data-Analytics/Spark/scripts/download_logs.py"

volume = k8s.V1Volume(
    name=SHARED_VOLUME_NAME,
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name=PVC_NAME),
)

volume_mount = k8s.V1VolumeMount(
    name=SHARED_VOLUME_NAME,
    mount_path=MOUNT_PATH,
)

dag = DAG(
    "download_minio_log_dag",
    default_args=default_args,
    schedule_interval=None,
    tags=["ezaf", "shared-volume"],
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
    access_control={"DAGs": "can_read", "can_edit", "can_delete"}},
)

# Task: Run Python script inside Kubernetes Pod
download_log = KubernetesPodOperator(
    task_id="download_log_from_minio",
    name="minio-downloader",
    dag=dag,
    image="python:3.10",  # Ensure the image has 'pip' and can install packages
    cmds=["python3", SCRIPT_PATH],
    labels={"app": "minio-downloader"},
    do_xcom_push=False,
    volumes=[volume],
    volume_mounts=[volume_mount],
    container_resources={
        "requests": {"memory": "128Mi", "cpu": "100m"},
        "limits": {"memory": "256Mi", "cpu": "200m"},
    },
    env_vars={
        "MINIO_ENDPOINT": "minio-api.ingress.pcai0108.sv11.hpecolo.net",
        "MINIO_ACCESS_KEY": "4JCA5L2jOci5eacIW24i",
        "MINIO_SECRET_KEY": "8ksfKLEoFOWcXAOGpq4oIRun96S9bvo0c6xOyxUA",
    }
)

download_log