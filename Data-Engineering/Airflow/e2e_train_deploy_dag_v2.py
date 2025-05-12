from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

from airflow.models.param import Param
import os
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
MLFLOW_TOKEN_VOLUME = "mlflow-sa-token"

volume = k8s.V1Volume(
    name=SHARED_VOLUME_NAME,
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name=PVC_NAME),
)

volume_mount = k8s.V1VolumeMount(
    name=SHARED_VOLUME_NAME,
    mount_path=MOUNT_PATH,
)

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
    "e2e_train_and_deploy_time_series_v2",
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
    access_control={"All": {"can_read", "can_edit", "can_delete"}}
)

# Task: Run Python script inside Kubernetes Pod
clean_and_merge_logs = KubernetesPodOperator(
    task_id="clean_and_merge_logs",
    name="clean_and_merge_logs_task",
    dag=dag,
    image="mendeza/python3.10-slim-airflow",  # Ensure it has 'requests', 'minio' installed
    cmds=["bash", "-cx"],
    arguments=[
        "python3 /mounts/shared-volume/shared/AI-Demos/Data-Analytics/Spark/scripts/clean_and_merge_logs.py"
    ],
    labels={"app": "minio-downloader"},
    do_xcom_push=False,
    volumes=[volume],
    working_dir="/tmp",
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

train_and_export_model = KubernetesPodOperator(
    task_id="train_and_export_model",
    name="train_and_export_model_task",
    dag=dag,
    image="mendeza/python3.10-slim-airflow",  # Same image, ensure it has tensorflow, mlflow, minio, matplotlib
    cmds=["bash", "-cx"],
    arguments=[
        "python3 /mounts/shared-volume/shared/AI-Demos/Data-Analytics/Spark/scripts/train_and_export_model.py"
    ],
    labels={"app": "model-trainer"},
    do_xcom_push=False,
    working_dir="/tmp",
    volumes=[volume, token_volume],
    volume_mounts=[volume_mount, token_volume_mount],
    container_resources={
        "requests": {"memory": "512Mi", "cpu": "500m"},
        "limits": {"memory": "8Gi", "cpu": "4"},
    },
    env_vars={
        "MINIO_ENDPOINT": "minio-api.ingress.pcai0108.sv11.hpecolo.net",
        "MINIO_ACCESS_KEY": "4JCA5L2jOci5eacIW24i",
        "MINIO_SECRET_KEY": "8ksfKLEoFOWcXAOGpq4oIRun96S9bvo0c6xOyxUA",
        "MLFLOW_TRACKING_URI": "http://mlflow.mlflow.svc.cluster.local:5000",
        "MLFLOW_TRACKING_TOKEN": os.getenv("MLFLOW_TRACKING_TOKEN", ""),  # Optionally mount this securely
    }
)

clean_and_merge_logs >> train_and_export_model
