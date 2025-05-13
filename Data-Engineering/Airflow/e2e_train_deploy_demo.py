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
                    expiration_seconds=36000,
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
    "e2e_train_and_deploy_time_series_demo",
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

# Task: Clean and merge logs
clean_and_merge_logs = KubernetesPodOperator(
    task_id="clean_and_merge_logs",
    name="clean_and_merge_logs_task",
    dag=dag,
    image="mendeza/python3.10-slim-airflow2",
    cmds=["bash", "-cx"],
    arguments=[
        "python3 /mounts/shared-volume/shared/AI-Demos/Data-Analytics/Spark/scripts/clean_and_merge_logs.py"
    ],
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

# Task: Train and export model
train_and_export_model = KubernetesPodOperator(
    task_id="train_and_export_model",
    name="train_and_export_model_task",
    dag=dag,
    image="mendeza/python3.10-slim-airflow2",
    cmds=["bash", "-cx"],
    arguments=[
        "python3 /mounts/shared-volume/shared/AI-Demos/Data-Analytics/Spark/scripts/train_and_export_model.py"
    ],
    labels={"app": "model-trainer"},
    do_xcom_push=False,
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
        "MLFLOW_TRACKING_TOKEN": "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJvZDR0MTIzbjhDdm9Vc0hCb2xpeEdTT2FvX3hYQUp1eXNwbi1IMmVoU0Y4In0.eyJleHAiOjE3NDcxNDcyNTgsImlhdCI6MTc0NzE0NTQ1OCwiYXV0aF90aW1lIjoxNzQ3MDU4OTM0LCJqdGkiOiIxMjhjMjdiMS05ZGM3LTQ0NDAtODgzOC04NjQ0YmFhMDVmZTkiLCJpc3MiOiJodHRwczovL2tleWNsb2FrLmluZ3Jlc3MucGNhaTAxMDguc3YxMS5ocGVjb2xvLm5ldC9yZWFsbXMvVUEiLCJzdWIiOiI5ZmRhMjVhZS1mY2Q0LTQ4NWQtYWIyNi0yNDMyNDdlNmY0YzMiLCJ0eXAiOiJCZWFyZXIiLCJhenAiOiJ1YSIsIm5vbmNlIjoiR25aaElIdkdnUnlfU0gzLWI0Qjhvam0wNjFDWWpnbUpEVmlmTmFzQnVuayIsInNlc3Npb25fc3RhdGUiOiJmZjc4ZmI1ZC0wMmQwLTRlOWUtOWViMS05ODkyNTgzOTBlODUiLCJhY3IiOiIxIiwic2NvcGUiOiJvcGVuaWQgZW1haWwgb2ZmbGluZV9hY2Nlc3Mgc3YxMXMxNzZyMTp1YSBwcm9maWxlIiwic2lkIjoiZmY3OGZiNWQtMDJkMC00ZTllLTllYjEtOTg5MjU4MzkwZTg1IiwidWlkIjoiMTAwMDAwMTciLCJlbWFpbF92ZXJpZmllZCI6ZmFsc2UsImdpZCI6IjEwMDEiLCJuYW1lIjoiQW5kcmV3IE1lbmRleiIsIm5hbWVzcGFjZSI6ImFuZHJldy1tZW5kZXotN2I5OGYyM2UiLCJncm91cHMiOlsidWEtZW5hYmxlZCIsIm9mZmxpbmVfYWNjZXNzIiwiYWRtaW4iLCJ1bWFfYXV0aG9yaXphdGlvbiIsImRlZmF1bHQtcm9sZXMtdWEiXSwicHJlZmVycmVkX3VzZXJuYW1lIjoiYW5kcmV3Lm1lbmRleiIsImdpdmVuX25hbWUiOiJBbmRyZXciLCJwb3NpeF91c2VybmFtZSI6ImFuZHJldy5tZW5kZXoiLCJmYW1pbHlfbmFtZSI6Ik1lbmRleiIsImVtYWlsIjoiYW5kcmV3Lm1lbmRlekBocGUuY29tIn0.CJcycvWzYFJ_BMwGL5vEo662Eo9ehgMcV8C4H4IAUrmaro4ZyiOgUGMFdpdZY_-b4aJGct9sjFsD4aH2N0Wk3ngth1oeZ0PsCHYi2jlAAAR_dKmRAVZ_TZNpvNz1RCtcst-_IDX8hsAac0RVt2dkmP7KzytTXAtK1_0vPJ-do9yFDFpCs-Qx5Pkbvq0nhKxrVNnCZbLusL7oHm5UkKPR8DKzPGEiRqbudPW96YlITcN6nz41DdsF4y4kuDoGwH3CKSOdfmSK7g4uxHVXfMdtS10GFO0IcKvSddg4ymqcVC9hXIdvyHiAL0rniO6wRmOyrEcLmntDxx6_Jdvw1qT97A",
    }
)

# Task: Deploy InferenceService
import textwrap

yaml_block = textwrap.dedent("""\
    apiVersion: "serving.kserve.io/v1beta1"
    kind: "InferenceService"
    metadata:
      name: "anomaly"
      namespace: andrew-mendez-7b98f23e
    spec:
      predictor:
        serviceAccountName: minio-sa
        tensorflow:
          storageUri: "s3://models2/anomaly_detection"
""")

deploy_inference_service = KubernetesPodOperator(
    task_id="deploy_inference_service",
    name="deploy_inference_service_task",
    dag=dag,
    image="bitnami/kubectl:latest",
    cmds=["bash", "-cx"],
    arguments=[
        f"""
        echo '{yaml_block}' | kubectl apply -f -
        echo "InferenceService 'anomaly' has been deployed."
        """
    ],
    get_logs=True,
    is_delete_operator_pod=True,
    in_cluster=True,
    service_account_name="minio-sa"
)

# Task order
clean_and_merge_logs >> train_and_export_model >> deploy_inference_service
