from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow import DAG
from airflow.utils.dates import days_ago
# Import necessary Kubernetes client models
from kubernetes.client import models as k8s

from airflow.models.param import Param
#v0.0.11 - Using Shared Volume

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

# Define the Shared Volume and Mount details
# Match the 'name' here with the volumeMount 'name'
SHARED_VOLUME_NAME = "shared-volume"
# The name of your existing PersistentVolumeClaim
PVC_NAME = "kubeflow-shared-pvc"
# The path inside the container where the volume should be mounted
MOUNT_PATH = "/mounts/shared-volume/shared"
# Define a file within the shared volume for communication
OUTPUT_FILE_PATH = f"{MOUNT_PATH}/hello_output.txt"

# 1. Define the Volume using the PVC
volume = k8s.V1Volume(
    name=SHARED_VOLUME_NAME,
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name=PVC_NAME),
)

# 2. Define the Volume Mount
volume_mount = k8s.V1VolumeMount(
    name=SHARED_VOLUME_NAME, # Must match volume name
    mount_path=MOUNT_PATH,
    # sub_path=None, # Optional: Mount a sub-directory of the volume
    # read_only=False, # Default is False (allow writing)
)


dag = DAG(
    # Changed DAG ID slightly to avoid confusion
    "hello-world-shared-volume-dag",
    default_args=default_args,
    schedule_interval=None,
    tags=["ezaf", "shared-volume"], # Updated tags
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

# Task 1: Writes "hi" to the shared volume
write_task = KubernetesPodOperator(
    name="hello-writer",
    dag=dag,
    image="debian",
    cmds=["bash", "-cx"],
    # Ensure the directory exists and write "hi" to the output file
    arguments=[f'mkdir -p {MOUNT_PATH} && echo "hi" > {OUTPUT_FILE_PATH}'],
    labels={"app": "hello-writer"},
    task_id="write_hello_to_volume",
    do_xcom_push=False, # Disable XCom push
    # Add the volume and mount to this pod
    volumes=[volume],
    volume_mounts=[volume_mount],
    # Resources for the main container (keep these)
    container_resources={
        "requests": {"memory": "512Mi", "cpu": "250m"},
        "limits": {"memory": "1Gi", "cpu": "1"},
    },
    # No sidecar overrides needed as do_xcom_push=False
)

# Task 2: Reads "hi" from the shared volume
read_task = KubernetesPodOperator(
    name="hello-reader",
    dag=dag,
    image="debian",
    cmds=["bash", "-cx"],
    # Read the file from the shared volume and echo it
    arguments=[f'echo "Reading from shared volume:" && cat {OUTPUT_FILE_PATH}'],
    labels={"app": "hello-reader"},
    task_id="read_hello_from_volume",
    do_xcom_push=False, # Not pushing XCom here either
    # Add the *same* volume and mount to this pod
    volumes=[volume],
    volume_mounts=[volume_mount],
    # Define resources for this container too (adjust if needed)
    container_resources={
        "requests": {"memory": "128Mi", "cpu": "100m"},
        "limits": {"memory": "256Mi", "cpu": "200m"},
    },
)

# Define the task dependency
write_task >> read_task