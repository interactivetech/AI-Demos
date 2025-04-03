# # 4/3/25 - THIS DOES NOT WORK, ISSUE WITH AIRFLOW XCOM 
# from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
# from airflow.operators.bash import BashOperator
# from airflow import DAG
# from airflow.utils.dates import days_ago
# # Need the k8s client models for pod_override
# from kubernetes.client import models as k8s

# from airflow.models.param import Param
# #v0.0.9

# default_args = {
#     "owner": "airflow",
#     "depends_on_past": False,
#     "start_date": days_ago(1),
#     "email": ["airflow@example.com"],
#     "email_on_failure": False,
#     "email_on_retry": False,
#     "max_active_runs": 1,
#     "retries": 0,
# }

# # ... (default_args, DAG definition, params remain the same) ...
# dag = DAG(
#     "hello-world-dag3", # Keep dag_id consistent
#     default_args=default_args,
#     schedule_interval=None,
#     tags=["ezaf", "xcom"],
#     params={ # Keep your params
#         "spark_image_url": Param(
#             "gcr.io/mapr-252711/apache-spark:3.5.1-en2",
#             type=["null", "string"], description="Provide Python-Spark image url",
#         ),
#         "spark_image_version": Param(
#             "3.5.1", type=["null", "string"], description="Provide Spark image Version",
#         )
#     },
#     render_template_as_native_obj=False,
#     access_control={"All": {"can_read", "can_edit", "can_delete"}},
# )

# # Task 1: Using xcom_sidecar_container_resources
# push_task = KubernetesPodOperator(
#     name="hello-world-dag3-pusher",
#     dag=dag,
#     image="debian",
#     cmds=["bash", "-cx"],
#     arguments=['echo "\\"hi\\"" > /airflow/xcom/return.json'],
#     labels={"app": "hello-world-pusher"},
#     task_id="push_hello",
#     do_xcom_push=True,
#     container_resources={ # Main container resources
#         "requests": {"memory": "512Mi", "cpu": "250m"},
#         "limits": {"memory": "1Gi", "cpu": "1"},
#     },
#     # Use the dedicated parameter with the k8s object
#     xcom_sidecar_container_resources=k8s.V1ResourceRequirements(
#         requests={"memory": "20Mi", "cpu": "10m"},
#         limits={"memory": "100Mi", "cpu": "100m"}, # Must include limits
#     )
#     # No pod_override needed here
# )

# # Task 2: BashOperator remains the same
# pull_task = BashOperator(
#     task_id="pull_hello",
#     bash_command="echo 'The first task said: {{ task_instance.xcom_pull(task_ids=\"push_hello\") }}'",
#     dag=dag,
# )

# push_task >> pull_task