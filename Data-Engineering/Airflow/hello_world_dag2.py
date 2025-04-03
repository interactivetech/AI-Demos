from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
#v0.0.1
k = KubernetesPodOperator(
    name="hello-world-dag2",
    image="debian",
    cmds=["bash", "-cx"],
    arguments=["echo", "10"],
    labels={"foo": "bar"},
    task_id="dry_run_demo",
    do_xcom_push=True,
)

k.dry_run()