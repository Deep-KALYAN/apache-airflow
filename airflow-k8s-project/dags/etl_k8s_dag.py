from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from datetime import datetime
from kubernetes.client import models as k8s

volume = k8s.V1Volume(
    name="data-volume",
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
        claim_name="airflow-pvc"
    )
)

volume_mount = k8s.V1VolumeMount(
    name="data-volume",
    mount_path="/data"
)

default_args = {
    "start_date": datetime(2024, 1, 1)
}

with DAG(
    dag_id="etl_k8s_pipeline",
    schedule=None,
    catchup=False,
    default_args=default_args
) as dag:

    extract = KubernetesPodOperator(
        task_id="extract",
        name="extract-pod",
        namespace="default",
        image="your-dockerhub/airflow-etl:latest",
        cmds=["python", "/opt/scripts/extract.py"],
        volumes=[volume],
        volume_mounts=[volume_mount],
        get_logs=True,
        is_delete_operator_pod=True
    )

    transform = KubernetesPodOperator(
        task_id="transform",
        name="transform-pod",
        namespace="default",
        image="your-dockerhub/airflow-etl:latest",
        cmds=["python", "/opt/scripts/transform.py"],
        volumes=[volume],
        volume_mounts=[volume_mount],
        get_logs=True,
        is_delete_operator_pod=True
    )

    load = KubernetesPodOperator(
        task_id="load",
        name="load-pod",
        namespace="default",
        image="your-dockerhub/airflow-etl:latest",
        cmds=["python", "/opt/scripts/load.py"],
        volumes=[volume],
        volume_mounts=[volume_mount],
        get_logs=True,
        is_delete_operator_pod=True
    )

    extract >> transform >> load