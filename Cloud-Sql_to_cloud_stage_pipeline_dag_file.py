import datetime
import os
from airflow import DAG
from airflow.contrib.operators import dataproc_operator
from airflow import models
from airflow.contrib.operators.gcp_sql_operator import CloudSqlInstanceExportOperator


my_dag = DAG(
    dag_id="my_dag_name_1",
    start_date=datetime.datetime(2023, 1, 1),
    schedule="@once"
)

export_body = {
    "exportContext": {
        "fileType": "csv",
        "uri": "gs://source_flights_table/airports_stage.csv",
        "csvExportOptions": {
            "selectQuery":"select * from first_db.first_table"
        }
    }

}

sql_to_gcs = CloudSqlInstanceExportOperator(
    project_id= "stoneke-demo",
    body=export_body,
    instance= "mysql1",
    task_id='sql_export_task',
    dag=my_dag)

create_dataproc_cluster = dataproc_operator.DataprocClusterCreateOperator(
        task_id='create_dataproc_cluster',
        # Give the cluster a unique name by appending the date scheduled.
        # See https://airflow.apache.org/code.html#default-variables
        cluster_name='quickstart-cluster-{{ ds_nodash }}',
        project_id = 'stoneke-demo',
        num_workers=2,
        image_version='2.0',
        region='asia-east1',
        master_machine_type='n1-standard-2',
        worker_machine_type='n1-standard-2',
        dag=my_dag)


start_job = dataproc_operator.DataProcPySparkOperator (
        task_id = 'pys_job',
        cluster_name='quickstart-cluster-{{ ds_nodash }}',
        region='asia-east1',
        main = 'gs://source_flights_table/CloudSQL_TO_GCS_PY.py'
)


delete_dataproc_cluster = dataproc_operator.DataprocClusterDeleteOperator(
        task_id='delete_dataproc_cluster',
        cluster_name='quickstart-cluster-{{ ds_nodash }}',
        region='asia-east1')


sql_to_gcs >> create_dataproc_cluster >> start_job >> delete_dataproc_cluster
        
