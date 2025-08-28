# Import libraries
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig
from cosmos.profiles import RedshiftUserPasswordProfileMapping
from cosmos.constants import TestBehavior
from datetime import timedelta, datetime
from transactions_utils import *
from upload_s3 import load_csv_s3

md_path = "/opt/airflow/dags/template/metadata.json"
csv_path = "/opt/airflow/dags/template/transc_data"
items_csv = "/opt/airflow/dags/template/transc_data/items.csv"
num_ord = 100
today = datetime.now().strftime('%Y-%m-%d')
table_names = ["customers", "items", "datetable", "orders"]
connection_id = "my_rds_conn"
db_name = "dev"
schema_name = "public"
dbt_project_path = "/opt/airflow/dags/dbt/redshift_dbt_project"

profile_config = ProfileConfig(
    profile_name="redshift_dbt_project",
    target_name=db_name,
    profile_mapping=RedshiftUserPasswordProfileMapping(
        conn_id=connection_id,
        profile_args={"schema": schema_name},
    )
)

default_args = {
    "owner": "Akinkunmi",
    "email": ["olalekanrasaq1331@gmail.com"],
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="my_store_transactions",
    schedule=None,
    start_date=datetime(2025, 6, 24),
    template_searchpath=['/opt/airflow/dags/template'],
    default_args=default_args,
    description="A DAG to generate transactions data and load to S3 bucket and then to Redshift"
) as dag:

    # operators : Python Operator
    src_customers_data = PythonOperator(
        task_id="generate_customers_data",
        python_callable=create_customers_data,
        op_kwargs={"csv_path": csv_path},
    )

    src_items_data = PythonOperator(
        task_id="generate_items_data",
        python_callable=create_items_data,
        op_kwargs={"csv_path": csv_path},
    )

    src_datetable = PythonOperator(
        task_id="generate_datetable",
        python_callable=create_dateTable,
        op_kwargs={"num_ord": num_ord, "md_path": md_path, "csv_path": csv_path},
    )

    src_orders_data = PythonOperator(
        task_id="generate_orders_data",
        python_callable=create_orders_data,
        op_kwargs={"num_ord": num_ord, "md_path": md_path, 
                   "csv_path": csv_path, "items_csv": items_csv},
    )

    load_to_S3 = PythonOperator(
        task_id='upload_all_csvs',
        python_callable=load_csv_s3,
        op_kwargs={"local_folder":csv_path}
    )

    create_rds_tables = RedshiftDataOperator(
        task_id="create_redshift_table",
        database='dev',
        region_name='us-east-2',
        workgroup_name="my-workgroup",
        sql="sql/create_rds_table.sql",
        aws_conn_id='airflow_s3_conn'
    )

    load_rds_tasks = {}
    for table in table_names:
        load_rds_tasks[table] = S3ToRedshiftOperator(
            task_id=f"load_{table}_to_redshift",
            schema="public",
            table=table,
            verify=False,
            s3_bucket="aws-myairflow-bucket",
            s3_key=f"transc_data/{today}/{table}.csv",
            copy_options=["CSV", "IGNOREHEADER 1"],
            redshift_conn_id="my_rds_conn",
            aws_conn_id="airflow_s3_conn",
        )

    run_dbt_models = DbtTaskGroup(
        group_id="transform_data",
        project_config=ProjectConfig(dbt_project_path),
        profile_config=profile_config,
        default_args={"retries": 2},
    )



    # Task pipeline

    src_customers_data >> src_items_data >> src_datetable >> src_orders_data >> load_to_S3 >> create_rds_tables >> load_rds_tasks["customers"] >> load_rds_tasks["items"] >> load_rds_tasks["datetable"] >> load_rds_tasks["orders"] >> run_dbt_models
