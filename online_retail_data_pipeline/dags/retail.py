#airflow
from airflow.decorators import dag, task
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.models.baseoperator import chain

#astro
from astro import sql as aql
from astro.files import File
from astro.sql.table import Table, Metadata
from astro.constants import FileType

# cosmos
from include.dbt.cosmos_config import DBT_PROJECT_CONFIG, DBT_CONFIG
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.constants import LoadMode
from cosmos.config import ProjectConfig, RenderConfig

from datetime import datetime


@dag(
    start_date=datetime.now(),
    schedule=None, # we want to trigger the dag manually
    catchup=False,
    tags=['retail'], # catogorise data pipelines/dags
)

def retail():

    #upload online_retail.csv file to GCP bucket
    upload_csv_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_csv_to_gcs', 
        src='include/data/online_retail.csv',
        dst='raw/online_retail.csv',
        bucket='online-retail-airflow-project-bucket',
        gcp_conn_id='gcp', # airflow connection 
        mime_type='text/csv',
    )

    #create dataset in BigQuery
    create_retail_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_retail_dataset',
        dataset_id='retail',
        gcp_conn_id='gcp',
    )

    #create table and load data
    gcs_to_raw = aql.load_file(
        task_id='gcs_to_raw',
        input_file=File(
            #'https://storage.cloud.google.com/online-retail-airflow-project-bucket/raw/online_retail.csv',
            'gs://online-retail-airflow-project-bucket/raw/online_retail.csv',
            conn_id='gcp',
            filetype=FileType.CSV,
        ),
        output_table=Table(
            name='raw_invoices', # BigQuery table name
            conn_id='gcp',
            metadata=Metadata(schema='retail') #dataset retail
        ),
        use_native_support=False,
    )

    # data quality checks 
    @task.external_python(python='/usr/local/airflow/soda_venv/bin/python')
    def check_load(scan_name='check_load', checks_subpath='sources'):
        from include.soda.check_function import check

        return check(scan_name, checks_subpath)
    
   
    # create fact and dim tables
    transform = DbtTaskGroup(
        group_id='transform',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['path:models/transform']
        )
    )

    # data quality checks for fact and dim tables
    @task.external_python(python='/usr/local/airflow/soda_venv/bin/python')
    def check_transform(scan_name='check_transform', checks_subpath='transform'):
        from include.soda.check_function import check

        return check(scan_name, checks_subpath)

    # run  analytics queries
    report = DbtTaskGroup(
            group_id='report',
            project_config=DBT_PROJECT_CONFIG,
            profile_config=DBT_CONFIG,
            render_config=RenderConfig(
                load_method=LoadMode.DBT_LS,
                select=['path:models/report']
            )
        )
    
    # data quality checks
    @task.external_python(python='/usr/local/airflow/soda_venv/bin/python')
    def check_report(scan_name='check_report', checks_subpath='report'):
        from include.soda.check_function import check

        return check(scan_name, checks_subpath)

    # specify dependencies between tasks 
    chain(
        upload_csv_to_gcs,
        create_retail_dataset,
        gcs_to_raw,
        check_load(),
        transform,
        check_transform(),
        report,
        check_report(),
    )

retail()