#from google.cloud import dataproc_v1
import base64
import json
from google.cloud import storage
from google.cloud import bigquery
import pandas as pd
import os
os.system('ls -l')

def instantiate_workflow_template(event, context):

    try:
        #Parâmetros recebidos pelo pub/sub
        pubsub_message = json.loads(base64.b64decode(event['data']).decode('utf-8'))
        project_id = pubsub_message['project_id']
        region = pubsub_message['region']
        workflow_template = pubsub_message['workflow_template']
        parameters = pubsub_message['parameters']

        #1) CONSTANTES
        path_tb_SalesOrderID_Distintos = "gs://big-data-demonstration.appspot.com/RoxPartner/work/sells/tb_item1.csv/part-00000-62b3f23f-2d33-4d52-ad96-04786d8dc080-c000.csv"
        tb_SalesOrderID_Distintos = "RoxPartner.tb_SalesOrderID_Distintos"

        path_tb_products_top3 = "gs://big-data-demonstration.appspot.com/RoxPartner/work/sells/tb_item2.csv/part-00000-4d4a7dd2-e9d0-40bd-95f5-275926f8f3ab-c000.csv"
        tb_products_top3 = "RoxPartner.tb_products_top3"

        path_tb_sells = "gs://big-data-demonstration.appspot.com/RoxPartner/work/sells/tb_item3.csv/part-00000-f56d5ab1-cdfa-4b1b-a6df-7778ce7f60da-c000.csv"
        tb_sells = "RoxPartner.tb_sell_final"

        path_tb_produtos_dia = "gs://big-data-demonstration.appspot.com/RoxPartner/work/sells/tb_item4.csv/part-00000-d2a694a5-d5ef-4a03-95db-5ea93aeed62f-c000.csv"
        tb_produtos_dia = "RoxPartner.tb_produtos_dia"

        path_tb_due_september = "gs://big-data-demonstration.appspot.com/RoxPartner/work/sells/tb_item5.csv/part-00000-d68a1f01-095c-4814-a047-46928b4fec23-c000.csv"
        tb_due_september = "RoxPartner.tb_due_september"     

        #START SERVICES (BQ, Storage)
        storage_client = storage.Client()
        client = bigquery.Client()

        #2) Delete rows to not do duplicates
        tables_list = tb_SalesOrderID_Distintos+","+tb_products_top3+","+tb_sells+","+tb_produtos_dia+","+tb_due_september
        for tb in tables_list.split(","):
            QUERY = """DELETE FROM `big-data-demonstration."""+tb+"""` WHERE 1=1"""
            query_job = client.query(QUERY)
            query_job.result()

        #3) Load CSV from storage to BigQuery
        
        #tb_SalesOrderID_Distintos
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("Qtd_SalesOrderID_Distintos", "INTEGER")
            ],
            skip_leading_rows=1,
            source_format=bigquery.SourceFormat.CSV,
            field_delimiter="|",
        )
        load_job = client.load_table_from_uri(
            path_tb_SalesOrderID_Distintos, tb_SalesOrderID_Distintos, job_config=job_config
        )
        load_job.result()  # Waits for the job to complete.
        destination_table = client.get_table(tb_SalesOrderID_Distintos)  # Make an API request.
        print("Loaded {} rows.".format(destination_table.num_rows))

        #tb_products_top3
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("Name", "STRING"),
                bigquery.SchemaField("DaysToManufacture","INTEGER"),
                bigquery.SchemaField("Total_OrderQty", "INTEGER")
            ],skip_leading_rows=1,
            source_format=bigquery.SourceFormat.CSV,
            field_delimiter="|",
        )
        load_job = client.load_table_from_uri(
            path_tb_products_top3, tb_products_top3, job_config=job_config
        )
        load_job.result() # Waits for the job to complete.
        destination_table = client.get_table(tb_products_top3)
        print("Loaded {} rows".format(destination_table.num_rows))
        #tb_sells
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("ClientName", "STRING"),
                bigquery.SchemaField("Qtd_SalesOrderID", "INTEGER")
            ],
            skip_leading_rows=1,
            source_format=bigquery.SourceFormat.CSV,
            field_delimiter="|",
        )
        load_job = client.load_table_from_uri(
            path_tb_sells, tb_sells, job_config=job_config
        )
        load_job.result()  # Waits for the job to complete.
        destination_table = client.get_table(tb_sells)  # Make an API request.
        print("Loaded {} rows.".format(destination_table.num_rows))
        #tb_produtos_dia
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("ProductID", "INTEGER"),
                bigquery.SchemaField("OrderDate", "DATE"),
                bigquery.SchemaField("total_OrderQty", "INTEGER")
            ],
            skip_leading_rows=1,
            source_format=bigquery.SourceFormat.CSV,
            field_delimiter="|",
        )
        load_job = client.load_table_from_uri(
            path_tb_produtos_dia, tb_produtos_dia, job_config=job_config
        )
        load_job.result()  # Waits for the job to complete.
        destination_table = client.get_table(tb_produtos_dia)  # Make an API request.
        print("Loaded {} rows.".format(destination_table.num_rows))
        #tb_due_september
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("SalesOrderID", "INTEGER"),
                bigquery.SchemaField("OrderDate", "DATE"),
                bigquery.SchemaField("TotalDue", "FLOAT")
            ],
            skip_leading_rows=1,
            source_format=bigquery.SourceFormat.CSV,
            field_delimiter="|",
        )
        load_job = client.load_table_from_uri(
            path_tb_due_september, tb_due_september, job_config=job_config
        )
        load_job.result()  # Waits for the job to complete.
        destination_table = client.get_table(tb_due_september)  # Make an API request.
        print("Loaded {} rows.".format(destination_table.num_rows))
        print("Sucesso!")
    except Exception as e:
        print(e)