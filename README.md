# Rox Partner
## Solução dos itens para os dados de uma empresa que produz bicicletas
### Tabela de conteúdos
=================
<!--ts-->
  * [Tecnologias utilizadas](#Tecnologias)
  * [Arquitetura GCP](#Arquitetura-GCP)
  * [Análise dos dados](#Análise-dos-dados)
    * [Item 1](#Item-1)
    * [Item 2](#Item-2)
    * [Item 3](#Item-3)
    * [Item 4](#Item-4)
    * [Item 5](#Item-5)
  * GCP
    * [Cloud Function](#Cloud-Function)
    * [Cloud Storage](#Cloud-Storage)
    * [Cloud Scheduler](#Cloud-Scheduler)
    * [Pub/Sub](#Pub/Sub)
    * [BigQuery](#BigQuery)
  * [Dataviz](https://datastudio.google.com/reporting/d12b4848-20ef-4386-8a28-5880ab9d54cf/page/myyMC) 
<!--te-->

### Tecnologias

As seguintes ferramentas foram usadas na resolução dos questionamentos:

- Jupyter
- Anaconda
- Apache Spark (Pyspark)
- Python
- Drawio
- Google Cloud Platform
- Data Studio

### Arquitetura GCP

![teste_rox_draw](https://user-images.githubusercontent.com/97997051/150478032-b8594496-e520-43ec-a7b2-d00b88ae03f6.jpeg)

### Análise dos dados
```python
# -*- coding:utf-8 -*-
from datetime import datetime, timedelta, date
from pyspark import SparkContext, SparkConf
import datetime as dt
from pyspark.sql import SparkSession, SQLContext
import pyspark.sql.functions as f
import pandas as pd
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from pyspark.sql.functions import concat, lit, col
from pyspark.sql.functions import *

#criando funçoes para o spark e sql

def cria_contexto_spark():
    global APP_NAME
    sparkSession = SparkSession.builder.appName(APP_NAME).getOrCreate()
    sc = SparkContext.getOrCreate() 
    return sc
def cria_contexto_sql(spark_context):
    sqlContext = SQLContext(spark_context) 
    return sqlContext

APP_NAME = "Teste_rox"
sc = cria_contexto_spark()
spark = cria_contexto_sql(sc)
```


#Dataset
```python
df_person_person = spark.read.csv("C:\\Users\manoe\\OneDrive\\Área de Trabalho\\TesteRox\\Engenheiro de Dados - CSV\\Person.Person.csv", sep=';', header="true")
df_production_product = spark.read.csv("C:\\Users\manoe\\OneDrive\\Área de Trabalho\\TesteRox\\Engenheiro de Dados - CSV\\Production.Product.csv", sep=';', header="true")
df_sales_customer = spark.read.csv("C:\\Users\manoe\\OneDrive\\Área de Trabalho\\TesteRox\\Engenheiro de Dados - CSV\\Sales.Customer.csv", sep=';', header="true")
df_sales_detail = spark.read.csv("C:\\Users\manoe\\OneDrive\\Área de Trabalho\\TesteRox\\Engenheiro de Dados - CSV\\Sales.SalesOrderDetail.csv", sep=';', header="true")
df_sales_header = spark.read.csv("C:\\Users\manoe\\OneDrive\\Área de Trabalho\\TesteRox\\Engenheiro de Dados - CSV\\Sales.SalesOrderHeader.csv", sep=';', header="true")
df_sales_special_offer = spark.read.csv("C:\\Users\manoe\\OneDrive\\Área de Trabalho\\TesteRox\\Engenheiro de Dados - CSV\\Sales.SpecialOfferProduct.csv", sep=';', header="true")
```

# Item 1
### 1.	Escreva uma query que retorna a quantidade de linhas na tabela Sales.SalesOrderDetail pelo campo SalesOrderID, desde que tenham pelo menos três linhas de detalhes.
Após um printSchema, percebe-se que todas as colunas estão definidas como String. 
Converter para Integer a coluna "SalesOrderDetailID" para tornar possivel operaçoes numéricas
```python
df_sales_detail = df_sales_detail.withColumn("SalesOrderDetailID", df_sales_detail.SalesOrderDetailID.cast('integer'))
```
Registrar como tabela para executar Queries em SQL
```python
spark.registerDataFrameAsTable(df_sales_detail, "df_sales_detail")
```
Consulta em SQL a quantidade de números distintos de SalesOrderID onde a coluna SalesOrderDetailID seja maior ou igual a 3.
```python
df_qtd_salesorderid_distincts = spark.sql("""SELECT
                count(distinct SalesOrderID) AS Qtd_SalesOrderID_Distintos
            FROM df_sales_detail
            WHERE SalesOrderDetailID >= 3
            """)
```
Path para salvar
```python
df_qtd_salesorderid_distincts.write.format("csv").option("header", "true").save("C:\\Users\\manoe\\OneDrive\\Área de Trabalho\\\EngDados\\roxpartner\\tb_item1.csv",sep="|", encoding="utf-8")
```


```python
df_qtd_salesorderid_distincts.show()

    +--------------------------+
    |Qtd_SalesOrderID_Distintos|
    +--------------------------+
    |                     31465|
    +--------------------------+
    
```
# Item 2 
### 2.	Escreva uma query que ligue as tabelas Sales.SalesOrderDetail, Sales.SpecialOfferProduct e Production.Product e retorne os 3 produtos (Name) mais vendidos (pela soma de OrderQty), agrupados pelo número de dias para manufatura (DaysToManufacture).
Passo 1:
Registrar DF´s como tabela para executar Queries em SQL
```python
spark.registerDataFrameAsTable(df_sales_special_offer, "df_sales_special_offer")
spark.registerDataFrameAsTable(df_production_product, "df_production_product")
```
Pegar os ProductID em ofertas e acrescentar a coluna "Name" e "DaysToManufacture" 
através de um Left Join com a tabela df_production_product com estratégia  a.ProductID = b.ProductID
```python
df_sales_offer_names = spark.sql("""SELECT 
        b.Name, 
        b.DaysToManufacture,
        a.ProductID
    FROM df_sales_special_offer AS a
    LEFT JOIN df_production_product AS b
    ON a.ProductID = b.ProductID
    """)
```
Registrar novamente o DF como tabela para executar Queries em SQL
```python
spark.registerDataFrameAsTable(df_sales_offer_names, "df_sales_offer_names")
```


```python
df_sales_offer_names.show()


    +--------------------+-----------------+---------+
    |                Name|DaysToManufacture|ProductID|
    +--------------------+-----------------+---------+
    |HL Road Frame - B...|                1|      680|
    |HL Road Frame - R...|                1|      706|
    |Sport-100 Helmet,...|                0|      707|
    |Sport-100 Helmet,...|                0|      708|
    |Mountain Bike Soc...|                0|      709|
    |Mountain Bike Soc...|                0|      710|
    |Sport-100 Helmet,...|                0|      711|
    |        AWC Logo Cap|                0|      712|
    |Long-Sleeve Logo ...|                0|      713|
    |Long-Sleeve Logo ...|                0|      714|
    |Long-Sleeve Logo ...|                0|      715|
    |Long-Sleeve Logo ...|                0|      716|
    |HL Road Frame - R...|                1|      717|
    |HL Road Frame - R...|                1|      718|
    |HL Road Frame - R...|                1|      719|
    |HL Road Frame - R...|                1|      720|
    |HL Road Frame - R...|                1|      721|
    |LL Road Frame - B...|                1|      722|
    |LL Road Frame - B...|                1|      723|
    |LL Road Frame - B...|                1|      724|
    +--------------------+-----------------+---------+
    only showing top 20 rows
    
    


```
Passo 2:
converter a coluna OrderQty para int,pois será posteriormente usada operaçao de soma 
```python
df_sales_detail = df_sales_detail.withColumn("OrderQty", df_sales_detail.OrderQty.cast('integer'))
```
Registrar como tabela para executar Queries em SQL
```python
spark.registerDataFrameAsTable(df_sales_detail, "df_sales_detail")
```
top 3 de ProductID com maiores números de OrderQty
```python
df_top3_orderQty = spark.sql("""SELECT 
        ProductID,
        SUM(OrderQty) as Total_OrderQty
    FROM df_sales_detail
    GROUP BY ProductID
    ORDER BY Total_OrderQty DESC
    LIMIT 3""")
spark.registerDataFrameAsTable(df_top3_orderQty, "df_top3_orderQty")
```


```python
df_top3_orderQty.show()


    +---------+--------------+
    |ProductID|Total_OrderQty|
    +---------+--------------+
    |      712|          8311|
    |      870|          6815|
    |      711|          6743|
    +---------+--------------+
    
```    


Passo3: 
Selectionar as colunas "Name" e "DaysToManufacture" da tabela df_sales_offer_names, 
Juntar com a coluna "Total_OrderQty" da tabela df_top3_orderQty por meio de um Left Join, onde "a.ProductID = b.ProductID"
Para retornar os 3 produtos com maior "Total_OrderQty"

```python
df_products_top3 = spark.sql("""SELECT 
        b.Name,
        b.DaysToManufacture,
        a.Total_OrderQty
    FROM df_top3_orderQty AS a
    LEFT JOIN df_sales_offer_names AS b
    ON a.ProductID = b.ProductID
    GROUP BY Name, DaysToManufacture, Total_OrderQty
    ORDER BY Total_OrderQty DESC
    """)
    
df_products_top3.write.format("csv").option("header", "true").save("C:\\Users\\manoe\\OneDrive\\Área de Trabalho\\\EngDados\\roxpartner\\tb_item2.csv",sep="|", encoding="utf-8")
```


```python
df_products_top3.show(truncate = False)

    +----------------------+-----------------+--------------+
    |Name                  |DaysToManufacture|Total_OrderQty|
    +----------------------+-----------------+--------------+
    |AWC Logo Cap          |0                |8311          |
    |Water Bottle - 30 oz. |0                |6815          |
    |Sport-100 Helmet, Blue|0                |6743          |
    +----------------------+-----------------+--------------+
    
 ```

# Item 3 
### 3.	Escreva uma query ligando as tabelas Person.Person, Sales.Customer e Sales.SalesOrderHeader de forma a obter uma lista de nomes de clientes e uma contagem de pedidos efetuados.


Passo 1:
Registrar DF´s como tabela para executar Queries em SQL
```python
spark.registerDataFrameAsTable(df_person_person, "df_person_person")
spark.registerDataFrameAsTable(df_sales_customer, "df_sales_customer")
spark.registerDataFrameAsTable(df_sales_header, "df_sales_header")
```
Considerando que BussinessEntityID do df_person_person é igual a CustomerID do df_sales_customer
```python
df_name_id = spark.sql("""SELECT 
        a.FirstName,
        a.MiddleName,
        a.LastName,
        b.CustomerID
    FROM df_sales_customer AS b
    LEFT JOIN df_person_person AS a
    ON a.BusinessEntityID = b.CustomerID
    """)
```
Registrar o df como tabela para executar Queries em SQL
```python
spark.registerDataFrameAsTable(df_name_id, "df_name_id")
```


```python
df_sell = spark.sql("""SELECT 
        a.FirstName,
        a.MiddleName,
        a.LastName,
        b.SalesOrderID   
    FROM df_sales_header AS b
    LEFT JOIN df_name_id AS a
    ON a.CustomerID = b.CustomerID
    WHERE FirstName is not Null
    """)
    
```
concatenar as colunas"FirstName","MiddleName","LastName" para gerar uma nova coluna "ClientName" 
e dropo as colunas antigas.
```python
df1 = df_sell.select("*", concat(col("FirstName"),lit(" "),col("MiddleName")).alias("New_col"))
df1 = df1.select("*", concat(col("New_col"),lit(" "),col("LastName")).alias("ClientName"))
```
Dropando as colunas antigas
```python
df1 = df1.drop("FirstName","MiddleName","LastName","New_col")
```
Remove os MiddleName "NULL" e troca por '' (espaço vazio) e Registrar DF como tabela para executar Queries em SQL
```python
df_sell_final = df1.withColumn('ClientName', regexp_replace('ClientName', 'NULL', ''))
spark.registerDataFrameAsTable(df_sell_final, "df_sell_final")
```
adquirindo as SalesOrderID diferentes e armazenando em Qtd_SalesOrderID.
```python
df_sell_final = spark.sql("""SELECT 
        ClientName,
        COUNT(Distinct SalesOrderID) AS Qtd_SalesOrderID
    FROM df_sell_final
    GROUP BY ClientName
    ORDER BY Qtd_SalesOrderID DESC
    """)

df_sell_final.write.format("csv").option("header", "true").save("C:\\Users\\manoe\\OneDrive\\Área de Trabalho\\\EngDados\\roxpartner\\tb_item3.csv",sep="|", encoding="utf-8")
```


```python
df_sell_final.show(10)


    +----------------+----------------+
    |      ClientName|Qtd_SalesOrderID|
    +----------------+----------------+
    | Morgan C Miller|              28|
    |Jennifer  Taylor|              28|
    |   Grace R Lewis|              27|
    | Marshall M Shen|              27|
    | Ruben B Vazquez|              27|
    |     Grace J Lee|              27|
    |   Morgan  Lewis|              27|
    |Isabella B Moore|              27|
    |Morgan P Jackson|              27|
    |Natalie M Martin|              27|
    +----------------+----------------+
    only showing top 10 rows
    
```    

# Item 4 
### 4.	Escreva uma query usando as tabelas Sales.SalesOrderHeader, Sales.SalesOrderDetail e Production.Product, de forma a obter a soma total de produtos (OrderQty) por ProductID e OrderDate.

passando o DataType da coluna OrderDate de String para Date
```python
df_sales_header = df_sales_header.withColumn("OrderDate", df_sales_header["OrderDate"].cast("date"))
```
passando o DataType da coluna OrderQty de String para integer
```python
df_sales_detail = df_sales_detail.withColumn("OrderQty",df_sales_detail.OrderQty.cast("integer"))
```
passando o DataType da coluna ProductID de String para integer
```python
df_sales_detail = df_sales_detail.withColumn("ProductID",df_sales_detail["ProductID"].cast("integer"))
```
Registra como tabela para executar Queries em sql
```python
spark.registerDataFrameAsTable(df_sales_header,"df_sales_header")
spark.registerDataFrameAsTable(df_sales_detail,"df_sales_detail")
spark.registerDataFrameAsTable(df_production_product,"df_production_product")
```

Fazer um join através da coluna em comum da df_sales_detail e df_sales_header, no caso a coluna "SalesOrderID"
```python
#df_sales_detail >>> SalesOrderID  OrderQty ProductID 
#df_sales_header >>> SalesOrderID  OrderDate
#df_production_pr>>>                        ProductID
df_total_produtos = spark.sql("""SELECT
        a.SalesOrderID,
        a.OrderQty,
        b.OrderDate,
        a.ProductID
    FROM df_sales_detail AS a 
    LEFT JOIN df_sales_header AS b
    ON a.SalesOrderID = b.SalesOrderID
    """)
```
Registra como tabela para executar Queries em sql
```python
spark.registerDataFrameAsTable(df_total_produtos,"df_total_produtos")
```


```python
df_total_produtos.show(1)

    +------------+--------+----------+---------+
    |SalesOrderID|OrderQty| OrderDate|ProductID|
    +------------+--------+----------+---------+
    |       43659|       1|2011-05-31|      776|
    +------------+--------+----------+---------+
    only showing top 1 row
    
```    

Fazer um join através da coluna ProductID dos df_total_produtos e df_production_product
```python
df_produtos_dia = spark.sql("""SELECT
        b.ProductID,
        a.OrderDate, 
        SUM(a.OrderQty) AS total_OrderQty
    FROM df_total_produtos AS a
    LEFT JOIN df_production_product AS b
    ON  a.ProductID = b.ProductID
    GROUP BY b.ProductID, a.OrderDate  
    ORDER BY a.OrderDate, ProductID
     
    """)
df_produtos_dia.write.format("csv").option("header", "true").save("C:\\Users\\manoe\\OneDrive\\Área de Trabalho\\\EngDados\\roxpartner\\tb_item4.csv",sep="|", encoding="utf-8")
```


```python
df_produtos_dia.show()

    +---------+----------+--------------+
    |ProductID| OrderDate|total_OrderQty|
    +---------+----------+--------------+
    |      707|2011-05-31|            24|
    |      708|2011-05-31|            27|
    |      709|2011-05-31|            38|
    |      710|2011-05-31|             5|
    |      711|2011-05-31|            33|
    |      712|2011-05-31|            40|
    |      714|2011-05-31|            16|
    |      715|2011-05-31|            49|
    |      716|2011-05-31|            19|
    |      722|2011-05-31|             8|
    |      725|2011-05-31|            15|
    |      726|2011-05-31|             9|
    |      729|2011-05-31|            16|
    |      730|2011-05-31|            14|
    |      732|2011-05-31|            16|
    |      733|2011-05-31|             4|
    |      738|2011-05-31|            19|
    |      741|2011-05-31|             2|
    |      742|2011-05-31|             3|
    |      743|2011-05-31|             1|
    +---------+----------+--------------+
    only showing top 20 rows
    
```    

# Item 5
### 5.	Escreva uma query mostrando os campos SalesOrderID, OrderDate e TotalDue da tabela Sales.SalesOrderHeader. Obtenha apenas as linhas onde a ordem tenha sido feita durante o mês de setembro/2011 e o total devido esteja acima de 1.000. Ordene pelo total devido decrescente.

```python
df_sales_header = df_sales_header.withColumn("SalesOrderID",df_sales_header["SalesOrderID"].cast("integer"))
df_sales_header = df_sales_header.withColumn("OrderDate",df_sales_header.OrderDate.cast("date"))
```
#Substituindo as "," (vírgulas) da TotalDue por "." (ponto), para ser possivel passar o DataType para float
```python
df_sales_header = df_sales_header.withColumn('TotalDue', regexp_replace('TotalDue', ',', '.'))
df_sales_header = df_sales_header.withColumn("TotalDue",df_sales_header["TotalDue"].cast("float"))
spark.registerDataFrameAsTable(df_sales_header,"df_sales_header")
```

Pegando o range de datas (OderDate) entre 01/setembro/2011 e 30/setembro/2011, com "TotalDue" superior a 1000

```python
df_due_september = spark.sql("""SELECT
        SalesOrderID,
        OrderDate,
        TotalDue
    FROM df_sales_header 
    WHERE (OrderDate BETWEEN '2011-09-01'AND '2011-09-30') AND TotalDue > 1000
    ORDER BY TotalDue DESC
    
        """)
df_due_september.write.format("csv").option("header", "true").save("C:\\Users\\manoe\\OneDrive\\Área de Trabalho\\\EngDados\\roxpartner\\tb_item5.csv",sep="|", encoding="utf-8")
```


```python
df_due_september.show()

   +------------+----------+---------+
   |SalesOrderID| OrderDate| TotalDue|
   +------------+----------+---------+
   |       44348|2011-09-07|3953.9883|
   |       44372|2011-09-09|3953.9883|
   |       44349|2011-09-07|3953.9883|
   |       44350|2011-09-07|3953.9883|
   |       44371|2011-09-09|3953.9883|
   |       44351|2011-09-07|3953.9883|
   |       44328|2011-09-02|3953.9883|
   |       44352|2011-09-07|3953.9883|
   |       44330|2011-09-02|3953.9883|
   |       44332|2011-09-03|3953.9883|
   |       44370|2011-09-09|3953.9883|
   |       44357|2011-09-07|3953.9883|
   |       44338|2011-09-04|3953.9883|
   |       44358|2011-09-07|3953.9883|
   |       44340|2011-09-04|3953.9883|
   |       44359|2011-09-08|3953.9883|
   |       44344|2011-09-06|3953.9883|
   |       44360|2011-09-08|3953.9883|
   |       44347|2011-09-06|3953.9883|
   |       44361|2011-09-08|3953.9883|
   +------------+----------+---------+
   only showing top 20 rows

    
    
```

```python

```
### Cloud Function

```python
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
```
Paths dos DF's no google storage e no big query
```python
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
```
Iniciando os services storage e bq
```python
        storage_client = storage.Client()
        client = bigquery.Client()
```
Deletando linhas tabela para nao dar append
```python
        tables_list = tb_SalesOrderID_Distintos+","+tb_products_top3+","+tb_sells+","+tb_produtos_dia+","+tb_due_september
        for tb in tables_list.split(","):
            QUERY = """DELETE FROM `big-data-demonstration."""+tb+"""` WHERE 1=1"""
            query_job = client.query(QUERY)
            query_job.result()
```
 Load CSV do storage para o big query
```python
```
 tb_SalesOrderID_Distintos. passando schema, csv, delimitador "|"
```python        
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
 ```
 tb_products_top3
 ```python
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
 ```       
   tb_sells
 ```python
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
  ```      
   tb_produtos_dia
  ```python
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
  ```      
   tb_due_september
  ```python
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
```

![CloudFunctions](https://user-images.githubusercontent.com/97997051/150527518-761262db-f9ba-4809-99d4-b7936020e042.jpeg)

### Cloud Storage
![Storage](https://user-images.githubusercontent.com/97997051/150527579-f3b8092b-2fdd-43a4-873a-aaa3fdce0a1c.jpeg)

### Cloud Scheduler
![CloudScheduler](https://user-images.githubusercontent.com/97997051/150527627-d87fe9c3-bf9d-42df-a565-e72647f73458.jpeg)

### Pub/Sub
![PubSub](https://user-images.githubusercontent.com/97997051/150527662-9f3a7d8d-edf1-440b-85cb-a73c62c880c7.jpeg)

### BigQuery
![BigQuery](https://user-images.githubusercontent.com/97997051/150527784-9723b902-e63b-459f-9f13-4de5e3123f2c.jpeg)

### Dataviz
![image](https://user-images.githubusercontent.com/97997051/150480262-ca839553-ae16-43e0-a446-d74de79d08e7.png)

