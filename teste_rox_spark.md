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

#declarando funçoes para o spark e sql

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

    C:\spark\spark\python\pyspark\sql\context.py:77: FutureWarning: Deprecated in 3.0.0. Use SparkSession.builder.getOrCreate() instead.
      warnings.warn(
    


```python
#Datasets
df_person_person = spark.read.csv("C:\\Users\manoe\\OneDrive\\Área de Trabalho\\TesteRox\\Engenheiro de Dados - CSV\\Person.Person.csv", sep=';', header="true")
df_production_product = spark.read.csv("C:\\Users\manoe\\OneDrive\\Área de Trabalho\\TesteRox\\Engenheiro de Dados - CSV\\Production.Product.csv", sep=';', header="true")
df_sales_customer = spark.read.csv("C:\\Users\manoe\\OneDrive\\Área de Trabalho\\TesteRox\\Engenheiro de Dados - CSV\\Sales.Customer.csv", sep=';', header="true")
df_sales_detail = spark.read.csv("C:\\Users\manoe\\OneDrive\\Área de Trabalho\\TesteRox\\Engenheiro de Dados - CSV\\Sales.SalesOrderDetail.csv", sep=';', header="true")
df_sales_header = spark.read.csv("C:\\Users\manoe\\OneDrive\\Área de Trabalho\\TesteRox\\Engenheiro de Dados - CSV\\Sales.SalesOrderHeader.csv", sep=';', header="true")
df_sales_special_offer = spark.read.csv("C:\\Users\manoe\\OneDrive\\Área de Trabalho\\TesteRox\\Engenheiro de Dados - CSV\\Sales.SpecialOfferProduct.csv", sep=';', header="true")
```

### Ítem 1 ###


```python
#Após um printSchema, percebe-se que todas as colunas estão definidas como String
#Converter para Integer a coluna "SalesOrderDetailID" para tornar possivel operaçoes matemáticas
df_sales_detail = df_sales_detail.withColumn("SalesOrderDetailID", df_sales_detail.SalesOrderDetailID.cast('integer'))

#Registrar como Tabela para executar comandos SQL
spark.registerDataFrameAsTable(df_sales_detail, "df_sales_detail")

#Consulta em SQL a quantidade de números distintos de SalesOrderID onde a coluna SalesOrderDetailID seja maior ou igual a 3.
df_qtd_salesorderid_distincts = spark.sql("""SELECT
                count(distinct SalesOrderID) AS Qtd_SalesOrderID_Distintos
            FROM df_sales_detail
            WHERE SalesOrderDetailID >= 3
            """)
#Path para salvar
df_qtd_salesorderid_distincts.write.format("csv").option("header", "true").save("C:\\Users\\manoe\\OneDrive\\Área de Trabalho\\\EngDados\\roxpartner\\tb_item1.csv",sep="|", encoding="utf-8")
```


```python
df_qtd_salesorderid_distincts.show()
```

    +--------------------------+
    |Qtd_SalesOrderID_Distintos|
    +--------------------------+
    |                     31465|
    +--------------------------+
    
    

### Ítem 2 ###


```python
#Passo 1:

##Registrar DF´s como tabela para executar Queries em SQL
spark.registerDataFrameAsTable(df_sales_special_offer, "df_sales_special_offer")
spark.registerDataFrameAsTable(df_production_product, "df_production_product")

#Primeiro passo Pegar os ProductID em ofertas e acrescentar a coluna "Name" e "DaysToManufacture" 
#através de um Left Join com a tabela df_production_product com estratégia => a.ProductID = b.ProductID

df_sales_offer_names = spark.sql("""SELECT 
        b.Name, 
        b.DaysToManufacture,
        a.ProductID
    FROM df_sales_special_offer AS a
    LEFT JOIN df_production_product AS b
    ON a.ProductID = b.ProductID
    """)

###Registrar novamente o DF como tabela para executar Queries em SQL
spark.registerDataFrameAsTable(df_sales_offer_names, "df_sales_offer_names")
```


```python
df_sales_offer_names.show()
```

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
    
    


```python
#Passo 2:

#converter a coluna OrderQty para int para posteriormente usar operaçao para somar 
df_sales_detail = df_sales_detail.withColumn("OrderQty", df_sales_detail.OrderQty.cast('integer'))

#Registrar como tabela para executar Queries em SQL
spark.registerDataFrameAsTable(df_sales_detail, "df_sales_detail")

# top 3 de ProductID com maiores números de OrderQty
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
```

    +---------+--------------+
    |ProductID|Total_OrderQty|
    +---------+--------------+
    |      712|          8311|
    |      870|          6815|
    |      711|          6743|
    +---------+--------------+
    
    


```python
#Passo3: Selectionar as colunas "Name" e "DaysToManufacture" da tabela df_sales_offer_names 
#Juntar com a coluna "Total_OrderQty" da tabela df_top3_orderQty por meio de um Left Join onde "a.ProductID = b.ProductID"
#Para retornar os 3 produtos com maior "Total_OrderQty"


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
```

    +----------------------+-----------------+--------------+
    |Name                  |DaysToManufacture|Total_OrderQty|
    +----------------------+-----------------+--------------+
    |AWC Logo Cap          |0                |8311          |
    |Water Bottle - 30 oz. |0                |6815          |
    |Sport-100 Helmet, Blue|0                |6743          |
    +----------------------+-----------------+--------------+
    
    

### Ítem 3 ###


```python
#Passo 1:

##Registrar DF´s como tabela para executar Queries em SQL
spark.registerDataFrameAsTable(df_person_person, "df_person_person")
spark.registerDataFrameAsTable(df_sales_customer, "df_sales_customer")
spark.registerDataFrameAsTable(df_sales_header, "df_sales_header")
```


```python
#Passo 2:
df_name_id = spark.sql("""SELECT 
        a.FirstName,
        a.MiddleName,
        a.LastName,
        b.CustomerID
    FROM df_sales_customer AS b
    LEFT JOIN df_person_person AS a
    ON a.BusinessEntityID = b.CustomerID
    """)

spark.registerDataFrameAsTable(df_name_id, "df_name_id")
```


```python
#Passo 3:
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

#concatenar as colunas"FirstName","MiddleName","LastName" para gerar uma nova coluna "ClientName" 
# e dropo as colunas antigas.
df1 = df_sell.select("*", concat(col("FirstName"),lit(" "),col("MiddleName")).alias("New_col"))
df1 = df1.select("*", concat(col("New_col"),lit(" "),col("LastName")).alias("ClientName"))
df1 = df1.drop("FirstName","MiddleName","LastName","New_col")
df_sell_final = df1.withColumn('ClientName', regexp_replace('ClientName', 'NULL', '')) #Remove os MiddleName "NULL"
spark.registerDataFrameAsTable(df_sell_final, "df_sell_final")

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
```

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
    
    

### Ítem 4 ###


```python
df_sales_header = df_sales_header.withColumn("OrderDate", df_sales_header["OrderDate"].cast("date"))
df_sales_detail = df_sales_detail.withColumn("OrderQty",df_sales_detail.OrderQty.cast("integer"))
df_sales_detail = df_sales_detail.withColumn("ProductID",df_sales_detail["ProductID"].cast("integer"))

#Registra como tabela para executar Queries em sql
spark.registerDataFrameAsTable(df_sales_header,"df_sales_header")
spark.registerDataFrameAsTable(df_sales_detail,"df_sales_detail")
spark.registerDataFrameAsTable(df_production_product,"df_production_product")
```


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

spark.registerDataFrameAsTable(df_total_produtos,"df_total_produtos")
```


```python
df_total_produtos.show(1)
```

    +------------+--------+----------+---------+
    |SalesOrderID|OrderQty| OrderDate|ProductID|
    +------------+--------+----------+---------+
    |       43659|       1|2011-05-31|      776|
    +------------+--------+----------+---------+
    only showing top 1 row
    
    


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
```

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
    
    

### Ítem 5 ###


```python
df_sales_header.count()   # SalesOrderID  OrderDate  TotalDue
```




    31465




```python
df_sales_header = df_sales_header.withColumn("SalesOrderID",df_sales_header["SalesOrderID"].cast("integer"))
df_sales_header = df_sales_header.withColumn("OrderDate",df_sales_header.OrderDate.cast("date"))

#Substituindo as "," da TotalDue por "."  , pra poder manipular em float
df_sales_header = df_sales_header.withColumn('TotalDue', regexp_replace('TotalDue', ',', '.'))
df_sales_header = df_sales_header.withColumn("TotalDue",df_sales_header["TotalDue"].cast("float"))
spark.registerDataFrameAsTable(df_sales_header,"df_sales_header")
```


```python
df_due_september = spark.sql("""SELECT
        SalesOrderID,
        OrderDate,
        TotalDue
    FROM df_sales_header 
    WHERE (OrderDate BETWEEN '2011-09-01'AND '2011-09-30') AND TotalDue > 1000
    ORDER BY SalesOrderID, OrderDate, TotalDue
    
        """)
df_due_september.write.format("csv").option("header", "true").save("C:\\Users\\manoe\\OneDrive\\Área de Trabalho\\\EngDados\\roxpartner\\tb_item5.csv",sep="|", encoding="utf-8")
```


```python
df_due_september.show()
```

    +------------+----------+---------+
    |SalesOrderID| OrderDate| TotalDue|
    +------------+----------+---------+
    |       44324|2011-09-01|3953.9883|
    |       44325|2011-09-01| 3729.364|
    |       44326|2011-09-01|3953.9883|
    |       44327|2011-09-02|3953.9883|
    |       44328|2011-09-02|3953.9883|
    |       44329|2011-09-02|3953.9883|
    |       44330|2011-09-02|3953.9883|
    |       44331|2011-09-03|3953.9883|
    |       44332|2011-09-03|3953.9883|
    |       44333|2011-09-04| 3756.989|
    |       44334|2011-09-04|3953.9883|
    |       44336|2011-09-04| 3756.989|
    |       44337|2011-09-04| 3729.364|
    |       44338|2011-09-04|3953.9883|
    |       44339|2011-09-04|3953.9883|
    |       44340|2011-09-04|3953.9883|
    |       44341|2011-09-05| 3729.364|
    |       44343|2011-09-05|3953.9883|
    |       44344|2011-09-06|3953.9883|
    |       44345|2011-09-06|3953.9883|
    +------------+----------+---------+
    only showing top 20 rows
    
    


```python

```
