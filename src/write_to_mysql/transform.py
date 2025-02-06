from datetime import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import mysql.connector
import cassandra
from cassandra.cluster import Cluster
import json
host = 'mysql'
port = '3306'
db_name = "project"
user = 'root'
password = '1'
url = 'jdbc:mysql://' + host + ':' + port + '/' + db_name
driver = "com.mysql.cj.jdbc.Driver"
# keyspace = "my_keyspace"
# cassandra_login = 'cassandra'
# cassandra_password = 'cassandra'
# cluster = Cluster(['cassandra'])
# session = cluster.connect()
def create_pyspark_session():
    spark = SparkSession.builder \
        .appName("SparkDataSteaming") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,org.apache.kafka:kafka-clients:2.8.0,mysql:mysql-connector-java:8.0.30,com.datastax.spark:spark-cassandra-connector_2.12:3.1.0") \
        .config("spark.cassandra.connection.host", "cassandra") \
        .getOrCreate()
    return spark
def create_mysql_connection():
    session_sql = mysql.connector.connect(user=user,password=password,host=host)
    database_query = f'CREATE DATABASE IF NOT EXISTS {db_name}'
    cursor = session_sql.cursor()
    cursor.execute(database_query)
    cursor.execute(f'USE {db_name}')
    table_query = f""" create table if not exists rfm_table (transaction_id int primary key auto_increment, customer_id varchar(255), purchase_date varchar(255), total_price varchar(255), recency_score varchar(255), frequency_score varchar(255), monetary_score varchar(255), RFM_score varchar(255))"""
    cursor.execute(table_query)
# def create_cassandra_keyspace():
#     keyspace_query = f""" CREATE KEYSPACE IF NOT EXISTS {keyspace} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': '1'}};"""
#     session.execute(keyspace_query)
#     session.set_keyspace(keyspace)
#     session.execute(f""" CREATE TABLE IF NOT EXISTS transaction (product text,customer_id text, purchase_date text, primary key (product, customer_id, purchase_date))""")
def transform(ds,batch_id):
    spark = create_pyspark_session()
    create_mysql_connection()
    # create_cassandra_keyspace()
    columns = StructType([
        StructField("product",StringType(),True),
        StructField("customer_id",StringType(),True),
        StructField("purchase_date",StringType(),True)
    ])
    ds = ds.selectExpr("CAST(value AS STRING)")
    ds = ds.withColumn('c1',from_json('value',schema=columns)).select('c1.*')
    product_schema = ArrayType(StructType([
        StructField("name", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("quantity", IntegerType(), True)
    ]))
    ds = ds.withColumn('product',from_json('product',product_schema))
    ds.printSchema()
    print("c"*80)
    # ds = ds.withColumn('transaction_id',monotonically_increasing_id())
    print("a"*80)
    ds.show() 
    print("b"*80)
    ds.select('product').show(truncate=False)
    # for row in ds.collect():
    #     product = row['product']
    #     customer_id = row['customer_id']
    #     purchase_date = row['purchase_date']
    #     cassandra_product = json.dumps(product)
    #     insert_cassandra = f""" insert into transaction (product, customer_id, purchase_date) values ('{cassandra_product}','{customer_id}','{purchase_date}')"""
    #     session.execute(insert_cassandra)
    ds = ds.withColumn('sub_product',explode('product'))
    ds = ds.select('sub_product.*','customer_id','purchase_date')
    ds = ds.withColumn('purchase_date',to_timestamp(col('purchase_date'),"MM/dd/yyyy"))
    ds_recency = ds.groupBy('customer_id').agg(max('purchase_date').alias('purchase_date'))
    today = datetime(2024,8,26)
    ds_recency = ds_recency.withColumn('days_diff',datediff(lit(today),col('purchase_date')))
    quantile = ds_recency.approxQuantile('days_diff',[0.25,0.5,0.75],0.0)
    # print(quantile)
    ds_recency = ds_recency.withColumn(
        'recency_score',
        when(col('days_diff')<=quantile[0],4).
        when((col('days_diff')>=quantile[0])&(col('days_diff')<=quantile[1]),3).
        when((col('days_diff')>=quantile[1])&(col('days_diff')<=quantile[2]),2).otherwise(1))
    # ds_recency.show(truncate=False)
    ds_monetary = ds.select('customer_id','quantity','price')
    ds_monetary = ds_monetary.withColumn('total_price',col('quantity')*col('price'))
    ds_monetary = ds_monetary.withColumn('total_price',round(col('total_price'),2))
    ds_monetary = ds_monetary.groupBy('customer_id').agg(sum('total_price').alias('total_price'))
    quantile = ds_monetary.approxQuantile('total_price',[0.25,0.5,0.75],0.0)
    ds_monetary = ds_monetary.withColumn(
        'monetary_score', when(col('total_price')<=quantile[0],1).when((col('total_price')>=quantile[0])&(col('total_price')<=quantile[1]),2).when((col('total_price')>=quantile[1])&(col('total_price')<=quantile[2]),3).otherwise(4)
    )
    ds_frequency = ds.groupBy('customer_id').agg(count('customer_id').alias('frequency'))
    quantile = ds_frequency.approxQuantile('frequency',[0.25,0.5,0.75],0.0)
    ds_frequency = ds_frequency.withColumn(
        'frequency_score',when(col('frequency')<=quantile[0],1).when((col('frequency')>=quantile[0])&(col('frequency')<=quantile[1]),2).when((col('frequency')>=quantile[1])&(col('frequency')<=quantile[2]),3).otherwise(4)
    )
    ds_final = ds_recency.join(ds_monetary,on='customer_id',how='inner').join(ds_frequency,on='customer_id',how='inner')
    ds_final = ds_final.select('customer_id','purchase_date','total_price','recency_score','frequency_score','monetary_score')
    ds_final = ds_final.withColumn("RFM_score",concat(col('recency_score'),col('frequency_score'),col('monetary_score')))
    ds_final.write.format('jdbc').options(url='jdbc:mysql://mysql:3306/project',driver='com.mysql.cj.jdbc.Driver',dbtable='rfm_table',user='root',password='1').mode('append').save()
    # df = spark.read.format('jdbc').options(
    # url='jdbc:mysql://mysql:3306/project',
    # driver='com.mysql.cj.jdbc.Driver',
    # dbtable='rfm_table',
    # user='root',
    # password='password'
    # ).load()
    # print("huhu"*80)
    
    print("WRITE SUCESSFULLY")
    

