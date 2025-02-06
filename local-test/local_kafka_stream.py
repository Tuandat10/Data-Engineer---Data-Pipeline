import requests
import os
import json
from kafka import KafkaProducer
# from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
os.environ['PYSPARK_PYTHON'] = 'python'
spark = SparkSession.builder.appName("KafkaStream").getOrCreate()
def get_data():
    # url = 'https://my.api.mockaroo.com/pipeline_project.json?key=66e54260'
    # r = requests.get(url)
    # data = r.json()
    # print(json.dumps(data, indent=4))
    # for row in data:
    #     key = str(row['transaction_id'])
    #     del row['transaction_id']
    #     value = row
    #     print(key,value)
    # url = 'https://my.api.mockaroo.com/pipeline_project.json?key=66e54260'
    # r = requests.get(url)
    # data = r.json()
    # print(json.dumps(data, indent=4))
    # ds = spark.read.json(spark.sparkContext.parallelize([json.dumps(data)]))
    ds = spark.read.json('D:\\Long Class Data\\DE\Recent class - Data Engineer\\Next Project\\pipeline_project.json')
    print("c"*80)
    ds.show()
    print("d"*80)
    ds.select('product').show(truncate=False)
    # file_path = "..//pipeline_project.json"
    # with open(file_path,'r') as file:
    #     data = json.load(file)
    # print(data)
    ds = ds.withColumn("sub_product",explode(col("product")))
    print("a"*80)
    ds.show()
    ds = ds.select("transaction_id","sub_product.*","customer_id",'purchase_date')
    print("b"*80)
    ds.show()
    # ds = ds.withColumn('purchase_date',to_timestamp(col('purchase_date'),"MM/dd/yyyy"))
    # ds_recency = ds.groupBy('customer_id').agg(max('purchase_date').alias('purchase_date'))
    # today = datetime(2024,8,26)
    # ds_recency = ds_recency.withColumn('days_diff',datediff(lit(today),col('purchase_date')))
    # quantile = ds_recency.approxQuantile('days_diff',[0.25,0.5,0.75],0.0)
    # # print(quantile)
    # ds_recency = ds_recency.withColumn(
    #     'recency_score',
    #     when(col('days_diff')<=quantile[0],4).
    #     when((col('days_diff')>=quantile[0])&(col('days_diff')<=quantile[1]),3).
    #     when((col('days_diff')>=quantile[1])&(col('days_diff')<=quantile[2]),2).otherwise(1))
    # ds_recency.show(truncate=False)
    # ds_monetary = ds.select('customer_id','quantity','price')
    # ds_monetary = ds_monetary.withColumn('total_price',col('quantity')*col('price'))
    # ds_monetary = ds_monetary.groupBy('customer_id').agg(sum('total_price').alias('total_price'))
    # quantile = ds_monetary.approxQuantile('total_price',[0.25,0.5,0.75],0.0)
    # ds_monetary = ds_monetary.withColumn(
    #     'monetary_score', when(col('total_price')<=quantile[0],1).when((col('total_price')>=quantile[0])&(col('total_price')<=quantile[1]),2).when((col('total_price')>=quantile[1])&(col('total_price')<=quantile[2]),3).otherwise(4)
    # )
    # ds_frequency = ds.groupBy('customer_id').agg(count('transaction_id').alias('frequency'))
    # quantile = ds_frequency.approxQuantile('frequency',[0.25,0.5,0.75],0.0)
    # ds_frequency = ds_frequency.withColumn(
    #     'frequency_score',when(col('frequency')<=quantile[0],1).when((col('frequency')>=quantile[0])&(col('frequency')<=quantile[1]),2).when((col('frequency')>=quantile[1])&(col('frequency')<=quantile[2]),3).otherwise(4)
    # )
    # ds_final = ds_recency.join(ds_monetary,on='customer_id',how='inner').join(ds_frequency,on='customer_id',how='inner')
    # ds_final = ds_final.select('customer_id','purchase_date','total_price','recency_score','monetary_score','frequency_score')
    # ds_final = ds_final.withColumn("RFM_score",concat(col('recency_score'),col('frequency_score'),col('monetary_score')))
    # ds_final.show(truncate=False)
    # ds_frequency.show(truncate=False)
    # ds_monetary.show(truncate=False)
    # ds.show(truncate=False)

    # producer = KafkaProducer(bootstrap_servers='localhost:9094',key_serializer=lambda k: k.encode('utf-8'),value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    # for row in data:
    #     key = str(row['transaction_id'])
    #     del row['transaction_id']
    #     value = row
    #     producer.send('test1',key=key,value=value)
    # producer.flush()
    # producer.close()
get_data()
# default_args = {
#     "owner": "airflow",
#     "retries": 1,
#     "retry_delay": timedelta(seconds=80),
# }

# with DAG(
#     dag_id = "run_kafka_stream",
#     default_args=default_args,
#     schedule_interval="*/5 * * * * *",
#     start_date = datetime.today()
# ) as dag:
#     stream_data = PythonOperator(
#         task_id = 'stream_data',
#         python_callable = get_data
#     )