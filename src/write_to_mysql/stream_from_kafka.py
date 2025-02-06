from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import json
from transform import transform
def create_pyspark_session():
    scala_version = '2.12'
    spark_version = '3.3.1'
    packages = [f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}','org.apache.kafka:kafka-clients:2.8.0']
    spark = SparkSession.builder.appName('KafkaStream').config('spark.jars.packages',','.join(packages)).getOrCreate()
    return spark
def read_stream(spark):
    df_readstream = spark.readStream.format('kafka').option('kafka.bootstrap.servers','kafka:9092').option('subscribe','test1').option("startingOffsets", "earliest").load()
    return df_readstream
def write_stream(ds):
    query = ds.writeStream.format('memory').outputMode('append').foreachBatch(transform).trigger(once=True).option("checkpointLocation","checkpoint").start()
    query.awaitTermination()
def main2():
    spark = create_pyspark_session()
    ds = read_stream(spark)
    ds = ds.selectExpr('CAST(key as STRING)','CAST(value as STRING)')
    write_stream(ds)
main2()
