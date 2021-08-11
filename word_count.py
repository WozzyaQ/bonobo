import os
import re

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, posexplode, from_json
from pyspark.sql.types import StructType, StringType, StructField

load_dotenv()

spark = SparkSession.builder \
    .appName("word-count") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.0") \
    .getOrCreate()

df = spark.read.format("mongo") \
    .option("spark.mongodb.input.uri",
            f"mongodb://{os.getenv('MONGO_USER')}:{os.getenv('MONGO_PASSWORD')}@{os.getenv('MONGO_IP')}/") \
    .option("spark.mongodb.input.database", f"{os.getenv('MONGO_DB')}") \
    .option("spark.mongodb.input.collection", f"{os.getenv('MONGO_COLLECTION')}").load()

json_text_scheme = StructType([
    StructField('html', StringType(), True),
    StructField('text_only', StringType(), True)
])

topic_related_page_text_df = df.select(df.origin, explode(df.title_page_url_pairs).alias("page_info")) \
    .select(col('origin'), posexplode(col('page_info'))) \
    .filter(col('pos') == 2).withColumn('col', from_json(col('col'), json_text_scheme)) \
    .selectExpr('origin', 'col.text_only').persist()

# lets make word count for topic `spark`
text_only = topic_related_page_text_df.where(col('origin') == 'spark') \
    .select('text_only')

text_only.rdd.flatMap(lambda row: row[0].split()) \
    .map(lambda token: re.sub(r'(?<!\S)[^\s\w]+|[^\s\w]+(?!\S)', '', token)) \
    .map(lambda token: (token, 1)) \
    .reduceByKey(lambda x, y: x + y) \
    .sortBy(lambda x: x[1], ascending=False) \
    .foreach(print)  # just for showcase
