import findspark
findspark.init('/home/emanalytics/spark-3.3.1-bin-hadoop3')

import pyspark 
import os 
import glob

from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql import SQLContext
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

def create_spark_session(): 
    spark = SparkSession.builder.config("spark.jars", "/home/emanalytics/Downloads/postgresql-42.5.1.jar") \
        .master("local").appName("Covid Analysis").getOrCreate()

    return spark 

print("Creating Spark Session")
spark = create_spark_session()
print("done")

def get_files(type): 
    dir = glob.glob(f'data/landing/*.{type}')
    return dir

schema = StructType([
    StructField('id', IntegerType(), True), 
    StructField('title', StringType(), True), 
    StructField('region', StringType(), True)
])

final = spark.createDataFrame([], schema)

print("Start Processing the Json Files")
for fn in get_files('json'):
    #print(fn)
    a = fn.split('/')
    b = a[2].split('_')
    #print(b[0])
    df = spark.read.format('org.apache.spark.sql.json').option('multiline', 'true').load(fn)
    df1 = df.withColumn('data', f.explode('items')).select('data').dropDuplicates()
    df2 = df1.withColumn('id', f.col("data.id")) \
                .withColumn('title', f.col("data.snippet.title")) \
                .withColumn('region', f.lit(b[0])).select('id', 'title', 'region').dropDuplicates()
    final = final.union(df2)

final.write.option('header', True) \
        .partitionBy('region') \
        .mode('overwrite') \
        .parquet('data/processed/category_by_region')

print("Json files processed and persited")


schema = StructType([
    StructField('video_id', IntegerType(), True), 
    StructField('trending_date', DateType(), True), 
    StructField('vid_title', StringType(), True),
    StructField('channel_title', IntegerType(), True), 
    StructField('category_id', IntegerType(), True), 
    StructField('tags', IntegerType(), True), 
    StructField('views', IntegerType(), True), 
    StructField('likes', IntegerType(), True),
    StructField('dislikes', IntegerType(), True), 
    StructField('comment_count', IntegerType(), True), 
    StructField('vid_region', StringType(), True)
 
])

ytfinal = spark.createDataFrame([], schema)

print("Process the csv files")
for file in get_files('csv'): 
    #print(file)
    a = file.split('/')
    b = a[2]
    #print(b[0:2])

    df = spark.read.csv(file, header=True, inferSchema=True)

    df2 = df.select('video_id', \
    'trending_date',  \
    'title', \
    'channel_title', \
    'category_id', \
    'tags', \
    'views', \
    'likes', \
    'dislikes', \
    'comment_count')

    df3 = df2.withColumn('region', f.lit(b[0:2]))


    ytfinal = ytfinal.union(df3)

ytfinal.write.option('header', True) \
        .partitionBy('vid_region') \
        .mode('overwrite') \
        .parquet('data/processed/video_by_region')

print("done processing and persited")


print('Creating the analytical dataset')

finalDF = ytfinal.join(final, (ytfinal.category_id == final.id) & (ytfinal.vid_region == final.region), "inner")

print("done")


print("loading data to postgres")
finalDF.write.mode('overwrite').format("jdbc"). \
    options(
        url=f'jdbc:postgresql://localhost:5432/emanalytics', # jdbc:postgresql://<host>:<port>/<database>
        user='emadera',
        dbtable='youtubedata',
        password='Yankees1',
        driver='org.postgresql.Driver').save()
print('Data loaded to postgres.')


spark.stop()