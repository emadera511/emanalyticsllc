import findspark
findspark.init('/home/emanalytics/spark-3.3.1-bin-hadoop3')

import pyspark

from pyspark.sql import SparkSession
from pyspark.sql import Row
from lib.DataFlow import *

ymlData = read_file('Project1/config/config.yaml')

host = ymlData['postgres']['host']
port = ymlData['postgres']['port']
db = ymlData['postgres']['database']
user = ymlData['postgres']['username']
password = ymlData['postgres']['password']



def get_spark(): 
    spark = SparkSession.builder.config("spark.jars", "/home/emanalytics/Downloads/postgresql-42.5.1.jar") \
    .master("local").appName("PySpark_Postgres_test1").getOrCreate()

    return spark 

def extract(spark): 
    df = spark.read.csv('Project1/data/Electric_Vehicle_Population_Data.csv', inferSchema=True, header=True)
    df.printSchema()

    return df 

def transform(df): 
    location = df.select(df['postal code'].alias('zipcode'), 'city', 'state', 'county').dropDuplicates()
    vehicle = df.select(df['VIN (1-10)'].alias('VIN'),'make', 'model', df['model year'].alias('model_year')).dropDuplicates()
    electric = df.select('VIN (1-10)','Electric Vehicle Type' \
    , df['Clean Alternative Fuel Vehicle (CAFV) Eligibility'].alias('CAFV Eligbility') \
        , 'Electric Utility').dropDuplicates()
    lookup = df.select(df['postal code'].alias('zipcode'), df['VIN (1-10)'].alias('VIN')).dropDuplicates()
    data = {'location': location, 'vehicle': vehicle, 'electric': electric, 'lookup': lookup}
    return data

def load(table, name): 
    table.write.mode('overwrite').format("jdbc"). \
    options(
         url=f'jdbc:postgresql://{host}:{port}/{db}', # jdbc:postgresql://<host>:<port>/<database>
         dbtable=name,
         user=user,
         table=name,
         password=password,
         driver='org.postgresql.Driver').save()

print('Get Spark Session')
spark = get_spark() 

print('Get the data')
vehicle = extract(spark)

print('Transform the data')
data = transform(vehicle)

for k, v in data.items(): 
    print(f'loading table {k}')
    load(v, k)




print('Stop Spark')
spark.stop()