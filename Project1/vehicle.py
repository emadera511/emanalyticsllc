import findspark
findspark.init('/home/emanalytics/spark-3.3.1-bin-hadoop3')

import pyspark

from pyspark.sql import SparkSession
from pyspark.sql import Row, functions as f
from lib.DataFlow import *
from pyspark.sql.window import Window

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
    df = df.withColumn('zipcode', df['postal code']).withColumn('VIN', df['VIN (1-10)'])
    df.createOrReplaceTempView('data')
    df = spark.sql("""

                with zips as ( 
                        select distinct *
                        , row_number() over(order by zipcode, City, County) ziprow
                    from (
                                select distinct zipcode, City, County
                                from data
                    ) t
                ), vins as (
                        select distinct *
                        , row_number() over(order by vin) vinrow
                        from (
                                select distinct VIN
                                from data 
                    ) t 
                )
                select d.*, z.ziprow, v.vinrow
                from data d 
                join zips z on d.zipcode = z.zipcode 
                    and d.City = z.City and d.County = z.County
                join vins v on d.VIN = v.VIN
                order by z.ziprow desc 
                    

                """)

    location = df.select('ziprow', df['postal code'].alias('zipcode'), 'city', 'state').dropDuplicates()

    vehicle = df.select('vinrow', df['VIN (1-10)'].alias('VIN'),'make', 'model', df['model year'].alias('model_year')).dropDuplicates()

    lookup = df.select('ziprow', 'vinrow')

    data = {'location': location, 'vehicle': vehicle, 'lookup': lookup}
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