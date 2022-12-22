import findspark
findspark.init('/home/emanalytics/spark-3.3.1-bin-hadoop3')

import pyspark

from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import SQLContext
import pyspark.sql.functions as FN
import seaborn as sns 
import matplotlib.pyplot as plt 
import pandas 
import numpy 

def get_spark():
    try:
        spark = SparkSession.builder.config("spark.jars", "/home/emanalytics/Downloads/postgresql-42.5.1.jar") \
            .master("local").appName("Covid Analysis").getOrCreate()

        return spark 
    except Exception as e: 
        print(f'Could not start a pyspark session {e}')

def get_data(filepath, spark):
    df = spark.read.csv(filepath, inferSchema=True, header=True)

    cleanDF = df.select('country', 
    'iso_code', 
    #FN.concat_ws('-','month','year').alias('monthyear'),
    'date',
    'total_vaccinations',
    'people_vaccinated',
    'people_fully_vaccinated',
    'daily_vaccinations_raw',
    'daily_vaccinations',
    'total_vaccinations_per_hundred',
    'people_vaccinated_per_hundred',
    'people_fully_vaccinated_per_hundred',
    'daily_vaccinations_per_million',
    'vaccines',
    'source_name') \
        .withColumn('monthyear', FN.concat_ws('-', FN.year('date'), FN.month('date'))) \
        .withColumn('year', FN.year('date') )\
        .withColumn('month', FN.month('date') ) \
        .withColumn('day', FN.dayofmonth('date')) \
        .withColumn('quater', FN.quarter('date')) \
        .withColumn('dayoftheweek', FN.date_format('date', 'EEEE')) \
        .withColumn('dayoftheweekindex', FN.when(FN.date_format('date', 'EEEE') == 'Sunday', 1 ) \
                                            .when(FN.date_format('date', 'EEEE') == 'Monday', 2) \
                                            .when(FN.date_format('date', 'EEEE') == 'Tuesday', 3) \
                                            .when(FN.date_format('date', 'EEEE')=='Wednesday', 4) \
                                            .when(FN.date_format('date', 'EEEE')=='Thursday', 5) \
                                            .when(FN.date_format('date', 'EEEE')=='Friday', 6) \
                                            .when(FN.date_format('date', 'EEEE')=='Saturday', 7)) \
        .withColumn('percent', FN.col('people_fully_vaccinated') / FN.col('total_vaccinations')).fillna(0).dropDuplicates().sort(FN.col('date'))

    return cleanDF

def load_to_pg(final):
    final.write.mode('overwrite').format("jdbc"). \
        options(
         url=f'jdbc:postgresql://localhost:5432/emanalytics', # jdbc:postgresql://<host>:<port>/<database>
         user='emadera',
         dbtable='coviddata',
         password='Yankees1',
         driver='org.postgresql.Driver').save()

def main(): 
    filepath = 'CovidAnalysis/data/country_vaccinations.csv'
    spark = get_spark()
    df = get_data(filepath, spark)
    load_to_pg(df)
    spark.stop()

if __name__ == "__main__":
    main()
