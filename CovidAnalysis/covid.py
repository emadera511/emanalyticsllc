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


spark = SparkSession.builder.config("spark.jars", "/home/emanalytics/Downloads/postgresql-42.5.1.jar") \
    .master("local").appName("Covid Analysis").getOrCreate()

