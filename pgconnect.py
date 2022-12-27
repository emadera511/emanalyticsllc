import psycopg2 as pg 
import pandas as pd 

conn = pg.connect(
    host='localhost', 
    port='5432', 
    database='emanalytics', 
    user = 'emadera', 
    password = 'Yankees1' 
)

cursor = conn.cursor() 

sql = """ 
    select * 
    from students 
"""

cursor.execute(sql)

print(cursor.fetchall())