import pandas as pd 
import numpy as np 

df = pd.read_csv('data\\us_baby_names.csv')

df1 = df.sort_values('Count', ascending=False)

df2=df1[(df1['Year'] == 2014) & (df1['Gender']=='F')]

#print(df1.groupby('Year').count())

for c in df2.columns: 
    if c == 'Name': 
        print(df2[df2[c] == 'Olivia'])

df3=df.loc[(df['Name'] == 'Olivia') & (df['Gender']=='F')].sort_values('Year', ascending=False)

print(df3.head())
        
        

