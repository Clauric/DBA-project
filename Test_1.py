import pymysql as psq
import pymongo as pmo
import pandas as pd
import re
import time


conn = psq.connect("localhost","root","fi3Keepo","world",cursorclass=psq.cursors.DictCursor)
query = "SELECT * FROM country"

with conn:
   cursor = conn.cursor()
   cursor.execute(query)
   country = cursor.fetchall()

df_C = pd.DataFrame(country, columns=["Code", "Name", "Continent", "Population", "HeadOfState"])

df_C["Population"] = df_C["Population"].apply(pd.to_numeric, errors="coerce")  


print()
print(dc_C[(dc_C["Population"] < 10000)].dtypes)
