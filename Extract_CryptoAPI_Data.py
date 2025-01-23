# Extracting data from an API and loading it to Mysql Uisng python

import requests
import pandas
import sqlalchemy

# It's an open api which provides real time crypto-currency data
url = 'http://api.coincap.io/v2/assets'

header = {"Content-Type":"application/json",
          "Accept-Encoding":"deflate"}

# Fecting the data using GET method
response = requests.get(url,headers=header)
print(response)

#Try-Except used for error handling
if response.status_code == 200:
    try:
        response_data = response.json() # Only try to parse if the status is OK
        print(response.json()) 
    except ValueError:
        print("Error: Response is not in JSON format")
else:
    print(f"Error: {response.status_code} - {response.text}")

#Normalizing the data uisng json_normalize function
df = pandas.json_normalize(response_data,'data')
print(df)

#Create a connection with Database using SQLAlchemy
engine = sqlalchemy.create_engine('mysql+mysqlconnector://root:shreya@localhost:3306/crypto')

df.to_sql(name='factcrypto', con=engine, index=True, if_exists='replace')

