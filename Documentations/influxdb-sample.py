import os, time
from influxdb_client_3 import InfluxDBClient3, Point
from influxdb_client_3 import flight_client_options
import pandas as pd
import numpy as np
import certifi #provides a carefully curated collection of trusted authority certificates that our code can use to verify authenticity in connecting remote servers such as (influxdb)

fh = open(certifi.where(), "r")     #open the root certificate content using open() function. certifi.where() finds the path to the trusted root certificate file. "r" stands for open that file in read mode.
cert = fh.read()    #This line reads the content of the root certificate file (fh) and stores it in the cert variable.
fh.close()          #This line closes the file handle (fh). It's a good practice to close files after you're done with them to free up system resources.

token = os.environ.get("INFLUXDB_TOKEN")
org = "Team FARAH 2022-2023"
host = "https://us-east-1-1.aws.cloud2.influxdata.com"


client = InfluxDBClient3(host=host, token=token, org=org, flight_client_options=flight_client_options(tls_root_certs=cert))

database="MQTT-DATA-ALL"

query = """SELECT * 
FROM census
WHERE time >= now() - interval '3 days'
AND ('bees' IS NOT NULL OR 'ants' IS NOT NULL)"""

# Execute the query
table = client.query(query=query, database="MQTT-DATA-ALL", language="sql") #we can put parameter mode="pandas" and no need to execute table.to_pandas()

# Convert to pandas dataframe because the table variable will return a py-arrow data format
df = table.to_pandas().sort_values(by="time")
print(df)
print(table)
