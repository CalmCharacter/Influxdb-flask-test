import os, time
from influxdb_client_3 import InfluxDBClient3, Point
from influxdb_client_3 import flight_client_options
import pandas as pd
import numpy as np
import certifi #provides a carefully curated collection of trusted authority certificates that our code can use to verify authenticity in connecting remote servers such as (influxdb)
from dataclasses import dataclass


fh = open(certifi.where(), "r")     #open the root certificate content using open() function. certifi.where() finds the path to the trusted root certificate file. "r" stands for open that file in read mode.
cert = fh.read()    #This line reads the content of the root certificate file (fh) and stores it in the cert variable.
fh.close()          #This line closes the file handle (fh). It's a good practice to close files after you're done with them to free up system resources.

token = os.environ.get("INFLUXDB_TOKEN")
org = "Team FARAH 2022-2023"
host = "https://us-east-1-1.aws.cloud2.influxdata.com"

client = InfluxDBClient3(host=host, token=token, org=org, flight_client_options=flight_client_options(tls_root_certs=cert))

database="MQTT-DATA-ALL"

@dataclass
class myInfluxData:
      curr_last_v: float
      volt_last_v: float

def q_data():
    query = """SELECT * 
    FROM census
    WHERE time >= now() - interval '12 hour'
    """

    # Execute the query
    table = client.query(query=query, database="MQTT-DATA-ALL", language="sql") #we can put parameter mode="pandas" and no need to execute table.to_pandas()
    # Convert to pandas dataframe because the table variable will return a py-arrow data format
    df = table.to_pandas().sort_values(by="time")

    data = myInfluxData(
        curr_last_v=df['ants'].iloc[-1],
        volt_last_v=df['bees'].iloc[-2]
    )

    return data

if __name__ == "__main__":
     q_data()
     #wtf = q_data()
     #print(wtf.curr_last_v)
     #print(wtf.volt_last_v)
