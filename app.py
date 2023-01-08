
import sqlite3
import requests
from tqdm import tqdm

from flask import Flask, request
import json 
import numpy as np
import pandas as pd


app = Flask(__name__) 


# Define a function to create connection for reusability purpose
def make_connection():
    connection = sqlite3.connect('austin_bikeshare.db')
    return connection

# Make a connection
conn = make_connection()

@app.route('/')
def home():
    return 'Hello World'

@app.route('/stations/')
def route_all_stations():
    conn = make_connection()
    stations = get_all_stations(conn)
    return stations.to_json()

@app.route('/trips/')
def route_all_trips():
    conn = make_connection()
    trips = get_all_trips(conn)
    return trips.to_json()

@app.route('/stations/<station_id>')
def route_stations_id(station_id):
    conn = make_connection()
    station = get_station_id(station_id, conn)
    return station.to_json()

@app.route('/trips/<trip_id>')
def route_trips_id(trip_id):
    conn = make_connection()
    trip = get_trip_id(trip_id, conn)
    return trip.to_json()

@app.route('/json', methods=['POST']) 
def json_example():

    req = request.get_json(force=True) # Parse the incoming json data as Dictionary

    name = req['name']
    age = req['age']
    address = req['address']

    return (f'''Hello {name}, your age is {age}, and your address in {address}
            ''')

@app.route('/json_post_endpoint', methods=['POST']) 
def json_post_endpoint():

    input_data = request.get_json(force=True) # Get the input as dictionary
    specified_bike = str(input_data['bikeid'])# Select specific items (bikeid) from the dictionary

# Subset the data with query 
    conn = make_connection()
    query = f"SELECT * FROM trips WHERE bikeid= {specified_bike} "
    selected_data = pd.read_sql_query(query, conn)

# Make the aggregate - frequency of rides and total duration for specified bike and period
    result = selected_data.groupby(['start_station_name', 'end_station_name']).agg({'id' : 'count', \
    'duration_minutes' : 'sum'})

#find sum of columns specified 
    #result = result0['duration_minutes'].sum(axis=0)
# Return the result
    return result.to_json()


@app.route('/stations/add', methods=['POST']) 
def route_add_station():
    # parse and transform incoming data into a tuple as we need 
    data = pd.Series(eval(request.get_json(force=True)))
    data = tuple(data.fillna('').values)

    conn = make_connection()
    result = insert_into_stations(data, conn)
    return result

@app.route('/trips/add', methods=['POST']) 
def route_add_trip():
    # parse and transform incoming data into a tuple as we need 
    data = pd.Series(eval(request.get_json(force=True)))
    data = tuple(data.fillna('').values)

    conn = make_connection()
    result = insert_into_trips(data, conn)
    return result

@app.route('/trips/total_duration_by_start_station/<end_station_id>')
def route_trips_total_duration_by_start_station(end_station_id):
    conn = make_connection()
    trip_by_start_station_at_end_station= \
    get_trips_total_duration_by_start_station(conn, end_station_id)
    return  trip_by_start_station_at_end_station.to_json()

@app.route('/trips/subscriber_types_duration') 
def route_trips_subscriber_types_duration():
    conn = make_connection()
    trip_duration = get_trips_subscriber_types_duration(conn)
    return trip_duration.to_json()

##################FUNCTIONS#########################

def get_all_stations(conn):
    query = f"""SELECT * FROM stations"""
    result = pd.read_sql_query(query, conn)
    return result

def get_station_id(station_id, conn):
    query = f"""SELECT * FROM stations WHERE station_id = {station_id}"""
    result = pd.read_sql_query(query, conn)
    return result 

def get_all_trips(conn):
    query = f"""SELECT * FROM trips"""
    result = pd.read_sql_query(query, conn)
    return result

def get_trip_id(trip_id, conn):
    query = f"""SELECT * FROM trips WHERE id = {trip_id}"""
    result = pd.read_sql_query(query, conn)
    return result 

def insert_into_stations(data, conn):
    query = f"""INSERT INTO stations values {data}"""
    try:
        conn.execute(query)
    except:
        return 'Error'
    conn.commit()
    return 'OK'

def insert_into_trips(data, conn):
    query = f"""INSERT INTO stations values {data}"""
    try:
        conn.execute(query)
    except:
        return 'Error'
    conn.commit()
    return 'OK'

def get_trips_total_duration_by_start_station(conn, end_station_id):
    #end_station_id=4059
    query = f"SELECT * FROM trips WHERE end_station_id=({end_station_id})"
    selected_data = pd.read_sql_query(query, conn) 
    # Make the aggregate
    result0 = selected_data.groupby(['start_station_name','start_station_id']).agg({ 'duration_minutes': 'sum'})
    result=result0.sort_values(by ='duration_minutes', \
    ascending=False).nlargest(n=5, columns=['duration_minutes'])
    return result 

def get_trips_subscriber_types_duration(conn):
    query = f"SELECT * FROM trips "
    selected_data = pd.read_sql_query(query, conn) 
    # Make the aggregate
    selected_data_agg = selected_data.groupby(['subscriber_type']).agg({'id': 'count',\
    'duration_minutes' : 'sum'}).reset_index().round(2)
    result=selected_data_agg.sort_values(by ='duration_minutes', \
    ascending=False).nlargest(n=5, columns=['duration_minutes'])
    return result 

if __name__ == '__main__':
    app.run(debug=True, port=5000)