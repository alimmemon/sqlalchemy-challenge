import numpy as np 
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy import create_engine, func
from flask import Flask, jsonify 
from sqlalchemy.orm import Session
import datetime as dt
import pandas as pd
import json

app = Flask(__name__)


# Database set up 

engine = create_engine('sqlite:///hawaii.sqlite')

Base = automap_base()
Base.prepare(engine,reflect=True)


measurements = Base.classes.measurement
stations = Base.classes.station




@app.route("/")
def home():
    return(
        "<h1> Hawaii Weather Station Data </h1>"
        "<br>Available Routes:</br>"
        "<br> <button> <a href=/api/v1.0/precipitation target=_blank> Precipitation Data </a> </button>"
        "<br> <button> <a href=/api/v1.0/stations target=_blank> Station Data </a> </button>"
        "<br> <button> <a href=/api/v1.0/tobs target=_blank> Temperature Observation Data </a> </button>"
        "<br><br> <div> To view the minimum, average, and maximum temperature after a start date, enter yyyy-mm-dd in the path: /api/v1.0/yyyy-mm-dd </div>"
        "<br> To view the minimum, average, and maximum temperature between to dates, enter yyyy-mm-dd for both the start and end dates: /api/v1.0/start_date(yyyy-mm-dd)/end_date(yyyy-mm-dd)"
        )

@app.route("/api/v1.0/precipitation")
def precip():
    session = Session(engine)


    maxdate = session.query(func.max(measurements.date))[0][0]

    year = int(maxdate[:4])
    month = int(maxdate[5:7])
    day = int(maxdate[8:10])

    query_date = dt.date(year,month,day) - dt.timedelta(days=365)
    
    prcp_at_station = "USC00519281"

    # Design a query to retrieve the last 12 months of precipitation data
    last12months = session.query(measurements.station,measurements.date,measurements.prcp).\
        filter(measurements.date > query_date).\
        filter(measurements.station == prcp_at_station).all()

    # # Save the query results as a Pandas DataFrame,sort the df by date and set the index to the date column
    df = pd.DataFrame(last12months).sort_values('date')
    df = df.rename(columns={'prcp':'precipitation'})

    df = df.to_json(orient="table")

    session.close()

    return df

@app.route("/api/v1.0/stations")
def station():

    session = Session(engine)

    station_name_code = session.query(measurements.station,stations.name,func.count(measurements.station)).\
        filter(stations.station==measurements.station).\
        group_by(measurements.station).\
        order_by(func.count(measurements.station).desc()).all()
    
    station_df = pd.DataFrame(station_name_code,columns=["station","name","observations"])

    station_json = station_df.to_json(orient='table')

    session.close()
    
    return station_json
    
@app.route("/api/v1.0/tobs")
def tobs():
    session = Session(engine)

    maxdate = session.query(func.max(measurements.date))[0][0]

    year = int(maxdate[:4])
    month = int(maxdate[5:7])
    day = int(maxdate[8:10])

    query_date = dt.date(year,month,day) - dt.timedelta(days=365)

    stationmost = session.query(measurements.station,func.count(measurements.station)).\
        group_by(measurements.station).\
        order_by(func.count(measurements.station).desc()).first()

    temp = session.query(measurements.station,measurements.date,measurements.tobs).\
        filter(measurements.date > query_date).\
        filter(measurements.station==stationmost[0]).all()
    
    temp_df = pd.DataFrame(temp)

    temp_df = temp_df.rename(columns={"tobs":"Temperature Observation"})

    temp_df = temp_df.to_json(orient='table')

    session.close()

    return temp_df

@app.route("/api/v1.0/<start_date>")
def starttobs(start_date):
    # yyyy-mm-dd
    session = Session(engine)

    temp_after_start = session.query(func.min(measurements.tobs),func.avg(measurements.tobs),func.max(measurements.tobs)).\
        filter(measurements.date >= start_date).all()

    temp_after_start_df = pd.DataFrame(temp_after_start,columns = ["Minimum Temperature","Average Temperature","Maximum Temperature"])

    temp_after_start_json = temp_after_start_df.to_json(orient='table')
    
    session.close()

    return temp_after_start_json

@app.route("/api/v1.0/<start_date>/<end_date>")
def betweentobs(start_date,end_date):

    session = Session(engine)

    temp_between_start = session.query(func.min(measurements.tobs),func.avg(measurements.tobs),func.max(measurements.tobs)).\
        filter(measurements.date >= start_date).\
        filter(measurements.date <= end_date).all()

    temp_between_start_df = pd.DataFrame(temp_between_start,columns = ["Minimum Temperature","Average Temperature","Maximum Temperature"])

    temp_between_start_json = temp_between_start_df.to_json(orient='table')
    
    session.close()

    return temp_between_start_json


if __name__ == '__main__':
    app.run(debug=True)