import csv
from sqlalchemy import create_engine, Table, Column, Integer, Float, String, MetaData, Date

engine = create_engine('sqlite:///weather_data.db')
metadata = MetaData()

stations = Table('stations', metadata,
                 Column('station', String, primary_key=True),
                 Column('latitude', Float),
                 Column('longitude', Float),
                 Column('elevation', Float),
                 Column('name', String),
                 Column('country', String),
                 Column('state', String))

measure = Table('measure', metadata,
                Column('station', String),
                Column('date', Date),
                Column('precip', Float),
                Column('tobs', Integer))

metadata.create_all(engine)

def load_csv_data(file_path, table):
    with open(file_path, 'r') as file:
        csv_reader = csv.reader(file)
        headers = next(csv_reader)
        data = [dict(zip(headers, row)) for row in csv_reader]

    with engine.connect() as conn:
        conn.execute(table.insert(), data)

load_csv_data('clean_stations.csv', stations)
load_csv_data('clean_measure.csv', measure)
