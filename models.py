from sqlalchemy import create_engine, Column, Integer, String, Float, Date
from sqlalchemy.ext.declarative  import declarative_base

Base = declarative_base()

class WeatherRecord(Base):
    __tablename__ = 'weather_records'
    
    id = Column(Integer, primary_key=True)
    date = Column(Date)
    station = Column(String)
    max_temp = Column(Float)
    min_temp = Column(Float)
    precipitation = Column(Float)