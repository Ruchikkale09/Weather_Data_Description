from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import Base, WeatherRecord

# Connect to the database
engine = create_engine('sqlite:///weather.db')
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()



# Ingest data from text files
import os

wx_dir = 'wx_data'
for filename in os.listdir(wx_dir):
    if not filename.endswith('.txt'):
        continue
        
    station = int(filename[3:-4])
    print(station)
    filepath = os.path.join(wx_dir, filename)
    
    with open(filepath) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
                
            parts = line.split('\t')
            date_str, max_temp_str, min_temp_str, precip_str = parts
            
            # Skip records with missing values
            if '-9999' in [max_temp_str, min_temp_str, precip_str]:
                continue
                
            # Convert data types
            date = datetime.strptime(date_str, '%Y%m%d').date()
            max_temp = float(max_temp_str) / 10.0
            min_temp = float(min_temp_str) / 10.0
            precipitation = float(precip_str) / 10.0
            
            # Check for duplicates
            existing_record = session.query(WeatherRecord).filter_by(date=date, station=station).first()
            if existing_record:
                continue
            
            # Add record to database 
            record = WeatherRecord(date=date, station=station, max_temp=max_temp, min_temp=min_temp, precipitation=precipitation) 
            session.add(record)
            
    session.commit()

# Log ingestion statistics
num_records = session.query(WeatherRecord).count()
print(f"Ingested {num_records} weather records.")
