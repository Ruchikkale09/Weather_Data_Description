import sqlite3

# Open a connection to the database
conn = sqlite3.connect('weather.db')
c = conn.cursor()
# create the tables if they don't already exist
c.execute('drop table if exists weather_statistics')
c.execute('''CREATE TABLE IF NOT EXISTS weather_statistics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    station INTEGER,
    year INTEGER,
    avg_max_temp REAL,
    avg_min_temp REAL,
    total_precipitation REAL,
    FOREIGN KEY(station) REFERENCES weather_records(station)
)''')
# Get a list of all weather stations
c.execute('SELECT distinct station FROM weather_records')

stations = [row[0] for row in c.fetchall()]
i = 0
# Loop over all weather stations

for station in stations:
# Loop over all years
    
    for year in range(1985, 2015):
       
        # Calculate the average maximum temperature for the year and station
        c.execute('''SELECT station,CAST(strftime('%Y', date) AS INTEGER),ROUND(AVG(max_temp),2) 
        FROM weather_records group by station,CAST(strftime('%Y', date) AS INTEGER) 
        having station = ? and CAST(strftime('%Y', date) AS INTEGER) = ?
        ''',(str(station), year))
        row = c.fetchone()
        if row is None:
            continue
        avg_max_temp = row[2] if row[2] is not None else 0
        # Calculate the average minimum temperature for the year and station
        c.execute('''SELECT station,CAST(strftime('%Y', date) AS INTEGER),ROUND(AVG(min_temp),2) 
        FROM weather_records group by station,CAST(strftime('%Y', date) AS INTEGER) 
        having station = ? and CAST(strftime('%Y', date) AS INTEGER) = ?
        ''',(str(station), year))
        row = c.fetchone()
        if row is None:
            continue
        avg_min_temp = row[2] if row[2] is not None else 0
        # Calculate the total accumulated precipitation for the year and station
        c.execute('''SELECT station,CAST(strftime('%Y', date) AS INTEGER),ROUND(SUM(precipitation),2)
        FROM weather_records group by station,CAST(strftime('%Y', date) AS INTEGER) 
        having station = ? and CAST(strftime('%Y', date) AS INTEGER) = ?
        ''',(str(station), year))
        row = c.fetchone()
        if row is None:
            continue
        total_precipitation = round(row[2]/10,2) if row[2] is not None else 0

        # Insert the results into the weather_statistics table
    
        c.execute('INSERT INTO weather_statistics (station, year, avg_max_temp, avg_min_temp, total_precipitation) VALUES (?, ?, ?, ?, ?)',
                    (station, year, avg_max_temp, avg_min_temp, total_precipitation))
            
        #print("Station : ",station, "year : ",year)
# Commit changes and close the connection
print("\nNumber of records after inserting rows:")
cursor = c.execute('select * from weather_statistics;')
print(len(cursor.fetchall()))
conn.commit()
conn.close()
