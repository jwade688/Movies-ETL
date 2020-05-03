# Movies-ETL
Python script that performs all three ETL steps on the Wikipedia and Kaggle data.

## Challenge Overview
- Create an automated ETL pipeline.
- Extract data from multiple sources.
- Clean and transform the data automatically using Pandas and regular expressions.
- Load new data into PostgreSQL.

# Documents used for the challenge:
- Challenge.py
- Data sets (not included in github repository):
    - Wikipedia.movies.json
    - movies_metadata.csv
    - ratings.csv

## Assumptions
There are some assumptions that must be made when running this script. First, this was built in and for the PythonData module. If it is not run from PythonData, there is a good chance that the imports will not work. To go with this, it's assumed that the psycopg2 module is installed. This will be needed to move the data to a SQL database. Some more assumptions, the user will need to have updated the file_dir variable to the path where their json and (2) CSVs are stored. To go with this, the script was built around the files wikipedia.movies.json, movies_metadata.csv, and ratings.csv. If user is using different data, the file names will need to be matching. Also, user must create a config file to store their db_password. With out this, the output will not be able to access SQL and to go with this, user needs to create a SQL database named movie_data to collect the output tables.