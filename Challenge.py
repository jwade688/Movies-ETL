# Import dependencies
import json
import pandas as pd
import numpy as np
import os
import re
import psycopg2
from sqlalchemy import create_engine
from config import db_password
import time
db_string = f"postgres://postgres:{db_password}@127.0.0.1:5432/movie_data"

# File directory for the data sets
file_dir = '/Users/jacoblorenwade/Desktop/Berkeley_Classwork/Module_8-ETL_with_movies/Movies-ETL/Resources'

# Load the wiki json file
with open(f'{file_dir}/wikipedia.movies.json', mode='r') as file:
    wiki_movies_raw = json.load(file)
    
# Load the kaggle data
kaggle_metadata = pd.read_csv(f'{file_dir}/movies_metadata.csv', low_memory=False)

# Load the ratings data
ratings = pd.read_csv(f'{file_dir}/ratings.csv')

def etl_pipeline(wiki_movies_raw, kaggle_metadata, ratings):    

    # Make a new json from data that has a director and imdb link.
    wiki_movies = [movie for movie in wiki_movies_raw
        if ('Director' in movie or 'Directed by' in movie) 
            and 'imdb_link' in movie     
            and 'No. of episodes' not in movie]

    # Make a DataFrame from the wiki json
    wiki_movies_df = pd.DataFrame(wiki_movies)
    
    # Create a function to clean the Columns
    def clean_movie(movie):
        movie = dict(movie)
        alt_titles = {}

        # Combine alternate titles into one list.
        for key in ['Also known as','Arabic','Cantonese','Chinese','French',
                    'Hangul','Hebrew','Hepburn','Japanese','Literally',
                    'Mandarin','McCune–Reischauer','Original title','Polish',
                    'Revised Romanization','Romanized','Russian',
                    'Simplified','Traditional','Yiddish']:
            if key in movie:
                alt_titles[key] = movie[key]
                movie.pop(key)
        if len(alt_titles) > 0:
            movie['alt_titles'] = alt_titles

        # Merge the column names.
        def change_column_name(old_name, new_name):
            if old_name in movie:
                movie[new_name] = movie.pop(old_name)
        change_column_name('Adaptation by', 'Writer(s)')
        change_column_name('Country of origin', 'Country')
        change_column_name('Directed by', 'Director')
        change_column_name('Distributed by', 'Distributor')
        change_column_name('Edited by', 'Editor(s)')
        change_column_name('Length', 'Running time')
        change_column_name('Original release', 'Release date')
        change_column_name('Music by', 'Composer(s)')
        change_column_name('Produced by', 'Producer(s)')
        change_column_name('Producer', 'Producer(s)')
        change_column_name('Productioncompanies ', 'Production company(s)')
        change_column_name('Productioncompany ', 'Production company(s)')
        change_column_name('Released', 'Release Date')
        change_column_name('Release Date', 'Release date')
        change_column_name('Screen story by', 'Writer(s)')
        change_column_name('Screenplay by', 'Writer(s)')
        change_column_name('Story by', 'Writer(s)')
        change_column_name('Theme music composer', 'Composer(s)')
        change_column_name('Written by', 'Writer(s)')

        return movie

    # Run the cleaning function
    clean_movies = [clean_movie(movie) for movie in wiki_movies]

    # Create a DataFrame with the clean data
    wiki_movies_df = pd.DataFrame(clean_movies)
        
    # Create a column 'imdb_ID' and remove duplicates
    wiki_movies_df['imdb_id'] = wiki_movies_df['imdb_link'].str.extract(r'(tt\d{7})')
    wiki_movies_df.drop_duplicates(subset='imdb_id', inplace=True)
    
    # Keep columns that are less than 90% null
    wiki_columns_to_keep = [column for column in wiki_movies_df.columns if 
                            wiki_movies_df[column].isnull().sum() < len(wiki_movies_df) * 0.9]
    wiki_movies_df = wiki_movies_df[wiki_columns_to_keep]
    
    # Create a series of box office data
    box_office = wiki_movies_df['Box office'].dropna()
    
    # Use join() to turn lists into a string USING LAMBDA
    box_office = box_office.apply(lambda x: ' '.join(x) if type(x) == list else x)
    
    # Create a variable for ex. "$19.45 million"
    form_one = r'\$\s*\d+\.?\d*\s*[mb]illi?on'

    # Create a variable for second form ex. “$123,456,789”
    form_two = r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)'

    # Fix values that are given as a range
    box_office = box_office.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)

    box_office.str.extract(f'({form_one}|{form_two})')
    
    # Reformat function to correct the money formats
    def parse_dollars(s):

        # if s is not a string, return NaN
        if type(s) != str:
            return np.nan

        # if input is of the form $###.# million
        if re.match(r'\$\s*\d+\.?\d*\s*milli?on', s, flags=re.IGNORECASE):

            # remove dollar sign and " million"
            s = re.sub(r'\$|\s|[a-zA-Z]','', s)

            # convert to float and multiply by a million
            value = float(s) * 10**6

            # return value
            return value

        # if input is of the form $###.# billion
        elif re.match(r'\$\s*\d+\.?\d*\s*billi?on', s, flags=re.IGNORECASE):

            # remove dollar sign and " billion"
            s = re.sub(r'\$|\s|[a-zA-Z]','', s)

            # convert to float and multiply by a billion
            value = float(s) * 10**9

            # return value
            return value

        # if input is of the form $###,###,###
        elif re.match(r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)', s, flags=re.IGNORECASE):

            # remove dollar sign and commas
            s = re.sub(r'\$|,','', s)

            # convert to float
            value = float(s)

            # return value
            return value

        # otherwise, return NaN
        else:
            return np.nan

    # Add this new reformatted column to the DataFrame
    wiki_movies_df['box_office'] = box_office.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)        [0].apply(parse_dollars)

    # Remove the original Box Office column
    wiki_movies_df.drop('Box office', axis=1, inplace=True)
    
    # Parse the budget column
    budget = wiki_movies_df['Budget'].dropna()

    # Convert lists to strings
    budget = budget.map(lambda x: ' '.join(x) if type(x) == list else x)

    # Remove any values with a hyphen
    budget = budget.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)

    # remove bracket numbers (ex. $40 [4] million)
    budget = budget.str.replace(r'\[\d+\]\s*', '')

    # Add the the reformatted budget column to the DataFrame
    wiki_movies_df['budget'] = budget.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)        [0].apply(parse_dollars)

    # Remove the OG Budget column
    wiki_movies_df.drop('Budget', axis=1, inplace=True)
    
    # Parse the release date column
    release_date = wiki_movies_df['Release date'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)

    # Different forms of the date

    # i.e., January 1, 2000
    date_form_one = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s[123]\d,\s\d{4}'

    # i.e., 2000-01-01
    date_form_two = r'\d{4}.[01]\d.[123]\d'

    # January 2000
    date_form_three = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s\d{4}'

    # Extract these dates
    wiki_movies_df['release date'] = pd.to_datetime(release_date.str.extract                                    (f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_three})')                                    [0], infer_datetime_format=True)

    # Remove the original Box Office column
    wiki_movies_df.drop('Release date', axis=1, inplace=True)
    
    # Parse the runtime column
    running_time = wiki_movies_df['Running time'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)

    # capture all formats r'(\d+)\s*ho?u?r?s?\s*(\d*)|
    running_time_extract = running_time.str.extract(r'(\d+)\s*ho?u?r?s?\s*(\d*)|(\d+)\s*m')

    # Convert to numbers
    running_time_extract = running_time_extract.apply(lambda col: pd.to_numeric(col, errors='coerce')).fillna(0)

    # Add the new column
    wiki_movies_df['running_time'] = running_time_extract.apply(lambda row: row[0]*60 + row[1] if row[2] == 0 else row[2], axis=1)

    # Remove the original run time column
    wiki_movies_df.drop('Running time', axis=1, inplace=True)
        
    # TRANSFORM THE KAGGLE DATA

    # Remove adult movies
    kaggle_metadata = kaggle_metadata[kaggle_metadata['adult'] == 'False'].drop('adult', axis='columns')
    
    # Convert Video to boolean
    kaggle_metadata['video'] == 'True'
    kaggle_metadata['video'] = kaggle_metadata['video'] == 'True'
    
    # Convert budget, id, and popularity to numeric
    kaggle_metadata['budget'] = kaggle_metadata['budget'].astype(int)
    kaggle_metadata['id'] = pd.to_numeric(kaggle_metadata['id'], errors='raise')
    kaggle_metadata['popularity'] = pd.to_numeric(kaggle_metadata['popularity'], errors='raise')
    
    # Convert release date to date time
    kaggle_metadata['release_date'] = pd.to_datetime(kaggle_metadata['release_date'])
    
    # Convert the time stamp to datetime
    pd.to_datetime(ratings['timestamp'], unit='s')

    # Assign it to the time stamp column
    ratings['timestamp'] = pd.to_datetime(ratings['timestamp'], unit='s')
    
    # Merge wikipedia and kaggle data
    movies_df = pd.merge(wiki_movies_df, kaggle_metadata, on='imdb_id', suffixes=['_wiki','_kaggle'])
    
    # Drop the redundant columns
    movies_df.drop(columns=['title_wiki', 'release date', 'Language', 'Production company(s)'], inplace=True)
    
    # Function that fills in missing data and drops the redundant column
    def fill_missing_kaggle_data(df, kaggle_column, wiki_column):
        df[kaggle_column] = df.apply(lambda row: row[wiki_column] if row[kaggle_column] == 0 else row[kaggle_column], axis=1)
        df.drop(columns=wiki_column, inplace=True)

    fill_missing_kaggle_data(movies_df, 'runtime', 'running_time')
    fill_missing_kaggle_data(movies_df, 'budget_kaggle', 'budget_wiki')
    fill_missing_kaggle_data(movies_df, 'revenue', 'box_office')
    
    # Drop video because it only has one value
    movies_df.drop(columns=['video'], inplace=True)
    
    # Reorder the columns
    movies_df = movies_df.loc[:, ['imdb_id','id','title_kaggle','original_title','tagline','belongs_to_collection','url','imdb_link',
                           'runtime','budget_kaggle','revenue','release_date','popularity','vote_average','vote_count',
                           'genres','original_language','overview','spoken_languages','Country',
                           'production_companies','production_countries','Distributor',
                           'Producer(s)','Director','Starring','Cinematography','Editor(s)','Writer(s)','Composer(s)','Based on',
                            ]]
    
    # Reame the columns to be consistent
    movies_df.rename({'id':'kaggle_id',
                      'title_kaggle':'title',
                      'url':'wikipedia_url',
                      'budget_kaggle':'budget',
                      'Country':'country',
                      'Distributor':'distributor',
                      'Producer(s)':'producers',
                      'Director':'director',
                      'Starring':'starring',
                      'Cinematography':'cinematography',
                      'Editor(s)':'editors',
                      'Writer(s)':'writers',
                      'Composer(s)':'composers',
                      'Based on':'based_on'
                      }, axis='columns', inplace=True)
    # Get a count of ratings from the ratings data
    rating_counts = ratings.groupby(['movieId','rating'], as_index=False).count()                    .rename({'userId':'count'}, axis=1)                    .pivot(index='movieId',columns='rating', values='count')

    # Left merge the ratings counts to the movie data
    movies_with_ratings_df = pd.merge(movies_df, rating_counts, left_on='kaggle_id', right_index=True, how='left')

    # Replaces missing ratings with zeros
    movies_with_ratings_df[rating_counts.columns] = movies_with_ratings_df[rating_counts.columns].fillna(0)
    
    # Create database engine
    engine = create_engine(db_string)
    
    # Convert movies_df to sql table
    movies_df.to_sql(name='movies', con=engine, if_exists='replace')
    
    # Convert ratings.csv to sql table
    rows_imported = 0
    start_time = time.time()

    for data in pd.read_csv(f'{file_dir}/ratings.csv', chunksize=1000000):
        if rows_imported == 0:
            print(f'importing rows {rows_imported} to {rows_imported + len(data)}...', end='')
            data.to_sql(name='ratings', con=engine, if_exists='replace')
            rows_imported += len(data)
            print(f'Done. {time.time() - start_time} total seconds have elapsed')
        else:
            print(f'importing rows {rows_imported} to {rows_imported + len(data)}...', end='')
            data.to_sql(name='ratings', con=engine, if_exists='append')
            rows_imported += len(data)
            print(f'Done. {time.time() - start_time} total seconds have elapsed')

etl_pipeline(wiki_movies_raw, kaggle_metadata, ratings)