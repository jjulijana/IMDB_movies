import os
import pandas as pd

from config.db_config import get_connection, end_connection
from scripts.data_profiling import generate_profile_report, generate_report_if_not_exists
from scripts.data_cleaning import drop_duplicates, drop_columns, fill_missing_values, remove_null_terminating_char, convert_to_int
from scripts.data_processing import process_actor_data

def main():
    # Connect to DB
    successful, connection, cursor, error_message = get_connection()
    if not successful:
        print("Connection to database failed:", error_message)
        return  # terminate the script
    
    # Load Data
    file_path = 'data/movie_metadata.csv'
    if not os.path.exists(file_path):
        print("File does not exist.")
        return
    raw_data = pd.read_csv(file_path)
    data = raw_data

    generate_report_if_not_exists(raw_data, 'reports/raw_data_report.html')

    # Clean data
    drop_duplicates(data)
    drop_columns(data)
    remove_null_terminating_char(data)
    fill_missing_values(data)
    convert_to_int(data)

    # Save Data
    cleaned_file_path = 'data/cleaned_movie_metadata.csv'
    data.to_csv(cleaned_file_path, index=False)

    generate_report_if_not_exists(data, 'reports/cleaned_data_report.html')

    # Menage data
    director_df = pd.DataFrame(data.director_name.value_counts()).reset_index().rename(columns = {'count' : 'movie_count'})
    
    actor_1 = process_actor_data(data, 'actor_1_name', 'actor_1_facebook_likes')
    actor_2 = process_actor_data(data, 'actor_2_name', 'actor_2_facebook_likes')
    actor_3 = process_actor_data(data, 'actor_3_name', 'actor_3_facebook_likes')
    actors_df = pd.concat([actor_1, actor_2], ignore_index=True)
    actors_df = pd.concat([actors_df, actor_3], ignore_index=True)
    actors_df = actors_df.groupby(['actor_name', 'actor_facebook_likes'])['movie_count'].sum().reset_index()

    movie_cols = ['movie_title', 'color', 'director_name', 'duration', 'gross', 
                   'movie_imdb_link', 'language', 'country', 'content_rating', 
                   'aspect_ratio', 'imdb_score', 'movie_facebook_likes']
    movie_df = data[movie_cols].copy()

    genres_df = data[['movie_title', 'genres']].copy()
    genres_df['genres'] = genres_df['genres'].str.split('|')
    genres_df = genres_df.explode('genres')
    genres_df = genres_df.dropna(subset=['movie_title'])

    print(director_df.head(), director_df.size, director_df.info)
    print(actors_df.head(), actors_df.size, actors_df.info)
    print(movie_df.head(), movie_df.size, movie_df.info)
    print(genres_df.head(), genres_df.size, genres_df.info)
    
    # End DB connection
    end_connection(connection, cursor)
    
if __name__ == '__main__':
    main()