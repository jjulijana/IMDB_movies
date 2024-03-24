import os
import pandas as pd
from jinja2 import Environment, FileSystemLoader

from config.db_config import get_connection, end_connection
from scripts.data_profiling import generate_profile_report, generate_report_if_not_exists
from scripts.data_cleaning import drop_duplicates, drop_columns, fill_missing_values, remove_null_terminating_char, convert_to_int
from scripts.data_processing import process_actor_data
import scripts.sql_query 


def main():
    # Connect to DB
    successful, connection, error_message = get_connection()
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
    directors_df = pd.DataFrame(data.director_name.value_counts()).reset_index().rename(columns = {'count' : 'movie_count'})
    
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

    # print(director_df.head(), director_df.size, director_df.info)
    # print(actors_df.head(), actors_df.size, actors_df.info)
    # print(movie_df.head(), movie_df.size, movie_df.info)
    # print(genres_df.head(), genres_df.size, genres_df.info)


    # Work with DB
    template_dir = 'queries'
    query_template_env = Environment(loader=FileSystemLoader(template_dir))

    # Instantiate SQLQuery class
    sql_query = scripts.sql_query.SQLQuery(connection)

    drop_template = query_template_env.get_template('drop.sql')
    
    sql_query.execute(drop_template.render(table="directors"))
    create_directors_template = query_template_env.get_template('create/director.sql')
    sql_query.execute(create_directors_template.render())

    sql_query.execute_insert("directors", directors_df)

    sql_query.execute(drop_template.render(table="actors"))
    create_actors_template = query_template_env.get_template('create/actors.sql')
    sql_query.execute(create_actors_template.render())

    sql_query.execute_insert("actors", actors_df)

    sql_query.execute(drop_template.render(table="movies"))
    create_movies_template = query_template_env.get_template('create/movies.sql')
    sql_query.execute(create_movies_template.render())

    sql_query.execute_insert("movies", movie_df)
    
    sql_query.execute(drop_template.render(table="genres_tmp"))
    create_genres_tmp_template = query_template_env.get_template('create/genres_tmp.sql')
    sql_query.execute(create_genres_tmp_template.render())

    insert_genres_tmp_template = query_template_env.get_template('insert/genres_tmp.sql')
    sql_query.copy_expert(insert_genres_tmp_template.render(), genres_df)
    
    sql_query.execute(drop_template.render(table="movie_genres"))
    create_movie_genres_template = query_template_env.get_template('create/movie_genres.sql')
    sql_query.execute(create_movie_genres_template.render())
    sql_query.execute(drop_template.render(table="genres_tmp"))

    template = query_template_env.get_template('select_limit.sql')
    query = template.render(table="movie_genres", limit=10)

    sql_query.execute(query)
    rows = sql_query.fetchall()
    for row in rows:
        print(row)

    sql_query.close()
    
    # End DB connection
    end_connection(connection)
    
if __name__ == '__main__':
    main()