import pandas as pd

def process_actor_data(data: pd.DataFrame, actor_column: str, actor_facebook_likes_column: str) -> pd.DataFrame:
    actor_count = data[actor_column].value_counts()
    actor_df = pd.DataFrame(actor_count).reset_index().rename(columns={actor_column: 'actor_name', 'count': 'movie_count'})
    actor_df = pd.merge(actor_df, data[[actor_column, actor_facebook_likes_column]], 
                        how='left', 
                        left_on='actor_name', 
                        right_on=actor_column)
    actor_df.drop(actor_column, axis=1, inplace=True)
    actor_df.rename(columns={actor_facebook_likes_column: 'actor_facebook_likes'}, inplace=True)
    actor_df.drop_duplicates(subset='actor_name', inplace=True)
    actor_df = actor_df[actor_df['actor_name'] != 'unknown']
    actor_df.reset_index(drop=True, inplace=True)
    return actor_df