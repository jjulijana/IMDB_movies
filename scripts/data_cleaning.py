import numpy as np
import pandas as pd

def drop_duplicates(data: pd.DataFrame) -> pd.DataFrame:
    return data.drop_duplicates(inplace=True)

def drop_columns(data):
    columns_to_drop = ['facenumber_in_poster']
    return data.drop(columns=columns_to_drop, inplace=True)

def remove_null_terminating_char(data):
    data['movie_title'] = data['movie_title'].apply(lambda x: x[:-1]).str.rstrip()

def convert_to_int(data):
    numerical_cols = data.select_dtypes(include=np.number).columns
    for col in numerical_cols:
        if (data[col] % 1 == 0).all():
            data[col] = data[col].astype('int64')
    
def fill_missing_values(data):
    # Fill missing values for specific columns
    name_cols = [col for col in data.columns if col.endswith('name')]
    for col in name_cols:
        data[col] = data[col].fillna('unknown')

    mode_cols = ['color', 'language', 'country', 'aspect_ratio']
    for col in mode_cols:
        data[col] = data[col].fillna(data[col].mode().iloc[0])

    fill_values = {
        'plot_keywords': 'none',
        'content_rating': 'Not Rated'
    }
    data.fillna(fill_values, inplace=True)

    # Fill missing numerical values with median
    num_cols = data.select_dtypes(include=np.number).columns
    for col in num_cols:
        data[col] = data[col].fillna(data[col].median())

def get_unique_values(data):
    for column in data.columns:
        unique_values = data[column].unique()
    
        print(f"Column: {column}")
        print("Unique values:")
        print(unique_values)