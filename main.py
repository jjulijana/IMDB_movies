import os
import pandas as pd

from scripts.data_profiling import generate_profile_report, generate_report_if_not_exists
from scripts.data_cleaning import drop_duplicates, drop_columns, fill_missing_values, remove_null_terminating_char, convert_to_int

def main():
    # Load Data
    file_path = 'data/movie_metadata.csv'
    if not os.path.exists(file_path):
        print("File does not exist.")
        return
    raw_data = pd.read_csv(file_path)
    data = raw_data

    generate_report_if_not_exists(raw_data, 'reports/raw_data_report.html')

    # Manage data
    drop_duplicates(data)
    drop_columns(data, ['facenumber_in_poster'])
    remove_null_terminating_char(data)
    fill_missing_values(data)
    convert_to_int(data)

    # Save Data
    cleaned_file_path = 'data/cleaned_movie_metadata.csv'
    data.to_csv(cleaned_file_path, index=False)
    
    generate_report_if_not_exists(data, 'reports/cleaned_data_report.html')
    
if __name__ == '__main__':
    main()
