# IMDB Movies Data Cleaning

This project involves cleaning and preprocessing data from the IMDb Movies dataset. The dataset contains information about movies, including details like the title, director, cast, budget, gross earnings, and more.

## Dataset
The dataset can be found [here](https://github.com/yash91sharma/IMDB-Movie-Dataset-Analysis/blob/master/movie_metadata.csv).

## Project Structure

The project is structured as follows:

- `data/`: This directory contains the input and output data files.
- `reports/`: Contains profiling reports generated for the raw and cleaned data.
- `scripts/`: Contains Python scripts for data cleaning and profiling.
- `queries/`: Contains SQL query templates and scripts.
- `config/`: Contains configuration files and scripts.
- `schema.json` : Schema with columns and insertion types for tables.
- `main.py`: Main script to execute the data cleaning and storing process.

## Dependencies

- Python 3.x
- pandas
- numpy
- ydata_profiling
- psycopg2-binary
- Jinja2

Install dependencies using:
`pip install -r requirements.txt`

## Usage

To run the data cleaning and storing process, execute the following command:
`python3 main.py`