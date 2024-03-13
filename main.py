import psycopg2
from psycopg2 import OperationalError
from config.db_config import get_db_info



def get_connection():
    filename='config/db_connection_info.ini'
    section='postgres-user123-db'
    db_info = get_db_info(filename, section)

    try:
        connection = psycopg2.connect(**db_info)
        print("Successfully connected to the database.")

        connection.autocommit = True

        cursor = connection.cursor()

        cursor.close()
        connection.close()

    except OperationalError:
        print("Error connecting to the database :/")
