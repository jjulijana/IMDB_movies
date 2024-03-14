from configparser import ConfigParser
import psycopg2

def get_connection():
    filename='config/db_connection_info.ini'
    section='postgres-user123-db'
    db_info = get_db_info(filename, section)

    try:
        connection = psycopg2.connect(**db_info)
        cursor = connection.cursor()
        connection.autocommit = True

        return True, connection, cursor, None
    
    except Exception as e:
        return False, None, None, str(e)

def end_connection(connection, cursor):
    cursor.close()
    connection.close()


def get_db_info(filename, section):
    parser=ConfigParser()
    parser.read(filename)

    db_info={}
    if parser.has_section(section):
         key_val_tuple = parser.items(section) 
         for item in key_val_tuple:
             db_info[item[0]]=item[1] # index 0: key & index 1: value

    return db_info