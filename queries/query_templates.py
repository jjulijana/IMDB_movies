from jinja2 import Environment, FileSystemLoader
import pymongo
from pymongo import MongoClient

TEMPLATE_DIR='queries'
env = Environment(loader=FileSystemLoader(TEMPLATE_DIR))

CONNECTION_STRING = "mongodb://user123:pass123@localhost:27017/"
DATABASE = "schema"
COLLECTION = "schema"

def load_table_info(table: str) -> dict:
    client = pymongo.MongoClient(CONNECTION_STRING)
    db = client[DATABASE]
    collection = db[COLLECTION]

    document = collection.find_one({"_id": table})
    if document is None:
        raise ValueError(f"No document found for table '{table}'")

    client.close()
    return document

def render_template(table_name: str, query_type: str) -> str:

    match query_type:
        
        case 'drop':
            return env.get_template('drop.sql').render(table=table_name)

        case 'insert':
            table_info = load_table_info(table_name)
            info_type = table_info.get("type")
            columns = table_info.get("columns")

            return env.get_template(f'insert/{info_type}.sql').render(table=table_name, columns=columns)
                    
        case _:
            print("Query type not suported")
            return ""

def render_query(path: str, **render_items: str) -> str:
    return env.get_template(path).render(render_items)
    