from jinja2 import Environment, FileSystemLoader
import json

TEMPLATE_DIR='queries'
SCHEMA_FILE_PATH='schema.json'
env = Environment(loader=FileSystemLoader(TEMPLATE_DIR))

def load_table_info(table: str) -> dict:
    try:
        with open(SCHEMA_FILE_PATH, 'r') as f:
            schema = json.load(f)
    except FileNotFoundError:
        raise FileNotFoundError("Schema file not found.")
    return schema[table]

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
    