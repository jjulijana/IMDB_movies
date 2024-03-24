from jinja2 import Environment, FileSystemLoader
import json

template_dir = 'queries'
query_template_env = Environment(loader=FileSystemLoader(template_dir))

schema_file_path = 'schema.json'

def generate_rendered_insert_queries() -> dict:
    with open(schema_file_path, 'r') as f:
        schema = json.load(f)

    insert_queries = {}
    insert_queries = {
        table: query_template_env.get_template('insert/insert.sql').render(table=table, columns=info["columns"])
        for table, info in schema.items() if info.get("type") == "regular"
    }

    return insert_queries

def generate_drop_template():
    drop_template = query_template_env.get_template('drop.sql')
    return drop_template