from jinja2 import Environment, FileSystemLoader
import json

class QueryTemplates:
    _instance = None

    def __new__(cls, template_dir='queries', schema_file_path='schema.json'):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.template_dir = template_dir
            cls._instance.schema_file_path = schema_file_path
            cls._instance.env = Environment(loader=FileSystemLoader(cls._instance.template_dir))
            cls._instance.load_templates()
        return cls._instance
    
    def load_templates(self):
        with open(self.schema_file_path, 'r') as f:
            schema = json.load(f)
        
        self.rendered_insert = {}
        self.drop_template = self.env.get_template('drop.sql')
        
        self.rendered_insert = {
            table: self.env.get_template('insert/insert.sql').render(table=table, columns=info["columns"])
            for table, info in schema.items() if info.get("type") == "regular"
        }

    def get_rendered_insert(self, table: str) -> str:
        return self.rendered_insert.get(table)

    def get_drop_template(self) -> str:
        return self.drop_template
    
    def render_drop_template(self, table: str) -> str:
        return self.drop_template.render(table=table)
    
    def get_template(self, path: str) -> str:
        return self.env.get_template(path)

    def render_query(self, path: str) -> str:
        return self.get_template(path).render()
    
    def render_query(self, path: str, **render_items: str) -> str:
        template = self.env.get_template(path)
        return template.render(render_items)
    