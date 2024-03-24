from jinja2 import Environment, FileSystemLoader
import json

class QueryTemplates:
    _instance = None

    def __new__(cls, template_dir='queries', schema_file_path='schema.json'):
        """
        Singleton class for managing query templates.

        Args:
            template_dir (str, optional): Directory containing query templates. Defaults to 'queries'.
            schema_file_path (str, optional): Path to the schema file. Defaults to 'schema.json'.
        """
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.template_dir = template_dir
            cls._instance.schema_file_path = schema_file_path
            cls._instance.env = Environment(loader=FileSystemLoader(cls._instance.template_dir))
            cls._instance._load_templates()
        return cls._instance
    
    def _load_templates(self):
        try:
            with open(self.schema_file_path, 'r') as f:
                schema = json.load(f)
        except FileNotFoundError:
            raise FileNotFoundError("Schema file not found.")
        
        self._rendered_copy = {}
        self._drop_template = self.env.get_template('drop.sql')

        self._rendered_insert = {                           # dictionary comprehension
            table: self.env.get_template('insert/insert.sql').render(table=table, columns=info["columns"])
            if info.get("type") == "regular"
            else self.env.get_template('insert/copy.sql').render(table=table, columns=info["columns"])
            if info.get("type") == "with_csv"
            else None 
            for table, info in schema.items()
        }

    def get_rendered_insert(self, table_name: str) -> str:
        return self._rendered_insert.get(table_name)

    def get_drop_template(self) -> str:
        return self._drop_template
    
    def render_drop_template(self, table_name: str) -> str:
        return self._drop_template.render(table=table_name)
    
    def get_template(self, path: str) -> str:
        return self.env.get_template(path)

    def render_query(self, path: str, **render_items: str) -> str:
        return self.get_template(path).render(render_items)
    