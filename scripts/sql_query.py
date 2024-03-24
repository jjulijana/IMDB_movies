import time
import pandas as pd
from io import StringIO
from psycopg2.extras import execute_values

from queries.query_templates import QueryTemplates

class SQLQuery:
    def __init__(self, connection, query_templates):
        self.connection = connection
        self.cursor = connection.cursor()
        self.query_templates = query_templates

    def execute(self, query: str) -> None:
        self.cursor.execute(query)
        self.connection.commit()

    def execute_path(self, path: str, render_items: dict) -> None:
        query = self.query_templates.render_query(path, **render_items)
        self.execute(query)
        
    def execute_values_wrapper(self, query: str, argslist: list, template: str = None) -> float:
        start_time = time.time()
        execute_values(
            cur=self.cursor,
            sql=query,
            argslist=argslist,
            template=template
        )
        self.connection.commit()
        end_time = time.time()
        
        total_time = end_time - start_time
        return total_time    

    def execute_drop(self, table_name: str) -> None:
        if self.query_templates.get_drop_template() is None:
            raise ValueError("Drop template is not set.")
        self.execute(self.query_templates.render_drop_template(table_name))
        
    def execute_insert(self, table_name: str, df: pd.DataFrame) -> float:
        insert = self.query_templates.get_rendered_insert(table_name)
        if insert is None:
            raise ValueError("Insert query for table " + table_name + " is not set in schema.")
        
        start_time = time.time()
        self.cursor.executemany(
            insert, 
            df.to_dict(orient="records")
        )
        self.connection.commit()
        end_time = time.time()
        
        total_time = end_time - start_time
        return total_time
    
    def execute_create(self, table_name: str) -> None:
        create_query = self.query_templates.render_query(f'create/{table_name}.sql')
        self.execute_drop(table_name)
        self.execute(create_query)
    
    def execute_create_insert(self, table_name: str, df: pd.DataFrame) -> float:
        self.execute_create(table_name)
        self.execute_insert(table_name, df)

    def copy_expert_insert(self, table_name: str, df: pd.DataFrame) -> float:
        start_time = time.time()
        sio = StringIO()
        df.to_csv(sio, index=None, header= None)
        sio.seek(0)
        query = self.query_templates.render_query(f'insert/{table_name}.sql')
        self.cursor.copy_expert(sql=query, file=sio)
        self.connection.commit()
        end_time = time.time()
        total_time = end_time - start_time
        return total_time

    def fetchall(self) -> list:
        return self.cursor.fetchall()

    def close(self) -> None:
        self.cursor.close()
        