import time
import pandas as pd
from io import StringIO
from psycopg2.extras import execute_values

import queries.query_templates 

class SQLQuery:
    def __init__(self, connection):
        self.connection = connection
        self.cursor = connection.cursor()
        self.rendered_insert_queries = None

    def execute(self, query: str) -> None:
        self.cursor.execute(query)
        self.connection.commit()

    def execute_values_wrapper(self, sql: str, argslist: list, template: str = None) -> float:
        start_time = time.time()
        execute_values(
            cur=self.cursor,
            sql=sql,
            argslist=argslist,
            template=template
        )
        self.connection.commit()
        end_time = time.time()
        
        total_time = end_time - start_time
        return total_time    
    
    def execute_insert(self, table_name: str, df: pd.DataFrame) -> float:
        if self.rendered_insert_queries is None:
            self.get_rendered_insert_queries()

        if self.rendered_insert_queries is None:
            raise ValueError("Insert query is not set.")
        
        start_time = time.time()
        self.cursor.executemany(
            self.rendered_insert_queries[table_name], 
            df.to_dict(orient="records")
        )
        self.connection.commit()
        end_time = time.time()
        
        total_time = end_time - start_time
        return total_time    

    def get_rendered_insert_queries(self):
        self.rendered_insert_queries = queries.query_templates.generate_rendered_insert_queries()

    def copy_expert(self, sql: str, df: pd.DataFrame) -> float:
        start_time = time.time()
        sio = StringIO()
        df.to_csv(sio, index=None, header= None)
        sio.seek(0)
        self.cursor.copy_expert(sql=sql, file=sio)
        self.connection.commit()
        end_time = time.time()
        total_time = end_time - start_time
        return total_time

    def fetchall(self) -> list:
        return self.cursor.fetchall()

    def close(self) -> None:
        self.cursor.close()
        