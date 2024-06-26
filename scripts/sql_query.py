import time
import pandas as pd
from io import StringIO
from functools import wraps
from psycopg2.extras import execute_values

from queries.query_templates import render_template, render_query

def timeit(func):
    @wraps(func)
    def timeit_wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        total_time = end_time - start_time
        args_str = args[1] if args else ""
        print(f'Function {func.__name__}({args_str}) took {total_time:.4f} seconds')
        return result
    return timeit_wrapper

import redis
import pickle

def redis_cache(func):
    r = redis.Redis(host='localhost', port=6379, db=0)

    @wraps(func)
    def wrapper(*args, **kwargs):
        cache_key = f"{func.__name__}:{args}"
        cached_result = r.get(cache_key)
        if cached_result:
            print(f"Cache hit for {func.__name__}({args})")
            return pickle.loads(cached_result)
        else:
            result = func(*args, **kwargs)
            r.set(cache_key, pickle.dumps(result))
            return result

    return wrapper


class SQLQuery:
    def __init__(self, connection):
        self.connection = connection
        self.cursor = connection.cursor()

    @redis_cache
    def execute(self, query: str) -> None:
        self.cursor.execute(query)
        self.connection.commit()

    @timeit
    def execute_path(self, path: str, render_items: dict) -> None:
        query = render_query(path, **render_items)
        self.execute(query)
        
    @redis_cache
    @timeit
    def execute_values_wrapper(self, query: str, argslist: list, template: str = "") -> None:
        execute_values(
            cur=self.cursor,
            sql=query,
            argslist=argslist,
            template=template
        )
        self.connection.commit()
        
    @timeit
    def execute_drop(self, table_name: str) -> None:
        self.execute(render_template(table_name, 'drop'))
        
    @redis_cache
    @timeit
    def execute_insert(self, table_name: str, df: pd.DataFrame) -> None:
        insert_query = render_template(table_name, 'insert')
        if insert_query is None:
            raise ValueError(f"Insert info for table '{table_name}' is not set in schema.")
        
        self.cursor.executemany(
            insert_query, 
            df.to_dict(orient="records")
        )
        self.connection.commit()

    @timeit
    def execute_create(self, table_name: str) -> None:
        create_query = render_query(f'create/{table_name}.sql')
        self.execute_drop(table_name)
        self.execute(create_query)
    
    def execute_create_insert(self, table_name: str, df: pd.DataFrame) -> None:
        self.execute_create(table_name)
        self.execute_insert(table_name, df)

    @redis_cache
    @timeit
    def copy_expert_insert(self, table_name: str, df: pd.DataFrame) -> None:
        insert_query = render_template(table_name, 'insert')
        if insert_query is None:
            raise ValueError(f"Insert info for table '{table_name}' is not set in schema.")
        
        sio = StringIO()
        df.to_csv(sio, index=False, header=False)
        sio.seek(0)
        self.cursor.copy_expert(sql=insert_query, file=sio)
        self.connection.commit()
        
    def fetchall(self) -> list:
        return self.cursor.fetchall()

    def close(self) -> None:
        self.cursor.close()
