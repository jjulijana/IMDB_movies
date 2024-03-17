import time
from io import StringIO
from psycopg2.extras import execute_values

class SQLQuery:
    def __init__(self, connection):
        self.connection = connection
        self.cursor = connection.cursor()

    def execute(self, query):
        self.cursor.execute(query)
        self.connection.commit()

    def execute_values(self, sql, argslist, template):
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

    def copy_expert(self, sql, db):
        start_time = time.time()
        sio = StringIO()
        db.to_csv(sio, index=None, header= None)
        sio.seek(0)
        self.cursor.copy_expert(sql=sql, file=sio)
        self.connection.commit()
        end_time = time.time()
        total_time = end_time - start_time
        return total_time

    def fetchall(self):
        return self.cursor.fetchall()

    def close(self):
        self.cursor.close()
        self.connection.close()