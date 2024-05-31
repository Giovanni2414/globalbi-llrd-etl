import pyodbc
import pandas as pd

class DatabaseManager:

    def __init__(self, config_file, database_name, driver='{ODBC Driver 17 for SQL Server}', use_pooling=True):
        self.config_file = config_file
        self.database_name = database_name
        self.driver = driver
        self.use_pooling = use_pooling

        self.connection = None

        if self.use_pooling:
            self._connect_pool = pyodbc.pooling.ConnectionPool(minimum=1, maximum=10,
                                                                connection_factory=self._create_connection)
        else:
            self._connect_pool = None

    def _create_connection(self):
        config = self.config_file[self.database_name]
        conn = pyodbc.connect(f'DRIVER={self.driver};SERVER={config["server"]};DATABASE={config["database"]};UID={config["username"]};PWD={config["password"]}')
        print(f"{self.database_name} connected!")
        return conn


    def connect(self):
        if self.use_pooling:
            self.connection = self._connect_pool.get()
        else:
            self.connection = self._create_connection()

    def disconnect(self):
        if self.connection:
            if self.use_pooling:
                self.connection.close()
            else:
                self.connection.close()
            self.connection = None

    def begin_transaction(self):
        if not self.connection:
            raise Exception("Connection is not established. Call connect() method first.")

        self.transaction = self.connection.cursor().execute('BEGIN TRANSACTION')

    def commit_transaction(self):
        if not self.transaction:
            raise Exception("No transaction in progress.")

        self.transaction.commit()
        self.transaction = None

    def rollback_transaction(self):
        if not self.transaction:
            raise Exception("No transaction in progress.")

        self.transaction.rollback()
        self.transaction = None

    def execute_query(self, query):
        if not self.connection:
            raise Exception("Connection is not established. Call connect() method first.")

        cursor = self.connection.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()
        return rows
    
    def execute_query_no_results(self, query):
        if not self.connection:
            raise Exception("Connection is not established. Call connect() method first.")

        cursor = self.connection.cursor()
        try:
            cursor.execute(query)
            #self.connection.commit()
        except Exception as e:
            self.connection.rollback()
            raise e
        finally:
            cursor.close()
    
    def execute_query_get_pandas(self, query):
        if not self.connection:
            raise Exception("Connection is not established. Call connect() method first.")

        data = pd.read_sql_query(query, self.connection)
        return data
    
    def execute_single_insert(self, base_query, dataset):
        if not self.connection:
            raise Exception("Connection is not established. Call connect() method first.")

        cursor = self.connection.cursor()
        try:
            cursor.execute(base_query, dataset)
            self.connection.commit()
        except Exception as e:
            self.connection.rollback()
            raise e
        finally:
            cursor.close()

    def execute_bulk_insert(self, base_query, dataset):
        if not self.connection:
            raise Exception("Connection is not established. Call connect() method first.")

        cursor = self.connection.cursor()

        try:
            cursor.fast_executemany = True
            cursor.executemany(base_query, dataset)
            cursor.fast_executemany = False
            self.connection.commit()
        except Exception as e:
            self.connection.rollback()
            raise e
        finally:
            cursor.close()
        