import psycopg2
import pandas as pd
from sqlalchemy import create_engine


class PostgresConnection:
    def __init__(self, host, database, user, password, port, schema=None):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.port = port
        self.schema = schema
        self.engine = create_engine(self.connection_uri)

    def __repr__(self):
        return f"host:{self.host} database:{self.database} schema:{self.schema}"

    @property
    def connection(self):
        return psycopg2.connect(
            host=self.host, database=self.database, user=self.user, password=self.password, port=self.port
        )

    @property
    def connection_uri(self):
        return f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

    def create_schema(self, schema_name=None):
        if not schema_name:
            schema_name = self.schema
        self.execute_sql(f'CREATE SCHEMA IF NOT EXISTS "{schema_name}";')

    def execute_sql(self, sql_query):
        with self.connection as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql_query)

    def read_table_from_df(self, schema, table):
        return pd.read_sql_query(f"SELECT * FROM {schema}.{table}", self.engine)

    def write_df_to_table(self, df, table, schema=None, dtypes=None):
        if schema is None:
            schema = self.schema
        df.to_sql(table, con=self.engine, if_exists="replace", index=False, schema=schema, dtype=dtypes)

    def read_table_to_df(self, table, schema=None, to_dict=False):
        if not schema:
            schema = self.schema
        df = pd.read_sql_query(f"SELECT * FROM {schema}.{table}", con=self.engine)
        if to_dict:
            return df.to_dict(orient="records")
        else:
            return df
