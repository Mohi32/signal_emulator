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
        self.conn = psycopg2.connect(
            host=host, database=database, user=user, password=password, port=port
        )
        self.engine = create_engine(self.connection_uri)

    def __repr__(self):
        return f"host:{self.host} database:{self.database} schema:{self.schema}"

    @property
    def connection_uri(self):
        return f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

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
