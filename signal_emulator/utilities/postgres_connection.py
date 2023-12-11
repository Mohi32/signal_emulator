import pandas as pd
import psycopg2
from psycopg2 import OperationalError
from sqlalchemy import create_engine


class PostgresConnection:
    def __init__(self, host, database, user, port, schema=None):
        self.host = host
        self.database = database
        self.user = user
        self.port = port
        self.schema = schema
        try:
            self.conn = psycopg2.connect(host=host, port=port, database=database, user=user)
        except OperationalError as e:
            raise OperationalError(
                f"{e}"
                f"Windows users: Postgres credentials pgpass should be stored in "
                "%APPDATA%/Roaming/postgresql/pgpass.conf"
            )
        self.engine = create_engine(self.connection_uri)

    def __repr__(self):
        return f"host:{self.host} database:{self.database} schema:{self.schema}"

    @property
    def connection(self):
        return psycopg2.connect(
            host=self.host, database=self.database, user=self.user, port=self.port
        )

    @property
    def connection_uri(self):
        return f"postgresql+psycopg2://{self.user}@{self.host}:{self.port}/{self.database}"

    def create_schema(self, schema_name=None):
        if not schema_name:
            schema_name = self.schema
        self.execute_sql(f'CREATE SCHEMA IF NOT EXISTS "{schema_name}";')

    def execute_sql(self, sql_query, return_data=False):
        with self.connection as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql_query)
                if return_data:
                    return cursor.fetchall()

    def read_table_from_df(self, schema, table):
        return pd.read_sql_query(f"SELECT * FROM {schema}.{table}", self.engine)

    def write_df_to_table(self, df, table, schema=None, dtypes=None):
        if schema is None:
            schema = self.schema
        df.to_sql(
            table, con=self.engine, if_exists="replace", index=False, schema=schema, dtype=dtypes
        )

    def read_table_to_df(self, table, schema=None, to_dict=False):
        if not schema:
            schema = self.schema
        table_exists = self.table_exists(schema, table)
        if table_exists:
            df = pd.read_sql_query(f"SELECT * FROM {schema}.{table}", con=self.engine)
        else:
            df = pd.DataFrame()
        if to_dict:
            return df.to_dict(orient="records")
        else:
            return df

    def table_exists(self, schema_name, table_name):
        table_exists = self.execute_sql(
            f"SELECT EXISTS(SELECT 1 FROM information_schema.tables "
            f"WHERE table_schema = '{schema_name}' AND table_name = '{table_name}')",
            return_data=True
        )
        return table_exists[0][0]
