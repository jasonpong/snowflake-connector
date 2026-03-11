import yaml
import keyring
import pandas as pd
import snowflake.connector


class SnowflakeClient:
    def __init__(self, config_path: str, profile: str = 'default'):
        """
        Initialize Snowflake connection from YAML config.

        Args:
            config_path: Path to YAML config file
            profile: Config profile name (default: 'default')
        """
        with open(config_path) as f:
            all_config = yaml.safe_load(f)
            self.config = all_config.get('snowflake', {}).get(profile)
            if not self.config:
                raise ValueError(f"No snowflake config found for profile: {profile}")

        required_keys = ['account', 'user', 'warehouse', 'database']
        missing = [k for k in required_keys if k not in self.config]
        if missing:
            raise ValueError(f"Missing required config keys: {', '.join(missing)}")

        self.profile = profile
        self.conn = None
        self.cursor = None

    def set_password(self, password: str):
        """Store password securely in system keyring."""
        service_name = f'snowflake_{self.profile}'
        keyring.set_password(service_name, self.config['user'], password)

    def _get_password(self) -> str:
        """Retrieve password from system keyring."""
        service_name = f'snowflake_{self.profile}'
        username = self.config['user']
        password = keyring.get_password(service_name, username)
        if not password:
            raise ValueError(
                f"No password found in keyring for {service_name} user {username}. "
                f"Use set_password() to store it."
            )
        return password

    def connect(self):
        """Establish connection to Snowflake."""
        if not self.conn:
            try:
                self.conn = snowflake.connector.connect(
                    account=self.config['account'],
                    user=self.config['user'],
                    password=self._get_password(),
                    warehouse=self.config['warehouse'],
                    database=self.config['database'],
                    schema=self.config.get('schema', 'PUBLIC'),
                    role=self.config.get('role'),
                )
                self.cursor = self.conn.cursor()
            except Exception as e:
                self.conn = None
                self.cursor = None
                raise ConnectionError(f"Failed to connect to Snowflake: {e}")

    def query(self, sql: str) -> list:
        """Execute query and return results as list of tuples."""
        if not self.cursor:
            self.connect()
        try:
            self.cursor.execute(sql)
        except Exception:
            self.close()
            self.connect()
            self.cursor.execute(sql)
        return self.cursor.fetchall()

    def query_df(self, sql: str) -> pd.DataFrame:
        """Execute query and return results as a DataFrame."""
        if not self.cursor:
            self.connect()
        try:
            self.cursor.execute(sql)
        except Exception:
            self.close()
            self.connect()
            self.cursor.execute(sql)
        columns = [desc[0] for desc in self.cursor.description]
        data = self.cursor.fetchall()
        return pd.DataFrame(data, columns=columns)

    def use(self, warehouse: str = None, database: str = None,
            schema: str = None, role: str = None):
        """Switch context without reconnecting."""
        if not self.cursor:
            self.connect()
        if role:
            self.cursor.execute(f"USE ROLE {role}")
        if warehouse:
            self.cursor.execute(f"USE WAREHOUSE {warehouse}")
        if database:
            self.cursor.execute(f"USE DATABASE {database}")
        if schema:
            self.cursor.execute(f"USE SCHEMA {schema}")

    def close(self):
        """Close connection."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        self.cursor = None
        self.conn = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def transfer(sf_client: 'SnowflakeClient', sdp_client, sql: str,
             target_table: str, batch_size: int = 10000,
             create_table: bool = True, overwrite: bool = False):
    """
    Pull data from Snowflake and load it into Impala/Hive via SDP.

    Args:
        sf_client: Connected SnowflakeClient instance
        sdp_client: Connected SDP instance
        sql: SELECT query to run against Snowflake
        target_table: Fully qualified target table in Impala/Hive (e.g. 'db.table')
        batch_size: Rows per INSERT batch (default: 10000)
        create_table: Auto-create target table if True (default: True)
        overwrite: DROP target table first if True (default: False)
    """
    SF_TO_HIVE_TYPES = {
        0: 'DECIMAL',
        1: 'FLOAT',
        2: 'STRING',
        3: 'TIMESTAMP',
        4: 'TIMESTAMP',
        5: 'STRING',
        6: 'TIMESTAMP',
        7: 'TIMESTAMP',
        8: 'TIMESTAMP',
        9: 'STRING',
        10: 'STRING',
        11: 'BINARY',
        12: 'TIMESTAMP',
        13: 'BOOLEAN',
    }

    df = sf_client.query_df(sql)

    if df.empty:
        print("No data returned from Snowflake query.")
        return 0

    if overwrite:
        sdp_client.query(f"DROP TABLE IF EXISTS {target_table}")
        create_table = True

    if create_table:
        col_defs = []
        for desc in sf_client.cursor.description:
            col_name = desc[0].lower()
            type_code = desc[1]
            hive_type = SF_TO_HIVE_TYPES.get(type_code, 'STRING')
            col_defs.append(f"  {col_name} {hive_type}")

        ddl = f"CREATE TABLE IF NOT EXISTS {target_table} (\n"
        ddl += ",\n".join(col_defs)
        ddl += "\n) STORED AS PARQUET"
        sdp_client.query(ddl)

    rows_loaded = 0
    for start in range(0, len(df), batch_size):
        batch = df.iloc[start:start + batch_size]
        values_list = []
        for _, row in batch.iterrows():
            vals = []
            for v in row:
                if pd.isna(v):
                    vals.append('NULL')
                elif isinstance(v, str):
                    vals.append("'" + v.replace("'", "\\'") + "'")
                else:
                    vals.append(str(v))
            values_list.append(f"({', '.join(vals)})")

        insert_sql = f"INSERT INTO {target_table} VALUES\n"
        insert_sql += ",\n".join(values_list)
        sdp_client.query(insert_sql)
        rows_loaded += len(batch)
        print(f"Loaded {rows_loaded}/{len(df)} rows...")

    print(f"Transfer complete. {rows_loaded} rows loaded to {target_table}.")
    return rows_loaded
