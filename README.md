# snowflake-client

Lightweight Python client for querying Snowflake and transferring results to Impala/Hive via [SDP](https://github.com/YOUR_USERNAME/sdp).

## Setup

```bash
pip install snowflake-connector-python pandas pyyaml keyring
```

Copy `config.example.yaml` to `config.yaml` and fill in your Snowflake connection details. **Do not commit `config.yaml` to version control.**

## Configuration

```yaml
snowflake:
  default:
    account: your-account.us-east-1
    user: your_username
    warehouse: YOUR_WAREHOUSE
    database: YOUR_DATABASE
    schema: PUBLIC
    role: YOUR_ROLE  # optional
```

Multiple profiles are supported — just add another block under `snowflake:` and pass the profile name to `SnowflakeClient`.

## Store credentials

Passwords are stored in your OS keyring (macOS Keychain, Windows Credential Manager, Linux Secret Service):

```python
from snowflake_client import SnowflakeClient

sf = SnowflakeClient('config.yaml')
sf.set_password('your_password')
```

Only needs to be run once per machine/profile.

## Usage

### Basic query

```python
from snowflake_client import SnowflakeClient

with SnowflakeClient('config.yaml') as sf:
    # Raw tuples
    rows = sf.query("SELECT * FROM events LIMIT 10")

    # DataFrame
    df = sf.query_df("SELECT * FROM events WHERE event_date >= '2025-01-01'")
```

### Switch context mid-session

```python
with SnowflakeClient('config.yaml') as sf:
    sf.use(database='FRAUD_DB', schema='RAW')
    df1 = sf.query_df("SELECT * FROM raw_events LIMIT 100")

    sf.use(schema='ANALYTICS')
    df2 = sf.query_df("SELECT * FROM enriched_events LIMIT 100")
```

### Transfer from Snowflake to Impala/Hive

Requires [SDP](https://github.com/YOUR_USERNAME/sdp) for the target connection.

```python
from snowflake_client import SnowflakeClient, transfer
from sdp import SDP

with SnowflakeClient('config.yaml') as sf, SDP('config.yaml', 'impala') as sdp:
    # Basic transfer — auto-creates target table
    transfer(
        sf_client=sf,
        sdp_client=sdp,
        sql="SELECT * FROM fraud_events WHERE event_date >= '2025-01-01'",
        target_table='fraud_analytics.sf_fraud_events'
    )

    # Overwrite existing table
    transfer(
        sf_client=sf,
        sdp_client=sdp,
        sql="SELECT * FROM fraud_events",
        target_table='fraud_analytics.sf_fraud_events',
        overwrite=True
    )

    # Append to existing table (skip DDL)
    transfer(
        sf_client=sf,
        sdp_client=sdp,
        sql="SELECT * FROM fraud_events WHERE event_date = '2025-03-10'",
        target_table='fraud_analytics.sf_fraud_events',
        create_table=False
    )

    # Custom batch size for large transfers
    transfer(
        sf_client=sf,
        sdp_client=sdp,
        sql="SELECT * FROM large_table",
        target_table='fraud_analytics.sf_large_table',
        batch_size=50000,
        overwrite=True
    )
```

### Multiple profiles

```yaml
snowflake:
  default:
    account: prod-account.us-east-1
    user: prod_user
    warehouse: PROD_WH
    database: PROD_DB
  dev:
    account: dev-account.us-east-1
    user: dev_user
    warehouse: DEV_WH
    database: DEV_DB
```

```python
prod = SnowflakeClient('config.yaml', profile='default')
dev = SnowflakeClient('config.yaml', profile='dev')
```

## Notes

- **Large transfers**: The `transfer` function uses batch INSERT statements. For very large datasets (millions of rows), consider writing to Parquet on HDFS and using `LOAD DATA` instead.
- **Credentials**: Never store passwords in config files. Use `set_password()` to store them in the OS keyring.
- **Config security**: Add `config.yaml` to `.gitignore`.

## License

MIT
