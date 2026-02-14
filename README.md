# whiskers 🐈

Python DB-API 2.0 driver for SQL Server. Standards-compliant. Boring-reliable. The sensible cat.

Part of [CopyCat](https://github.com/copycatdb) 🐱

## What is this?

A synchronous Python driver for SQL Server that follows the [DB-API 2.0](https://peps.python.org/pep-0249/) spec. Drop-in compatible with SQLAlchemy, Django, pandas `read_sql`, and everything else that speaks DB-API.

Built on [tabby](https://github.com/copycatdb/tabby) (Rust TDS protocol). No ODBC required.

```python
import whiskers

conn = whiskers.connect("Server=localhost,1433;UID=sa;PWD=pass;TrustServerCertificate=yes")
cur = conn.cursor()

cur.execute("SELECT * FROM users WHERE id = ?", [42])
rows = cur.fetchall()

# Works with pandas
import pandas as pd
df = pd.read_sql("SELECT * FROM big_table", conn)

conn.close()
```

## Why not psycopg2... oh wait, wrong database

This is psycopg2 for SQL Server. Same energy. Same reliability. Same "it just works." But instead of talking to Postgres over libpq, whiskers talks to SQL Server over tabby.

## Features

- DB-API 2.0 compliant (`connect`, `cursor`, `execute`, `fetch*`, `commit`, `rollback`)
- Parameterized queries (no SQL injection, you animal)
- Transaction support
- SQLAlchemy dialect (planned)
- Django backend (planned)

## Status

🚧 Coming soon.

## Attribution

Inspired by [psycopg2](https://github.com/psycopg/psycopg2), the driver that taught Python developers what database access should feel like. We copied the vibes.

## License

MIT
