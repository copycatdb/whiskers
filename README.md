# whiskers 🐈

PEP 249 DB-API 2.0 Python driver for SQL Server. Drop-in pyodbc alternative.

Part of [CopyCat](https://github.com/copycatdb) 🐱

## What is this?

A synchronous DB-API 2.0 compliant Python driver for SQL Server built on [tabby](https://github.com/copycatdb/tabby) (Rust TDS protocol) and PyO3. No ODBC driver manager, no unixODBC, no FreeTDS — just pure Rust talking TDS on the wire.

ODBC is the OG. It's been connecting databases since 1992 and it's earned its respect. But sometimes you just want `pip install` to work without hunting down platform-specific driver packages. That's whiskers.

```python
import whiskers

conn = whiskers.connect(
    "Server=localhost,1433;Database=master;UID=sa;PWD=pass;TrustServerCertificate=yes;"
)
cursor = conn.cursor()
cursor.execute("SELECT @@VERSION")
print(cursor.fetchone())
conn.close()
```

## Features

- **PEP 249 DB-API 2.0** compliant — works with any code that uses the standard Python database API
- **Drop-in pyodbc replacement** — same `connect()` / `Cursor` / `fetchone()` patterns
- **No ODBC dependency** — powered by [tabby](https://github.com/copycatdb/tabby), a pure Rust TDS implementation
- **Full type support** — datetime, decimal, UUID, binary, all the SQL Server types
- **Connection pooling** — built-in pool management
- **Catalog functions** — `tables()`, `columns()`, `primaryKeys()`, `foreignKeys()`, `statistics()`, `procedures()`
- **Thread-safe** — threadsafety level 1

## Test Results

Tested against the pyodbc compatibility test suite:

```
552 passed, 3 failed, 10 skipped
```

The 3 failures are edge cases around large data truncation, aggressive threading, and error message formatting — not correctness issues.

## Installation

```bash
pip install copycatdb-whiskers
```

## Connection String

Same format you're used to from pyodbc/ODBC:

```
Server=hostname,port;Database=dbname;UID=user;PWD=password;TrustServerCertificate=yes;
```

## Part of CopyCat

- [**tabby**](https://github.com/copycatdb/tabby) — Rust TDS protocol library (the engine)
- [**whiskers**](https://github.com/copycatdb/whiskers) — Sync DB-API 2.0 driver (you are here)
- [**hiss**](https://github.com/copycatdb/hiss) — Async Python driver (coming soon)

## License

MIT
