"""Four-way benchmark: pyodbc(msodbc) vs pyodbc(furball) vs mssql-python vs whiskers

Compares per-operation latency across 18 benchmarks.
Based on mssql-python's official bench_mssql.py benchmark suite.

Requires: pyodbc, mssql-python, whiskers installed.
Furball ODBC driver must be registered in /etc/odbcinst.ini.
Set DB_CONNECTION_STRING env var for TDS connection string.
"""

import os
import sys
import pyodbc
import mssql_python
import whiskers
import threading
import time
import statistics

# Connection strings
TDS_CS = os.environ.get(
    "DB_CONNECTION_STRING",
    "Server=localhost;UID=SA;PWD=TestPass123!;TrustServerCertificate=yes",
)
ODBC_CS = (
    "Driver={ODBC Driver 18 for SQL Server};"
    f"Server=localhost;UID=SA;PWD=TestPass123!;TrustServerCertificate=yes"
)
FURBALL_CS = (
    "Driver={Furball};"
    f"Server=localhost;UID=SA;PWD=TestPass123!;TrustServerCertificate=yes"
)

WARMUP = 2
ITERATIONS = 10


def connect_pyodbc():
    return pyodbc.connect(ODBC_CS)


def connect_furball():
    return pyodbc.connect(FURBALL_CS)


def connect_mssql():
    return mssql_python.connect(TDS_CS)


def connect_whiskers():
    return whiskers.connect(TDS_CS)


DRIVERS = [
    ("pyodbc", connect_pyodbc),
    ("furball", connect_furball),
    ("mssql-py", connect_mssql),
    ("whiskers", connect_whiskers),
]


def setup():
    conn = pyodbc.connect(ODBC_CS)
    cur = conn.cursor()
    cur.execute("""
        IF OBJECT_ID('perfbenchmark_child_table', 'U') IS NOT NULL DROP TABLE perfbenchmark_child_table;
        IF OBJECT_ID('perfbenchmark_parent_table', 'U') IS NOT NULL DROP TABLE perfbenchmark_parent_table;
        IF OBJECT_ID('perfbenchmark_table', 'U') IS NOT NULL DROP TABLE perfbenchmark_table;
        IF OBJECT_ID('perfbenchmark_stored_procedure', 'P') IS NOT NULL DROP PROCEDURE perfbenchmark_stored_procedure;
    """)
    cur.execute("CREATE TABLE perfbenchmark_table (id INT, name NVARCHAR(50), age INT)")
    cur.execute(
        "CREATE TABLE perfbenchmark_parent_table (id INT PRIMARY KEY, name NVARCHAR(50))"
    )
    cur.execute("""CREATE TABLE perfbenchmark_child_table (
        id INT PRIMARY KEY, parent_id INT, description NVARCHAR(100),
        FOREIGN KEY (parent_id) REFERENCES perfbenchmark_parent_table(id))""")
    cur.execute("""CREATE PROCEDURE perfbenchmark_stored_procedure AS BEGIN
        SELECT * FROM perfbenchmark_table; END""")
    conn.commit()
    cur.close()
    conn.close()


def cleanup():
    conn = pyodbc.connect(ODBC_CS)
    cur = conn.cursor()
    cur.execute("""
        IF OBJECT_ID('perfbenchmark_child_table', 'U') IS NOT NULL DROP TABLE perfbenchmark_child_table;
        IF OBJECT_ID('perfbenchmark_parent_table', 'U') IS NOT NULL DROP TABLE perfbenchmark_parent_table;
        IF OBJECT_ID('perfbenchmark_table', 'U') IS NOT NULL DROP TABLE perfbenchmark_table;
        IF OBJECT_ID('perfbenchmark_stored_procedure', 'P') IS NOT NULL DROP PROCEDURE perfbenchmark_stored_procedure;
    """)
    conn.commit()
    cur.close()
    conn.close()


def time_fn(fn, warmup=WARMUP, iterations=ITERATIONS):
    for _ in range(warmup):
        fn()
    times = []
    for _ in range(iterations):
        t0 = time.perf_counter()
        fn()
        times.append(time.perf_counter() - t0)
    return statistics.mean(times) * 1000


# ── Benchmark factories ──────────────────────────────────────────────


def make_select(cf):
    def fn():
        c = cf()
        cur = c.cursor()
        cur.execute("SELECT * FROM perfbenchmark_table")
        cur.fetchall()
        cur.close()
        c.close()

    return fn


def make_insert(cf):
    def fn():
        c = cf()
        cur = c.cursor()
        cur.execute(
            "INSERT INTO perfbenchmark_table (id, name, age) VALUES (1, 'John Doe', 30)"
        )
        c.commit()
        cur.close()
        c.close()

    return fn


def make_update(cf):
    def fn():
        c = cf()
        cur = c.cursor()
        cur.execute("UPDATE perfbenchmark_table SET age = 31 WHERE id = 1")
        c.commit()
        cur.close()
        c.close()

    return fn


def make_delete(cf):
    def fn():
        c = cf()
        cur = c.cursor()
        cur.execute("DELETE FROM perfbenchmark_table WHERE id = 1")
        c.commit()
        cur.close()
        c.close()

    return fn


def make_complex(cf):
    def fn():
        c = cf()
        cur = c.cursor()
        cur.execute(
            "SELECT name, COUNT(*) FROM perfbenchmark_table GROUP BY name HAVING COUNT(*) > 1"
        )
        cur.fetchall()
        cur.close()
        c.close()

    return fn


def make_fetchone(cf):
    def fn():
        c = cf()
        cur = c.cursor()
        cur.execute("SELECT * FROM perfbenchmark_table")
        cur.fetchone()
        cur.close()
        c.close()

    return fn


def make_fetchmany(cf):
    def fn():
        c = cf()
        cur = c.cursor()
        cur.execute("SELECT * FROM perfbenchmark_table")
        cur.fetchmany(10)
        cur.close()
        c.close()

    return fn


def make_stored_proc(cf):
    def fn():
        c = cf()
        cur = c.cursor()
        cur.execute("{CALL perfbenchmark_stored_procedure}")
        cur.fetchall()
        cur.close()
        c.close()

    return fn


def make_nested(cf):
    def fn():
        c = cf()
        cur = c.cursor()
        cur.execute(
            "SELECT * FROM (SELECT name, age FROM perfbenchmark_table) AS subquery WHERE age > 25"
        )
        cur.fetchall()
        cur.close()
        c.close()

    return fn


def make_join(cf):
    def fn():
        c = cf()
        cur = c.cursor()
        cur.execute(
            "SELECT a.name, b.age FROM perfbenchmark_table a JOIN perfbenchmark_table b ON a.id = b.id"
        )
        cur.fetchall()
        cur.close()
        c.close()

    return fn


def make_large_data(cf):
    def fn():
        c = cf()
        cur = c.cursor()
        cur.execute("SELECT * FROM perfbenchmark_table")
        while cur.fetchone():
            pass
        cur.close()
        c.close()

    return fn


def make_transaction(cf):
    def fn():
        c = cf()
        cur = c.cursor()
        cur.execute("BEGIN TRANSACTION")
        cur.execute(
            "INSERT INTO perfbenchmark_table (id, name, age) VALUES (1, 'John Doe', 30)"
        )
        cur.execute("UPDATE perfbenchmark_table SET age = 31 WHERE id = 1")
        cur.execute("DELETE FROM perfbenchmark_table WHERE id = 1")
        cur.execute("COMMIT")
        cur.close()
        c.close()

    return fn


def make_update_join(cf):
    def fn():
        c = cf()
        cur = c.cursor()
        cur.execute("""UPDATE perfbenchmark_child_table SET description = 'Updated'
            FROM perfbenchmark_child_table c
            JOIN perfbenchmark_parent_table p ON c.parent_id = p.id
            WHERE p.name = 'Parent 1'""")
        c.commit()
        cur.close()
        c.close()

    return fn


def make_delete_join(cf):
    def fn():
        c = cf()
        cur = c.cursor()
        cur.execute("""DELETE c FROM perfbenchmark_child_table c
            JOIN perfbenchmark_parent_table p ON c.parent_id = p.id
            WHERE p.name = 'Parent 1'""")
        c.commit()
        cur.close()
        c.close()

    return fn


def make_multi_conn(cf):
    def fn():
        conns = [cf() for _ in range(10)]
        for c in conns:
            cur = c.cursor()
            cur.execute("SELECT * FROM perfbenchmark_table")
            cur.fetchall()
            cur.close()
        for c in conns:
            c.close()

    return fn


def make_1000_conns(cf):
    def fn():
        threads = []
        for _ in range(1000):
            t = threading.Thread(target=lambda: cf().close())
            threads.append(t)
            t.start()
        for t in threads:
            t.join()

    return fn


def make_executemany(cf, fast=False):
    def fn():
        c = cf()
        cur = c.cursor()
        if fast:
            cur.fast_executemany = True
        data = [(i, "John Doe", 30) for i in range(100)]
        cur.executemany(
            "INSERT INTO perfbenchmark_table (id, name, age) VALUES (?, ?, ?)", data
        )
        c.commit()
        cur.close()
        c.close()

    return fn


def make_100_inserts(cf):
    def fn():
        c = cf()
        cur = c.cursor()
        for i in range(100):
            cur.execute(
                "INSERT INTO perfbenchmark_table (id, name, age) VALUES (?, ?, ?)",
                (i, "John Doe", 30),
            )
        c.commit()
        cur.close()
        c.close()

    return fn


# ── Test list ─────────────────────────────────────────────────────────

GENERIC_TESTS = [
    ("SELECT", make_select),
    ("INSERT", make_insert),
    ("UPDATE", make_update),
    ("DELETE", make_delete),
    ("Complex query", make_complex),
    ("Fetchone", make_fetchone),
    ("Fetchmany", make_fetchmany),
    ("Stored proc", make_stored_proc),
    ("Nested query", make_nested),
    ("Join query", make_join),
    ("Large dataset", make_large_data),
    ("Transaction", make_transaction),
    ("Update+join", make_update_join),
    ("Delete+join", make_delete_join),
    ("10 connections", make_multi_conn),
]


# ── Runner ────────────────────────────────────────────────────────────


def run_bench(label, fns, warmup=WARMUP, iterations=ITERATIONS):
    results = {}
    for name, fn in fns:
        results[name] = time_fn(fn, warmup, iterations)
    best_name = min(results, key=results.get)
    line = f"{label:<20s}"
    for name, _ in DRIVERS:
        line += f"  {results[name]:8.2f}ms"
    line += f"  {best_name:>10s}"
    print(line)
    return best_name


def main():
    print("Setting up benchmark tables...")
    setup()
    print("Setup done.\n")

    hdr = f"{'Benchmark':<20s}"
    for name, _ in DRIVERS:
        hdr += f"  {name:>10s}"
    hdr += f"  {'winner':>10s}"
    print(hdr)
    print("=" * len(hdr))

    wins = {name: 0 for name, _ in DRIVERS}

    # Generic tests
    for label, maker in GENERIC_TESTS:
        fns = [(name, maker(cf)) for name, cf in DRIVERS]
        winner = run_bench(label, fns)
        wins[winner] += 1

    # Executemany (driver-specific fast_executemany flag)
    print(f"\n--- executemany ---")
    fns = [
        ("pyodbc", make_executemany(connect_pyodbc, fast=True)),
        ("furball", make_executemany(connect_furball, fast=False)),
        ("mssql-py", make_executemany(connect_mssql)),
        ("whiskers", make_executemany(connect_whiskers)),
    ]
    winner = run_bench("Executemany(100)", fns)
    wins[winner] += 1

    fns = [(name, make_100_inserts(cf)) for name, cf in DRIVERS]
    winner = run_bench("100 inserts", fns)
    wins[winner] += 1

    # 1000 connections
    print(f"\n--- connection stress ---")
    results = {}
    for name, cf in DRIVERS:
        results[name] = time_fn(make_1000_conns(cf), warmup=1, iterations=3)
    best_name = min(results, key=results.get)
    wins[best_name] += 1
    line = f"{'1000 conns':<20s}"
    for name, _ in DRIVERS:
        line += f"  {results[name]:8.1f}ms"
    line += f"  {best_name:>10s}"
    print(line)

    # Summary
    print(f"\n{'=' * 60}")
    print("WINS:")
    for name, _ in DRIVERS:
        print(f"  {name}: {wins[name]}")

    print("\nCleaning up...")
    cleanup()
    print("Done.")


if __name__ == "__main__":
    main()
