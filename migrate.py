#!/usr/bin/env python3
"""Apply pending migrations in order. Supports .sql and .py migration files."""

import importlib.util
import sys
from pathlib import Path

from catalog.db import get_connection

MIGRATIONS_DIR = Path(__file__).parent / "migrations"


def ensure_migrations_table(conn):
    with conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    filename   VARCHAR(255) PRIMARY KEY,
                    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
            """)


def applied_migrations(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT filename FROM schema_migrations ORDER BY filename")
        return {row[0] for row in cur.fetchall()}


def record_migration(conn, filename):
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO schema_migrations (filename) VALUES (%s)",
                (filename,),
            )


def apply_sql(conn, path):
    with conn:
        with conn.cursor() as cur:
            cur.execute(path.read_text())


def apply_py(conn, path):
    spec = importlib.util.spec_from_file_location(path.stem, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    mod.run(conn)


def run():
    migration_files = sorted(
        f for f in MIGRATIONS_DIR.iterdir()
        if f.suffix in ('.sql', '.py') and f.name != '__init__.py'
    )
    if not migration_files:
        print("No migration files found.")
        return

    conn = get_connection()
    try:
        ensure_migrations_table(conn)
        done = applied_migrations(conn)
        pending = [f for f in migration_files if f.name not in done]

        if not pending:
            print("All migrations up to date.")
            return

        for path in pending:
            print(f"Applying {path.name}...")
            if path.suffix == '.sql':
                apply_sql(conn, path)
            elif path.suffix == '.py':
                apply_py(conn, path)
            record_migration(conn, path.name)
            print(f"  done.")
    finally:
        conn.close()


if __name__ == "__main__":
    try:
        run()
    except Exception as exc:
        print(f"Migration failed: {exc}", file=sys.stderr)
        sys.exit(1)
