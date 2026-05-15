#!/usr/bin/env python3
"""Apply pending SQL migrations in order."""

import os
import sys
from pathlib import Path

from catalog.db import get_connection

MIGRATIONS_DIR = Path(__file__).parent / "migrations"


def ensure_migrations_table(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS schema_migrations (
            filename   VARCHAR(255) PRIMARY KEY,
            applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
    """)


def applied_migrations(cur):
    cur.execute("SELECT filename FROM schema_migrations ORDER BY filename")
    return {row[0] for row in cur.fetchall()}


def run():
    migration_files = sorted(MIGRATIONS_DIR.glob("*.sql"))
    if not migration_files:
        print("No migration files found.")
        return

    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                ensure_migrations_table(cur)
                done = applied_migrations(cur)

                pending = [f for f in migration_files if f.name not in done]
                if not pending:
                    print("All migrations up to date.")
                    return

                for path in pending:
                    print(f"Applying {path.name}...")
                    cur.execute(path.read_text())
                    cur.execute(
                        "INSERT INTO schema_migrations (filename) VALUES (%s)",
                        (path.name,),
                    )
                    print(f"  done.")
    finally:
        conn.close()


if __name__ == "__main__":
    try:
        run()
    except Exception as exc:
        print(f"Migration failed: {exc}", file=sys.stderr)
        sys.exit(1)
