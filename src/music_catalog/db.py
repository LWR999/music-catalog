from __future__ import annotations
import sqlite3, pathlib, time

def connect(db_path: str) -> sqlite3.Connection:
    pathlib.Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(db_path)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA foreign_keys=ON;")
    return con

def migrate(con: sqlite3.Connection):
    from importlib.resources import files
    schema_sql = files("music_catalog").joinpath("schema.sql").read_text(encoding="utf-8")
    con.executescript(schema_sql)
    con.commit()

def now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
