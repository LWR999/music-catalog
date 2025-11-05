import sqlite3

def get_db(db_path: str) -> sqlite3.Connection:
    # Read-only SQLite with URI
    uri = f"file:{db_path}?mode=ro"
    con = sqlite3.connect(uri, uri=True, check_same_thread=False)
    con.row_factory = sqlite3.Row
    # lightweight perf for reads
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    return con

def row_to_dict(cur: sqlite3.Cursor, row: sqlite3.Row | None):
    if row is None:
        return None
    cols = [c[0] for c in cur.description]
    return {k: row[idx] for idx, k in enumerate(cols)}
