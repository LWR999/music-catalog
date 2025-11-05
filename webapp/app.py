# webapp/app.py
from __future__ import annotations
import os
import argparse
import sqlite3
from urllib.parse import quote
from flask import Flask, g, render_template, request, redirect, url_for, abort, jsonify
from werkzeug.middleware.proxy_fix import ProxyFix
import yaml

from db import get_db, row_to_dict
from jobrunner import start_job, job_status


# ---------- config helpers ----------

def _load_yaml(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

def _resolve_db_path(cfg: dict) -> str:
    """
    Prefer catalog.db_path; fall back to top-level db_path.
    """
    if not isinstance(cfg, dict):
        raise RuntimeError("Config YAML did not parse to a mapping.")
    cat = cfg.get("catalog") or {}
    if isinstance(cat, dict) and cat.get("db_path"):
        return str(cat["db_path"])
    if cfg.get("db_path"):
        return str(cfg["db_path"])
    raise RuntimeError("Could not find 'catalog.db_path' (or top-level 'db_path') in config YAML.")


# ---------- app factory ----------

def create_app(config_path: str) -> Flask:
    cfg_dict = _load_yaml(config_path)
    db_path = _resolve_db_path(cfg_dict)
    web_cfg = (cfg_dict.get("web") or {})

    secret_key = web_cfg.get("secret_key") or "dev-secret"
    deep_limit = (web_cfg.get("deep_limit") or 0) or None  # 0/None = unlimited
    debounce_sec = int(web_cfg.get("debounce_sec") or 10)

    app = Flask(__name__, static_folder="static", template_folder="templates")
    app.config.update(
        SECRET_KEY=secret_key,
        CONFIG_PATH=config_path,
        DB_PATH=db_path,
        DEEP_LIMIT=deep_limit,
        DEBOUNCE_SEC=debounce_sec,
    )
    # If behind a reverse proxy later
    app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1)

    # ---------- DB helpers ----------
    @app.before_request
    def open_db():
        # open read-only connection using the DB path from the YAML
        g.db = get_db(app.config["DB_PATH"])

    @app.teardown_request
    def close_db(exc):
        db = getattr(g, "db", None)
        if db is not None:
            db.close()

    def q_one(sql, params=()):
        cur = g.db.execute(sql, params)
        row = cur.fetchone()
        return row_to_dict(cur, row) if row else None

    def q_all(sql, params=()):
        cur = g.db.execute(sql, params)
        rows = cur.fetchall()
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, r)) for r in rows]

    # ---------- routes ----------
    @app.get("/")
    def home():
        rows = q_all("""
          SELECT id, folder_artist, folder_title, folder_path, item_count, updated_at
          FROM album
          ORDER BY COALESCE(updated_at, '') DESC
          LIMIT 48
        """)
        return render_template("albums.html", albums=rows, title="Recent Albums")

    @app.get("/albums")
    def albums():
        rows = q_all("""
          SELECT id, folder_artist, folder_title, folder_path, item_count
          FROM album
          ORDER BY COALESCE(folder_artist,''), COALESCE(folder_title,'')
          LIMIT 500
        """)
        return render_template("albums.html", albums=rows, title="Albums (A–Z)")

    @app.get("/album/<int:album_id>")
    def album_detail(album_id: int):
        album = q_one("""
          SELECT id, folder_artist, folder_title, folder_path, item_count, updated_at, album_fingerprint
          FROM album WHERE id=?
        """, (album_id,))
        if not album:
            abort(404)

        tracks = q_all("""
          SELECT path, size_bytes, mtime_ns, codec, bit_depth, sample_rate_hz, channels,
                 duration_sec, status, last_seen, is_missing
          FROM track
          WHERE album_id=?
          ORDER BY path
        """, (album_id,))
        for t in tracks:
            t["path_token"] = quote(t["path"], safe="")

        cover_url = url_for("static", filename="placeholder.jpg")
        return render_template("album.html", album=album, tracks=tracks, cover_url=cover_url)

    @app.get("/search")
    def search():
        q = request.args.get("q", "").strip()
        results = {"albums": [], "tracks": []}
        if q:
            like = f"%{q}%"
            results["albums"] = q_all("""
              SELECT id, folder_artist, folder_title, folder_path
              FROM album
              WHERE COALESCE(folder_artist,'') LIKE ? OR COALESCE(folder_title,'') LIKE ?
              ORDER BY folder_artist, folder_title
              LIMIT 100
            """, (like, like))
            results["tracks"] = q_all("""
              SELECT t.path, t.album_id, t.codec, t.bit_depth, t.sample_rate_hz, t.duration_sec,
                     a.folder_artist, a.folder_title
              FROM track t
              JOIN album a ON a.id = t.album_id
              WHERE t.path LIKE ?
                 OR a.folder_artist LIKE ?
                 OR a.folder_title LIKE ?
              ORDER BY a.folder_artist, a.folder_title
              LIMIT 150
            """, (like, like, like))
        return render_template("search.html", q=q, results=results)

    # ---------- utils ----------
    @app.get("/utils")
    def utils_page():
        stats = q_one("""
          SELECT
            (SELECT COUNT(*) FROM track) AS total,
            (SELECT COUNT(*) FROM track WHERE status IN ('DIRTY_META','NEW','DEEP_PENDING')) AS dirty,
            (SELECT COUNT(*) FROM track WHERE status='TAGGED') AS tagged,
            (SELECT COUNT(*) FROM track WHERE status='ERROR') AS errors,
            (SELECT COUNT(*) FROM track WHERE is_missing=1) AS missing,
            (SELECT COUNT(*) FROM album) AS albums,
            (SELECT COUNT(*) FROM album WHERE album_fingerprint IS NOT NULL) AS albums_fp,
            (SELECT COUNT(*) FROM disc) AS discs
        """)
        last = q_one("""
          SELECT id, started_at, command FROM run_event ORDER BY id DESC LIMIT 1
        """)
        jobs = job_status()
        return render_template("utils.html", stats=stats, last=last, jobs=jobs)

    @app.post("/utils/run")
    def utils_run():
        action = request.form.get("action")
        cfg_path = app.config["CONFIG_PATH"]
        if action == "changed":
            jid = start_job([
                "mc", "inventory", "changed",
                "--config", cfg_path,
                "--debounce-sec", str(app.config["DEBOUNCE_SEC"]),
            ])
        elif action == "deep":
            args = ["mc", "inventory", "deep", "--config", cfg_path]
            if app.config.get("DEEP_LIMIT"):
                args += ["--limit", str(app.config["DEEP_LIMIT"])]
            jid = start_job(args)
        else:
            abort(400, "Unknown action")
        return redirect(url_for("utils_page"))

    @app.get("/utils/jobs.json")
    def utils_jobs_json():
        return jsonify(job_status())

    return app

    @app.get("/albums")
    def albums():
        # pagination
        try:
            page = max(1, int(request.args.get("page", "1")))
        except ValueError:
            page = 1
        try:
            per_page = max(12, min(240, int(request.args.get("per_page", "96"))))  # sane defaults
        except ValueError:
            per_page = 96

        total = q_one("SELECT COUNT(*) AS n FROM album")["n"]
        pages = max(1, (total + per_page - 1) // per_page)
        if page > pages:
            page = pages

        offset = (page - 1) * per_page

        rows = q_all(
            """
            SELECT id, folder_artist, folder_title, folder_path, item_count
            FROM album
            ORDER BY COALESCE(folder_artist,''), COALESCE(folder_title,'')
            LIMIT ? OFFSET ?
            """,
            (per_page, offset),
        )

        return render_template(
            "albums.html",
            albums=rows,
            title="Albums (A–Z)",
            total_albums=total,
            page=page,
            pages=pages,
            per_page=per_page,
            showing=len(rows),
        )

    @app.get("/albums/all")
    def albums_all():
        total = q_one("SELECT COUNT(*) AS n FROM album")["n"]
        rows = q_all("""
          SELECT id, folder_artist, folder_title, folder_path, item_count
          FROM album
          ORDER BY COALESCE(folder_artist,''), COALESCE(folder_title,'')
        """)
        return render_template("albums.html",
                               albums=rows, title="All Albums",
                               total_albums=total, page=1, pages=1,
                               per_page=total, showing=len(rows))

# ---------- entrypoint ----------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Music Catalog Web (YAML-driven)")
    parser.add_argument("--config", required=True, help="Path to YAML config (e.g., dev.yaml)")
    parser.add_argument("--port", type=int, default=5001, help="Port to bind (default 5001)")
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind (default 0.0.0.0)")
    args = parser.parse_args()

    if not os.path.exists(args.config):
        raise SystemExit(f"ERROR: config path does not exist: {args.config}")

    app = create_app(config_path=args.config)
    app.run(host=args.host, port=args.port, debug=False)
