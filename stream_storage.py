#!/usr/bin/env python3
"""
Local session log for on-demand camera streaming (rancho-cam-pi).

Mirrors growatt_storage.py's pattern (growatt-spf-monitor repo): a small
durable SQLite log written locally regardless of whether main-pi5/
Prometheus happen to be reachable at the time, so session history (how
much was streamed, how often, for how long) is never silently lost.
"""
import sqlite3
from contextlib import contextmanager


@contextmanager
def _connect(db_path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db(db_path):
    with _connect(db_path) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at TEXT NOT NULL,
                ended_at TEXT,
                duration_s REAL,
                bytes_sent INTEGER DEFAULT 0,
                bitrate_kbps INTEGER,
                stop_reason TEXT
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_sessions_started ON sessions(started_at)")


def start_session(db_path, started_at_iso, bitrate_kbps):
    with _connect(db_path) as conn:
        cur = conn.execute(
            "INSERT INTO sessions (started_at, bitrate_kbps) VALUES (?, ?)",
            (started_at_iso, bitrate_kbps),
        )
        return cur.lastrowid


def end_session(db_path, session_id, ended_at_iso, duration_s, bytes_sent, stop_reason):
    with _connect(db_path) as conn:
        conn.execute(
            """UPDATE sessions SET ended_at = ?, duration_s = ?, bytes_sent = ?, stop_reason = ?
               WHERE id = ?""",
            (ended_at_iso, duration_s, bytes_sent, stop_reason, session_id),
        )


def get_totals(db_path):
    """All-time totals, for the Prometheus textfile export."""
    with _connect(db_path) as conn:
        row = conn.execute(
            """SELECT COUNT(*) AS session_count,
                      COALESCE(SUM(bytes_sent), 0) AS total_bytes,
                      COALESCE(SUM(duration_s), 0) AS total_duration_s
               FROM sessions WHERE ended_at IS NOT NULL"""
        ).fetchone()
        return dict(row)


def get_last_session(db_path):
    with _connect(db_path) as conn:
        row = conn.execute(
            """SELECT * FROM sessions WHERE ended_at IS NOT NULL
               ORDER BY id DESC LIMIT 1"""
        ).fetchone()
        return dict(row) if row else None
