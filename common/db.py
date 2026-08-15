"""Shared Postgres connection helper for the app database.

All credentials come from environment variables — sourced from `.env` via
docker-compose's `env_file:` directive (see docker-compose.yml). Nothing
here is hardcoded; if these env vars aren't set, connecting fails loudly
rather than falling back to a guessed default password.
"""
import os
import psycopg2
from psycopg2.extras import RealDictCursor


def get_connection():
    return psycopg2.connect(
        host=os.environ["APP_DB_HOST"],
        port=os.environ["APP_DB_PORT"],
        dbname=os.environ["APP_DB_NAME"],
        user=os.environ["APP_DB_USER"],
        password=os.environ["APP_DB_PASSWORD"],
        cursor_factory=RealDictCursor,
    )
