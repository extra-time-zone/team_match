#!/usr/bin/env python3
"""Refresh the wide our_team -> source IDs lookup table."""

import argparse
import os
from pathlib import Path

import db_connection_test as dbt
import run_confirmed_pipeline as confirmed


SCRIPT_DIR = Path(__file__).resolve().parent


def parse_args():
    parser = argparse.ArgumentParser(description="Refresh team_mapping.our_team_source_map from source_team_mapping.")
    parser.add_argument("--env-file", default=".env.market", help="Env file fallback. Default: .env.market")
    parser.add_argument("--mapping-db", default=os.environ.get("MAPPING_MYSQL_DB", "team_mapping"))
    parser.add_argument("--init-schema", action="store_true")
    parser.add_argument("--our-team-id", type=int, action="append", help="Refresh one our_team_id. Can be repeated.")
    return parser.parse_args()


def load_env(path):
    env_path = Path(path).expanduser()
    if not env_path.is_absolute():
        env_path = SCRIPT_DIR / env_path
    if env_path.exists():
        dbt.load_env_file(env_path)


def main():
    args = parse_args()
    load_env(args.env_file)
    if args.init_schema:
        confirmed.init_schema()
    conn, process = confirmed.mapping_connection(args.mapping_db)
    try:
        with conn.cursor() as cur:
            confirmed.refresh_our_team_source_map(cur, args.our_team_id)
            if args.our_team_id:
                refreshed = len(set(args.our_team_id))
            else:
                cur.execute("SELECT COUNT(*) AS row_count FROM our_team_source_map")
                refreshed = cur.fetchone()["row_count"]
        conn.commit()
        print(f"refreshed_our_team_source_map_rows={refreshed}")
    except Exception:
        conn.rollback()
        raise
    finally:
        confirmed.close_mapping_connection(conn, process)


if __name__ == "__main__":
    main()
