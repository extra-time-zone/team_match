#!/usr/bin/env python3
"""
Import TheSports official SportRadar match mapping into team_mapping.ts_offical_match.

The table name intentionally keeps the requested spelling: ts_offical_match.
"""

import argparse
import json
import os
import urllib.parse
import urllib.request
from pathlib import Path
from types import SimpleNamespace

import db_connection_test as dbt
import team_mapping_core as core


SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_SPORTS = ("football", "basketball")


def parse_args():
    parser = argparse.ArgumentParser(description="Import TheSports official SportRadar match mapping.")
    parser.add_argument("--env-file", default=".env.market", help="Local env file fallback. Default: .env.market")
    parser.add_argument("--mapping-db", default=os.environ.get("MAPPING_MYSQL_DB", "team_mapping"))
    parser.add_argument("--sports", default="football,basketball", help="Comma separated sports. Default: football,basketball")
    parser.add_argument("--api-user", default=os.environ.get("THESPORTS_API_USER"))
    parser.add_argument("--api-secret", default=os.environ.get("THESPORTS_API_SECRET"))
    parser.add_argument("--timeout", type=float, default=30)
    parser.add_argument("--test1-mysql-password", default=os.environ.get("TEST1_MYSQL_PASSWORD"))
    parser.add_argument("--mysql-user", default=os.environ.get("TEST1_MYSQL_USER", "root"))
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def load_env_file(path):
    env_path = Path(path).expanduser()
    if not env_path.is_absolute():
        env_path = SCRIPT_DIR / env_path
    if env_path.exists():
        dbt.load_env_file(env_path)


def require_api_credentials(args):
    if not args.api_user or not args.api_secret:
        raise SystemExit("Missing API credentials. Set THESPORTS_API_USER and THESPORTS_API_SECRET.")


def fetch_sport_rows(sport, args):
    query = urllib.parse.urlencode({"user": args.api_user, "secret": args.api_secret})
    url = f"https://api.thesports.com/v1/{sport}/sport_radar/match/list?{query}"
    with urllib.request.urlopen(url, timeout=args.timeout) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if payload.get("code") not in (0, 200):
        raise RuntimeError(f"TheSports API returned code={payload.get('code')} for sport={sport}")
    results = payload.get("results") or []
    if not isinstance(results, list):
        raise RuntimeError(f"TheSports API results is not a list for sport={sport}")
    return results


def ensure_table(conn, mapping_db):
    with conn.cursor() as cur:
        cur.execute(
            f"""
            CREATE DATABASE IF NOT EXISTS `{mapping_db}`
              CHARACTER SET utf8mb4
              COLLATE utf8mb4_unicode_ci
            """
        )
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS `{mapping_db}`.`ts_offical_match` (
                id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
                sport VARCHAR(32) NOT NULL,
                sport_radar_match_id VARCHAR(120) NOT NULL,
                thesports_uuid VARCHAR(120) NOT NULL,
                is_same TINYINT NOT NULL DEFAULT 1,
                raw_payload JSON NULL,
                fetched_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY (id),
                UNIQUE KEY uk_ts_offical_match_sport_sr (sport, sport_radar_match_id),
                UNIQUE KEY uk_ts_offical_match_sport_ts (sport, thesports_uuid),
                KEY idx_ts_offical_match_sport_same (sport, is_same),
                KEY idx_ts_offical_match_ts_uuid (thesports_uuid),
                KEY idx_ts_offical_match_sr_id (sport_radar_match_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """
        )


def upsert_rows(conn, mapping_db, sport, rows):
    if not rows:
        return 0
    values = []
    for row in rows:
        if row.get("match_id") is None or not row.get("thesports_uuid"):
            continue
        values.append(
            (
                sport,
                str(row["match_id"]),
                str(row["thesports_uuid"]),
                int(row.get("is_same") or 0),
                json.dumps(row, ensure_ascii=False),
            )
        )
    if not values:
        return 0
    with conn.cursor() as cur:
        cur.executemany(
            f"""
            INSERT INTO `{mapping_db}`.`ts_offical_match` (
                sport, sport_radar_match_id, thesports_uuid, is_same, raw_payload, fetched_at
            )
            VALUES (%s, %s, %s, %s, %s, NOW())
            ON DUPLICATE KEY UPDATE
                sport_radar_match_id=VALUES(sport_radar_match_id),
                thesports_uuid=VALUES(thesports_uuid),
                is_same=VALUES(is_same),
                raw_payload=VALUES(raw_payload),
                fetched_at=NOW()
            """,
            values,
        )
    return len(values)


def main():
    args = parse_args()
    load_env_file(args.env_file)
    args.api_user = args.api_user or os.environ.get("THESPORTS_API_USER")
    args.api_secret = args.api_secret or os.environ.get("THESPORTS_API_SECRET")
    args.test1_mysql_password = args.test1_mysql_password or os.environ.get("TEST1_MYSQL_PASSWORD")
    require_api_credentials(args)

    sports = [item.strip() for item in args.sports.split(",") if item.strip()]
    all_rows = {}
    for sport in sports:
        rows = fetch_sport_rows(sport, args)
        all_rows[sport] = rows
        print(f"{sport}_api_rows={len(rows)}")

    if args.dry_run:
        print("dry_run=true; not writing DB")
        return

    with core.mysql_connection("test1", mysql_password=args.test1_mysql_password, mysql_user=args.mysql_user) as conn:
        ensure_table(conn, args.mapping_db)
        total = 0
        for sport, rows in all_rows.items():
            written = upsert_rows(conn, args.mapping_db, sport, rows)
            total += written
            print(f"{sport}_upserted={written}")
        conn.commit()
    print(f"total_upserted={total}")


if __name__ == "__main__":
    main()
