#!/usr/bin/env python3
"""
Compare ls_sport_event.live_market_count with active ls_odds_change rows.

The comparison is read-only:
  - select events by ls_sport_event.scheduled in [start, end);
  - count ls_odds_change rows with the same event_id and status = 1;
  - report whether the count equals live_market_count.
"""

import argparse
import subprocess
from types import SimpleNamespace

import db_connection_test as dbt


DB_NAME = "test1-lsports-db"
SPORT_EVENT_TABLE = "ls_sport_event"
ODDS_CHANGE_TABLE = "ls_odds_change"
TEST1_HOST = "test-db.cluster-cdgqiwig2x00.us-west-2.rds.amazonaws.com"


def parse_args():
    parser = argparse.ArgumentParser(
        description="Check live_market_count against status=1 ls_odds_change counts."
    )
    parser.add_argument("--env-file", default=".env.market", help="Path to env file. Default: .env.market")
    parser.add_argument(
        "--mysql",
        choices=("test", "test1"),
        default="test",
        help="Named MySQL target. test=.env.market original MySQL, test1=new RDS. Default: test",
    )
    parser.add_argument("--mysql-user", help="Override MySQL user.")
    parser.add_argument("--mysql-password", help="Override MySQL password. Prefer --prompt-mysql-password.")
    parser.add_argument("--prompt-mysql-password", action="store_true", help="Prompt for the MySQL password without echoing it.")
    parser.add_argument("--start", required=True, help="Scheduled start time, inclusive. Example: 2026-05-14 00:00:00")
    parser.add_argument("--end", required=True, help="Scheduled end time, exclusive. Example: 2026-05-15 00:00:00")
    parser.add_argument("--limit", type=int, default=50, help="Max mismatch rows to print. Default: 50")
    parser.add_argument("--show-matched", action="store_true", help="Print matched rows too, up to --limit.")
    return parser.parse_args()


def make_connection_args(args):
    mysql_host = TEST1_HOST if args.mysql == "test1" else None
    prompt_password = args.prompt_mysql_password or (args.mysql == "test1" and not args.mysql_password)
    return SimpleNamespace(
        mysql_host=mysql_host,
        mysql_port=None,
        mysql_user=args.mysql_user,
        mysql_password=args.mysql_password,
        prompt_mysql_password=prompt_password,
        mysql_db="",
    )


def connect(config, local_port):
    return dbt.pymysql.connect(
        host="127.0.0.1",
        port=local_port,
        user=config["mysql_user"],
        password=config["mysql_password"],
        database=None,
        charset="utf8mb4",
        connect_timeout=10,
        read_timeout=120,
        write_timeout=20,
        cursorclass=dbt.pymysql.cursors.DictCursor,
    )


def fetch_summary(conn, start, end):
    sql = f"""
        SELECT
            COUNT(*) AS total_events,
            SUM(compared.live_market_count = compared.odds_status_1_count) AS matched_events,
            SUM(compared.live_market_count <> compared.odds_status_1_count) AS mismatched_events,
            COALESCE(SUM(compared.live_market_count), 0) AS total_live_market_count,
            COALESCE(SUM(compared.odds_status_1_count), 0) AS total_odds_status_1_count
        FROM (
            SELECT
                e.event_id,
                e.live_market_count,
                COUNT(o.id) AS odds_status_1_count
            FROM `{DB_NAME}`.`{SPORT_EVENT_TABLE}` e
            LEFT JOIN `{DB_NAME}`.`{ODDS_CHANGE_TABLE}` o
                ON o.event_id = e.event_id
                AND o.status = 1
            WHERE e.scheduled >= %s
                AND e.scheduled < %s
            GROUP BY e.event_id, e.live_market_count
        ) compared
    """
    with conn.cursor() as cur:
        cur.execute(sql, (start, end))
        return cur.fetchone()


def fetch_rows(conn, start, end, limit, show_matched):
    having = "" if show_matched else "HAVING e.live_market_count <> COUNT(o.id)"
    sql = f"""
        SELECT
            e.event_id,
            e.scheduled,
            e.live_market_count,
            COUNT(o.id) AS odds_status_1_count,
            e.live_market_count - COUNT(o.id) AS diff
        FROM `{DB_NAME}`.`{SPORT_EVENT_TABLE}` e
        LEFT JOIN `{DB_NAME}`.`{ODDS_CHANGE_TABLE}` o
            ON o.event_id = e.event_id
            AND o.status = 1
        WHERE e.scheduled >= %s
            AND e.scheduled < %s
        GROUP BY e.event_id, e.scheduled, e.live_market_count
        {having}
        ORDER BY ABS(e.live_market_count - COUNT(o.id)) DESC, e.scheduled, e.event_id
        LIMIT %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (start, end, limit))
        return cur.fetchall()


def main():
    args = parse_args()
    dbt.load_env_file(args.env_file)
    config = dbt.get_config(make_connection_args(args))

    tunnel_process = None
    try:
        local_port, tunnel_process = dbt.start_tunnel(config)
        conn = connect(config, local_port)
        try:
            summary = fetch_summary(conn, args.start, args.end)
            rows = fetch_rows(conn, args.start, args.end, args.limit, args.show_matched)
        finally:
            conn.close()
    finally:
        if tunnel_process:
            tunnel_process.terminate()
            try:
                tunnel_process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                tunnel_process.kill()

    print(f"mysql={args.mysql}")
    print(f"database={DB_NAME}")
    print(f"scheduled_range=[{args.start}, {args.end})")
    print(f"total_events={summary['total_events']}")
    print(f"matched_events={summary['matched_events'] or 0}")
    print(f"mismatched_events={summary['mismatched_events'] or 0}")
    print(f"total_live_market_count={summary['total_live_market_count']}")
    print(f"total_odds_status_1_count={summary['total_odds_status_1_count']}")

    title = "rows" if args.show_matched else "mismatches"
    print(f"{title}_shown={len(rows)}")
    if rows:
        print("event_id\tscheduled\tlive_market_count\todds_status_1_count\tdiff")
        for row in rows:
            print(
                f"{row['event_id']}\t{row['scheduled']}\t{row['live_market_count']}"
                f"\t{row['odds_status_1_count']}\t{row['diff']}"
            )


if __name__ == "__main__":
    main()
