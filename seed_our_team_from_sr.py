#!/usr/bin/env python3
"""
Seed our_team from every SR source_team already loaded in team_mapping.

This writes only to the mapping database:
  - our_team.status = confirmed
  - source_team_mapping.status = confirmed
  - confirmed_method = sr_seed

It treats SR as the internal baseline source. Cross-source mappings can later
attach thesports/ls/bc teams to these SR-seeded our_team rows.
"""

import argparse
import os
from pathlib import Path

import db_connection_test as dbt
import run_confirmed_pipeline as confirmed


SCRIPT_DIR = Path(__file__).resolve().parent


def parse_args():
    parser = argparse.ArgumentParser(description="Seed confirmed our_team rows from SR source_team inventory.")
    parser.add_argument("--env-file", default=".env.market", help="Env file fallback. Default: .env.market")
    parser.add_argument("--sport", default="all", help="Sport to seed. Use all for every sport in source_team. Default: all")
    parser.add_argument("--mapping-db", default=os.environ.get("MAPPING_MYSQL_DB", "team_mapping"))
    parser.add_argument("--confidence", type=float, default=1.0)
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--init-schema", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def load_env(path):
    env_path = Path(path).expanduser()
    if not env_path.is_absolute():
        env_path = SCRIPT_DIR / env_path
    if env_path.exists():
        dbt.load_env_file(env_path)


def normalize_name(name):
    return confirmed.normalize_name(name or "")


def fetch_sr_source_teams(cur, sport):
    params = []
    sport_filter = ""
    if sport != "all":
        sport_filter = "AND st.sport=%s"
        params.append(sport)
    cur.execute(
        f"""
        SELECT
            st.source,
            st.sport,
            st.source_team_id,
            st.source_team_name,
            COALESCE(events.source_event_count, 0) AS source_event_count,
            stm.id AS mapping_id,
            stm.our_team_id,
            stm.status AS mapping_status,
            stm.confirmed_method
        FROM source_team st
        LEFT JOIN source_team_mapping stm
            ON stm.source = st.source
            AND stm.sport = st.sport
            AND stm.source_team_id = st.source_team_id
        LEFT JOIN (
            SELECT source, sport, source_team_id, SUM(event_count) AS source_event_count
            FROM (
                SELECT source, sport, home_source_team_id AS source_team_id, COUNT(*) AS event_count
                FROM source_event
                WHERE source='sr'
                GROUP BY source, sport, home_source_team_id
                UNION ALL
                SELECT source, sport, away_source_team_id AS source_team_id, COUNT(*) AS event_count
                FROM source_event
                WHERE source='sr'
                GROUP BY source, sport, away_source_team_id
            ) event_counts
            GROUP BY source, sport, source_team_id
        ) events
            ON events.source = st.source
            AND events.sport = st.sport
            AND events.source_team_id = st.source_team_id
        WHERE st.source='sr'
            {sport_filter}
        ORDER BY st.sport, st.source_team_name, st.source_team_id
        """,
        params,
    )
    return cur.fetchall()


def create_seed_our_team(cur, row, confidence):
    cur.execute(
        """
        INSERT INTO our_team (
            sport, canonical_name, normalized_name, status, confidence,
            confirmed_method, confirmed_at
        )
        VALUES (%s, %s, %s, 'confirmed', %s, 'sr_seed', NOW())
        """,
        (
            row["sport"],
            row["source_team_name"],
            normalize_name(row["source_team_name"]),
            confidence,
        ),
    )
    return cur.lastrowid


def promote_existing_our_team(cur, our_team_id, row, confidence):
    cur.execute(
        """
        UPDATE our_team
        SET sport=%s,
            canonical_name=%s,
            normalized_name=%s,
            status='confirmed',
            confidence=GREATEST(confidence, %s),
            confirmed_method=COALESCE(confirmed_method, 'sr_seed'),
            confirmed_at=COALESCE(confirmed_at, NOW())
        WHERE id=%s
        """,
        (
            row["sport"],
            row["source_team_name"],
            normalize_name(row["source_team_name"]),
            confidence,
            our_team_id,
        ),
    )


def upsert_sr_mapping(cur, our_team_id, row, confidence):
    cur.execute(
        """
        INSERT INTO source_team_mapping (
            our_team_id, source, sport, source_team_id, source_team_name,
            normalized_name, confidence, status, evidence_count, source_event_count,
            confirmed_method, confirmed_at
        )
        VALUES (%s, 'sr', %s, %s, %s, %s, %s, 'confirmed', 0, %s, 'sr_seed', NOW())
        ON DUPLICATE KEY UPDATE
            our_team_id=VALUES(our_team_id),
            source_team_name=VALUES(source_team_name),
            normalized_name=VALUES(normalized_name),
            confidence=GREATEST(confidence, VALUES(confidence)),
            status='confirmed',
            source_event_count=GREATEST(source_event_count, VALUES(source_event_count)),
            confirmed_method='sr_seed',
            confirmed_at=COALESCE(confirmed_at, NOW())
        """,
        (
            our_team_id,
            row["sport"],
            row["source_team_id"],
            row["source_team_name"],
            normalize_name(row["source_team_name"]),
            confidence,
            int(row.get("source_event_count") or 0),
        ),
    )


def seed_rows(conn, rows, args):
    created = 0
    promoted = 0
    already_confirmed = 0
    processed = 0
    touched_our_team_ids = []
    with conn.cursor() as cur:
        for row in rows:
            processed += 1
            if row.get("mapping_status") == "confirmed" and row.get("our_team_id"):
                our_team_id = row["our_team_id"]
                promote_existing_our_team(cur, row["our_team_id"], row, args.confidence)
                upsert_sr_mapping(cur, row["our_team_id"], row, args.confidence)
                already_confirmed += 1
            else:
                our_team_id = create_seed_our_team(cur, row, args.confidence)
                upsert_sr_mapping(cur, our_team_id, row, args.confidence)
                if row.get("mapping_id"):
                    promoted += 1
                else:
                    created += 1
            touched_our_team_ids.append(our_team_id)
            if processed % args.batch_size == 0:
                confirmed.refresh_our_team_source_map(cur, touched_our_team_ids)
                touched_our_team_ids = []
                conn.commit()
                print(f"processed={processed} created={created} promoted={promoted} already_confirmed={already_confirmed}")
        if touched_our_team_ids:
            confirmed.refresh_our_team_source_map(cur, touched_our_team_ids)
    conn.commit()
    return {
        "processed": processed,
        "created": created,
        "promoted": promoted,
        "already_confirmed": already_confirmed,
    }


def main():
    args = parse_args()
    load_env(args.env_file)
    if args.init_schema:
        confirmed.init_schema()
    conn, process = confirmed.mapping_connection(args.mapping_db)
    try:
        with conn.cursor() as cur:
            rows = fetch_sr_source_teams(cur, args.sport)
        print(f"sr_source_teams={len(rows)} sport={args.sport}")
        if args.dry_run:
            existing = sum(1 for row in rows if row.get("mapping_id"))
            confirmed_count = sum(1 for row in rows if row.get("mapping_status") == "confirmed")
            print(f"dry_run=true existing_mappings={existing} already_confirmed={confirmed_count} to_create={len(rows) - existing}")
            return
        result = seed_rows(conn, rows, args)
        print(
            "done "
            f"processed={result['processed']} created={result['created']} "
            f"promoted={result['promoted']} already_confirmed={result['already_confirmed']}"
        )
    except Exception:
        conn.rollback()
        raise
    finally:
        confirmed.close_mapping_connection(conn, process)


if __name__ == "__main__":
    main()
