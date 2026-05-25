#!/usr/bin/env python3
"""Compare TheSports official SportRadar match mapping with our mapping DB."""

import argparse
import json
from pathlib import Path

import db_connection_test as dbt
import run_confirmed_pipeline as confirmed


SCRIPT_DIR = Path(__file__).resolve().parent


def parse_args():
    parser = argparse.ArgumentParser(description="Compare ts_offical_match with our event/team mappings.")
    parser.add_argument("--env-file", default=".env.market", help="Env file fallback. Default: .env.market")
    parser.add_argument("--mapping-db", default="team_mapping")
    parser.add_argument("--sport", default="all", help="football, basketball, or all. Default: all")
    parser.add_argument("--limit", type=int, default=30, help="Mismatch sample limit. Default: 30")
    parser.add_argument("--format", choices=("text", "json"), default="text")
    return parser.parse_args()


def load_env(path):
    env_path = Path(path).expanduser()
    if not env_path.is_absolute():
        env_path = SCRIPT_DIR / env_path
    if env_path.exists():
        dbt.load_env_file(env_path)


def sport_filter(args, alias="o"):
    if args.sport == "all":
        return "", []
    return f"AND {alias}.sport=%s", [args.sport]


def fetch_summary(cur, args):
    where, params = sport_filter(args)
    cur.execute(
        f"""
        WITH official AS (
            SELECT
                sport,
                sport_radar_match_id,
                CONCAT('sr:match:', sport_radar_match_id) AS sr_event_id,
                thesports_uuid
            FROM ts_offical_match o
            WHERE is_same=1
            {where}
        )
        SELECT
            COUNT(*) AS official_rows,
            SUM(ts.id IS NOT NULL) AS thesports_event_found,
            SUM(sr.id IS NOT NULL) AS sr_event_found,
            SUM(ts.id IS NOT NULL AND sr.id IS NOT NULL) AS both_events_found,
            SUM(ev.id IS NOT NULL) AS our_event_evidence_found,
            SUM(tsh.our_team_id IS NOT NULL AND tsa.our_team_id IS NOT NULL
                AND srh.our_team_id IS NOT NULL AND sra.our_team_id IS NOT NULL) AS all_four_teams_mapped,
            SUM(tsh.our_team_id IS NOT NULL AND tsh.our_team_id = srh.our_team_id
                AND tsa.our_team_id IS NOT NULL AND tsa.our_team_id = sra.our_team_id) AS normal_team_agree,
            SUM(tsh.our_team_id IS NOT NULL AND tsh.our_team_id = sra.our_team_id
                AND tsa.our_team_id IS NOT NULL AND tsa.our_team_id = srh.our_team_id) AS reversed_team_agree,
            SUM(
                tsh.our_team_id IS NOT NULL AND tsa.our_team_id IS NOT NULL
                AND srh.our_team_id IS NOT NULL AND sra.our_team_id IS NOT NULL
                AND NOT (
                    (tsh.our_team_id = srh.our_team_id AND tsa.our_team_id = sra.our_team_id)
                    OR (tsh.our_team_id = sra.our_team_id AND tsa.our_team_id = srh.our_team_id)
                )
            ) AS mapped_but_disagree
        FROM official o
        LEFT JOIN source_event ts
            ON ts.source='thesports'
            AND ts.sport=o.sport
            AND ts.source_event_id=o.thesports_uuid
        LEFT JOIN source_event sr
            ON sr.source='sr'
            AND sr.sport=o.sport
            AND sr.source_event_id=o.sr_event_id
        LEFT JOIN team_mapping_evidence ev
            ON ev.sport=o.sport
            AND (
                (
                    ev.source_a='thesports'
                    AND ev.source_b='sr'
                    AND ev.source_a_event_id=o.thesports_uuid
                    AND ev.source_b_event_id=o.sr_event_id
                )
                OR (
                    ev.source_a='sr'
                    AND ev.source_b='thesports'
                    AND ev.source_a_event_id=o.sr_event_id
                    AND ev.source_b_event_id=o.thesports_uuid
                )
            )
        LEFT JOIN source_team_mapping tsh
            ON tsh.source='thesports'
            AND tsh.sport=ts.sport
            AND tsh.source_team_id=ts.home_source_team_id
            AND tsh.status='confirmed'
        LEFT JOIN source_team_mapping tsa
            ON tsa.source='thesports'
            AND tsa.sport=ts.sport
            AND tsa.source_team_id=ts.away_source_team_id
            AND tsa.status='confirmed'
        LEFT JOIN source_team_mapping srh
            ON srh.source='sr'
            AND srh.sport=sr.sport
            AND srh.source_team_id=sr.home_source_team_id
            AND srh.status='confirmed'
        LEFT JOIN source_team_mapping sra
            ON sra.source='sr'
            AND sra.sport=sr.sport
            AND sra.source_team_id=sr.away_source_team_id
            AND sra.status='confirmed'
        """,
        params,
    )
    row = cur.fetchone()
    return {key: int(value or 0) for key, value in row.items()}


def fetch_disagree_rows(cur, args):
    where, params = sport_filter(args)
    params.append(args.limit)
    cur.execute(
        f"""
        WITH official AS (
            SELECT
                sport,
                sport_radar_match_id,
                CONCAT('sr:match:', sport_radar_match_id) AS sr_event_id,
                thesports_uuid
            FROM ts_offical_match o
            WHERE is_same=1
            {where}
        )
        SELECT
            o.sport,
            o.thesports_uuid,
            o.sr_event_id,
            ts.home_source_team_name AS ts_home,
            ts.away_source_team_name AS ts_away,
            sr.home_source_team_name AS sr_home,
            sr.away_source_team_name AS sr_away,
            tsh.our_team_id AS ts_home_our_team_id,
            tsa.our_team_id AS ts_away_our_team_id,
            srh.our_team_id AS sr_home_our_team_id,
            sra.our_team_id AS sr_away_our_team_id
        FROM official o
        JOIN source_event ts
            ON ts.source='thesports'
            AND ts.sport=o.sport
            AND ts.source_event_id=o.thesports_uuid
        JOIN source_event sr
            ON sr.source='sr'
            AND sr.sport=o.sport
            AND sr.source_event_id=o.sr_event_id
        LEFT JOIN source_team_mapping tsh
            ON tsh.source='thesports'
            AND tsh.sport=ts.sport
            AND tsh.source_team_id=ts.home_source_team_id
            AND tsh.status='confirmed'
        LEFT JOIN source_team_mapping tsa
            ON tsa.source='thesports'
            AND tsa.sport=ts.sport
            AND tsa.source_team_id=ts.away_source_team_id
            AND tsa.status='confirmed'
        LEFT JOIN source_team_mapping srh
            ON srh.source='sr'
            AND srh.sport=sr.sport
            AND srh.source_team_id=sr.home_source_team_id
            AND srh.status='confirmed'
        LEFT JOIN source_team_mapping sra
            ON sra.source='sr'
            AND sra.sport=sr.sport
            AND sra.source_team_id=sr.away_source_team_id
            AND sra.status='confirmed'
        WHERE tsh.our_team_id IS NOT NULL
            AND tsa.our_team_id IS NOT NULL
            AND srh.our_team_id IS NOT NULL
            AND sra.our_team_id IS NOT NULL
            AND NOT (
                (tsh.our_team_id = srh.our_team_id AND tsa.our_team_id = sra.our_team_id)
                OR (tsh.our_team_id = sra.our_team_id AND tsa.our_team_id = srh.our_team_id)
            )
        ORDER BY o.sport, o.thesports_uuid
        LIMIT %s
        """,
        params,
    )
    return cur.fetchall()


def print_text(summary, disagreements):
    print("official_rows={official_rows}".format(**summary))
    print(
        "event_found thesports={thesports_event_found} sr={sr_event_found} both={both_events_found}".format(
            **summary
        )
    )
    print("our_event_evidence_found={our_event_evidence_found}".format(**summary))
    print(
        "team_mapping all_four_mapped={all_four_teams_mapped} normal_agree={normal_team_agree} "
        "reversed_agree={reversed_team_agree} disagree={mapped_but_disagree}".format(**summary)
    )
    if disagreements:
        print("\nDisagreement samples:")
        for row in disagreements:
            print(
                "#{sport} ts={thesports_uuid} sr={sr_event_id} "
                "TS({ts_home}/{ts_home_our_team_id} vs {ts_away}/{ts_away_our_team_id}) "
                "SR({sr_home}/{sr_home_our_team_id} vs {sr_away}/{sr_away_our_team_id})".format(**row)
            )
    else:
        print("\nNo mapped disagreement samples found.")


def main():
    args = parse_args()
    load_env(args.env_file)
    conn, process = confirmed.mapping_connection(args.mapping_db)
    try:
        with conn.cursor() as cur:
            summary = fetch_summary(cur, args)
            disagreements = fetch_disagree_rows(cur, args)
        if args.format == "json":
            print(json.dumps({"summary": summary, "disagreements": disagreements}, ensure_ascii=False, default=str))
        else:
            print_text(summary, disagreements)
    finally:
        confirmed.close_mapping_connection(conn, process)


if __name__ == "__main__":
    main()
