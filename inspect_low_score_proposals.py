#!/usr/bin/env python3
"""Inspect proposals skipped by LLM because their evidence score is low."""

import argparse
import json
from pathlib import Path

import db_connection_test as dbt
import run_confirmed_pipeline as confirmed


def parse_args():
    parser = argparse.ArgumentParser(
        description="Show needs_review proposals that were skipped before LLM because event evidence was below the confirmed threshold."
    )
    parser.add_argument("--sport", default="all", help="Sport to inspect, or all. Default: all")
    parser.add_argument("--min-score", type=float, default=0.85, help="Inclusive lower score bound. Default: 0.85")
    parser.add_argument("--max-score", type=float, default=0.95, help="Exclusive upper score bound. Default: 0.95")
    parser.add_argument("--limit", type=int, default=50, help="Number of proposals to print. Default: 50")
    parser.add_argument("--format", choices=("table", "jsonl"), default="table", help="Output format. Default: table")
    parser.add_argument(
        "--env-file",
        action="append",
        default=[],
        help="Optional env file to load. Can be repeated. Docker normally does not need this.",
    )
    return parser.parse_args()


def load_env_files(paths):
    default_paths = [".env.market", ".env.llm"]
    for raw_path in [*default_paths, *paths]:
        path = Path(raw_path)
        if path.exists():
            dbt.load_env_file(path)


def fetch_summary(cur, args):
    sport_filter = "" if args.sport == "all" else "AND e.sport = %s"
    params = [args.min_score, args.max_score]
    if args.sport != "all":
        params.append(args.sport)
    cur.execute(
        f"""
        SELECT
            COUNT(DISTINCT e.our_team_id) AS low_score_our_teams,
            COUNT(*) AS low_score_evidence_rows,
            ROUND(MIN(e.event_match_score), 4) AS min_score,
            ROUND(MAX(e.event_match_score), 4) AS max_score,
            ROUND(AVG(e.event_match_score), 4) AS avg_score
        FROM team_mapping_evidence e
        LEFT JOIN llm_verification lv ON lv.our_team_id = e.our_team_id
        WHERE e.status = 'needs_review'
          AND e.event_match_score >= %s
          AND e.event_match_score < %s
          AND lv.id IS NULL
          {sport_filter}
        """,
        params,
    )
    return cur.fetchone()


def fetch_rows(cur, args):
    sport_filter = "" if args.sport == "all" else "AND e.sport = %s"
    params = [args.min_score, args.max_score]
    if args.sport != "all":
        params.append(args.sport)
    params.append(args.limit)
    cur.execute(
        f"""
        SELECT
            e.our_team_id,
            ot.sport,
            ot.canonical_name AS our_team_name,
            ot.status AS our_team_status,
            ROUND(AVG(e.event_match_score), 4) AS avg_event_score,
            ROUND(MAX(e.event_match_score), 4) AS best_event_score,
            COUNT(*) AS evidence_rows,
            COUNT(DISTINCT CONCAT(e.source_a, ':', e.source_a_team_id)) +
              COUNT(DISTINCT CONCAT(e.source_b, ':', e.source_b_team_id)) AS evidence_team_refs,
            GROUP_CONCAT(
                DISTINCT CONCAT(stm.source, ':', stm.source_team_id, ':', stm.source_team_name)
                ORDER BY stm.source
                SEPARATOR ' | '
            ) AS mapped_sources
        FROM team_mapping_evidence e
        JOIN our_team ot ON ot.id = e.our_team_id
        LEFT JOIN source_team_mapping stm ON stm.our_team_id = e.our_team_id
        LEFT JOIN llm_verification lv ON lv.our_team_id = e.our_team_id
        WHERE e.status = 'needs_review'
          AND e.event_match_score >= %s
          AND e.event_match_score < %s
          AND lv.id IS NULL
          {sport_filter}
        GROUP BY e.our_team_id, ot.sport, ot.canonical_name, ot.status
        ORDER BY avg_event_score DESC, evidence_rows DESC, e.our_team_id
        LIMIT %s
        """,
        params,
    )
    return cur.fetchall()


def print_table(summary, rows):
    print(
        "low_score_our_teams={low_score_our_teams} "
        "low_score_evidence_rows={low_score_evidence_rows} "
        "score_range={min_score}-{max_score} avg_score={avg_score}".format(**summary)
    )
    for row in rows:
        print(
            "#{our_team_id} [{sport}] score={avg_event_score} best={best_event_score} "
            "evidence={evidence_rows} status={our_team_status} name={our_team_name}".format(**row)
        )
        print(f"  sources: {row.get('mapped_sources') or ''}")


def main():
    args = parse_args()
    load_env_files(args.env_file)
    conn, process = confirmed.mapping_connection("team_mapping")
    try:
        with conn.cursor() as cur:
            summary = fetch_summary(cur, args)
            rows = fetch_rows(cur, args)
        if args.format == "jsonl":
            print(json.dumps({"summary": summary}, ensure_ascii=False, default=str))
            for row in rows:
                print(json.dumps(row, ensure_ascii=False, default=str))
        else:
            print_table(summary, rows)
    finally:
        conn.close()
        if process:
            process.terminate()


if __name__ == "__main__":
    main()
