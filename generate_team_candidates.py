#!/usr/bin/env python3
"""
Generate read-only cross-source event/team match candidates.

This script writes local review artifacts only. It does not modify MySQL.
"""

import argparse
import getpass
from pathlib import Path

import db_connection_test as dbt
import team_mapping_core as core


SCRIPT_DIR = Path(__file__).resolve().parent


def parse_args():
    parser = argparse.ArgumentParser(description="Generate event/team match candidates for review.")
    parser.add_argument("--env-file", default=".env.market", help="Path to env file. Default: .env.market")
    parser.add_argument(
        "--source",
        required=True,
        choices=("thesports_fb", "thesports_bb", "sr", "ls"),
        help="Source event table adapter.",
    )
    parser.add_argument(
        "--target",
        required=True,
        choices=("thesports_fb", "thesports_bb", "sr", "ls"),
        help="Target event table adapter.",
    )
    parser.add_argument(
        "--sport",
        required=True,
        choices=("football", "basketball"),
        help="Canonical sport. Football and basketball are matched separately.",
    )
    parser.add_argument("--start", required=True, help="Start time inclusive. Example: 2026-05-14 00:00:00")
    parser.add_argument("--end", required=True, help="End time exclusive. Example: 2026-05-15 00:00:00")
    parser.add_argument("--test1-mysql-password", help="Password for named MySQL test1. Prefer --prompt-test1-password.")
    parser.add_argument("--prompt-test1-password", action="store_true", help="Prompt for test1 MySQL password.")
    parser.add_argument("--mysql-user", help="Override MySQL user.")
    parser.add_argument("--per-source-limit", type=int, default=500, help="Max rows to fetch from each source. Default: 500")
    parser.add_argument("--time-window-hours", type=float, default=24, help="Max kickoff time gap. Default: 24")
    parser.add_argument("--top-k", type=int, default=5, help="Candidates per source event. Default: 5")
    parser.add_argument("--min-score", type=float, default=0.55, help="Minimum candidate score. Default: 0.55")
    parser.add_argument("--output-dir", default="outputs", help="Output directory. Default: outputs")
    return parser.parse_args()


def load_source_events(source, sport, start_ts, end_ts, limit, test1_password, mysql_user):
    mysql_name = core.SOURCE_MYSQL[source]
    with core.mysql_connection(mysql_name, mysql_password=test1_password if mysql_name == "test1" else None, mysql_user=mysql_user) as conn:
        return core.fetch_events(conn, source, sport, start_ts, end_ts, limit)


def generate_candidates(source_events, target_events, max_time_diff_hours, top_k, min_score):
    candidates = []
    for source_event in source_events:
        ranked = []
        for target_event in target_events:
            detail = core.candidate_score(source_event, target_event, max_time_diff_hours)
            if not detail or detail["score"] < min_score:
                continue
            ranked.append((detail["score"], core.build_candidate(source_event, target_event, detail)))
        ranked.sort(key=lambda item: item[0], reverse=True)
        candidates.extend(item for _, item in ranked[:top_k])
    candidates.sort(key=lambda item: item["score_detail"]["score"], reverse=True)
    return candidates


def main():
    args = parse_args()
    if args.source == args.target:
        raise SystemExit("--source and --target must be different.")

    dbt.load_env_file(args.env_file)
    start_ts = core.parse_time(args.start)
    end_ts = core.parse_time(args.end)
    if not start_ts or not end_ts or end_ts <= start_ts:
        raise SystemExit("--start/--end must parse to a valid positive time range.")

    needs_test1 = any(core.SOURCE_MYSQL[src] == "test1" for src in (args.source, args.target))
    test1_password = args.test1_mysql_password
    if needs_test1 and args.prompt_test1_password:
        test1_password = getpass.getpass("test1 MySQL password: ")

    print(f"Fetching source={args.source} sport={args.sport}")
    source_events = load_source_events(
        args.source, args.sport, start_ts, end_ts, args.per_source_limit, test1_password, args.mysql_user
    )
    print(f"source_events={len(source_events)}")

    print(f"Fetching target={args.target} sport={args.sport}")
    target_events = load_source_events(
        args.target, args.sport, start_ts, end_ts, args.per_source_limit, test1_password, args.mysql_user
    )
    print(f"target_events={len(target_events)}")

    candidates = generate_candidates(
        source_events,
        target_events,
        max_time_diff_hours=args.time_window_hours,
        top_k=args.top_k,
        min_score=args.min_score,
    )
    print(f"candidates={len(candidates)}")

    out_dir = Path(args.output_dir).expanduser()
    if not out_dir.is_absolute():
        out_dir = SCRIPT_DIR / out_dir
    stem = f"{args.source}_to_{args.target}_{args.sport}_{args.start[:10]}_{args.end[:10]}".replace(":", "").replace(" ", "_")
    jsonl_path = out_dir / f"{stem}.candidates.jsonl"
    html_path = out_dir / f"{stem}.review.html"
    core.write_jsonl(jsonl_path, candidates)
    core.write_review_html(html_path, candidates)

    print(f"jsonl={jsonl_path}")
    print(f"html={html_path}")


if __name__ == "__main__":
    main()
