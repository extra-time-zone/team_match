#!/usr/bin/env python3
"""
Run the mapping pipeline and persist LLM-verified confirmed mappings to test1.

Writes only to the dedicated mapping database on test1, default `team_mapping`.
Source databases remain read-only.
"""

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import db_connection_test as dbt
import run_team_mapping_pipeline as pipeline
import team_mapping_core as core
from verify_team_proposals import provider_config, verify_one, normalize_verification
from openai import OpenAI


SCRIPT_DIR = Path(__file__).resolve().parent
SCHEMA_PATH = SCRIPT_DIR / "schema" / "team_mapping_schema.sql"


def parse_args():
    parser = argparse.ArgumentParser(description="Run confirmed team mapping pipeline.")
    parser.add_argument("--env-file", default=".env.market", help="Local env file fallback. Default: .env.market")
    parser.add_argument("--llm-env-file", default=".env.llm", help="Local LLM env file fallback. Default: .env.llm")
    parser.add_argument("--sport", required=True, help="Sport to process: football, basketball, all, or a canonical sport key.")
    parser.add_argument("--sources", help="Comma separated sources. Default from run_team_mapping_pipeline.")
    parser.add_argument("--start", required=True, help="Start time inclusive. Example: 2026-04-01 00:00:00")
    parser.add_argument("--end", required=True, help="End time exclusive. Example: 2026-05-15 00:00:00")
    parser.add_argument(
        "--per-source-limit",
        type=int,
        default=0,
        help="Max events per source. Use 0 for all events in the time range. Default: 0",
    )
    parser.add_argument("--top-k", type=int, default=3)
    parser.add_argument("--min-score", type=float, default=0.55)
    parser.add_argument("--time-window-hours", type=float, default=24)
    parser.add_argument(
        "--review-evidence-min-score",
        type=float,
        default=0.85,
        help="Minimum event score to persist a proposal as needs_review. Default: 0.85",
    )
    parser.add_argument(
        "--evidence-min-score",
        type=float,
        default=0.95,
        help="Minimum event score for a proposal to enter LLM confirmation. Default: 0.95",
    )
    parser.add_argument(
        "--max-proposals",
        type=int,
        default=0,
        help="Max proposals to verify/write. Use 0 for all proposals. Default: 0",
    )
    parser.add_argument("--db-retries", type=int, default=3)
    parser.add_argument("--db-retry-sleep", type=float, default=3)
    parser.add_argument("--llm-provider", choices=("openai", "deepseek"), default=os.environ.get("LLM_PROVIDER", "deepseek"))
    parser.add_argument("--model", help="LLM model name.")
    parser.add_argument("--base-url", default=os.environ.get("LLM_BASE_URL"))
    parser.add_argument("--api-key-env", help="Environment variable that stores the API key.")
    parser.add_argument("--llm-timeout", type=float, default=35)
    parser.add_argument("--llm-retries", type=int, default=2)
    parser.add_argument("--confirm-llm-confidence", type=float, default=0.90)
    parser.add_argument("--confirm-event-score", type=float, default=0.95)
    parser.add_argument("--confirm-source-count", type=int, default=2)
    parser.add_argument("--confirm-evidence-count", type=int, default=1)
    parser.add_argument("--mapping-db", default=os.environ.get("MAPPING_MYSQL_DB", "team_mapping"))
    parser.add_argument("--init-schema", action="store_true", help="Create mapping schema on test1 before running.")
    parser.add_argument("--dry-run", action="store_true", help="Run matching and LLM but do not write mapping DB.")
    return parser.parse_args()


def load_env_files(args):
    for path in (args.env_file, args.llm_env_file):
        env_path = Path(path).expanduser()
        if not env_path.is_absolute():
            env_path = SCRIPT_DIR / env_path
        if env_path.exists():
            dbt.load_env_file(env_path)


def test1_config(mapping_db=None):
    return dbt.get_config(
        SimpleNamespace(
            mysql_host=os.environ.get("TEST1_MYSQL_HOST", core.TEST1_HOST),
            mysql_port=int(os.environ.get("TEST1_MYSQL_PORT", "3306")),
            mysql_user=os.environ.get("TEST1_MYSQL_USER", "root"),
            mysql_password=os.environ.get("TEST1_MYSQL_PASSWORD"),
            prompt_mysql_password=False,
            mysql_db=mapping_db or "",
        )
    )


def mapping_connection(mapping_db=None):
    config = test1_config(mapping_db)
    process = None
    if core.is_direct_mysql("test1"):
        connect_host = config["mysql_host"]
        connect_port = config["mysql_port"]
    else:
        connect_port, process = dbt.start_tunnel(config)
        connect_host = "127.0.0.1"
    conn = dbt.pymysql.connect(
        host=connect_host,
        port=connect_port,
        user=config["mysql_user"],
        password=config["mysql_password"],
        database=mapping_db or None,
        charset="utf8mb4",
        connect_timeout=10,
        read_timeout=180,
        write_timeout=20,
        autocommit=False,
        cursorclass=dbt.pymysql.cursors.DictCursor,
    )
    return conn, process


def close_mapping_connection(conn, process):
    if conn:
        conn.close()
    if process:
        process.terminate()
        try:
            process.wait(timeout=5)
        except Exception:
            process.kill()


def init_schema():
    conn, process = mapping_connection(None)
    try:
        sql_text = SCHEMA_PATH.read_text(encoding="utf-8")
        with conn.cursor() as cur:
            for statement in split_sql_statements(sql_text):
                cur.execute(statement)
        conn.commit()
    finally:
        close_mapping_connection(conn, process)


def split_sql_statements(sql_text):
    statements = []
    current = []
    for line in sql_text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("--"):
            continue
        current.append(line)
        if stripped.endswith(";"):
            statements.append("\n".join(current).rstrip(";"))
            current = []
    if current:
        statements.append("\n".join(current))
    return statements


def make_pipeline_args(args):
    return SimpleNamespace(
        env_file=args.env_file,
        llm_env_file=args.llm_env_file,
        sport=args.sport,
        sources=args.sources,
        start=args.start,
        end=args.end,
        test1_mysql_password=os.environ.get("TEST1_MYSQL_PASSWORD"),
        prompt_test1_password=False,
        mysql_user=None,
        per_source_limit=args.per_source_limit,
        db_retries=args.db_retries,
        db_retry_sleep=args.db_retry_sleep,
        time_window_hours=args.time_window_hours,
        top_k=args.top_k,
        min_score=args.min_score,
        llm_provider="none",
        model=None,
        base_url=None,
        api_key_env=None,
        llm_limit=None,
        llm_timeout=args.llm_timeout,
        llm_retries=args.llm_retries,
        proposal_method="evidence",
        evidence_min_score=args.review_evidence_min_score,
        allow_score_conflict=False,
        min_proposal_evidence=1,
        max_proposals=args.max_proposals or None,
        accept_event_confidence=0.85,
        accept_team_confidence=0.85,
        output_dir="outputs",
    )


def generate_proposals(args):
    p_args = make_pipeline_args(args)
    sources = pipeline.parse_sources(p_args)
    start_ts = core.parse_time(args.start)
    end_ts = core.parse_time(args.end)
    events_by_source = pipeline.fetch_all_events(sources, p_args, start_ts, end_ts, os.environ.get("TEST1_MYSQL_PASSWORD"))

    candidates = []
    import itertools

    for left_source, right_source in itertools.combinations(sources, 2):
        candidates.extend(
            pipeline.generate_pair_candidates(
                left_source,
                right_source,
                events_by_source[left_source],
                events_by_source[right_source],
                p_args,
            )
        )
    candidates.sort(key=lambda item: item["score_detail"]["score"], reverse=True)
    proposals = pipeline.build_our_team_proposals_from_evidence(candidates, p_args)
    return events_by_source, candidates, proposals


def verify_proposals(args, proposals):
    model, base_url, api_key = provider_config(
        SimpleNamespace(
            provider=args.llm_provider,
            model=args.model,
            base_url=args.base_url,
            api_key_env=args.api_key_env,
            dry_run=False,
        )
    )
    client = OpenAI(api_key=api_key, base_url=base_url, timeout=args.llm_timeout, max_retries=0)
    rows = []
    for proposal in proposals:
        result = None
        last_error = None
        for _ in range(args.llm_retries):
            try:
                result = normalize_verification(verify_one(client, model, proposal))
                break
            except Exception as exc:
                last_error = exc
        if result is None:
            result = {
                "same_team": False,
                "confidence": 0,
                "recommended_status": "needs_review",
                "risk_flags": ["llm_error"],
                "reason": f"LLM request failed: {last_error}",
            }
        rows.append(
            {
                "proposal": proposal,
                "llm_provider": args.llm_provider,
                "llm_model": model,
                "llm_verification": result,
            }
        )
        print(
            f"verified {proposal['proposed_our_team_id']} {proposal['canonical_name']} "
            f"-> {result['recommended_status']} confidence={result['confidence']}"
        )
    return rows


def verify_and_write_proposals(args, proposals, run_id):
    client = None
    model = None
    counts = {
        "verified": 0,
        "llm_requested": 0,
        "llm_skipped_low_score": 0,
        "llm_skipped_existing": 0,
        "llm_verified": 0,
        "confirmed": 0,
        "needs_review": 0,
    }
    for index, proposal in enumerate(proposals, 1):
        skip_reason = llm_skip_reason(args, proposal)
        if skip_reason:
            result = skipped_llm_result(proposal, skip_reason)
            counts[f"llm_skipped_{skip_reason}"] += 1
            llm_provider = "skipped"
            llm_model = "none"
            skip_llm = True
        else:
            if client is None:
                model, base_url, api_key = provider_config(
                    SimpleNamespace(
                        provider=args.llm_provider,
                        model=args.model,
                        base_url=args.base_url,
                        api_key_env=args.api_key_env,
                        dry_run=False,
                    )
                )
                client = OpenAI(api_key=api_key, base_url=base_url, timeout=args.llm_timeout, max_retries=0)
            result = None
            last_error = None
            for _ in range(args.llm_retries):
                try:
                    result = normalize_verification(verify_one(client, model, proposal))
                    break
                except Exception as exc:
                    last_error = exc
            if result is None:
                result = {
                    "same_team": False,
                    "confidence": 0,
                    "recommended_status": "needs_review",
                    "risk_flags": ["llm_error"],
                    "reason": f"LLM request failed: {last_error}",
                }
            counts["llm_requested"] += 1
            llm_provider = args.llm_provider
            llm_model = model
            skip_llm = False
        row = {
            "run_id": run_id,
            "proposal": proposal,
            "llm_provider": llm_provider,
            "llm_model": llm_model,
            "llm_verification": result,
            "skip_llm_verification": skip_llm,
        }
        status = upsert_single_verification_row(args, row)
        counts["verified"] += 1
        if result.get("recommended_status") == "llm_verified":
            counts["llm_verified"] += 1
        counts["confirmed" if status == "confirmed" else "needs_review"] += 1
        print(
            f"verified {index}/{len(proposals)} {proposal['proposed_our_team_id']} {proposal['canonical_name']} "
            f"-> {result['recommended_status']} confidence={result['confidence']} written={status} "
            f"llm={'skipped:' + skip_reason if skip_reason else 'requested'}"
        )
    return counts


def llm_skip_reason(args, proposal):
    if float(proposal.get("avg_event_score") or 0) < args.evidence_min_score:
        return "low_score"
    if proposal_already_confirmed(args, proposal):
        return "existing"
    return None


def skipped_llm_result(proposal, reason):
    if reason == "existing":
        return {
            "same_team": True,
            "confidence": 1,
            "recommended_status": "llm_verified",
            "risk_flags": ["already_confirmed"],
            "reason": "All proposal members are already confirmed to the same our_team_id; LLM skipped.",
        }
    return {
        "same_team": None,
        "confidence": float(proposal.get("team_confidence") or proposal.get("avg_event_score") or 0),
        "recommended_status": "needs_review",
        "risk_flags": ["below_llm_threshold"],
        "reason": "Event score is below LLM confirmation threshold; saved for anchor propagation or manual review.",
    }


def proposal_already_confirmed(args, proposal):
    members = proposal.get("members") or []
    if not members:
        return False
    conn, process = mapping_connection(args.mapping_db)
    try:
        with conn.cursor() as cur:
            placeholders = ", ".join(["(%s,%s,%s)"] * len(members))
            params = []
            for member in members:
                params.extend([member["source"], member["sport"], member["source_team_id"]])
            cur.execute(
                f"""
                SELECT COUNT(*) AS member_count, COUNT(DISTINCT our_team_id) AS our_team_count
                FROM source_team_mapping
                WHERE status='confirmed'
                    AND (source, sport, source_team_id) IN ({placeholders})
                """,
                params,
            )
            row = cur.fetchone()
            return int(row["member_count"] or 0) == len(members) and int(row["our_team_count"] or 0) == 1
    finally:
        close_mapping_connection(conn, process)


def count_llm_plan(args, proposals):
    counts = {"llm_would_request": 0, "needs_review_without_llm": 0}
    for proposal in proposals:
        if float(proposal.get("avg_event_score") or 0) < args.evidence_min_score:
            counts["needs_review_without_llm"] += 1
        else:
            counts["llm_would_request"] += 1
    return counts


def should_confirm(args, row):
    proposal = row["proposal"]
    verification = row["llm_verification"]
    return (
        verification.get("recommended_status") == "llm_verified"
        and float(verification.get("confidence") or 0) >= args.confirm_llm_confidence
        and int(proposal.get("conflict_count") or 0) == 0
        and float(proposal.get("avg_event_score") or 0) >= args.confirm_event_score
        and int(proposal.get("source_count") or 0) >= args.confirm_source_count
        and int(proposal.get("evidence_count") or 0) >= args.confirm_evidence_count
    )


def normalize_name(name):
    return core.normalize_name(name or "")


def upsert_confirmed_rows(args, events_by_source, verification_rows, candidates):
    conn, process = mapping_connection(args.mapping_db)
    try:
        with conn.cursor() as cur:
            run_id = create_pipeline_run(cur, args)
            inventory_stats = upsert_source_inventory(cur, events_by_source)
            confirmed_count = 0
            for row in verification_rows:
                status = "confirmed" if should_confirm(args, row) else "needs_review"
                our_team_id = upsert_our_team(cur, row, status)
                upsert_source_teams_and_mappings(cur, our_team_id, row, status)
                upsert_evidence(cur, our_team_id, row, status)
                upsert_llm_verification(cur, our_team_id, row)
                if status == "confirmed":
                    confirmed_count += 1
            coverage_stats = compute_and_store_source_stats(cur, run_id, args.sport, inventory_stats)
            finish_pipeline_run(
                cur,
                run_id,
                "completed",
                {
                    "verified": len(verification_rows),
                    "confirmed": confirmed_count,
                    "needs_review": len(verification_rows) - confirmed_count,
                    "candidate_count": len(candidates),
                    "inventory": inventory_stats,
                    "coverage": coverage_stats,
                },
            )
        conn.commit()
        return confirmed_count
    except Exception:
        conn.rollback()
        raise
    finally:
        close_mapping_connection(conn, process)


def initialize_incremental_run(args, events_by_source):
    conn, process = mapping_connection(args.mapping_db)
    try:
        with conn.cursor() as cur:
            run_id = create_pipeline_run(cur, args)
            inventory_stats = upsert_source_inventory(cur, events_by_source)
        conn.commit()
        return run_id, inventory_stats
    except Exception:
        conn.rollback()
        raise
    finally:
        close_mapping_connection(conn, process)


def upsert_single_verification_row(args, row):
    conn, process = mapping_connection(args.mapping_db)
    try:
        with conn.cursor() as cur:
            status = "confirmed" if should_confirm(args, row) else "needs_review"
            our_team_id = upsert_our_team(cur, row, status)
            upsert_source_teams_and_mappings(cur, our_team_id, row, status)
            upsert_evidence(cur, our_team_id, row, status)
            if not row.get("skip_llm_verification"):
                upsert_llm_verification(cur, our_team_id, row)
        conn.commit()
        return status
    except Exception:
        conn.rollback()
        raise
    finally:
        close_mapping_connection(conn, process)


def finish_incremental_run(args, run_id, inventory_stats, candidate_count, write_counts):
    conn, process = mapping_connection(args.mapping_db)
    try:
        with conn.cursor() as cur:
            coverage_stats = compute_and_store_source_stats(cur, run_id, args.sport, inventory_stats)
            finish_pipeline_run(
                cur,
                run_id,
                "completed",
                {
                    "verified": write_counts["verified"],
                    "llm_requested": write_counts["llm_requested"],
                    "llm_skipped_low_score": write_counts["llm_skipped_low_score"],
                    "llm_skipped_existing": write_counts["llm_skipped_existing"],
                    "llm_verified": write_counts["llm_verified"],
                    "confirmed": write_counts["confirmed"],
                    "needs_review": write_counts["needs_review"],
                    "candidate_count": candidate_count,
                    "inventory": inventory_stats,
                    "coverage": coverage_stats,
                },
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        close_mapping_connection(conn, process)


def create_pipeline_run(cur, args):
    run_key = f"{args.sport}:{args.start}:{args.end}:{datetime.now(timezone.utc).isoformat()}"
    sources = args.sources or ",".join(pipeline.DEFAULT_SOURCES.get(args.sport, ["sr", "ls"]))
    cur.execute(
        """
        INSERT INTO pipeline_run (run_key, sport, start_time, end_time, sources, params)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (
            run_key,
            args.sport,
            args.start,
            args.end,
            sources,
            json.dumps(vars(args), ensure_ascii=False, default=str),
        ),
    )
    return cur.lastrowid


def finish_pipeline_run(cur, run_id, status, summary):
    cur.execute(
        "UPDATE pipeline_run SET status=%s, summary=%s WHERE id=%s",
        (status, json.dumps(summary, ensure_ascii=False), run_id),
    )


def upsert_source_inventory(cur, events_by_source):
    stats = {}
    for events in events_by_source.values():
        for event in events:
            source_stats = stats.setdefault(
                event.source,
                {
                    "source_events": 0,
                    "source_team_ids": set(),
                    "source_events_by_sport": {},
                    "source_team_ids_by_sport": {},
                },
            )
            source_stats["source_events"] += 1
            source_stats["source_events_by_sport"][event.sport] = (
                source_stats["source_events_by_sport"].get(event.sport, 0) + 1
            )
            source_stats["source_team_ids_by_sport"].setdefault(event.sport, set())
            upsert_source_event(cur, event)
            for side in ("home", "away"):
                team_id = getattr(event, f"{side}_team_id")
                team_name = getattr(event, f"{side}_team_name")
                if not team_id:
                    continue
                source_stats["source_team_ids"].add((event.sport, team_id))
                source_stats["source_team_ids_by_sport"][event.sport].add(team_id)
                upsert_source_team(
                    cur,
                    event.source,
                    event.sport,
                    team_id,
                    team_name,
                    event.start_time,
                    event.start_time,
                )
    return {
        source: {
            "source_events": values["source_events"],
            "source_teams": len(values["source_team_ids"]),
            "sports": {
                sport: {
                    "source_events": values["source_events_by_sport"].get(sport, 0),
                    "source_teams": len(team_ids),
                }
                for sport, team_ids in sorted(values["source_team_ids_by_sport"].items())
            },
        }
        for source, values in sorted(stats.items())
    }


def upsert_source_event(cur, event):
    cur.execute(
        """
        INSERT INTO source_event (
            source, sport, source_event_id, start_time,
            home_source_team_id, home_source_team_name,
            away_source_team_id, away_source_team_name,
            home_score, away_score, competition_id, competition_name, raw_payload
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            start_time=VALUES(start_time),
            home_source_team_id=VALUES(home_source_team_id),
            home_source_team_name=VALUES(home_source_team_name),
            away_source_team_id=VALUES(away_source_team_id),
            away_source_team_name=VALUES(away_source_team_name),
            home_score=VALUES(home_score),
            away_score=VALUES(away_score),
            competition_id=VALUES(competition_id),
            competition_name=VALUES(competition_name),
            raw_payload=VALUES(raw_payload)
        """,
        (
            event.source,
            event.sport,
            event.event_id,
            event.start_time.replace(" UTC", "") if event.start_time else None,
            event.home_team_id,
            event.home_team_name,
            event.away_team_id,
            event.away_team_name,
            event.home_score,
            event.away_score,
            event.competition_id,
            event.competition_name,
            json.dumps(
                {
                    "category_id": event.category_id,
                    "raw_sport_id": event.raw_sport_id,
                },
                ensure_ascii=False,
            ),
        ),
    )


def upsert_source_team(cur, source, sport, source_team_id, source_team_name, first_seen_at=None, last_seen_at=None):
    cur.execute(
        """
        INSERT INTO source_team (
            source, sport, source_team_id, source_team_name, normalized_name,
            first_seen_at, last_seen_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            source_team_name=VALUES(source_team_name),
            normalized_name=VALUES(normalized_name),
            first_seen_at=CASE
                WHEN first_seen_at IS NULL THEN VALUES(first_seen_at)
                WHEN VALUES(first_seen_at) IS NULL THEN first_seen_at
                ELSE LEAST(first_seen_at, VALUES(first_seen_at))
            END,
            last_seen_at=CASE
                WHEN last_seen_at IS NULL THEN VALUES(last_seen_at)
                WHEN VALUES(last_seen_at) IS NULL THEN last_seen_at
                ELSE GREATEST(last_seen_at, VALUES(last_seen_at))
            END
        """,
        (
            source,
            sport,
            source_team_id,
            source_team_name or "",
            normalize_name(source_team_name),
            first_seen_at.replace(" UTC", "") if isinstance(first_seen_at, str) else first_seen_at,
            last_seen_at.replace(" UTC", "") if isinstance(last_seen_at, str) else last_seen_at,
        ),
    )


def compute_and_store_source_stats(cur, run_id, sport, inventory_stats):
    coverage = {}
    for source in sorted(inventory_stats):
        sports = sorted(inventory_stats[source].get("sports") or {sport: inventory_stats[source]})
        if sport != "all":
            sports = [sport]
        coverage[source] = {}
        for sport_key in sports:
            row = compute_source_sport_stats(cur, run_id, source, sport_key, inventory_stats[source])
            coverage[source][sport_key] = row
    return coverage


def compute_source_sport_stats(cur, run_id, source, sport, source_inventory):
    sport_inventory = (source_inventory.get("sports") or {}).get(sport, source_inventory)
    cur.execute(
        """
        SELECT COUNT(*) AS total_source_teams
        FROM source_team
        WHERE source=%s AND sport=%s
        """,
        (source, sport),
    )
    total_source_teams = int(cur.fetchone()["total_source_teams"] or 0)
    cur.execute(
        """
        SELECT COUNT(*) AS mapped_source_teams
        FROM source_team_mapping
        WHERE source=%s AND sport=%s AND status='confirmed'
        """,
        (source, sport),
    )
    mapped_source_teams = int(cur.fetchone()["mapped_source_teams"] or 0)
    unmapped_source_teams = max(0, total_source_teams - mapped_source_teams)
    mapped_ratio = round(mapped_source_teams / total_source_teams, 6) if total_source_teams else 0
    cur.execute(
        """
        SELECT COUNT(*) AS events_with_unmapped_team
        FROM source_event e
        LEFT JOIN source_team_mapping hm
            ON hm.source=e.source
            AND hm.sport=e.sport
            AND hm.source_team_id=e.home_source_team_id
            AND hm.status='confirmed'
        LEFT JOIN source_team_mapping am
            ON am.source=e.source
            AND am.sport=e.sport
            AND am.source_team_id=e.away_source_team_id
            AND am.status='confirmed'
        WHERE e.source=%s
            AND e.sport=%s
            AND (hm.id IS NULL OR am.id IS NULL)
        """,
        (source, sport),
    )
    events_with_unmapped_team = int(cur.fetchone()["events_with_unmapped_team"] or 0)
    row = {
        "source_events_in_run": sport_inventory["source_events"],
        "source_teams_in_run": sport_inventory["source_teams"],
        "total_source_teams": total_source_teams,
        "mapped_source_teams": mapped_source_teams,
        "unmapped_source_teams": unmapped_source_teams,
        "mapped_ratio": mapped_ratio,
        "events_with_unmapped_team": events_with_unmapped_team,
    }
    cur.execute(
        """
        INSERT INTO source_team_match_stats (
            run_id, source, sport, source_events_in_run, source_teams_in_run,
            total_source_teams, mapped_source_teams, unmapped_source_teams,
            mapped_ratio, events_with_unmapped_team
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            source_events_in_run=VALUES(source_events_in_run),
            source_teams_in_run=VALUES(source_teams_in_run),
            total_source_teams=VALUES(total_source_teams),
            mapped_source_teams=VALUES(mapped_source_teams),
            unmapped_source_teams=VALUES(unmapped_source_teams),
            mapped_ratio=VALUES(mapped_ratio),
            events_with_unmapped_team=VALUES(events_with_unmapped_team)
        """,
        (
            run_id,
            source,
            sport,
            row["source_events_in_run"],
            row["source_teams_in_run"],
            row["total_source_teams"],
            row["mapped_source_teams"],
            row["unmapped_source_teams"],
            row["mapped_ratio"],
            row["events_with_unmapped_team"],
        ),
    )
    return row


def upsert_our_team(cur, row, status):
    proposal = row["proposal"]
    verification = row["llm_verification"]
    confidence = float(verification.get("confidence") or proposal.get("team_confidence") or 0)
    existing_id = find_existing_our_team_id(cur, proposal)
    if existing_id:
        cur.execute(
            """
            UPDATE our_team
            SET canonical_name=%s,
                normalized_name=%s,
                status=IF(status='confirmed' AND %s<>'confirmed', status, %s),
                confidence=GREATEST(confidence, %s),
                confirmed_method=IF(status='confirmed' AND %s<>'confirmed', confirmed_method, %s),
                confirmed_at=IF(%s='confirmed', COALESCE(confirmed_at, NOW()), confirmed_at)
            WHERE id=%s
            """,
            (
                proposal["canonical_name"],
                normalize_name(proposal["canonical_name"]),
                status,
                status,
                confidence,
                status,
                "llm_verified_event_evidence" if status == "confirmed" else None,
                status,
                existing_id,
            ),
        )
        return existing_id
    cur.execute(
        """
        INSERT INTO our_team (
            sport, canonical_name, normalized_name, status, confidence,
            confirmed_method, confirmed_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, IF(%s='confirmed', NOW(), NULL))
        """,
        (
            proposal["sport"],
            proposal["canonical_name"],
            normalize_name(proposal["canonical_name"]),
            status,
            confidence,
            "llm_verified_event_evidence" if status == "confirmed" else None,
            status,
        ),
    )
    return cur.lastrowid


def find_existing_our_team_id(cur, proposal):
    members = proposal.get("members") or []
    if not members:
        return None
    placeholders = ", ".join(["(%s,%s,%s)"] * len(members))
    params = []
    for member in members:
        params.extend([member["source"], member["sport"], member["source_team_id"]])
    cur.execute(
        f"""
        SELECT our_team_id, COUNT(*) AS hit_count
        FROM source_team_mapping
        WHERE (source, sport, source_team_id) IN ({placeholders})
        GROUP BY our_team_id
        ORDER BY hit_count DESC, our_team_id
        LIMIT 1
        """,
        params,
    )
    row = cur.fetchone()
    return row["our_team_id"] if row else None


def upsert_source_teams_and_mappings(cur, our_team_id, row, status):
    proposal = row["proposal"]
    confidence = row_confidence(row)
    for member in proposal.get("members", []):
        upsert_source_team(
            cur,
            member["source"],
            member["sport"],
            member["source_team_id"],
            member["source_team_name"],
        )
        cur.execute(
            """
            INSERT INTO source_team_mapping (
                our_team_id, source, sport, source_team_id, source_team_name,
                normalized_name, confidence, status, evidence_count,
                confirmed_method, confirmed_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, IF(%s='confirmed', NOW(), NULL))
            ON DUPLICATE KEY UPDATE
                our_team_id=VALUES(our_team_id),
                source_team_name=VALUES(source_team_name),
                normalized_name=VALUES(normalized_name),
                confidence=GREATEST(confidence, VALUES(confidence)),
                status=IF(status='confirmed' AND VALUES(status)<>'confirmed', status, VALUES(status)),
                evidence_count=GREATEST(evidence_count, VALUES(evidence_count)),
                confirmed_method=IF(status='confirmed' AND VALUES(status)<>'confirmed', confirmed_method, VALUES(confirmed_method)),
                confirmed_at=IF(VALUES(status)='confirmed', COALESCE(confirmed_at, NOW()), confirmed_at)
            """,
            (
                our_team_id,
                member["source"],
                member["sport"],
                member["source_team_id"],
                member["source_team_name"],
                normalize_name(member["source_team_name"]),
                confidence,
                status,
                int(proposal.get("evidence_count") or 0),
                "llm_verified_event_evidence" if status == "confirmed" else None,
                status,
            ),
        )


def parse_team_key(team_key):
    source, sport, source_team_id = team_key.split(":", 2)
    return source, sport, source_team_id


def upsert_evidence(cur, our_team_id, row, status):
    proposal = row["proposal"]
    for evidence in proposal.get("evidence", []):
        left_source, sport, left_team_id = parse_team_key(evidence["left_team_key"])
        right_source, _, right_team_id = parse_team_key(evidence["right_team_key"])
        left_event_id, right_event_id = parse_candidate_event_ids(evidence["candidate_key"])
        cur.execute(
            """
            INSERT INTO team_mapping_evidence (
                our_team_id, source_a, source_a_team_id, source_b, source_b_team_id,
                source_a_event_id, source_b_event_id, sport, evidence_type,
                event_match_score, name_score, time_diff_minutes, score_match,
                side_match, home_away_reversed, conflict_count, confidence,
                status, details
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'event_match',
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                our_team_id=VALUES(our_team_id),
                event_match_score=VALUES(event_match_score),
                confidence=GREATEST(confidence, VALUES(confidence)),
                status=VALUES(status),
                details=VALUES(details)
            """,
            (
                our_team_id,
                left_source,
                left_team_id,
                right_source,
                right_team_id,
                left_event_id,
                right_event_id,
                sport,
                float(evidence.get("event_score") or 0),
                evidence.get("name_score"),
                evidence.get("time_diff_minutes"),
                none_bool(evidence.get("score_match")),
                1,
                1 if evidence.get("home_away_reversed") else 0,
                int(proposal.get("conflict_count") or 0),
                row_confidence(row),
                "confirmed" if status == "confirmed" else "needs_review",
                json.dumps(evidence, ensure_ascii=False),
            ),
        )


def none_bool(value):
    if value is None:
        return None
    return 1 if value else 0


def parse_candidate_event_ids(candidate_key):
    left, right = candidate_key.split("::", 1)
    return left.split(":", 1)[1], right.split(":", 1)[1]


def upsert_llm_verification(cur, our_team_id, row):
    proposal = row["proposal"]
    verification = row["llm_verification"]
    proposal_key = proposal["proposed_our_team_id"]
    if row.get("run_id"):
        proposal_key = f"run-{row['run_id']}:{proposal_key}"
    cur.execute(
        """
        INSERT INTO llm_verification (
            our_team_id, proposal_key, provider, model, same_team, confidence,
            recommended_status, risk_flags, reason, response_payload
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            our_team_id=VALUES(our_team_id),
            same_team=VALUES(same_team),
            confidence=VALUES(confidence),
            recommended_status=VALUES(recommended_status),
            risk_flags=VALUES(risk_flags),
            reason=VALUES(reason),
            response_payload=VALUES(response_payload)
        """,
        (
            our_team_id,
            proposal_key,
            row["llm_provider"],
            row["llm_model"],
            none_bool(verification.get("same_team")),
            float(verification.get("confidence") or 0),
            verification.get("recommended_status"),
            json.dumps(verification.get("risk_flags") or [], ensure_ascii=False),
            verification.get("reason"),
            json.dumps(verification, ensure_ascii=False),
        ),
    )


def row_confidence(row):
    proposal = row["proposal"]
    verification = row.get("llm_verification") or {}
    return float(
        verification.get("confidence")
        or proposal.get("team_confidence")
        or proposal.get("avg_event_score")
        or 0
    )


def main():
    args = parse_args()
    load_env_files(args)
    if args.init_schema:
        print("Initializing mapping schema on test1...")
        init_schema()
    events_by_source, candidates, proposals = generate_proposals(args)
    print(f"candidate_count={len(candidates)}")
    print(f"proposal_count={len(proposals)}")
    if args.dry_run:
        plan_counts = count_llm_plan(args, proposals)
        print(f"llm_would_request_count={plan_counts['llm_would_request']}")
        print(f"needs_review_without_llm_count={plan_counts['needs_review_without_llm']}")
        print("dry_run=true; not writing mapping DB")
        return
    run_id, inventory_stats = initialize_incremental_run(args, events_by_source)
    print(f"pipeline_run_id={run_id}")
    print("source_inventory_written=true")
    write_counts = verify_and_write_proposals(args, proposals, run_id)
    finish_incremental_run(args, run_id, inventory_stats, len(candidates), write_counts)
    print(f"llm_verified_count={write_counts['llm_verified']}")
    print(f"confirmed_eligible_count={write_counts['confirmed']}")
    print(f"confirmed_written={write_counts['confirmed']}")
    print(f"llm_requested_count={write_counts['llm_requested']}")
    print(f"llm_skipped_low_score_count={write_counts['llm_skipped_low_score']}")
    print(f"llm_skipped_existing_count={write_counts['llm_skipped_existing']}")


if __name__ == "__main__":
    main()
