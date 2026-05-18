#!/usr/bin/env python3
"""
Run the multi-source team mapping pipeline.

This is a local/offline pipeline:
  1. read events from multiple sources;
  2. generate pairwise event/team candidates;
  3. optionally judge candidates with an OpenAI-compatible LLM;
  4. build local our_team proposals from accepted judgments.

It does not write to remote MySQL and never marks mappings as confirmed.
"""

import argparse
import csv
import itertools
import json
import os
import time
from dataclasses import asdict
from pathlib import Path

from openai import OpenAI

import db_connection_test as dbt
import team_mapping_core as core
from judge_team_candidates import PROVIDER_DEFAULTS, judge_candidate, normalize_judgment


SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_SOURCES = {
    "football": ["thesports_fb", "sr", "ls"],
    "basketball": ["thesports_bb", "sr", "ls"],
}
SOURCE_PRIORITY = {"thesports": 0, "sr": 1, "ls": 2, "bc": 3}


class UnionFind:
    def __init__(self):
        self.parent = {}

    def add(self, key):
        self.parent.setdefault(key, key)

    def find(self, key):
        self.add(key)
        if self.parent[key] != key:
            self.parent[key] = self.find(self.parent[key])
        return self.parent[key]

    def union(self, left, right):
        left_root = self.find(left)
        right_root = self.find(right)
        if left_root != right_root:
            self.parent[right_root] = left_root

    def groups(self):
        grouped = {}
        for key in list(self.parent):
            grouped.setdefault(self.find(key), []).append(key)
        return list(grouped.values())


def parse_args():
    parser = argparse.ArgumentParser(description="Run multi-source team mapping candidate pipeline.")
    parser.add_argument("--env-file", default=".env.market", help="Path to env file. Default: .env.market")
    parser.add_argument("--llm-env-file", default=".env.llm", help="Optional LLM env file. Default: .env.llm")
    parser.add_argument("--sport", required=True, choices=("football", "basketball"))
    parser.add_argument("--sources", help="Comma separated sources. Default: thesports/sr/ls for the sport.")
    parser.add_argument("--start", required=True, help="Start time inclusive. Example: 2026-05-14 00:00:00")
    parser.add_argument("--end", required=True, help="End time exclusive. Example: 2026-05-15 00:00:00")
    parser.add_argument("--test1-mysql-password", help="Password for named MySQL test1. Prefer --prompt-test1-password.")
    parser.add_argument("--prompt-test1-password", action="store_true", help="Prompt for test1 MySQL password.")
    parser.add_argument("--mysql-user", help="Override MySQL user.")
    parser.add_argument("--per-source-limit", type=int, default=1000, help="Max events per source. Default: 1000")
    parser.add_argument("--db-retries", type=int, default=3, help="DB/SSH fetch retries per source. Default: 3")
    parser.add_argument("--db-retry-sleep", type=float, default=3, help="Seconds between DB retries. Default: 3")
    parser.add_argument("--time-window-hours", type=float, default=24, help="Max kickoff time gap. Default: 24")
    parser.add_argument("--top-k", type=int, default=5, help="Candidates per event per pair. Default: 5")
    parser.add_argument("--min-score", type=float, default=0.55, help="Minimum pair candidate score. Default: 0.55")
    parser.add_argument(
        "--llm-provider",
        choices=("none", "openai", "deepseek"),
        default=os.environ.get("LLM_PROVIDER", "deepseek"),
        help="LLM provider for judgments. Use none to only generate candidates. Default: env LLM_PROVIDER or deepseek.",
    )
    parser.add_argument("--model", help="LLM model name.")
    parser.add_argument("--base-url", default=os.environ.get("LLM_BASE_URL"), help="OpenAI-compatible base URL override.")
    parser.add_argument("--api-key-env", help="Environment variable that stores the API key.")
    parser.add_argument("--llm-limit", type=int, help="Max candidates to judge across all pairs.")
    parser.add_argument("--llm-timeout", type=float, default=45, help="LLM request timeout seconds. Default: 45")
    parser.add_argument("--llm-retries", type=int, default=2, help="LLM retries per candidate. Default: 2")
    parser.add_argument(
        "--proposal-method",
        choices=("evidence", "llm"),
        default="evidence",
        help="Build our_team proposals from rule evidence or accepted LLM judgments. Default: evidence",
    )
    parser.add_argument(
        "--evidence-min-score",
        type=float,
        default=0.72,
        help="Minimum event candidate score to become team-pair evidence. Default: 0.72",
    )
    parser.add_argument(
        "--allow-score-conflict",
        action="store_true",
        help="Allow candidates with explicit score conflicts into evidence proposals.",
    )
    parser.add_argument(
        "--min-proposal-evidence",
        type=int,
        default=1,
        help="Minimum accepted event edges per our_team proposal. Default: 1",
    )
    parser.add_argument("--max-proposals", type=int, help="Keep only the top N proposals after ranking.")
    parser.add_argument("--accept-event-confidence", type=float, default=0.85, help="Min LLM same-event confidence. Default: 0.85")
    parser.add_argument("--accept-team-confidence", type=float, default=0.85, help="Min LLM same-team confidence. Default: 0.85")
    parser.add_argument("--output-dir", default="outputs", help="Output directory. Default: outputs")
    return parser.parse_args()


def parse_sources(args):
    if args.sources:
        sources = [item.strip() for item in args.sources.split(",") if item.strip()]
    else:
        sources = DEFAULT_SOURCES[args.sport]
    unsupported = [source for source in sources if source not in core.SOURCE_MYSQL or source == "bc"]
    if unsupported:
        raise SystemExit(f"Unsupported sources for now: {', '.join(unsupported)}")
    if len(sources) < 2:
        raise SystemExit("At least two sources are required.")
    return sources


def get_provider_config(args):
    if args.llm_provider == "none":
        return None, None, None, None
    defaults = PROVIDER_DEFAULTS[args.llm_provider]
    model_env = f"{args.llm_provider.upper()}_MODEL"
    model = args.model or os.environ.get(model_env) or defaults["model"]
    base_url = args.base_url if args.base_url is not None else defaults["base_url"]
    api_key_env = args.api_key_env or defaults["api_key_env"]
    api_key = os.environ.get(api_key_env)
    if not api_key:
        raise SystemExit(f"Missing {api_key_env}. Run: export {api_key_env}='your_api_key'")
    return model, base_url, api_key_env, api_key


def fetch_all_events(sources, args, start_ts, end_ts, test1_password):
    events_by_source = {}
    sources_by_mysql = {}
    for source in sources:
        sources_by_mysql.setdefault(core.SOURCE_MYSQL[source], []).append(source)

    for mysql_name, mysql_sources in sources_by_mysql.items():
        password = test1_password if mysql_name == "test1" else None
        print(f"Opening {mysql_name} for sources={','.join(mysql_sources)}...")
        last_error = None
        for attempt in range(1, args.db_retries + 1):
            try:
                with core.mysql_connection(mysql_name, mysql_password=password, mysql_user=args.mysql_user) as conn:
                    for source in mysql_sources:
                        print(f"  Fetching {source}...")
                        events = core.fetch_events(conn, source, args.sport, start_ts, end_ts, args.per_source_limit)
                        events_by_source[source] = events
                        print(f"  {source}_events={len(events)}")
                break
            except Exception as exc:
                last_error = exc
                if attempt >= args.db_retries:
                    raise
                print(f"  fetch failed attempt={attempt}/{args.db_retries}: {exc}")
                time.sleep(args.db_retry_sleep)
        if any(source not in events_by_source for source in mysql_sources):
            raise last_error
    return events_by_source


def generate_pair_candidates(left_source, right_source, left_events, right_events, args):
    candidates = []
    sorted_right_events = sorted(right_events, key=lambda event: event.start_ts or 0)
    right_start_times = [event.start_ts or 0 for event in sorted_right_events]
    max_diff_seconds = int(args.time_window_hours * 3600)
    for left_event in left_events:
        ranked = []
        if left_event.start_ts:
            right_iterable = iter_time_window(sorted_right_events, right_start_times, left_event.start_ts, max_diff_seconds)
        else:
            right_iterable = sorted_right_events
        for right_event in right_iterable:
            detail = core.candidate_score(left_event, right_event, args.time_window_hours)
            if not detail or detail["score"] < args.min_score:
                continue
            candidate = core.build_candidate(left_event, right_event, detail)
            candidate["source_adapter"] = left_source
            candidate["candidate_adapter"] = right_source
            ranked.append((detail["score"], candidate))
        ranked.sort(key=lambda item: item[0], reverse=True)
        candidates.extend(item for _, item in ranked[: args.top_k])
    return candidates


def iter_time_window(events, start_times, center_ts, max_diff_seconds):
    import bisect

    left = bisect.bisect_left(start_times, center_ts - max_diff_seconds)
    right = bisect.bisect_right(start_times, center_ts + max_diff_seconds)
    return events[left:right]


def judge_candidates(candidates, args, model, base_url, api_key, judgments_path):
    if args.llm_provider == "none":
        return []
    client = OpenAI(api_key=api_key, base_url=base_url, timeout=args.llm_timeout, max_retries=0)
    judgments = []
    to_judge = candidates[: args.llm_limit] if args.llm_limit else candidates
    judgments_path.parent.mkdir(parents=True, exist_ok=True)
    with judgments_path.open("w", encoding="utf-8") as fh:
        for index, candidate in enumerate(to_judge, 1):
            judgment = None
            last_error = None
            for attempt in range(1, args.llm_retries + 1):
                try:
                    judgment = normalize_judgment(judge_candidate(client, model, args.llm_provider, candidate))
                    break
                except Exception as exc:
                    last_error = exc
                    if attempt < args.llm_retries:
                        print(f"  llm failed attempt={attempt}/{args.llm_retries} {candidate['candidate_key']}: {exc}")
                        time.sleep(2)
            if judgment is None:
                judgment = {
                    "same_event": False,
                    "event_confidence": 0,
                    "home_team_same": False,
                    "home_team_confidence": 0,
                    "away_team_same": False,
                    "away_team_confidence": 0,
                    "home_away_reversed": False,
                    "requires_sofascore_verification": True,
                    "recommended_review_status": "manual_review",
                    "reason": f"LLM request failed: {last_error}",
                }

            row = {
                "candidate_key": candidate["candidate_key"],
                "source_adapter": candidate["source_adapter"],
                "candidate_adapter": candidate["candidate_adapter"],
                "source_event": candidate["source_event"],
                "candidate_event": candidate["candidate_event"],
                "score_detail": candidate["score_detail"],
                "sofascore_search_url": candidate["sofascore_search_url"],
                "llm_provider": args.llm_provider,
                "llm_model": model,
                "llm_judgment": judgment,
                "review_status": judgment["recommended_review_status"],
            }
            judgments.append(row)
            fh.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")
            fh.flush()
            print(f"  judged {index}/{len(to_judge)} {candidate['candidate_key']} -> {row['review_status']}")
    return judgments


def team_key(event, side):
    return f"{event['source']}:{event['sport']}:{event[f'{side}_team_id']}"


def team_payload(event, side):
    return {
        "source": event["source"],
        "sport": event["sport"],
        "source_team_id": event[f"{side}_team_id"],
        "source_team_name": event[f"{side}_team_name"],
        "source_event_id": event["event_id"],
        "competition_id": event.get("competition_id"),
        "competition_name": event.get("competition_name"),
    }


def is_accepted_judgment(row, args):
    judgment = row["llm_judgment"]
    return (
        judgment.get("same_event") is True
        and float(judgment.get("event_confidence") or 0) >= args.accept_event_confidence
        and judgment.get("home_team_same") is True
        and judgment.get("away_team_same") is True
        and float(judgment.get("home_team_confidence") or 0) >= args.accept_team_confidence
        and float(judgment.get("away_team_confidence") or 0) >= args.accept_team_confidence
        and row.get("review_status") != "reject"
    )


def build_our_team_proposals(judgments, args):
    uf = UnionFind()
    teams = {}
    evidence_by_edge = []

    for row in judgments:
        if not is_accepted_judgment(row, args):
            continue
        source_event = row["source_event"]
        candidate_event = row["candidate_event"]
        reversed_pair = row["llm_judgment"].get("home_away_reversed") is True
        pairs = [("home", "away"), ("away", "home")] if reversed_pair else [("home", "home"), ("away", "away")]
        for left_side, right_side in pairs:
            left_key = team_key(source_event, left_side)
            right_key = team_key(candidate_event, right_side)
            uf.union(left_key, right_key)
            teams[left_key] = team_payload(source_event, left_side)
            teams[right_key] = team_payload(candidate_event, right_side)
            evidence_by_edge.append(
                {
                    "left_team_key": left_key,
                    "right_team_key": right_key,
                    "candidate_key": row["candidate_key"],
                    "event_confidence": row["llm_judgment"].get("event_confidence"),
                    "sofascore_search_url": row["sofascore_search_url"],
                    "status": "needs_sofascore_verification",
                }
            )

    proposals = []
    for index, group in enumerate(uf.groups(), 1):
        members = [teams[key] for key in sorted(group)]
        if len(members) < 2:
            continue
        canonical = choose_canonical_member(members)
        proposals.append(
            {
                "proposed_our_team_id": f"proposal-{args.sport}-{index:06d}",
                "sport": args.sport,
                "canonical_name": canonical["source_team_name"],
                "review_status": "needs_sofascore_verification",
                "members": members,
                "evidence": [
                    edge
                    for edge in evidence_by_edge
                    if edge["left_team_key"] in group or edge["right_team_key"] in group
                ],
            }
        )
    return proposals


def build_our_team_proposals_from_evidence(candidates, args):
    uf = UnionFind()
    teams = {}
    evidence_by_edge = []

    for candidate in candidates:
        detail = candidate["score_detail"]
        if detail["score"] < args.evidence_min_score:
            continue
        if detail.get("score_match") is False and not args.allow_score_conflict:
            continue

        source_event = candidate["source_event"]
        candidate_event = candidate["candidate_event"]
        reversed_pair = detail.get("home_away_reversed") is True
        pairs = [("home", "away"), ("away", "home")] if reversed_pair else [("home", "home"), ("away", "away")]
        for left_side, right_side in pairs:
            left_key = team_key(source_event, left_side)
            right_key = team_key(candidate_event, right_side)
            uf.union(left_key, right_key)
            teams[left_key] = team_payload(source_event, left_side)
            teams[right_key] = team_payload(candidate_event, right_side)
            evidence_by_edge.append(
                {
                    "left_team_key": left_key,
                    "right_team_key": right_key,
                    "candidate_key": candidate["candidate_key"],
                    "event_score": detail["score"],
                    "name_score": detail.get("name_score"),
                    "time_diff_minutes": detail.get("time_diff_minutes"),
                    "score_match": detail.get("score_match"),
                    "home_away_reversed": detail.get("home_away_reversed"),
                    "sofascore_search_url": candidate["sofascore_search_url"],
                    "status": "event_evidence",
                }
            )

    proposals = []
    for index, group in enumerate(uf.groups(), 1):
        members = [teams[key] for key in sorted(group)]
        if len(members) < 2:
            continue
        evidence = [
            edge
            for edge in evidence_by_edge
            if edge["left_team_key"] in group or edge["right_team_key"] in group
        ]
        if len(evidence) < args.min_proposal_evidence:
            continue
        source_count = len({member["source"] for member in members})
        conflict_count = sum(1 for edge in evidence if edge.get("score_match") is False)
        avg_score = sum(edge["event_score"] for edge in evidence) / len(evidence)
        team_confidence = calculate_team_confidence(source_count, len(evidence), avg_score, conflict_count)
        canonical = choose_canonical_member(members)
        proposals.append(
            {
                "proposed_our_team_id": f"proposal-{args.sport}-{index:06d}",
                "sport": args.sport,
                "canonical_name": canonical["source_team_name"],
                "review_status": proposal_status(team_confidence, source_count, len(evidence), conflict_count),
                "source_count": source_count,
                "evidence_count": len(evidence),
                "avg_event_score": round(avg_score, 4),
                "team_confidence": team_confidence,
                "conflict_count": conflict_count,
                "members": members,
                "evidence": evidence,
            }
        )

    proposals.sort(
        key=lambda item: (
            -item["team_confidence"],
            -item["source_count"],
            -item["evidence_count"],
            -item["avg_event_score"],
            item["canonical_name"],
        )
    )
    if args.max_proposals:
        proposals = proposals[: args.max_proposals]
    for index, proposal in enumerate(proposals, 1):
        proposal["proposed_our_team_id"] = f"proposal-{args.sport}-{index:06d}"
    return proposals


def calculate_team_confidence(source_count, evidence_count, avg_score, conflict_count):
    source_score = min(source_count, 4) / 4
    evidence_score = min(evidence_count, 6) / 6
    conflict_penalty = min(conflict_count * 0.18, 0.54)
    confidence = (avg_score * 0.50) + (source_score * 0.30) + (evidence_score * 0.20) - conflict_penalty
    return round(max(0.0, min(confidence, 0.9999)), 4)


def proposal_status(team_confidence, source_count, evidence_count, conflict_count):
    if conflict_count:
        return "needs_review"
    if source_count >= 3 and evidence_count >= 3 and team_confidence >= 0.82:
        return "high_confidence"
    if team_confidence >= 0.72:
        return "evidence_candidate"
    return "needs_more_evidence"


def name_specificity(name):
    text = (name or "").casefold()
    score = 0
    if any(marker in text for marker in (" u21", " u20", " u19", " u18", " u17", " ii", " 2")):
        score += 100
    if any(marker in text for marker in (" women", "wfc", "femenino", "feminino")):
        score += 100
    if any(marker in text for marker in (" fc", " club", " ca ", " sc", " ac ")):
        score += 10
    score += len(text)
    return score


def choose_canonical_member(members):
    return sorted(
        members,
        key=lambda item: (
            -name_specificity(item["source_team_name"]),
            SOURCE_PRIORITY.get(item["source"], 99),
            item["source_team_name"] or "",
        ),
    )[0]


def write_json(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def write_mapping_csv(path, proposals):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=[
                "proposed_our_team_id",
                "sport",
                "canonical_name",
                "source",
                "source_team_id",
                "source_team_name",
                "source_count",
                "evidence_count",
                "avg_event_score",
                "team_confidence",
                "conflict_count",
                "review_status",
            ],
        )
        writer.writeheader()
        for proposal in proposals:
            for member in proposal["members"]:
                writer.writerow(
                    {
                        "proposed_our_team_id": proposal["proposed_our_team_id"],
                        "sport": proposal["sport"],
                        "canonical_name": proposal["canonical_name"],
                        "source": member["source"],
                        "source_team_id": member["source_team_id"],
                        "source_team_name": member["source_team_name"],
                        "source_count": proposal.get("source_count", ""),
                        "evidence_count": proposal.get("evidence_count", len(proposal.get("evidence", []))),
                        "avg_event_score": proposal.get("avg_event_score", ""),
                        "team_confidence": proposal.get("team_confidence", ""),
                        "conflict_count": proposal.get("conflict_count", ""),
                        "review_status": proposal["review_status"],
                    }
                )


def main():
    args = parse_args()
    sources = parse_sources(args)
    dbt.load_env_file(args.env_file)
    llm_env_path = Path(args.llm_env_file).expanduser()
    if not llm_env_path.is_absolute():
        llm_env_path = SCRIPT_DIR / llm_env_path
    if llm_env_path.exists():
        dbt.load_env_file(llm_env_path)

    start_ts = core.parse_time(args.start)
    end_ts = core.parse_time(args.end)
    if not start_ts or not end_ts or end_ts <= start_ts:
        raise SystemExit("--start/--end must parse to a valid positive time range.")

    test1_password = args.test1_mysql_password
    needs_test1 = any(core.SOURCE_MYSQL[source] == "test1" for source in sources)
    if needs_test1 and args.prompt_test1_password:
        import getpass

        test1_password = getpass.getpass("test1 MySQL password: ")

    model, base_url, _, api_key = get_provider_config(args)
    out_dir = Path(args.output_dir).expanduser()
    if not out_dir.is_absolute():
        out_dir = SCRIPT_DIR / out_dir
    stem = f"all_{args.sport}_{args.start[:10]}_{args.end[:10]}".replace(":", "").replace(" ", "_")

    events_by_source = fetch_all_events(sources, args, start_ts, end_ts, test1_password)
    all_events = {
        source: [asdict(event) for event in events]
        for source, events in events_by_source.items()
    }
    write_json(out_dir / f"{stem}.events.json", all_events)

    all_candidates = []
    for left_source, right_source in itertools.combinations(sources, 2):
        pair_candidates = generate_pair_candidates(
            left_source,
            right_source,
            events_by_source[left_source],
            events_by_source[right_source],
            args,
        )
        print(f"{left_source}->{right_source}_candidates={len(pair_candidates)}")
        all_candidates.extend(pair_candidates)

    all_candidates.sort(key=lambda item: item["score_detail"]["score"], reverse=True)
    candidates_path = out_dir / f"{stem}.candidates.jsonl"
    core.write_jsonl(candidates_path, all_candidates)
    core.write_review_html(out_dir / f"{stem}.candidates.review.html", all_candidates)

    judgments_path = out_dir / f"{stem}.judgments.jsonl"
    if args.llm_provider == "none" or args.proposal_method == "evidence":
        judgments = []
    else:
        judgments = judge_candidates(all_candidates, args, model, base_url, api_key, judgments_path)

    if args.proposal_method == "evidence":
        proposals = build_our_team_proposals_from_evidence(all_candidates, args)
    else:
        proposals = build_our_team_proposals(judgments, args) if judgments else []
    proposals_path = out_dir / f"{stem}.our_team_proposals.json"
    mappings_path = out_dir / f"{stem}.team_mapping_proposals.csv"
    write_json(proposals_path, proposals)
    write_mapping_csv(mappings_path, proposals)

    print(f"events_json={out_dir / f'{stem}.events.json'}")
    print(f"candidates_jsonl={candidates_path}")
    print(f"candidates_html={out_dir / f'{stem}.candidates.review.html'}")
    if judgments:
        print(f"judgments_jsonl={judgments_path}")
    print(f"our_team_proposals_json={proposals_path}")
    print(f"team_mapping_proposals_csv={mappings_path}")
    print(f"proposal_count={len(proposals)}")


if __name__ == "__main__":
    main()
