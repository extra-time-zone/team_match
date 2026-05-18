#!/usr/bin/env python3
"""
Inspect one local our_team proposal.

This is the local-file version of the future our_team lookup tool. It reads
proposal JSON and optional LLM verification JSONL files from outputs/.
"""

import argparse
import json
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent


def parse_args():
    parser = argparse.ArgumentParser(description="Inspect a local our_team proposal.")
    parser.add_argument("--our-team-id", "--proposal-id", dest="our_team_id", required=True)
    parser.add_argument(
        "--proposals",
        default="outputs/high_score_100_football_2026-04-01_2026-05-15.our_team_proposals.json",
        help="Proposal JSON file.",
    )
    parser.add_argument(
        "--verifications",
        default="outputs/high_score_100_football_2026-04-01_2026-05-15.our_team_proposals.llm_verifications.jsonl",
        help="Optional LLM verification JSONL file.",
    )
    parser.add_argument("--format", choices=("text", "json"), default="text")
    return parser.parse_args()


def resolve_path(path):
    candidate = Path(path).expanduser()
    if candidate.is_absolute() or candidate.exists():
        return candidate
    script_relative = SCRIPT_DIR / candidate
    if script_relative.exists():
        return script_relative
    return candidate


def load_proposals(path):
    resolved = resolve_path(path)
    return json.loads(resolved.read_text(encoding="utf-8"))


def load_verifications(path):
    resolved = resolve_path(path)
    if not resolved.exists():
        return {}
    verifications = {}
    for line in resolved.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        proposal = row.get("proposal", {})
        proposal_id = proposal.get("proposed_our_team_id")
        if proposal_id:
            verifications[proposal_id] = row
    return verifications


def find_proposal(proposals, our_team_id):
    for proposal in proposals:
        if proposal.get("proposed_our_team_id") == our_team_id:
            return proposal
    normalized = our_team_id.lower()
    matches = [
        proposal
        for proposal in proposals
        if normalized in (proposal.get("canonical_name") or "").lower()
    ]
    if len(matches) == 1:
        return matches[0]
    if matches:
        names = ", ".join(f"{p['proposed_our_team_id']}:{p['canonical_name']}" for p in matches[:20])
        raise SystemExit(f"Multiple proposals matched name '{our_team_id}': {names}")
    raise SystemExit(f"Proposal not found: {our_team_id}")


def build_payload(proposal, verification):
    return {
        "our_team": {
            "id": proposal.get("proposed_our_team_id"),
            "sport": proposal.get("sport"),
            "canonical_name": proposal.get("canonical_name"),
            "review_status": proposal.get("review_status"),
            "source_count": proposal.get("source_count"),
            "evidence_count": proposal.get("evidence_count"),
            "avg_event_score": proposal.get("avg_event_score"),
            "team_confidence": proposal.get("team_confidence"),
            "conflict_count": proposal.get("conflict_count"),
        },
        "source_mappings": proposal.get("members", []),
        "evidence": proposal.get("evidence", []),
        "llm_verification": verification.get("llm_verification") if verification else None,
        "llm_provider": verification.get("llm_provider") if verification else None,
        "llm_model": verification.get("llm_model") if verification else None,
    }


def print_text(payload):
    team = payload["our_team"]
    print(f"our_team_id: {team['id']}")
    print(f"sport: {team['sport']}")
    print(f"canonical_name: {team['canonical_name']}")
    print(f"review_status: {team['review_status']}")
    print()
    print("metrics:")
    print(f"  source_count: {team['source_count']}")
    print(f"  evidence_count: {team['evidence_count']}")
    print(f"  avg_event_score: {team['avg_event_score']}")
    print(f"  team_confidence: {team['team_confidence']}")
    print(f"  conflict_count: {team['conflict_count']}")

    verification = payload.get("llm_verification")
    if verification:
        print()
        print("llm_verification:")
        print(f"  provider: {payload.get('llm_provider')}")
        print(f"  model: {payload.get('llm_model')}")
        print(f"  status: {verification.get('recommended_status')}")
        print(f"  confidence: {verification.get('confidence')}")
        flags = verification.get("risk_flags") or []
        print(f"  risk_flags: {', '.join(flags) if flags else '<none>'}")
        print(f"  reason: {verification.get('reason')}")

    print()
    print("source_mappings:")
    for member in payload["source_mappings"]:
        print(
            f"  - {member.get('source'):<10} {member.get('source_team_id'):<28} "
            f"{member.get('source_team_name')}"
        )

    print()
    print("evidence:")
    for index, evidence in enumerate(payload["evidence"], 1):
        print(f"  {index}. candidate_key: {evidence.get('candidate_key')}")
        print(f"     event_score: {evidence.get('event_score')}")
        print(f"     score_match: {evidence.get('score_match')}")
        print(f"     time_diff_minutes: {evidence.get('time_diff_minutes')}")
        print(f"     home_away_reversed: {evidence.get('home_away_reversed')}")
        if evidence.get("sofascore_search_url"):
            print(f"     sofascore_search_url: {evidence.get('sofascore_search_url')}")


def main():
    args = parse_args()
    proposals = load_proposals(args.proposals)
    verifications = load_verifications(args.verifications)
    proposal = find_proposal(proposals, args.our_team_id)
    verification = verifications.get(proposal.get("proposed_our_team_id"), {})
    payload = build_payload(proposal, verification)

    if args.format == "json":
        print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
    else:
        print_text(payload)


if __name__ == "__main__":
    main()
