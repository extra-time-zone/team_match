#!/usr/bin/env python3
"""
Verify our_team proposals with an OpenAI-compatible LLM.

This is a local review step. It reads proposal JSON and writes verification
JSONL/CSV files. It does not write to remote MySQL.
"""

import argparse
import csv
import json
import os
import time
from pathlib import Path

from openai import OpenAI

import db_connection_test as dbt
from judge_team_candidates import PROVIDER_DEFAULTS


SCRIPT_DIR = Path(__file__).resolve().parent

SYSTEM_PROMPT = """
You are a strict sports team identity verification judge.
You receive one proposed our_team grouping built from multiple source team IDs
and event-match evidence.

Decide whether all source_team_id members represent the same real-world team.

Rules:
- Football and basketball must never be mixed.
- Youth, reserve, women, and senior teams are different teams unless the source
  names and evidence clearly indicate the same level.
- Same event evidence is strong, but one event is not enough for automatic final
  confirmation if there is ambiguity.
- Source IDs are source-specific and do not prove equality.
- Equivalent aliases, abbreviations, punctuation, accents, and suffixes like FC
  are acceptable.
- Penalize conflicts such as U19 vs U21, women vs men, reserve vs senior, or
  multiple different IDs from the same source in one proposal.

Return JSON only with these keys:
same_team, confidence, recommended_status, risk_flags, reason.
recommended_status must be one of: llm_verified, needs_review, reject.
risk_flags must be an array of short strings.
"""


def parse_args():
    parser = argparse.ArgumentParser(description="Verify team proposals with an LLM.")
    parser.add_argument("--llm-env-file", default=".env.llm", help="Optional LLM env file. Default: .env.llm")
    parser.add_argument("--input", required=True, help="our_team_proposals.json file.")
    parser.add_argument("--output-jsonl", help="Output JSONL. Default: <input>.llm_verifications.jsonl")
    parser.add_argument("--output-csv", help="Output CSV. Default: <input>.llm_verifications.csv")
    parser.add_argument(
        "--provider",
        choices=tuple(PROVIDER_DEFAULTS),
        default=os.environ.get("LLM_PROVIDER", "deepseek"),
        help="LLM provider. Default: env LLM_PROVIDER or deepseek.",
    )
    parser.add_argument("--model", help="LLM model name.")
    parser.add_argument("--base-url", default=os.environ.get("LLM_BASE_URL"), help="OpenAI-compatible base URL override.")
    parser.add_argument("--api-key-env", help="Environment variable that stores the API key.")
    parser.add_argument("--limit", type=int, help="Max proposals to verify.")
    parser.add_argument("--timeout", type=float, default=35, help="LLM request timeout seconds. Default: 35")
    parser.add_argument("--retries", type=int, default=2, help="Retries per proposal. Default: 2")
    parser.add_argument("--dry-run", action="store_true", help="Write prompts without calling LLM.")
    return parser.parse_args()


def resolve_path(path):
    candidate = Path(path).expanduser()
    if candidate.is_absolute() or candidate.exists():
        return candidate
    script_relative = SCRIPT_DIR / candidate
    if script_relative.exists():
        return script_relative
    return candidate


def load_optional_env(path):
    env_path = resolve_path(path)
    if env_path.exists():
        dbt.load_env_file(env_path)


def provider_config(args):
    defaults = PROVIDER_DEFAULTS[args.provider]
    model_env = f"{args.provider.upper()}_MODEL"
    model = args.model or os.environ.get(model_env) or defaults["model"]
    base_url = args.base_url if args.base_url is not None else defaults["base_url"]
    api_key_env = args.api_key_env or defaults["api_key_env"]
    api_key = os.environ.get(api_key_env)
    if not args.dry_run and not api_key:
        raise SystemExit(f"Missing {api_key_env}. Run: export {api_key_env}='your_api_key'")
    return model, base_url, api_key


def proposal_payload(proposal):
    return {
        "task": "Verify whether all source team IDs in this proposal are the same real-world team.",
        "proposal": {
            "proposed_our_team_id": proposal.get("proposed_our_team_id"),
            "sport": proposal.get("sport"),
            "canonical_name": proposal.get("canonical_name"),
            "source_count": proposal.get("source_count"),
            "evidence_count": proposal.get("evidence_count"),
            "avg_event_score": proposal.get("avg_event_score"),
            "team_confidence": proposal.get("team_confidence"),
            "conflict_count": proposal.get("conflict_count"),
            "members": proposal.get("members", []),
            "evidence": proposal.get("evidence", [])[:12],
        },
    }


def verify_one(client, model, proposal):
    payload = proposal_payload(proposal)
    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT.strip()},
            {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
        ],
        response_format={"type": "json_object"},
        temperature=0,
        extra_body={"thinking": {"type": "disabled"}},
    )
    text = response.choices[0].message.content
    if not text:
        raise RuntimeError("LLM returned empty content.")
    return json.loads(text)


def normalize_verification(result):
    status = result.get("recommended_status")
    if status not in {"llm_verified", "needs_review", "reject"}:
        status = "needs_review"
    result["recommended_status"] = status
    result["confidence"] = float(result.get("confidence") or 0)
    if not isinstance(result.get("risk_flags"), list):
        result["risk_flags"] = []
    return result


def csv_rows(rows):
    for row in rows:
        proposal = row["proposal"]
        verification = row["llm_verification"]
        yield {
            "proposed_our_team_id": proposal.get("proposed_our_team_id"),
            "canonical_name": proposal.get("canonical_name"),
            "sport": proposal.get("sport"),
            "source_count": proposal.get("source_count"),
            "evidence_count": proposal.get("evidence_count"),
            "avg_event_score": proposal.get("avg_event_score"),
            "team_confidence": proposal.get("team_confidence"),
            "llm_status": verification.get("recommended_status"),
            "llm_confidence": verification.get("confidence"),
            "risk_flags": "|".join(verification.get("risk_flags", [])),
            "reason": verification.get("reason"),
            "members": " ; ".join(
                f"{m.get('source')}:{m.get('source_team_id')}:{m.get('source_team_name')}"
                for m in proposal.get("members", [])
            ),
        }


def main():
    args = parse_args()
    load_optional_env(args.llm_env_file)
    model, base_url, api_key = provider_config(args)

    input_path = resolve_path(args.input)
    proposals = json.loads(input_path.read_text(encoding="utf-8"))
    if args.limit:
        proposals = proposals[: args.limit]

    output_jsonl = Path(args.output_jsonl).expanduser() if args.output_jsonl else input_path.with_suffix(".llm_verifications.jsonl")
    output_csv = Path(args.output_csv).expanduser() if args.output_csv else input_path.with_suffix(".llm_verifications.csv")
    if not output_jsonl.is_absolute():
        output_jsonl = SCRIPT_DIR / output_jsonl
    if not output_csv.is_absolute():
        output_csv = SCRIPT_DIR / output_csv
    output_jsonl.parent.mkdir(parents=True, exist_ok=True)
    output_csv.parent.mkdir(parents=True, exist_ok=True)

    client = None if args.dry_run else OpenAI(api_key=api_key, base_url=base_url, timeout=args.timeout, max_retries=0)
    rows = []
    with output_jsonl.open("w", encoding="utf-8") as fh:
        for index, proposal in enumerate(proposals, 1):
            result = None
            last_error = None
            if args.dry_run:
                result = {
                    "same_team": None,
                    "confidence": 0,
                    "recommended_status": "needs_review",
                    "risk_flags": ["dry_run"],
                    "reason": json.dumps(proposal_payload(proposal), ensure_ascii=False),
                }
            else:
                for attempt in range(1, args.retries + 1):
                    try:
                        result = normalize_verification(verify_one(client, model, proposal))
                        break
                    except Exception as exc:
                        last_error = exc
                        if attempt < args.retries:
                            print(f"failed attempt={attempt}/{args.retries} {proposal.get('proposed_our_team_id')}: {exc}")
                            time.sleep(2)
                if result is None:
                    result = {
                        "same_team": False,
                        "confidence": 0,
                        "recommended_status": "needs_review",
                        "risk_flags": ["llm_error"],
                        "reason": f"LLM request failed: {last_error}",
                    }

            row = {
                "proposal": proposal,
                "llm_provider": args.provider if not args.dry_run else None,
                "llm_model": model if not args.dry_run else None,
                "llm_verification": result,
            }
            rows.append(row)
            fh.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")
            fh.flush()
            print(
                f"verified {index}/{len(proposals)} {proposal.get('proposed_our_team_id')} "
                f"-> {result.get('recommended_status')} confidence={result.get('confidence')}"
            )

    with output_csv.open("w", encoding="utf-8", newline="") as fh:
        fieldnames = [
            "proposed_our_team_id",
            "canonical_name",
            "sport",
            "source_count",
            "evidence_count",
            "avg_event_score",
            "team_confidence",
            "llm_status",
            "llm_confidence",
            "risk_flags",
            "reason",
            "members",
        ]
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(csv_rows(rows))

    print(f"input_proposals={len(proposals)}")
    print(f"output_jsonl={output_jsonl}")
    print(f"output_csv={output_csv}")


if __name__ == "__main__":
    main()
