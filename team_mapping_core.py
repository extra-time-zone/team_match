#!/usr/bin/env python3
"""
Shared read-only helpers for cross-source team/event matching.
"""

import html
import json
import re
import subprocess
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from difflib import SequenceMatcher
from pathlib import Path
from types import SimpleNamespace
from typing import Optional
from urllib.parse import quote_plus

import db_connection_test as dbt


TEST1_HOST = "test-db.cluster-cdgqiwig2x00.us-west-2.rds.amazonaws.com"
SOURCE_MYSQL = {
    "sr": "test",
    "ls": "test",
    "thesports_fb": "test1",
    "thesports_bb": "test1",
    "bc": "test1",
}

SPORT_ALIASES = {
    "football": {"football", "soccer", "1", "sr:sport:1"},
    "basketball": {"basketball", "2", "sr:sport:2"},
}

STOP_WORDS = {
    "fc",
    "f.c",
    "cf",
    "c.f",
    "sc",
    "s.c",
    "club",
    "team",
    "the",
    "de",
    "ac",
    "afc",
    "bc",
}


@dataclass
class EventRecord:
    source: str
    sport: str
    event_id: str
    start_time: str
    start_ts: int
    home_team_id: str
    home_team_name: str
    away_team_id: str
    away_team_name: str
    home_score: Optional[int] = None
    away_score: Optional[int] = None
    competition_id: Optional[str] = None
    competition_name: Optional[str] = None
    category_id: Optional[str] = None
    raw_sport_id: Optional[str] = None


def mysql_args(mysql_name, mysql_password=None, mysql_user=None):
    return SimpleNamespace(
        mysql_host=TEST1_HOST if mysql_name == "test1" else None,
        mysql_port=None,
        mysql_user=mysql_user or ("root" if mysql_name == "test1" else None),
        mysql_password=mysql_password,
        prompt_mysql_password=False,
        mysql_db="",
    )


@contextmanager
def mysql_connection(mysql_name, mysql_password=None, mysql_user=None):
    args = mysql_args(mysql_name, mysql_password, mysql_user)
    config = dbt.get_config(args)
    process = None
    conn = None
    try:
        if is_direct_mysql(mysql_name):
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
            database=None,
            charset="utf8mb4",
            connect_timeout=10,
            read_timeout=180,
            write_timeout=20,
            cursorclass=dbt.pymysql.cursors.DictCursor,
        )
        yield conn
    finally:
        if conn:
            conn.close()
        if process:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()


def is_direct_mysql(mysql_name):
    import os

    if os.environ.get("MYSQL_DIRECT", "").lower() in ("1", "true", "yes", "y"):
        return True
    env_name = "TEST1_MYSQL_DIRECT" if mysql_name == "test1" else "MARKET_MYSQL_DIRECT"
    return os.environ.get(env_name, "").lower() in ("1", "true", "yes", "y")


def parse_time(value):
    if value is None:
        return 0
    if isinstance(value, datetime):
        dt = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    if isinstance(value, (int, float)):
        timestamp = float(value)
        if timestamp > 20_000_000_000:
            timestamp = timestamp / 1000
        return int(timestamp)

    text = str(value).strip()
    if not text:
        return 0
    if re.fullmatch(r"\d{10,13}", text):
        return parse_time(int(text))

    normalized = text.replace("T", " ").replace("Z", "+00:00")
    for fmt in ("%Y-%m-%d %H:%M:%S%z", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            dt = datetime.strptime(normalized, fmt)
            if not dt.tzinfo:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp())
        except ValueError:
            pass
    try:
        dt = datetime.fromisoformat(text)
        if not dt.tzinfo:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    except ValueError:
        return 0


def fmt_utc(ts):
    if not ts:
        return ""
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def normalize_name(name):
    text = (name or "").casefold()
    text = re.sub(r"[^\w\s]", " ", text)
    tokens = [token for token in text.split() if token not in STOP_WORDS]
    return " ".join(tokens)


def token_sort(name):
    return " ".join(sorted(normalize_name(name).split()))


def similarity(a, b):
    norm_a = normalize_name(a)
    norm_b = normalize_name(b)
    if not norm_a or not norm_b:
        return 0.0
    direct = SequenceMatcher(None, norm_a, norm_b).ratio()
    sorted_ratio = SequenceMatcher(None, token_sort(norm_a), token_sort(norm_b)).ratio()
    containment = 0.0
    if norm_a in norm_b or norm_b in norm_a:
        containment = min(len(norm_a), len(norm_b)) / max(len(norm_a), len(norm_b))
    return max(direct, sorted_ratio, containment)


def sport_matches(canonical_sport, raw_sport_id, sport_name=None):
    accepted = SPORT_ALIASES.get(canonical_sport, {canonical_sport})
    values = {str(raw_sport_id or "").casefold(), str(sport_name or "").casefold()}
    return bool(accepted & values) or not raw_sport_id


def sofa_search_url(event_a, event_b):
    query = " ".join(
        part
        for part in [
            event_a.home_team_name,
            event_a.away_team_name,
            event_b.home_team_name,
            event_b.away_team_name,
            event_a.sport,
            event_a.start_time[:10],
        ]
        if part
    )
    return f"https://www.sofascore.com/search?q={quote_plus(query)}"


def candidate_score(event_a, event_b, max_time_diff_hours):
    if event_a.sport != event_b.sport:
        return None

    time_diff_seconds = abs(event_a.start_ts - event_b.start_ts) if event_a.start_ts and event_b.start_ts else None
    if time_diff_seconds is not None and time_diff_seconds > max_time_diff_hours * 3600:
        return None

    home_home = similarity(event_a.home_team_name, event_b.home_team_name)
    away_away = similarity(event_a.away_team_name, event_b.away_team_name)
    home_away = similarity(event_a.home_team_name, event_b.away_team_name)
    away_home = similarity(event_a.away_team_name, event_b.home_team_name)

    normal_name_score = (home_home + away_away) / 2
    reversed_name_score = (home_away + away_home) / 2
    reversed_home_away = reversed_name_score > normal_name_score + 0.08
    name_score = max(normal_name_score, reversed_name_score)
    normal_pair_min = min(home_home, away_away)
    reversed_pair_min = min(home_away, away_home)
    pair_min_score = reversed_pair_min if reversed_home_away else normal_pair_min
    if pair_min_score < 0.38:
        return None

    score_match = None
    if (
        event_a.home_score is not None
        and event_a.away_score is not None
        and event_b.home_score is not None
        and event_b.away_score is not None
    ):
        has_meaningful_score = any(
            value not in (0, None)
            for value in (event_a.home_score, event_a.away_score, event_b.home_score, event_b.away_score)
        )
        if has_meaningful_score:
            normal_score_match = event_a.home_score == event_b.home_score and event_a.away_score == event_b.away_score
            reversed_score_match = event_a.home_score == event_b.away_score and event_a.away_score == event_b.home_score
            score_match = reversed_score_match if reversed_home_away else normal_score_match

    if time_diff_seconds is None:
        time_score = 0.35
    else:
        time_score = max(0.0, 1.0 - (time_diff_seconds / (max_time_diff_hours * 3600)))

    competition_score = 0.0
    if event_a.competition_name and event_b.competition_name:
        competition_score = similarity(event_a.competition_name, event_b.competition_name)

    final = (name_score * 0.52) + (time_score * 0.28) + (competition_score * 0.10)
    final *= 0.60 + (0.40 * pair_min_score)
    if score_match is True:
        final += 0.10
    elif score_match is False:
        final -= 0.08

    return {
        "score": round(max(0.0, min(final, 1.0)), 4),
        "name_score": round(name_score, 4),
        "normal_name_score": round(normal_name_score, 4),
        "reversed_name_score": round(reversed_name_score, 4),
        "pair_min_score": round(pair_min_score, 4),
        "time_score": round(time_score, 4),
        "time_diff_minutes": round(time_diff_seconds / 60, 2) if time_diff_seconds is not None else None,
        "competition_score": round(competition_score, 4),
        "score_match": score_match,
        "home_away_reversed": reversed_home_away,
    }


def build_candidate(event_a, event_b, score_detail):
    return {
        "candidate_key": f"{event_a.source}:{event_a.event_id}::{event_b.source}:{event_b.event_id}",
        "source_event": asdict(event_a),
        "candidate_event": asdict(event_b),
        "score_detail": score_detail,
        "sofascore_search_url": sofa_search_url(event_a, event_b),
        "review_status": "pending_llm",
    }


def write_jsonl(path, rows):
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")


def write_review_html(path, candidates):
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    rows = []
    for item in candidates:
        a = item["source_event"]
        b = item["candidate_event"]
        detail = item["score_detail"]
        rows.append(
            "<tr>"
            f"<td>{html.escape(a['source'])}<br><code>{html.escape(str(a['event_id']))}</code></td>"
            f"<td>{html.escape(b['source'])}<br><code>{html.escape(str(b['event_id']))}</code></td>"
            f"<td>{html.escape(a['sport'])}</td>"
            f"<td>{html.escape(a['start_time'])}<br>{html.escape(b['start_time'])}</td>"
            f"<td>{html.escape(a['home_team_name'])}<br>{html.escape(a['away_team_name'])}</td>"
            f"<td>{html.escape(b['home_team_name'])}<br>{html.escape(b['away_team_name'])}</td>"
            f"<td>{detail['score']}</td>"
            f"<td>{html.escape(str(detail['time_diff_minutes']))}</td>"
            f"<td>{html.escape(str(detail['score_match']))}</td>"
            f"<td><a href=\"{html.escape(item['sofascore_search_url'])}\" target=\"_blank\">SofaScore</a></td>"
            "</tr>"
        )

    document = f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Team Match Candidates</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 24px; color: #1f2933; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 14px; }}
    th, td {{ border: 1px solid #d8dee6; padding: 8px; vertical-align: top; text-align: left; }}
    th {{ background: #eef3f8; }}
    code {{ background: #eef2f6; padding: 1px 4px; border-radius: 4px; }}
  </style>
</head>
<body>
  <h1>Team Match Candidates</h1>
  <p>候选数量：{len(candidates)}。LLM 判断后，仍需要 SofaScore 外部确认再进入 confirmed。</p>
  <table>
    <thead>
      <tr>
        <th>Source Event</th><th>Candidate Event</th><th>Sport</th><th>Time</th>
        <th>Source Teams</th><th>Candidate Teams</th><th>Score</th><th>Time Diff Min</th><th>Score Match</th><th>Verify</th>
      </tr>
    </thead>
    <tbody>
      {''.join(rows)}
    </tbody>
  </table>
</body>
</html>
"""
    out.write_text(document, encoding="utf-8")


def fetch_events(conn, source, sport, start_ts, end_ts, limit):
    if source == "thesports_fb":
        return fetch_thesports_football(conn, sport, start_ts, end_ts, limit)
    if source == "thesports_bb":
        return fetch_thesports_basketball(conn, sport, start_ts, end_ts, limit)
    if source == "sr":
        return fetch_sr(conn, sport, start_ts, end_ts, limit)
    if source == "ls":
        return fetch_ls(conn, sport, start_ts, end_ts, limit)
    raise ValueError(f"Unsupported source: {source}")


def fetch_thesports_football(conn, sport, start_ts, end_ts, limit):
    if sport != "football":
        return []
    sql = """
        SELECT
            m.match_id,
            m.match_time,
            m.home_team_id,
            ht.name AS home_team_name,
            m.away_team_id,
            at.name AS away_team_name,
            s.home_score,
            s.away_score,
            m.competition_id,
            c.name AS competition_name,
            c.category_id
        FROM `test-thesports-db`.`ts_fb_match` m
        LEFT JOIN `test-thesports-db`.`ts_fb_team` ht ON ht.team_id = m.home_team_id
        LEFT JOIN `test-thesports-db`.`ts_fb_team` at ON at.team_id = m.away_team_id
        LEFT JOIN `test-thesports-db`.`ts_fb_match_score` s ON s.match_id = m.match_id
        LEFT JOIN `test-thesports-db`.`ts_fb_competition` c ON c.competition_id = m.competition_id
        WHERE m.match_time >= %s AND m.match_time < %s
        ORDER BY m.match_time
        LIMIT %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (start_ts, end_ts, limit))
        return [
            EventRecord(
                source="thesports",
                sport="football",
                event_id=str(row["match_id"]),
                start_time=fmt_utc(parse_time(row["match_time"])),
                start_ts=parse_time(row["match_time"]),
                home_team_id=str(row["home_team_id"]),
                home_team_name=row["home_team_name"] or "",
                away_team_id=str(row["away_team_id"]),
                away_team_name=row["away_team_name"] or "",
                home_score=row["home_score"],
                away_score=row["away_score"],
                competition_id=row["competition_id"],
                competition_name=row["competition_name"],
                category_id=row["category_id"],
                raw_sport_id="football",
            )
            for row in cur.fetchall()
        ]


def fetch_thesports_basketball(conn, sport, start_ts, end_ts, limit):
    if sport != "basketball":
        return []
    sql = """
        SELECT
            m.match_id,
            m.match_time,
            m.home_team_id,
            ht.name AS home_team_name,
            m.away_team_id,
            at.name AS away_team_name,
            COALESCE(s.home_s1, 0) + COALESCE(s.home_s2, 0) + COALESCE(s.home_s3, 0)
                + COALESCE(s.home_s4, 0) + COALESCE(s.home_s_over, 0) AS home_score,
            COALESCE(s.away_s1, 0) + COALESCE(s.away_s2, 0) + COALESCE(s.away_s3, 0)
                + COALESCE(s.away_s4, 0) + COALESCE(s.away_s_over, 0) AS away_score,
            m.competition_id,
            c.name AS competition_name,
            c.category_id
        FROM `test-thesports-db`.`ts_bb_match` m
        LEFT JOIN `test-thesports-db`.`ts_bb_team` ht ON ht.team_id = m.home_team_id
        LEFT JOIN `test-thesports-db`.`ts_bb_team` at ON at.team_id = m.away_team_id
        LEFT JOIN `test-thesports-db`.`ts_bb_match_score` s ON s.match_id = m.match_id
        LEFT JOIN `test-thesports-db`.`ts_bb_competition` c ON c.competition_id = m.competition_id
        WHERE m.match_time >= %s AND m.match_time < %s
        ORDER BY m.match_time
        LIMIT %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (start_ts, end_ts, limit))
        return [
            EventRecord(
                source="thesports",
                sport="basketball",
                event_id=str(row["match_id"]),
                start_time=fmt_utc(parse_time(row["match_time"])),
                start_ts=parse_time(row["match_time"]),
                home_team_id=str(row["home_team_id"]),
                home_team_name=row["home_team_name"] or "",
                away_team_id=str(row["away_team_id"]),
                away_team_name=row["away_team_name"] or "",
                home_score=row["home_score"],
                away_score=row["away_score"],
                competition_id=row["competition_id"],
                competition_name=row["competition_name"],
                category_id=row["category_id"],
                raw_sport_id="basketball",
            )
            for row in cur.fetchall()
        ]


def fetch_sr(conn, sport, start_ts, end_ts, limit):
    sql = """
        SELECT
            e.sport_event_id,
            e.scheduled,
            e.start_time,
            e.home_competitor_id,
            hc.name AS home_team_name,
            e.away_competitor_id,
            ac.name AS away_team_name,
            e.home_score,
            e.away_score,
            e.tournament_id,
            t.name AS tournament_name,
            e.category_id,
            e.sport_id,
            sp.name AS sport_name
        FROM `test2-sportdata-syncer`.`sr_sport_event` e
        LEFT JOIN `test2-sportdata-syncer`.`sr_competitor_en` hc ON hc.competitor_id = e.home_competitor_id
        LEFT JOIN `test2-sportdata-syncer`.`sr_competitor_en` ac ON ac.competitor_id = e.away_competitor_id
        LEFT JOIN `test2-sportdata-syncer`.`sr_tournament_en` t ON t.tournament_id = e.tournament_id
        LEFT JOIN `test2-sportdata-syncer`.`sr_sport_en` sp ON sp.sport_id = e.sport_id
        WHERE COALESCE(NULLIF(e.start_time, ''), e.scheduled) >= %s
            AND COALESCE(NULLIF(e.start_time, ''), e.scheduled) < %s
        ORDER BY COALESCE(NULLIF(e.start_time, ''), e.scheduled)
        LIMIT %s
    """
    start_text = fmt_utc(start_ts).replace(" UTC", "")
    end_text = fmt_utc(end_ts).replace(" UTC", "")
    rows = []
    with conn.cursor() as cur:
        cur.execute(sql, (start_text, end_text, limit))
        for row in cur.fetchall():
            if not sport_matches(sport, row["sport_id"], row["sport_name"]):
                continue
            time_value = row["start_time"] or row["scheduled"]
            rows.append(
                EventRecord(
                    source="sr",
                    sport=sport,
                    event_id=str(row["sport_event_id"]),
                    start_time=fmt_utc(parse_time(time_value)),
                    start_ts=parse_time(time_value),
                    home_team_id=str(row["home_competitor_id"]),
                    home_team_name=row["home_team_name"] or "",
                    away_team_id=str(row["away_competitor_id"]),
                    away_team_name=row["away_team_name"] or "",
                    home_score=row["home_score"],
                    away_score=row["away_score"],
                    competition_id=row["tournament_id"],
                    competition_name=row["tournament_name"],
                    category_id=row["category_id"],
                    raw_sport_id=row["sport_id"],
                )
            )
    return rows


def fetch_ls(conn, sport, start_ts, end_ts, limit):
    sql = """
        SELECT
            e.event_id,
            e.scheduled,
            e.home_competitor_id,
            hc.name AS home_team_name,
            e.away_competitor_id,
            ac.name AS away_team_name,
            e.home_score,
            e.away_score,
            e.tournament_id,
            t.name AS tournament_name,
            e.category_id,
            e.sport_id,
            sp.name AS sport_name
        FROM `test1-lsports-db`.`ls_sport_event` e
        LEFT JOIN `test1-lsports-db`.`ls_competitor_en` hc ON CAST(hc.competitor_id AS CHAR) = e.home_competitor_id
        LEFT JOIN `test1-lsports-db`.`ls_competitor_en` ac ON CAST(ac.competitor_id AS CHAR) = e.away_competitor_id
        LEFT JOIN `test1-lsports-db`.`ls_tournament_en` t ON CAST(t.tournament_id AS CHAR) = e.tournament_id
        LEFT JOIN `test1-lsports-db`.`ls_sport_en` sp ON sp.sport_id = e.sport_id
        WHERE e.scheduled >= %s AND e.scheduled < %s
        ORDER BY e.scheduled
        LIMIT %s
    """
    start_text = fmt_utc(start_ts).replace(" UTC", "")
    end_text = fmt_utc(end_ts).replace(" UTC", "")
    rows = []
    with conn.cursor() as cur:
        cur.execute(sql, (start_text, end_text, limit))
        for row in cur.fetchall():
            if not sport_matches(sport, row["sport_id"], row["sport_name"]):
                continue
            rows.append(
                EventRecord(
                    source="ls",
                    sport=sport,
                    event_id=str(row["event_id"]),
                    start_time=fmt_utc(parse_time(row["scheduled"])),
                    start_ts=parse_time(row["scheduled"]),
                    home_team_id=str(row["home_competitor_id"]),
                    home_team_name=row["home_team_name"] or "",
                    away_team_id=str(row["away_competitor_id"]),
                    away_team_name=row["away_team_name"] or "",
                    home_score=row["home_score"],
                    away_score=row["away_score"],
                    competition_id=row["tournament_id"],
                    competition_name=row["tournament_name"],
                    category_id=row["category_id"],
                    raw_sport_id=row["sport_id"],
                )
            )
    return rows
