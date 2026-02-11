"""SQLite cache for LLM game analyses (Phase L).

Each game (event_slug) is analyzed at most once. Subsequent DCA entries,
hedge jobs, and merge checks all use the cached result.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

from src.store.schema import DEFAULT_DB_PATH, _connect

logger = logging.getLogger(__name__)


def get_cached_analysis(
    event_slug: str,
    db_path: Path | str = DEFAULT_DB_PATH,
):
    """Get cached LLM analysis for an event_slug.

    Returns GameAnalysis or None if not cached.
    """
    from src.strategy.llm_analyzer import GameAnalysis

    conn = _connect(db_path)
    try:
        row = conn.execute(
            "SELECT * FROM llm_analyses WHERE event_slug = ?",
            (event_slug,),
        ).fetchone()
        if not row:
            return None

        d = dict(row)
        risk_flags = []
        try:
            risk_flags = json.loads(d.get("risk_flags", "[]") or "[]")
        except (json.JSONDecodeError, TypeError):
            pass

        expert_analyses = {}
        try:
            expert_analyses = json.loads(d.get("expert_analyses", "{}") or "{}")
        except (json.JSONDecodeError, TypeError):
            pass

        return GameAnalysis(
            favored_team=d["favored_team"],
            home_win_prob=d["home_win_prob"],
            away_win_prob=d["away_win_prob"],
            confidence=d["confidence"],
            sizing_modifier=d.get("sizing_modifier", 1.0),
            hedge_ratio=d.get("hedge_ratio", 0.5),
            risk_flags=risk_flags,
            reasoning=d.get("reasoning", ""),
            expert_analyses=expert_analyses,
            model_id=d.get("model_id", ""),
            latency_ms=d.get("latency_ms", 0),
        )
    finally:
        conn.close()


def save_analysis(
    event_slug: str,
    game_date: str,
    analysis,  # GameAnalysis
    db_path: Path | str = DEFAULT_DB_PATH,
) -> int:
    """Save an LLM analysis to cache. Returns row id."""
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc).isoformat()
    conn = _connect(db_path)
    try:
        cur = conn.execute(
            """INSERT OR REPLACE INTO llm_analyses
               (event_slug, game_date, favored_team, confidence,
                home_win_prob, away_win_prob, sizing_modifier, hedge_ratio,
                risk_flags, reasoning, expert_analyses,
                model_id, latency_ms, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                event_slug,
                game_date,
                analysis.favored_team,
                analysis.confidence,
                analysis.home_win_prob,
                analysis.away_win_prob,
                analysis.sizing_modifier,
                analysis.hedge_ratio,
                json.dumps(analysis.risk_flags),
                analysis.reasoning,
                json.dumps(analysis.expert_analyses),
                analysis.model_id,
                analysis.latency_ms,
                now,
            ),
        )
        conn.commit()
        logger.info(
            "Cached LLM analysis for %s: favored=%s confidence=%.2f",
            event_slug,
            analysis.favored_team,
            analysis.confidence,
        )
        return cur.lastrowid  # type: ignore[return-value]
    finally:
        conn.close()


def get_or_analyze(
    event_slug: str,
    game_date: str,
    context,  # GameContext
    db_path: Path | str = DEFAULT_DB_PATH,
):
    """Get cached analysis or run LLM analysis and cache the result.

    Returns GameAnalysis or None on failure.
    """
    # キャッシュチェック
    cached = get_cached_analysis(event_slug, db_path=db_path)
    if cached is not None:
        logger.info("LLM cache hit for %s: favored=%s", event_slug, cached.favored_team)
        return cached

    # LLM 分析実行
    from src.strategy.llm_analyzer import analyze_game_sync

    analysis = analyze_game_sync(context)
    if analysis is None:
        return None

    # キャッシュ保存
    save_analysis(event_slug, game_date, analysis, db_path=db_path)
    return analysis
