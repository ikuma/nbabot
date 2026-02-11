"""LLM-based game analysis: 3-persona parallel + synthesis (Phase L).

Method B: Three independent expert calls (parallelized) followed by
a synthesis call that integrates their analyses into a final decision.

Default model: Opus 4.6 (~$0.24/game, ~$72/month at 10 games/day).
Configurable via LLM_MODEL env var.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field

from src.config import settings
from src.connectors.nba_data import GameContext
from src.strategy.prompts.game_analysis import (
    POLYMARKET_SPECIALIST_SYSTEM,
    POLYMARKET_SPECIALIST_USER,
    QUANT_TRADER_SYSTEM,
    QUANT_TRADER_USER,
    RISK_MANAGER_SYSTEM,
    RISK_MANAGER_USER,
    SHARED_KNOWLEDGE_BASE,
    SYNTHESIS_SYSTEM,
    SYNTHESIS_USER,
    format_game_context,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class GameAnalysis:
    """Integrated LLM analysis result for a single game."""

    favored_team: str  # LLM が推すチーム (directional に使用)
    home_win_prob: float  # ホーム勝利確率
    away_win_prob: float  # アウェイ勝利確率
    confidence: float  # 0.0-1.0 (分析の確信度)
    sizing_modifier: float  # 0.5-1.5 (ポジションサイズ調整)
    hedge_ratio: float  # 0.3-0.8 (hedge Kelly 乗数)
    risk_flags: list[str] = field(default_factory=list)
    reasoning: str = ""
    expert_analyses: dict[str, str] = field(default_factory=dict)
    model_id: str = ""
    latency_ms: int = 0


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


async def _call_llm(
    system_prompt: str,
    user_prompt: str,
    model: str | None = None,
    timeout: int | None = None,
) -> str:
    """Call Anthropic API with given prompts. Returns raw text response.

    Uses structured system messages with prompt caching: the shared
    knowledge base (>1024 tokens) is marked with cache_control so it
    is cached across calls within the same 5-minute TTL window.
    """
    import anthropic

    model = model or settings.llm_model
    timeout_sec = timeout or settings.llm_timeout_sec

    client = anthropic.AsyncAnthropic(
        api_key=settings.anthropic_api_key,
        timeout=float(timeout_sec),
    )

    # 構造化システムメッセージ: ナレッジベース (cached) + ペルソナ固有指示
    system_blocks = [
        {
            "type": "text",
            "text": SHARED_KNOWLEDGE_BASE,
            "cache_control": {"type": "ephemeral"},
        },
        {
            "type": "text",
            "text": system_prompt,
        },
    ]

    response = await client.messages.create(
        model=model,
        max_tokens=1024,
        system=system_blocks,
        messages=[{"role": "user", "content": user_prompt}],
    )

    # キャッシュ使用状況ログ
    usage = response.usage
    cache_read = getattr(usage, "cache_read_input_tokens", 0)
    cache_create = getattr(usage, "cache_creation_input_tokens", 0)
    if cache_read or cache_create:
        logger.debug(
            "LLM cache: read=%d create=%d input=%d",
            cache_read,
            cache_create,
            usage.input_tokens,
        )

    return response.content[0].text


def _extract_json(text: str) -> dict:
    """Extract JSON from LLM response, handling markdown code blocks."""
    text = text.strip()

    # markdown code block
    if "```" in text:
        parts = text.split("```")
        for part in parts:
            part = part.strip()
            if part.startswith("json"):
                part = part[4:].strip()
            try:
                return json.loads(part)
            except (json.JSONDecodeError, ValueError):
                continue

    # raw JSON
    try:
        return json.loads(text)
    except (json.JSONDecodeError, ValueError):
        pass

    # find first { ... } block
    start = text.find("{")
    end = text.rfind("}")
    if start >= 0 and end > start:
        try:
            return json.loads(text[start : end + 1])
        except (json.JSONDecodeError, ValueError):
            pass

    logger.warning("Failed to parse JSON from LLM response: %.100s...", text)
    return {}


async def analyze_game(context: GameContext) -> GameAnalysis | None:
    """Run 3-persona parallel analysis + synthesis for a single game.

    Returns GameAnalysis or None on failure (caller should fall back).
    """
    if not settings.anthropic_api_key:
        logger.warning("ANTHROPIC_API_KEY not set, skipping LLM analysis")
        return None

    start_ms = time.monotonic_ns() // 1_000_000

    try:
        # Phase 1: 3 ペルソナ並列実行
        expert1_prompt = format_game_context(POLYMARKET_SPECIALIST_USER, context)
        expert2_prompt = format_game_context(QUANT_TRADER_USER, context)
        expert3_prompt = format_game_context(RISK_MANAGER_USER, context)

        expert1_raw, expert2_raw, expert3_raw = await asyncio.gather(
            _call_llm(POLYMARKET_SPECIALIST_SYSTEM, expert1_prompt),
            _call_llm(QUANT_TRADER_SYSTEM, expert2_prompt),
            _call_llm(RISK_MANAGER_SYSTEM, expert3_prompt),
        )

        logger.info(
            "LLM Phase 1 complete (%s): 3 experts responded",
            context.home.name,
        )

        # Phase 2: シンセシス (直列)
        synthesis_user = SYNTHESIS_USER.format(
            home_team=context.home.name,
            away_team=context.away.name,
            home_price=context.poly_home_price,
            away_price=context.poly_away_price,
            expert1_response=expert1_raw,
            expert2_response=expert2_raw,
            expert3_response=expert3_raw,
        )

        synthesis_raw = await _call_llm(SYNTHESIS_SYSTEM, synthesis_user)
        elapsed_ms = int(time.monotonic_ns() // 1_000_000 - start_ms)

        logger.info(
            "LLM Phase 2 complete (%s): synthesis in %dms total",
            context.home.name,
            elapsed_ms,
        )

        # Parse synthesis JSON
        data = _extract_json(synthesis_raw)
        if not data:
            logger.error("Failed to parse synthesis response")
            return None

        return _parse_synthesis(
            data,
            context=context,
            expert_analyses={
                "polymarket_specialist": expert1_raw,
                "quant_trader": expert2_raw,
                "risk_manager": expert3_raw,
            },
            model_id=settings.llm_model,
            latency_ms=elapsed_ms,
        )

    except Exception:
        elapsed_ms = int(time.monotonic_ns() // 1_000_000 - start_ms)
        logger.exception(
            "LLM analysis failed for %s vs %s (%dms)",
            context.away.name,
            context.home.name,
            elapsed_ms,
        )
        return None


def _parse_synthesis(
    data: dict,
    *,
    context: GameContext,
    expert_analyses: dict[str, str] | None = None,
    model_id: str = "",
    latency_ms: int = 0,
) -> GameAnalysis | None:
    """Parse synthesis JSON into GameAnalysis with validation."""
    favored = data.get("favored_team", "")
    if not favored:
        logger.error("Synthesis missing favored_team")
        return None

    # favored_team がいずれかのチーム名に一致するか確認
    valid_teams = {context.home.name, context.away.name}
    if favored not in valid_teams:
        # 部分一致を試行
        for team in valid_teams:
            if favored.lower() in team.lower() or team.lower() in favored.lower():
                favored = team
                break
        else:
            logger.error("favored_team '%s' not in %s", favored, valid_teams)
            return None

    home_prob = float(data.get("home_win_prob", 0.5))
    away_prob = float(data.get("away_win_prob", 0.5))
    confidence = _clamp(float(data.get("confidence", 0.5)), 0.0, 1.0)
    sizing_mod = _clamp(
        float(data.get("sizing_modifier", 1.0)),
        settings.llm_min_sizing_modifier,
        settings.llm_max_sizing_modifier,
    )
    hedge_ratio = _clamp(float(data.get("hedge_ratio", 0.5)), 0.3, 0.8)
    risk_flags = data.get("risk_flags", [])
    if not isinstance(risk_flags, list):
        risk_flags = []
    reasoning = data.get("reasoning", "")

    return GameAnalysis(
        favored_team=favored,
        home_win_prob=home_prob,
        away_win_prob=away_prob,
        confidence=confidence,
        sizing_modifier=sizing_mod,
        hedge_ratio=hedge_ratio,
        risk_flags=risk_flags,
        reasoning=reasoning,
        expert_analyses=expert_analyses or {},
        model_id=model_id,
        latency_ms=latency_ms,
    )


def analyze_game_sync(context: GameContext) -> GameAnalysis | None:
    """Synchronous wrapper for analyze_game(). For use in non-async code."""
    try:
        return asyncio.run(analyze_game(context))
    except RuntimeError:
        # Already in an event loop — create a new one
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(analyze_game(context))
        finally:
            loop.close()


def determine_directional(
    analysis: GameAnalysis | None,
    home_outcome_name: str,
    away_outcome_name: str,
) -> tuple[str, str]:
    """Determine directional and hedge outcomes based on LLM analysis.

    Returns (directional_outcome_name, hedge_outcome_name).
    If analysis is None, returns (home, away) as safe default.
    """
    if analysis is None:
        return home_outcome_name, away_outcome_name

    # LLM の favored_team に対応する outcome を directional に
    # outcome_name は short name (e.g. "Knicks") or full name
    favored = analysis.favored_team.lower()

    if favored in home_outcome_name.lower() or home_outcome_name.lower() in favored:
        return home_outcome_name, away_outcome_name
    elif favored in away_outcome_name.lower() or away_outcome_name.lower() in favored:
        return away_outcome_name, home_outcome_name
    else:
        # マッチ失敗 — デフォルト
        logger.warning(
            "favored_team '%s' didn't match outcomes (%s, %s)",
            analysis.favored_team,
            home_outcome_name,
            away_outcome_name,
        )
        return home_outcome_name, away_outcome_name
