"""Tests for LLM game analysis (Phase L)."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.connectors.nba_data import GameContext, TeamContext
from src.strategy.llm_analyzer import (
    GameAnalysis,
    _call_llm,
    _extract_json,
    _parse_synthesis,
    analyze_game,
    determine_directional,
)
from src.strategy.prompts.game_analysis import SHARED_KNOWLEDGE_BASE


def _make_team(name: str = "Boston Celtics", **kwargs) -> TeamContext:
    defaults = {
        "name": name,
        "record": "42-15",
        "win_pct": 0.737,
        "home_record": "24-5",
        "away_record": "18-10",
        "last_10": "7-3",
        "streak": "W3",
        "conference_rank": 1,
        "rest_days": 2,
        "is_back_to_back": False,
        "injuries": [],
    }
    defaults.update(kwargs)
    return TeamContext(**defaults)


def _make_context(
    home_name: str = "Boston Celtics",
    away_name: str = "New York Knicks",
    home_price: float = 0.65,
    away_price: float = 0.35,
) -> GameContext:
    return GameContext(
        home=_make_team(home_name),
        away=_make_team(away_name, record="30-27", win_pct=0.526),
        game_time_utc="2026-02-11T00:30:00Z",
        poly_home_price=home_price,
        poly_away_price=away_price,
    )


def _make_analysis(
    favored: str = "Boston Celtics",
    confidence: float = 0.85,
    sizing_modifier: float = 1.2,
    hedge_ratio: float = 0.4,
) -> GameAnalysis:
    return GameAnalysis(
        favored_team=favored,
        home_win_prob=0.72,
        away_win_prob=0.28,
        confidence=confidence,
        sizing_modifier=sizing_modifier,
        hedge_ratio=hedge_ratio,
        risk_flags=["injury_risk"],
        reasoning="Test reasoning",
        model_id="claude-opus-4-6",
        latency_ms=2500,
    )


# ---------------------------------------------------------------------------
# _extract_json
# ---------------------------------------------------------------------------


class TestExtractJson:
    def test_raw_json(self):
        text = '{"favored_team": "Celtics", "confidence": 0.9}'
        result = _extract_json(text)
        assert result["favored_team"] == "Celtics"

    def test_markdown_code_block(self):
        text = '```json\n{"favored_team": "Celtics"}\n```'
        result = _extract_json(text)
        assert result["favored_team"] == "Celtics"

    def test_text_with_json(self):
        text = 'Here is my analysis:\n{"favored_team": "Celtics", "confidence": 0.8}\nDone.'
        result = _extract_json(text)
        assert result["favored_team"] == "Celtics"

    def test_invalid_json(self):
        result = _extract_json("This is not JSON at all")
        assert result == {}

    def test_empty_string(self):
        result = _extract_json("")
        assert result == {}


# ---------------------------------------------------------------------------
# _parse_synthesis
# ---------------------------------------------------------------------------


class TestParseSynthesis:
    def test_valid_synthesis(self):
        ctx = _make_context()
        data = {
            "favored_team": "Boston Celtics",
            "home_win_prob": 0.72,
            "away_win_prob": 0.28,
            "confidence": 0.85,
            "sizing_modifier": 1.2,
            "hedge_ratio": 0.4,
            "risk_flags": ["injury"],
            "reasoning": "Celtics dominant at home",
        }
        result = _parse_synthesis(data, context=ctx, model_id="test")
        assert result is not None
        assert result.favored_team == "Boston Celtics"
        assert result.confidence == 0.85
        assert result.sizing_modifier == 1.2
        assert result.hedge_ratio == 0.4

    def test_missing_favored_team(self):
        ctx = _make_context()
        result = _parse_synthesis({}, context=ctx)
        assert result is None

    def test_invalid_favored_team(self):
        ctx = _make_context()
        data = {"favored_team": "Los Angeles Lakers"}
        result = _parse_synthesis(data, context=ctx)
        assert result is None

    def test_partial_match_favored_team(self):
        ctx = _make_context()
        data = {
            "favored_team": "Celtics",
            "home_win_prob": 0.7,
            "away_win_prob": 0.3,
            "confidence": 0.8,
        }
        result = _parse_synthesis(data, context=ctx)
        assert result is not None
        assert result.favored_team == "Boston Celtics"

    def test_sizing_modifier_clamped(self):
        ctx = _make_context()
        data = {
            "favored_team": "Boston Celtics",
            "home_win_prob": 0.7,
            "away_win_prob": 0.3,
            "sizing_modifier": 5.0,  # should be clamped to 1.5
            "confidence": 0.8,
        }
        result = _parse_synthesis(data, context=ctx)
        assert result is not None
        assert result.sizing_modifier == 1.5

    def test_hedge_ratio_clamped(self):
        ctx = _make_context()
        data = {
            "favored_team": "Boston Celtics",
            "home_win_prob": 0.7,
            "away_win_prob": 0.3,
            "hedge_ratio": 0.1,  # should be clamped to 0.3
            "confidence": 0.8,
        }
        result = _parse_synthesis(data, context=ctx)
        assert result is not None
        assert result.hedge_ratio == 0.3

    def test_risk_flags_not_list(self):
        ctx = _make_context()
        data = {
            "favored_team": "Boston Celtics",
            "home_win_prob": 0.7,
            "away_win_prob": 0.3,
            "confidence": 0.8,
            "risk_flags": "not a list",
        }
        result = _parse_synthesis(data, context=ctx)
        assert result is not None
        assert result.risk_flags == []


# ---------------------------------------------------------------------------
# determine_directional
# ---------------------------------------------------------------------------


class TestDetermineDirectional:
    def test_llm_favors_home(self):
        analysis = _make_analysis(favored="Boston Celtics")
        d, h = determine_directional(analysis, "Celtics", "Knicks")
        assert d == "Celtics"
        assert h == "Knicks"

    def test_llm_favors_away(self):
        analysis = _make_analysis(favored="New York Knicks")
        d, h = determine_directional(analysis, "Celtics", "Knicks")
        assert d == "Knicks"
        assert h == "Celtics"

    def test_none_analysis_returns_default(self):
        d, h = determine_directional(None, "Celtics", "Knicks")
        assert d == "Celtics"
        assert h == "Knicks"

    def test_partial_match(self):
        analysis = _make_analysis(favored="Boston Celtics")
        d, h = determine_directional(analysis, "Celtics", "Knicks")
        assert d == "Celtics"

    def test_no_match_returns_default(self):
        analysis = _make_analysis(favored="Los Angeles Lakers")
        d, h = determine_directional(analysis, "Celtics", "Knicks")
        assert d == "Celtics"  # fallback


# ---------------------------------------------------------------------------
# analyze_game (mocked LLM calls)
# ---------------------------------------------------------------------------


class TestAnalyzeGame:
    @pytest.mark.asyncio
    async def test_analyze_game_success(self, monkeypatch):
        """Full pipeline with mocked LLM calls."""
        monkeypatch.setattr(
            "src.strategy.llm_analyzer.settings.anthropic_api_key", "test-key"
        )
        monkeypatch.setattr(
            "src.strategy.llm_analyzer.settings.llm_model", "claude-haiku-4-5-20251001"
        )

        expert_resp = json.dumps({
            "analysis": "Test analysis",
            "win_prob_home": 0.72,
            "win_prob_away": 0.28,
        })

        synthesis_resp = json.dumps({
            "favored_team": "Boston Celtics",
            "home_win_prob": 0.72,
            "away_win_prob": 0.28,
            "confidence": 0.85,
            "sizing_modifier": 1.2,
            "hedge_ratio": 0.4,
            "risk_flags": [],
            "reasoning": "Celtics dominant",
        })

        call_count = 0

        async def mock_call_llm(system, user, model=None, timeout=None):
            nonlocal call_count
            call_count += 1
            if call_count <= 3:
                return expert_resp
            return synthesis_resp

        monkeypatch.setattr("src.strategy.llm_analyzer._call_llm", mock_call_llm)

        ctx = _make_context()
        result = await analyze_game(ctx)

        assert result is not None
        assert result.favored_team == "Boston Celtics"
        assert result.confidence == 0.85
        assert result.sizing_modifier == 1.2
        assert call_count == 4  # 3 experts + 1 synthesis

    @pytest.mark.asyncio
    async def test_analyze_game_no_api_key(self, monkeypatch):
        """No API key → returns None."""
        monkeypatch.setattr("src.strategy.llm_analyzer.settings.anthropic_api_key", "")
        ctx = _make_context()
        result = await analyze_game(ctx)
        assert result is None

    @pytest.mark.asyncio
    async def test_analyze_game_llm_error(self, monkeypatch):
        """LLM call raises exception → returns None."""
        monkeypatch.setattr(
            "src.strategy.llm_analyzer.settings.anthropic_api_key", "test-key"
        )

        async def mock_call_llm(*args, **kwargs):
            raise RuntimeError("API error")

        monkeypatch.setattr("src.strategy.llm_analyzer._call_llm", mock_call_llm)

        ctx = _make_context()
        result = await analyze_game(ctx)
        assert result is None

    @pytest.mark.asyncio
    async def test_analyze_game_bad_synthesis_json(self, monkeypatch):
        """Synthesis returns unparseable JSON → returns None."""
        monkeypatch.setattr(
            "src.strategy.llm_analyzer.settings.anthropic_api_key", "test-key"
        )

        call_count = 0

        async def mock_call_llm(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count <= 3:
                return '{"analysis": "ok"}'
            return "this is not json at all"

        monkeypatch.setattr("src.strategy.llm_analyzer._call_llm", mock_call_llm)

        ctx = _make_context()
        result = await analyze_game(ctx)
        assert result is None


# ---------------------------------------------------------------------------
# LLM cache
# ---------------------------------------------------------------------------


class TestLlmCache:
    def test_save_and_get(self, tmp_path):
        db_path = tmp_path / "test.db"
        analysis = _make_analysis()

        from src.strategy.llm_cache import get_cached_analysis, save_analysis

        save_analysis("nba-nyk-bos-2026-02-11", "2026-02-11", analysis, db_path=db_path)
        cached = get_cached_analysis("nba-nyk-bos-2026-02-11", db_path=db_path)

        assert cached is not None
        assert cached.favored_team == "Boston Celtics"
        assert cached.confidence == 0.85
        assert cached.sizing_modifier == 1.2
        assert cached.hedge_ratio == 0.4

    def test_cache_miss(self, tmp_path):
        db_path = tmp_path / "test.db"

        from src.strategy.llm_cache import get_cached_analysis

        cached = get_cached_analysis("nba-nonexistent", db_path=db_path)
        assert cached is None

    def test_get_or_analyze_cache_hit(self, tmp_path, monkeypatch):
        """get_or_analyze returns cached result without calling LLM."""
        db_path = tmp_path / "test.db"
        analysis = _make_analysis()

        from src.strategy.llm_cache import get_or_analyze, save_analysis

        save_analysis("nba-nyk-bos-2026-02-11", "2026-02-11", analysis, db_path=db_path)

        ctx = _make_context()
        # analyze_game_sync should NOT be called
        monkeypatch.setattr(
            "src.strategy.llm_analyzer.analyze_game_sync",
            lambda c: pytest.fail("Should not call LLM on cache hit"),
        )

        result = get_or_analyze(
            "nba-nyk-bos-2026-02-11", "2026-02-11", ctx, db_path=db_path
        )
        assert result is not None
        assert result.favored_team == "Boston Celtics"


# ---------------------------------------------------------------------------
# Prompt caching (Phase L-cache)
# ---------------------------------------------------------------------------


class TestPromptCaching:
    def test_shared_knowledge_base_length(self):
        """SHARED_KNOWLEDGE_BASE must be >= 4096 tokens for Opus 4.6 caching."""
        # Conservative estimate: 1 token ≈ 4 chars for English text.
        # Actual Anthropic tokenizer gives ~3.4 chars/token, so char/4 is
        # a safe lower bound. Minimum for Opus 4.6/4.5 is 4096 tokens.
        estimated_tokens = len(SHARED_KNOWLEDGE_BASE) / 4
        assert estimated_tokens >= 3500, (
            f"SHARED_KNOWLEDGE_BASE may be too short for Opus 4.6 caching: "
            f"~{estimated_tokens:.0f} tokens estimated (need >= 4096 actual)"
        )

    def test_shared_knowledge_base_no_calibration_leak(self):
        """Knowledge base must NOT contain calibration table or sizing parameters."""
        text_lower = SHARED_KNOWLEDGE_BASE.lower()
        for forbidden in ["calibration table", "sweet_spot", "kelly_fraction", "0.25 kelly"]:
            assert forbidden not in text_lower, (
                f"SHARED_KNOWLEDGE_BASE contains forbidden term: {forbidden}"
            )

    @pytest.mark.asyncio
    async def test_call_llm_uses_structured_system(self, monkeypatch):
        """_call_llm sends system as list of blocks with cache_control."""
        monkeypatch.setattr(
            "src.strategy.llm_analyzer.settings.anthropic_api_key", "test-key"
        )
        monkeypatch.setattr(
            "src.strategy.llm_analyzer.settings.llm_model", "claude-haiku-4-5-20251001"
        )
        monkeypatch.setattr(
            "src.strategy.llm_analyzer.settings.llm_timeout_sec", 30
        )

        # Mock the Anthropic client
        mock_response = MagicMock()
        mock_response.content = [MagicMock(text='{"result": "ok"}')]
        mock_response.usage = MagicMock(
            input_tokens=100,
            cache_read_input_tokens=80,
            cache_creation_input_tokens=0,
        )

        mock_create = AsyncMock(return_value=mock_response)

        mock_client = MagicMock()
        mock_client.messages.create = mock_create

        def mock_async_anthropic(**kwargs):
            return mock_client

        monkeypatch.setattr(
            "anthropic.AsyncAnthropic", mock_async_anthropic
        )

        result = await _call_llm("persona instructions", "user question")

        assert result == '{"result": "ok"}'

        # Verify structured system message
        call_kwargs = mock_create.call_args.kwargs
        system_blocks = call_kwargs["system"]
        assert isinstance(system_blocks, list)
        assert len(system_blocks) == 2

        # First block: shared knowledge base with cache_control
        assert system_blocks[0]["type"] == "text"
        assert system_blocks[0]["text"] == SHARED_KNOWLEDGE_BASE
        assert system_blocks[0]["cache_control"] == {"type": "ephemeral"}

        # Second block: persona-specific instructions (no cache_control)
        assert system_blocks[1]["type"] == "text"
        assert system_blocks[1]["text"] == "persona instructions"
        assert "cache_control" not in system_blocks[1]


# ---------------------------------------------------------------------------
# Prompt formatting
# ---------------------------------------------------------------------------


class TestPromptFormatting:
    def test_format_game_context(self):
        from src.strategy.prompts.game_analysis import (
            POLYMARKET_SPECIALIST_USER,
            format_game_context,
        )

        ctx = _make_context()
        formatted = format_game_context(POLYMARKET_SPECIALIST_USER, ctx)
        assert "Boston Celtics" in formatted
        assert "New York Knicks" in formatted
        assert "42-15" in formatted
