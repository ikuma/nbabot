"""Tests for runtime hedge ratio resolution."""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.strategy.hedge_ratio_runtime import resolve_hedge_kelly_mult


def test_static_mode_returns_default(monkeypatch):
    monkeypatch.setattr(
        "src.strategy.hedge_ratio_runtime.settings.bothside_hedge_ratio_mode",
        "static",
    )
    assert resolve_hedge_kelly_mult(0.55) == 0.55


def test_optimized_mode_reads_file(monkeypatch, tmp_path: Path):
    p = tmp_path / "hedge_ratio.json"
    p.write_text(json.dumps({"best_ratio": 0.62}))
    monkeypatch.setattr(
        "src.strategy.hedge_ratio_runtime.settings.bothside_hedge_ratio_mode",
        "optimized",
    )
    monkeypatch.setattr(
        "src.strategy.hedge_ratio_runtime.settings.bothside_hedge_ratio_file",
        str(p),
    )
    assert resolve_hedge_kelly_mult(0.50) == 0.62


def test_optimized_mode_fallback_on_missing(monkeypatch, tmp_path: Path):
    p = tmp_path / "missing.json"
    monkeypatch.setattr(
        "src.strategy.hedge_ratio_runtime.settings.bothside_hedge_ratio_mode",
        "optimized",
    )
    monkeypatch.setattr(
        "src.strategy.hedge_ratio_runtime.settings.bothside_hedge_ratio_file",
        str(p),
    )
    assert resolve_hedge_kelly_mult(0.50) == 0.50


def test_clamped_range(monkeypatch, tmp_path: Path):
    p = tmp_path / "hedge_ratio.json"
    p.write_text(json.dumps({"best_ratio": 1.2}))  # out of range
    monkeypatch.setattr(
        "src.strategy.hedge_ratio_runtime.settings.bothside_hedge_ratio_mode",
        "optimized",
    )
    monkeypatch.setattr(
        "src.strategy.hedge_ratio_runtime.settings.bothside_hedge_ratio_file",
        str(p),
    )
    assert resolve_hedge_kelly_mult(0.50) == 0.8
