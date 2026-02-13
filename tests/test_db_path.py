"""Tests for execution-mode DB path resolution."""

from __future__ import annotations

from pathlib import Path

from src.store.db_path import resolve_db_path


def test_resolve_db_path_uses_paper_for_paper_mode(monkeypatch):
    monkeypatch.setattr("src.store.db_path.settings.paper_db_path", "data/paper_a.db")
    out = resolve_db_path(execution_mode="paper")
    assert out.endswith("data/paper_a.db")
    assert Path(out).is_absolute()


def test_resolve_db_path_uses_live_for_live_mode(monkeypatch):
    monkeypatch.setattr("src.store.db_path.settings.live_db_path", "data/live_a.db")
    out = resolve_db_path(execution_mode="live")
    assert out.endswith("data/live_a.db")
    assert Path(out).is_absolute()


def test_resolve_db_path_dry_run_falls_back_to_paper(monkeypatch):
    monkeypatch.setattr("src.store.db_path.settings.paper_db_path", "data/paper_x.db")
    monkeypatch.setattr("src.store.db_path.settings.dry_run_db_path", "")
    out = resolve_db_path(execution_mode="dry-run")
    assert out.endswith("data/paper_x.db")


def test_resolve_db_path_prefers_explicit(monkeypatch):
    monkeypatch.setattr("src.store.db_path.settings.live_db_path", "data/live_x.db")
    out = resolve_db_path(execution_mode="live", explicit_db_path="/tmp/custom.db")
    assert out == "/tmp/custom.db"
