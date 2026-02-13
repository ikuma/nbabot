"""Tests for DB-derived backtest inputs."""

from __future__ import annotations

from pathlib import Path

from src.store.db import (
    _connect,
    get_position_group_backtest_games,
    log_result,
    log_signal,
    update_order_status,
)


def _insert_directional_with_result(
    db_path: Path,
    *,
    event_slug: str,
    price: float,
    won: bool,
    created_at: str,
    settled_at: str,
) -> int:
    sid = log_signal(
        game_title="Game",
        event_slug=event_slug,
        team="Celtics",
        side="BUY",
        poly_price=price,
        book_prob=0.5,
        edge_pct=1.0,
        kelly_size=10.0,
        token_id=f"tok-dir-{event_slug}",
        signal_role="directional",
        db_path=db_path,
    )
    update_order_status(sid, f"oid-{sid}", "filled", fill_price=price, db_path=db_path)
    log_result(
        signal_id=sid,
        outcome="Celtics",
        won=won,
        settlement_price=1.0 if won else 0.0,
        pnl=1.0 if won else -1.0,
        db_path=db_path,
    )
    conn = _connect(db_path)
    conn.execute("UPDATE signals SET created_at = ? WHERE id = ?", (created_at, sid))
    conn.execute("UPDATE results SET settled_at = ? WHERE signal_id = ?", (settled_at, sid))
    conn.commit()
    conn.close()
    return sid


def _insert_hedge(
    db_path: Path,
    *,
    event_slug: str,
    price: float,
    created_at: str,
) -> int:
    sid = log_signal(
        game_title="Game",
        event_slug=event_slug,
        team="Knicks",
        side="BUY",
        poly_price=price,
        book_prob=0.5,
        edge_pct=1.0,
        kelly_size=10.0,
        token_id=f"tok-hedge-{event_slug}",
        signal_role="hedge",
        db_path=db_path,
    )
    update_order_status(sid, f"oid-{sid}", "filled", fill_price=price, db_path=db_path)
    conn = _connect(db_path)
    conn.execute("UPDATE signals SET created_at = ? WHERE id = ?", (created_at, sid))
    conn.commit()
    conn.close()
    return sid


def test_get_position_group_backtest_games_with_hedge(tmp_path: Path):
    db_path = tmp_path / "pg_backtest.db"
    event_slug = "nba-nyk-bos-2026-02-10"
    _insert_hedge(
        db_path,
        event_slug=event_slug,
        price=0.51,
        created_at="2026-02-10T00:10:00+00:00",
    )
    _insert_directional_with_result(
        db_path,
        event_slug=event_slug,
        price=0.46,
        won=True,
        created_at="2026-02-10T00:00:00+00:00",
        settled_at="2026-02-10T04:00:00+00:00",
    )

    rows = get_position_group_backtest_games(db_path=db_path)
    assert len(rows) == 1
    assert rows[0]["event_slug"] == event_slug
    assert rows[0]["directional_price"] == 0.46
    assert rows[0]["opposite_price"] == 0.51
    assert rows[0]["directional_won"] is True


def test_get_position_group_backtest_games_without_hedge(tmp_path: Path):
    db_path = tmp_path / "pg_backtest.db"
    _insert_directional_with_result(
        db_path,
        event_slug="nba-lal-gsw-2026-02-10",
        price=0.44,
        won=False,
        created_at="2026-02-10T00:00:00+00:00",
        settled_at="2026-02-10T04:00:00+00:00",
    )
    rows = get_position_group_backtest_games(db_path=db_path)
    assert len(rows) == 1
    assert rows[0]["opposite_price"] is None


def test_get_position_group_backtest_games_date_filter(tmp_path: Path):
    db_path = tmp_path / "pg_backtest.db"
    _insert_directional_with_result(
        db_path,
        event_slug="nba-a",
        price=0.45,
        won=True,
        created_at="2026-02-10T00:00:00+00:00",
        settled_at="2026-02-10T01:00:00+00:00",
    )
    _insert_directional_with_result(
        db_path,
        event_slug="nba-b",
        price=0.55,
        won=False,
        created_at="2026-02-11T00:00:00+00:00",
        settled_at="2026-02-11T01:00:00+00:00",
    )
    rows = get_position_group_backtest_games(
        db_path=db_path,
        start_at="2026-02-11T00:00:00+00:00",
        end_at="2026-02-12T00:00:00+00:00",
    )
    assert len(rows) == 1
    assert rows[0]["event_slug"] == "nba-b"
