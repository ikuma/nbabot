"""Tests for both-side scheduling in trade_scheduler.py (Phase B)."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.scheduler.hedge_executor import (
    _schedule_hedge_job,
    process_hedge_job,
)
from src.store.db import (
    TradeJob,
    _connect,
    get_hedge_job_for_slug,
    log_signal,
    upsert_trade_job,
)
from src.strategy.calibration_scanner import BothsideOpportunity, CalibrationOpportunity


@pytest.fixture()
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "test_sched.db"


def _insert_job(db_path: Path, **overrides) -> int:
    defaults = {
        "game_date": "2026-02-10",
        "event_slug": "nba-nyk-bos-2026-02-10",
        "home_team": "Boston Celtics",
        "away_team": "New York Knicks",
        "game_time_utc": "2026-02-11T01:00:00+00:00",
        "execute_after": "2026-02-10T17:00:00+00:00",
        "execute_before": "2026-02-11T01:00:00+00:00",
        "db_path": db_path,
    }
    defaults.update(overrides)
    upsert_trade_job(**defaults)
    conn = _connect(db_path)
    job_id = conn.execute(
        "SELECT id FROM trade_jobs WHERE event_slug = ? AND job_side = ?",
        (defaults["event_slug"], defaults.get("job_side", "directional")),
    ).fetchone()[0]
    conn.close()
    return job_id


def _make_opp(
    outcome: str = "Celtics",
    price: float = 0.35,
    ev: float = 1.5,
    position_usd: float = 25.0,
    token_id: str = "tok_0",
) -> CalibrationOpportunity:
    return CalibrationOpportunity(
        event_slug="nba-nyk-bos-2026-02-10",
        event_title="Knicks vs Celtics",
        market_type="moneyline",
        outcome_name=outcome,
        token_id=token_id,
        poly_price=price,
        calibration_edge_pct=50.0,
        expected_win_rate=0.90,
        ev_per_dollar=ev,
        price_band="0.35-0.40",
        in_sweet_spot=True,
        band_confidence="high",
        position_usd=position_usd,
    )


class TestScheduleHedgeJob:
    def test_hedge_job_created_after_directional(self, db_path):
        """After directional job, a hedge job should be created as pending."""
        job_id = _insert_job(db_path)

        from src.store.db import TradeJob

        conn = _connect(db_path)
        row = conn.execute("SELECT * FROM trade_jobs WHERE id = ?", (job_id,)).fetchone()
        conn.close()
        dir_job = TradeJob(**dict(row))

        dir_opp = _make_opp("Celtics", 0.35, 1.5, 25.0, "tok_dir")
        hedge_opp = _make_opp("Knicks", 0.50, 0.5, 12.0, "tok_hedge")
        bothside = BothsideOpportunity(
            directional=dir_opp,
            hedge=hedge_opp,
            combined_price=0.85,
            hedge_position_usd=6.0,
        )

        with patch("src.scheduler.hedge_executor.settings") as mock_settings:
            mock_settings.bothside_hedge_delay_min = 30

            _schedule_hedge_job(dir_job, bothside, str(db_path))

        hedge = get_hedge_job_for_slug("nba-nyk-bos-2026-02-10", db_path=db_path)
        assert hedge is not None
        assert hedge.job_side == "hedge"
        assert hedge.status == "pending"
        assert hedge.paired_job_id == job_id
        assert hedge.bothside_group_id is not None

    def test_no_hedge_when_disabled(self, db_path, monkeypatch):
        """When bothside_enabled=False, no hedge job should be created."""
        monkeypatch.setattr("src.scheduler.hedge_executor.settings.bothside_enabled", False)

        # just verify no crash when bothside is disabled
        hedge = get_hedge_job_for_slug("nba-nyk-bos-2026-02-10", db_path=db_path)
        assert hedge is None

    def test_hedge_respects_delay(self, db_path):
        """Hedge job's execute_after should reflect the delay setting."""
        job_id = _insert_job(db_path)

        from src.store.db import TradeJob

        conn = _connect(db_path)
        row = conn.execute("SELECT * FROM trade_jobs WHERE id = ?", (job_id,)).fetchone()
        conn.close()
        dir_job = TradeJob(**dict(row))

        hedge_opp = _make_opp("Knicks", 0.50, 0.5, 12.0, "tok_hedge")
        bothside = BothsideOpportunity(
            directional=_make_opp(),
            hedge=hedge_opp,
            combined_price=0.85,
            hedge_position_usd=6.0,
        )

        with patch("src.scheduler.hedge_executor.settings") as mock_settings:
            mock_settings.bothside_hedge_delay_min = 60  # 60 min delay

            _schedule_hedge_job(dir_job, bothside, str(db_path))

        hedge = get_hedge_job_for_slug("nba-nyk-bos-2026-02-10", db_path=db_path)
        assert hedge is not None
        # execute_after should be ~now + 60min (delay from current time)
        from datetime import datetime, timedelta, timezone

        hedge_after = datetime.fromisoformat(hedge.execute_after.replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        # hedge_after should be approximately now + 60 min (within 5 min tolerance)
        expected_after = now + timedelta(minutes=60)
        diff = abs((hedge_after - expected_after).total_seconds())
        assert diff < 300  # within 5 minutes


class TestHedgeIndependentDca:
    def test_hedge_independent_dca(self, db_path):
        """Hedge job should get its own dca_group_id distinct from directional."""
        job_id = _insert_job(db_path)

        from src.store.db import TradeJob

        conn = _connect(db_path)
        row = conn.execute("SELECT * FROM trade_jobs WHERE id = ?", (job_id,)).fetchone()
        conn.close()
        dir_job = TradeJob(**dict(row))

        bothside = BothsideOpportunity(
            directional=_make_opp("Celtics", 0.35),
            hedge=_make_opp("Knicks", 0.50),
            combined_price=0.85,
            hedge_position_usd=6.0,
        )

        with patch("src.scheduler.hedge_executor.settings") as mock_settings:
            mock_settings.bothside_hedge_delay_min = 30

            _schedule_hedge_job(dir_job, bothside, str(db_path))

        hedge = get_hedge_job_for_slug("nba-nyk-bos-2026-02-10", db_path=db_path)
        assert hedge is not None
        # hedge job has its own dca_group_id (None at creation, set on execution)
        # bothside_group_id links both jobs
        assert hedge.bothside_group_id is not None

        # directional also gets the bothside_group_id
        conn = _connect(db_path)
        dir_row = conn.execute(
            "SELECT bothside_group_id FROM trade_jobs WHERE id = ?", (job_id,)
        ).fetchone()
        conn.close()
        assert dir_row[0] == hedge.bothside_group_id


def _make_moneyline(
    outcomes=("Knicks", "Celtics"),
    prices=(0.855, 0.14),
    token_ids=("tok_knicks", "tok_celtics"),
    slug="nba-nyk-bos-2026-02-10",
):
    """Create a MoneylineMarket for hedge tests."""
    from src.connectors.polymarket import MoneylineMarket

    return MoneylineMarket(
        condition_id="cond_test",
        event_slug=slug,
        event_title="Knicks vs Celtics",
        home_team="Boston Celtics",
        away_team="New York Knicks",
        outcomes=list(outcomes),
        prices=list(prices),
        token_ids=list(token_ids),
        sports_market_type="moneyline",
        active=True,
    )


def _create_hedge_job(
    db_path: Path, dir_price: float = 0.855, dir_team: str = "Knicks"
) -> tuple[TradeJob, int]:
    """Insert directional + hedge job pair and return (hedge TradeJob, dir_signal_id)."""
    # directional ジョブ作成
    dir_job_id = _insert_job(db_path)

    # directional シグナル作成
    dir_signal_id = log_signal(
        game_title="Knicks vs Celtics",
        event_slug="nba-nyk-bos-2026-02-10",
        team=dir_team,
        side="BUY",
        poly_price=dir_price,
        book_prob=0.0,
        edge_pct=5.0,
        kelly_size=25.0,
        token_id="tok_knicks",
        strategy_mode="calibration",
        signal_role="directional",
        dca_group_id="dca_dir_001",
        dca_sequence=1,
        db_path=db_path,
    )

    # directional ジョブに dca_group_id をセット
    conn = _connect(db_path)
    conn.execute(
        "UPDATE trade_jobs SET dca_group_id = ? WHERE id = ?", ("dca_dir_001", dir_job_id)
    )
    conn.commit()
    conn.close()

    # hedge ジョブ作成
    _insert_job(db_path, job_side="hedge", event_slug="nba-nyk-bos-2026-02-10")
    conn = _connect(db_path)
    row = conn.execute(
        "SELECT * FROM trade_jobs WHERE event_slug = ? AND job_side = 'hedge'",
        ("nba-nyk-bos-2026-02-10",),
    ).fetchone()
    conn.execute(
        "UPDATE trade_jobs SET paired_job_id = ?, bothside_group_id = ? WHERE id = ?",
        (dir_job_id, "bs_group_001", row["id"]),
    )
    conn.commit()
    row = conn.execute(
        "SELECT * FROM trade_jobs WHERE id = ?", (row["id"],)
    ).fetchone()
    conn.close()

    hedge_job = TradeJob(**dict(row))
    return hedge_job, dir_signal_id


class TestHedgeMergeOnlyOutOfRange:
    """校正カーブ域外 (price < 0.20) でも MERGE-only パスで hedge が実行されることを検証."""

    @patch("src.scheduler.hedge_executor._compute_hedge_order_price")
    @patch("src.strategy.calibration_curve.get_default_curve")
    @patch("src.scheduler.hedge_executor._preflight_check")
    @patch("src.scheduler.hedge_executor.settings")
    def test_out_of_range_hedge_reaches_merge_only_path(
        self,
        mock_settings,
        mock_preflight,
        mock_curve_factory,
        mock_order_price,
        db_path,
    ):
        """price=0.14 → curve.estimate()=None → MERGE-only path → dca_active."""
        # Settings
        mock_settings.bothside_target_combined = 0.97
        mock_settings.bothside_target_combined_max = 0.998
        mock_settings.bothside_target_combined_min = 0.94
        mock_settings.bothside_target_mode = "dynamic"
        mock_settings.bothside_max_combined_vwap = 0.998
        mock_settings.bothside_dynamic_estimated_fee_usd = 0.05
        mock_settings.merge_min_profit_usd = 0.10
        mock_settings.merge_est_gas_usd = 0.05
        mock_settings.merge_min_shares_floor = 20.0
        mock_settings.bothside_hedge_kelly_mult = 0.5
        mock_settings.dca_max_entries = 3
        mock_settings.max_position_usd = 100.0
        mock_settings.capital_risk_pct = 2.0
        mock_settings.sweet_spot_lo = 0.20
        mock_settings.sweet_spot_hi = 0.55
        mock_settings.llm_analysis_enabled = False

        # 注文板 → order_price=0.13 (below market)
        mock_order_price.return_value = (0.14, 0.13)

        # 校正カーブが None を返す (域外)
        mock_curve = MagicMock()
        mock_curve.estimate.return_value = None
        mock_curve_factory.return_value = mock_curve

        mock_preflight.return_value = True

        hedge_job, _ = _create_hedge_job(db_path)

        # Moneyline: Knicks @ 0.855, Celtics @ 0.14
        ml = _make_moneyline()

        mock_log_signal = MagicMock(return_value=999)
        mock_place = MagicMock(return_value={"orderID": "ord_hedge_001"})
        mock_update_order = MagicMock()

        result = process_hedge_job(
            job=hedge_job,
            execution_mode="paper",
            db_path=str(db_path),
            fetch_moneyline_for_game=lambda a, h, d: ml,
            log_signal=mock_log_signal,
            place_limit_buy=mock_place,
            update_order_status=mock_update_order,
        )

        # skipped ではなく executed or dca_active
        assert result.status != "skipped", (
            f"MERGE-only path should not skip, got: {result.status}"
        )

        # log_signal が呼ばれている
        mock_log_signal.assert_called_once()
        call_kwargs = mock_log_signal.call_args[1]

        # 校正域外のメタデータ
        assert call_kwargs["band_confidence"] == "none"
        assert call_kwargs["edge_pct"] == 0.0
        assert call_kwargs["expected_win_rate"] == 0.0
        assert call_kwargs["signal_role"] == "hedge"

    @patch("src.scheduler.hedge_executor._compute_hedge_order_price")
    @patch("src.strategy.calibration_curve.get_default_curve")
    @patch("src.scheduler.hedge_executor.settings")
    def test_out_of_range_uses_margin_multiplier_sizing(
        self,
        mock_settings,
        mock_curve_factory,
        mock_order_price,
        db_path,
    ):
        """MERGE-only path should use _hedge_margin_multiplier for sizing."""
        mock_settings.bothside_target_combined = 0.97
        mock_settings.bothside_target_combined_max = 0.998
        mock_settings.bothside_target_combined_min = 0.94
        mock_settings.bothside_target_mode = "dynamic"
        mock_settings.bothside_max_combined_vwap = 0.998
        mock_settings.bothside_dynamic_estimated_fee_usd = 0.05
        mock_settings.merge_min_profit_usd = 0.10
        mock_settings.merge_est_gas_usd = 0.05
        mock_settings.merge_min_shares_floor = 20.0
        mock_settings.bothside_hedge_kelly_mult = 0.5
        mock_settings.dca_max_entries = 3
        mock_settings.max_position_usd = 100.0
        mock_settings.capital_risk_pct = 2.0
        mock_settings.sweet_spot_lo = 0.20
        mock_settings.sweet_spot_hi = 0.55
        mock_settings.llm_analysis_enabled = False

        # order_price=0.13, combined=0.855+0.13=0.985, margin=0.015
        mock_order_price.return_value = (0.14, 0.13)

        mock_curve = MagicMock()
        mock_curve.estimate.return_value = None
        mock_curve_factory.return_value = mock_curve

        hedge_job, _ = _create_hedge_job(db_path)
        ml = _make_moneyline()

        mock_log_signal = MagicMock(return_value=888)
        mock_place = MagicMock()
        mock_update_order = MagicMock()

        result = process_hedge_job(
            job=hedge_job,
            execution_mode="paper",
            db_path=str(db_path),
            fetch_moneyline_for_game=lambda a, h, d: ml,
            log_signal=mock_log_signal,
            place_limit_buy=mock_place,
            update_order_status=mock_update_order,
        )

        assert result.status != "skipped"

        # kelly_size が MERGE-only sizing を使用していることを確認
        # dir_total_cost=25.0, margin=1.0-0.985=0.015, mult=max(0.3,0.015*15)=0.3
        # kelly_usd=min(25.0*0.3, 100.0)=7.5
        call_kwargs = mock_log_signal.call_args[1]
        kelly_size = call_kwargs["kelly_size"]
        assert kelly_size > 0, "Hedge should have positive sizing via margin multiplier"

    @patch("src.scheduler.hedge_executor._compute_hedge_order_price")
    @patch("src.strategy.calibration_curve.get_default_curve")
    @patch("src.scheduler.hedge_executor.settings")
    def test_in_range_negative_ev_still_reaches_merge_only(
        self,
        mock_settings,
        mock_curve_factory,
        mock_order_price,
        db_path,
    ):
        """price in range but EV <= 0 → MERGE-only path (既存動作の回帰テスト)."""
        mock_settings.bothside_target_combined = 0.97
        mock_settings.bothside_target_combined_max = 0.998
        mock_settings.bothside_target_combined_min = 0.94
        mock_settings.bothside_target_mode = "dynamic"
        mock_settings.bothside_max_combined_vwap = 0.998
        mock_settings.bothside_dynamic_estimated_fee_usd = 0.05
        mock_settings.merge_min_profit_usd = 0.10
        mock_settings.merge_est_gas_usd = 0.05
        mock_settings.merge_min_shares_floor = 20.0
        mock_settings.bothside_hedge_kelly_mult = 0.5
        mock_settings.dca_max_entries = 3
        mock_settings.max_position_usd = 100.0
        mock_settings.capital_risk_pct = 2.0
        mock_settings.sweet_spot_lo = 0.20
        mock_settings.sweet_spot_hi = 0.55
        mock_settings.llm_analysis_enabled = False

        # hedge price=0.45, order_price=0.44
        mock_order_price.return_value = (0.45, 0.44)

        # カーブ推定あり、ただし lower_bound < order_price → EV <= 0
        mock_est = MagicMock()
        mock_est.lower_bound = 0.40  # < 0.44 → negative EV
        mock_est.effective_sample_size = 50
        mock_curve = MagicMock()
        mock_curve.estimate.return_value = mock_est
        mock_curve_factory.return_value = mock_curve

        # dir_price=0.50 なので combined=0.50+0.44=0.94 < 0.998
        hedge_job, _ = _create_hedge_job(db_path, dir_price=0.50)
        ml = _make_moneyline(prices=(0.50, 0.45), token_ids=("tok_knicks", "tok_celtics"))

        mock_log_signal = MagicMock(return_value=777)

        result = process_hedge_job(
            job=hedge_job,
            execution_mode="paper",
            db_path=str(db_path),
            fetch_moneyline_for_game=lambda a, h, d: ml,
            log_signal=mock_log_signal,
            place_limit_buy=MagicMock(),
            update_order_status=MagicMock(),
        )

        assert result.status != "skipped"
        call_kwargs = mock_log_signal.call_args[1]
        # est が存在するので band_confidence は "none" ではない
        assert call_kwargs["band_confidence"] != "none"
        assert call_kwargs["expected_win_rate"] == 0.40
