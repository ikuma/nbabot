"""Continuous monotonic calibration curve with uncertainty quantification.

Replaces the discrete 5-cent band lookup with a smooth, monotone-increasing
price→win_rate function.  Uses:
  - PAVA (Pool Adjacent Violators) for isotonic regression
  - PCHIP interpolation for monotone smoothness between knots
  - Beta posterior (Jeffreys prior) for credible-interval lower bounds

Dependencies: scipy >= 1.12  (isotonic_regression, PchipInterpolator, beta)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import numpy as np
from scipy.interpolate import PchipInterpolator
from scipy.optimize import isotonic_regression
from scipy.stats import beta as beta_dist

from src.strategy.calibration import NBA_ML_CALIBRATION, CalibrationBand

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class WinRateEstimate:
    """Continuous win rate estimate with uncertainty."""

    price: float
    point_estimate: float  # Isotonic + PCHIP interpolated value
    lower_bound: float  # Beta posterior lower percentile (conservative)
    upper_bound: float  # Beta posterior upper percentile
    effective_sample_size: float  # nearest knot sample size (diagnostic)


class ContinuousCalibration:
    """Continuous monotonic price->win_rate function with uncertainty."""

    def __init__(
        self,
        knot_prices: list[float],
        knot_point_estimates: list[float],
        knot_lower_bounds: list[float],
        knot_upper_bounds: list[float],
        knot_sample_sizes: list[float],
        confidence_level: float = 0.90,
        train_start: str = "",
        train_end: str = "",
        n_observations: int = 0,
        price_range: tuple[float, float] | None = None,
    ):
        if len(knot_prices) < 2:
            raise ValueError("Need at least 2 knots for interpolation")

        self.knot_prices = list(knot_prices)
        self.knot_point_estimates = list(knot_point_estimates)
        self.knot_lower_bounds = list(knot_lower_bounds)
        self.knot_upper_bounds = list(knot_upper_bounds)
        self.knot_sample_sizes = list(knot_sample_sizes)
        self.confidence_level = confidence_level
        self.train_start = train_start
        self.train_end = train_end
        self.n_observations = n_observations

        # PCHIP 補間器 (単調性保持)
        self._point_interp = PchipInterpolator(knot_prices, knot_point_estimates)
        self._lower_interp = PchipInterpolator(knot_prices, knot_lower_bounds)
        self._upper_interp = PchipInterpolator(knot_prices, knot_upper_bounds)

        # 有効価格範囲 (バンド端を含む — ミッドポイントより広い)
        if price_range is not None:
            self._price_lo, self._price_hi = price_range
        else:
            self._price_lo = knot_prices[0]
            self._price_hi = knot_prices[-1]

    def estimate(self, price: float) -> WinRateEstimate | None:
        """Return continuous win rate estimate for a price.

        Returns None if the price falls outside the knot range.
        """
        if price < self._price_lo or price > self._price_hi:
            return None

        point = float(np.clip(self._point_interp(price), 0.0, 1.0))
        lower = float(np.clip(self._lower_interp(price), 0.0, 1.0))
        upper = float(np.clip(self._upper_interp(price), 0.0, 1.0))

        # 最近傍の knot からサンプルサイズ取得
        idx = int(np.argmin([abs(p - price) for p in self.knot_prices]))
        ess = self.knot_sample_sizes[idx]

        return WinRateEstimate(
            price=price,
            point_estimate=point,
            lower_bound=lower,
            upper_bound=upper,
            effective_sample_size=ess,
        )

    @classmethod
    def from_bands(
        cls,
        bands: list[CalibrationBand],
        confidence_level: float = 0.90,
    ) -> ContinuousCalibration:
        """Fit from CalibrationBand list (hardcoded table compatible)."""
        if len(bands) < 2:
            raise ValueError("Need at least 2 bands")

        midpoints = [(b.price_lo + b.price_hi) / 2 for b in bands]
        win_rates = [b.expected_win_rate for b in bands]
        sample_sizes = [b.sample_size for b in bands]

        knot_point, knot_lower, knot_upper = _fit_isotonic_beta(
            midpoints, win_rates, sample_sizes, confidence_level
        )

        n_obs = sum(b.sample_size for b in bands)
        # バンド端で有効範囲を設定 (ミッドポイントより広い)
        price_range = (bands[0].price_lo, bands[-1].price_hi)

        return cls(
            knot_prices=midpoints,
            knot_point_estimates=knot_point,
            knot_lower_bounds=knot_lower,
            knot_upper_bounds=knot_upper,
            knot_sample_sizes=[float(s) for s in sample_sizes],
            confidence_level=confidence_level,
            n_observations=n_obs,
            price_range=price_range,
        )

    @classmethod
    def from_conditions(
        cls,
        conditions: list[dict],
        confidence_level: float = 0.90,
        price_key: str = "avg_buy_price",
        train_start: str | None = None,
        train_end: str | None = None,
    ) -> ContinuousCalibration:
        """Fit directly from condition data (walk-forward compatible)."""
        # 日付フィルタ
        filtered = []
        for c in conditions:
            d = c.get("date", "")
            if not d:
                continue
            if train_start and d < train_start:
                continue
            if train_end and d >= train_end:
                continue
            if c.get("status") in ("WIN", "LOSS_OR_OPEN"):
                filtered.append(c)

        if not filtered:
            raise ValueError("No eligible conditions after filtering")

        # 5-cent bands で集約
        midpoints: list[float] = []
        win_rates: list[float] = []
        sample_sizes: list[int] = []

        for lo_int in range(20, 95, 5):
            lo = lo_int / 100
            hi = (lo_int + 5) / 100
            mid = (lo + hi) / 2

            band_conds = [c for c in filtered if lo <= c.get(price_key, 0) < hi]
            if not band_conds:
                continue

            wins = sum(1 for c in band_conds if c["status"] == "WIN")
            n = len(band_conds)

            midpoints.append(mid)
            win_rates.append(wins / n)
            sample_sizes.append(n)

        if len(midpoints) < 2:
            raise ValueError("Need at least 2 non-empty bands")

        knot_point, knot_lower, knot_upper = _fit_isotonic_beta(
            midpoints, win_rates, sample_sizes, confidence_level
        )

        dates = [c["date"] for c in filtered if c.get("date")]
        actual_start = min(dates) if dates else (train_start or "")
        actual_end = max(dates) if dates else (train_end or "")
        # バンド端で有効範囲 (midpoints - 0.025 ~ midpoints + 0.025)
        price_range = (midpoints[0] - 0.025, midpoints[-1] + 0.025)

        return cls(
            knot_prices=midpoints,
            knot_point_estimates=knot_point,
            knot_lower_bounds=knot_lower,
            knot_upper_bounds=knot_upper,
            knot_sample_sizes=[float(s) for s in sample_sizes],
            confidence_level=confidence_level,
            train_start=actual_start,
            train_end=actual_end,
            n_observations=len(filtered),
            price_range=price_range,
        )

    def to_dict(self) -> dict:
        """JSON serialization."""
        return {
            "knot_prices": self.knot_prices,
            "knot_point_estimates": self.knot_point_estimates,
            "knot_lower_bounds": self.knot_lower_bounds,
            "knot_upper_bounds": self.knot_upper_bounds,
            "knot_sample_sizes": self.knot_sample_sizes,
            "confidence_level": self.confidence_level,
            "train_start": self.train_start,
            "train_end": self.train_end,
            "n_observations": self.n_observations,
            "price_range": [self._price_lo, self._price_hi],
        }

    @classmethod
    def from_dict(cls, data: dict) -> ContinuousCalibration:
        """JSON deserialization."""
        pr = data.get("price_range")
        price_range = tuple(pr) if pr else None
        return cls(
            knot_prices=data["knot_prices"],
            knot_point_estimates=data["knot_point_estimates"],
            knot_lower_bounds=data["knot_lower_bounds"],
            knot_upper_bounds=data["knot_upper_bounds"],
            knot_sample_sizes=data["knot_sample_sizes"],
            confidence_level=data.get("confidence_level", 0.90),
            train_start=data.get("train_start", ""),
            train_end=data.get("train_end", ""),
            n_observations=data.get("n_observations", 0),
            price_range=price_range,
        )


def _fit_isotonic_beta(
    midpoints: list[float],
    win_rates: list[float],
    sample_sizes: list[int],
    confidence_level: float,
) -> tuple[list[float], list[float], list[float]]:
    """Core fitting: PAVA isotonic + Beta posterior bounds.

    Returns (point_estimates, lower_bounds, upper_bounds) as lists.
    """
    weights = np.array(sample_sizes, dtype=float)
    wr_arr = np.array(win_rates, dtype=float)

    # Step 1: PAVA 単調非減少化 (重みはサンプルサイズ)
    iso_result = isotonic_regression(wr_arr, weights=weights)
    monotone_wr = list(np.clip(iso_result.x, 0.0, 1.0))

    # Step 2: Beta 事後分布 (Jeffreys prior: alpha=0.5, beta=0.5)
    alpha_prior, beta_prior = 0.5, 0.5
    tail = (1 - confidence_level) / 2

    lower_raw: list[float] = []
    upper_raw: list[float] = []

    for wr, n in zip(win_rates, sample_sizes):
        wins = int(round(wr * n))
        losses = n - wins
        a = wins + alpha_prior
        b = losses + beta_prior
        lower_raw.append(float(beta_dist.ppf(tail, a, b)))
        upper_raw.append(float(beta_dist.ppf(1 - tail, a, b)))

    # Step 3: 下限・上限も PAVA で単調化
    lower_iso = isotonic_regression(
        np.array(lower_raw), weights=weights
    )
    lower_bounds = list(np.clip(lower_iso.x, 0.0, 1.0))

    upper_iso = isotonic_regression(
        np.array(upper_raw), weights=weights
    )
    upper_bounds = list(np.clip(upper_iso.x, 0.0, 1.0))

    return monotone_wr, lower_bounds, upper_bounds


def _confidence_from_sample_size(ess: float) -> str:
    """Derive confidence label from effective sample size."""
    if ess >= 100:
        return "high"
    elif ess >= 40:
        return "medium"
    return "low"


# --- デフォルトカーブ (遅延初期化 + キャッシュ) ---

_default_curve_cache: dict[float, ContinuousCalibration] = {}


def get_default_curve(confidence_level: float | None = None) -> ContinuousCalibration:
    """Build default curve from hardcoded table (cached per confidence_level)."""
    from src.config import settings

    cl = confidence_level if confidence_level is not None else settings.calibration_confidence_level

    if cl not in _default_curve_cache:
        _default_curve_cache[cl] = ContinuousCalibration.from_bands(
            NBA_ML_CALIBRATION, confidence_level=cl
        )
    return _default_curve_cache[cl]
