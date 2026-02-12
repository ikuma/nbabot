from src.scheduler.pricing import apply_price_ceiling, below_market_price


def test_below_market_price_normal():
    assert below_market_price(0.50) == 0.49


def test_below_market_price_floor():
    assert below_market_price(0.01) == 0.01


def test_apply_price_ceiling_caps_high_price():
    assert apply_price_ceiling(0.30, 0.29) == 0.29


def test_apply_price_ceiling_keeps_lower_price():
    assert apply_price_ceiling(0.24, 0.29) == 0.24


def test_apply_price_ceiling_floor_when_ceiling_negative():
    assert apply_price_ceiling(0.24, -0.1) == 0.01

