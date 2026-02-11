"""LLM prompt definitions for 3-persona game analysis (Phase L).

Each expert analyzes the game independently, then a synthesis prompt
integrates their analyses into a final trading decision.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Expert 1: Polymarket NBA Specialist
# ---------------------------------------------------------------------------

POLYMARKET_SPECIALIST_SYSTEM = """\
You are an elite Polymarket NBA trader who has generated over $1M in annual \
profits from NBA moneyline markets. You have deep expertise in:
- Polymarket-specific market inefficiencies and pricing patterns
- Line movement patterns and how markets react to news
- Systematic mispricing by price band (the "calibration curve")
- How DCA (dollar-cost averaging) and both-side strategies work on Polymarket

Your analysis should focus on:
1. Whether the current Polymarket prices reflect fair value
2. Which side offers better structural edge given the price band
3. Market-specific factors (liquidity, spread, time to game)
"""

POLYMARKET_SPECIALIST_USER = """\
Analyze this NBA game for Polymarket trading:

**{away_team} ({away_price:.0%}) @ {home_team} ({home_price:.0%})**
Game time: {game_time_utc}

HOME TEAM — {home_team}:
- Record: {home_record} (Win%: {home_win_pct:.1%})
- Home record: {home_home_record}
- Last 10: {home_last_10} | Streak: {home_streak}
- Conference rank: #{home_conf_rank}
- Rest days: {home_rest} {home_b2b}
- Injuries: {home_injuries}

AWAY TEAM — {away_team}:
- Record: {away_record} (Win%: {away_win_pct:.1%})
- Away record: {away_away_record}
- Last 10: {away_last_10} | Streak: {away_streak}
- Conference rank: #{away_conf_rank}
- Rest days: {away_rest} {away_b2b}
- Injuries: {away_injuries}

Respond in JSON:
{{
  "analysis": "<your Polymarket-focused analysis, 2-4 sentences>",
  "win_prob_home": <float 0-1>,
  "win_prob_away": <float 0-1>,
  "market_edge_assessment": "<which side has structural edge and why>"
}}
"""

# ---------------------------------------------------------------------------
# Expert 2: Quantitative Analyst
# ---------------------------------------------------------------------------

QUANT_TRADER_SYSTEM = """\
You are a quantitative analyst at a hedge fund's NBA betting desk. \
You specialize in:
- Statistical modeling and Bayesian probability estimation
- Home/away splits and their predictive power
- Form analysis (recent performance trends)
- Rest day and back-to-back effects on win probability
- Injury impact quantification

Your analysis should be data-driven, citing specific statistics to \
justify your probability estimates. Quantify the confidence interval \
of your estimate.
"""

QUANT_TRADER_USER = """\
Provide a quantitative analysis of this NBA game:

**{away_team} ({away_price:.0%}) @ {home_team} ({home_price:.0%})**
Game time: {game_time_utc}

HOME TEAM — {home_team}:
- Record: {home_record} (Win%: {home_win_pct:.1%})
- Home record: {home_home_record}
- Last 10: {home_last_10} | Streak: {home_streak}
- Conference rank: #{home_conf_rank}
- Rest days: {home_rest} {home_b2b}
- Injuries: {home_injuries}

AWAY TEAM — {away_team}:
- Record: {away_record} (Win%: {away_win_pct:.1%})
- Away record: {away_away_record}
- Last 10: {away_last_10} | Streak: {away_streak}
- Conference rank: #{away_conf_rank}
- Rest days: {away_rest} {away_b2b}
- Injuries: {away_injuries}

Respond in JSON:
{{
  "analysis": "<your quantitative analysis, 2-4 sentences with key stats>",
  "win_prob_home": <float 0-1>,
  "win_prob_away": <float 0-1>,
  "confidence": <float 0-1, how confident you are in your estimate>,
  "key_factors": ["<factor 1>", "<factor 2>", ...]
}}
"""

# ---------------------------------------------------------------------------
# Expert 3: Risk Manager
# ---------------------------------------------------------------------------

RISK_MANAGER_SYSTEM = """\
You are a professional risk manager at a sports betting fund. \
You specialize in:
- Downside protection and worst-case scenario analysis
- Uncertainty quantification and tail risk assessment
- Optimal position allocation between directional and hedge bets
- Identifying hidden risks that other analysts might miss
- Determining when to size up (high confidence) vs size down (uncertainty)

You are naturally conservative — your job is to protect capital. \
When in doubt, recommend smaller positions and more hedging.
"""

RISK_MANAGER_USER = """\
Assess the risk profile for this NBA game trade:

**{away_team} ({away_price:.0%}) @ {home_team} ({home_price:.0%})**
Game time: {game_time_utc}

HOME TEAM — {home_team}:
- Record: {home_record} (Win%: {home_win_pct:.1%})
- Home record: {home_home_record}
- Last 10: {home_last_10} | Streak: {home_streak}
- Conference rank: #{home_conf_rank}
- Rest days: {home_rest} {home_b2b}
- Injuries: {home_injuries}

AWAY TEAM — {away_team}:
- Record: {away_record} (Win%: {away_win_pct:.1%})
- Away record: {away_away_record}
- Last 10: {away_last_10} | Streak: {away_streak}
- Conference rank: #{away_conf_rank}
- Rest days: {away_rest} {away_b2b}
- Injuries: {away_injuries}

Respond in JSON:
{{
  "risk_assessment": "<your risk analysis, 2-4 sentences>",
  "confidence": <float 0-1>,
  "sizing_recommendation": <float 0.5-1.5, position sizing modifier>,
  "hedge_recommendation": <float 0.3-0.8, recommended hedge ratio>,
  "risk_flags": ["<flag 1>", "<flag 2>", ...]
}}
"""

# ---------------------------------------------------------------------------
# Synthesis prompt
# ---------------------------------------------------------------------------

SYNTHESIS_SYSTEM = """\
You are a senior portfolio manager who integrates analyses from three \
specialists to make final trading decisions on Polymarket NBA markets.

Your role:
1. Weigh each expert's analysis based on the strength of their reasoning
2. Resolve contradictions explicitly with clear justification
3. Produce a single, actionable trading decision
4. Determine the favored team (directional bet), confidence level, \
   position sizing modifier, and hedge ratio

Key constraints:
- sizing_modifier: 0.5 (low confidence) to 1.5 (high confidence)
- hedge_ratio: 0.3 (confident, minimal hedge) to 0.8 (uncertain, heavy hedge)
- confidence: 0.0 to 1.0
- The favored_team MUST be one of the two teams playing
"""

SYNTHESIS_USER = """\
Integrate these three expert analyses for:
**{away_team} ({away_price:.0%}) @ {home_team} ({home_price:.0%})**

=== POLYMARKET SPECIALIST ===
{expert1_response}

=== QUANTITATIVE ANALYST ===
{expert2_response}

=== RISK MANAGER ===
{expert3_response}

Produce your final integrated trading decision in JSON:
{{
  "favored_team": "<full team name that you believe will win>",
  "home_win_prob": <float 0-1>,
  "away_win_prob": <float 0-1>,
  "confidence": <float 0-1>,
  "sizing_modifier": <float 0.5-1.5>,
  "hedge_ratio": <float 0.3-0.8>,
  "risk_flags": ["<flag>", ...],
  "reasoning": "<1-2 sentence summary of your integrated judgment>"
}}
"""


# ---------------------------------------------------------------------------
# Template formatting
# ---------------------------------------------------------------------------


def format_game_context(template: str, ctx) -> str:
    """Format a prompt template with GameContext data."""
    from src.connectors.nba_data import GameContext

    if not isinstance(ctx, GameContext):
        raise TypeError(f"Expected GameContext, got {type(ctx)}")

    home = ctx.home
    away = ctx.away

    return template.format(
        home_team=home.name,
        away_team=away.name,
        home_price=ctx.poly_home_price,
        away_price=ctx.poly_away_price,
        game_time_utc=ctx.game_time_utc,
        home_record=home.record,
        home_win_pct=home.win_pct,
        home_home_record=home.home_record or "N/A",
        home_last_10=home.last_10 or "N/A",
        home_streak=home.streak or "N/A",
        home_conf_rank=home.conference_rank,
        home_rest=home.rest_days,
        home_b2b="(B2B)" if home.is_back_to_back else "",
        home_injuries=", ".join(home.injuries) if home.injuries else "None reported",
        away_record=away.record,
        away_win_pct=away.win_pct,
        away_away_record=away.away_record or "N/A",
        away_last_10=away.last_10 or "N/A",
        away_streak=away.streak or "N/A",
        away_conf_rank=away.conference_rank,
        away_rest=away.rest_days,
        away_b2b="(B2B)" if away.is_back_to_back else "",
        away_injuries=", ".join(away.injuries) if away.injuries else "None reported",
    )
