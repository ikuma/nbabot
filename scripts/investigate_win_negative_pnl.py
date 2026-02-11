"""Investigate WIN conditions with negative P&L in lhtsports data.

Reads condition_pnl.json and lhtsports_all_trades.json to analyze
why some conditions marked as WIN have negative P&L.

Output: JSON to stdout (no files written).
"""
from __future__ import annotations

import json
import sys
from collections import defaultdict
from pathlib import Path

CONDITION_PNL_PATH = Path("/Users/taro/dev/nbabot/data/reports/lhtsports-pnl/condition_pnl.json")
ALL_TRADES_PATH = Path("/Users/taro/dev/nbabot/data/reports/lhtsports-analysis/lhtsports_all_trades.json")


def main() -> None:
    # Load data
    with open(CONDITION_PNL_PATH) as f:
        conditions = json.load(f)
    with open(ALL_TRADES_PATH) as f:
        all_trades = json.load(f)

    # Build trade index by conditionId
    trades_by_cid: dict[str, list[dict]] = defaultdict(list)
    for t in all_trades:
        cid = t.get("conditionId", "")
        if cid:
            trades_by_cid[cid].append(t)

    # =========================================================================
    # PART 1: All conditions with net_cost > 50000
    # =========================================================================
    high_cost_conditions = [
        c for c in conditions if c.get("net_cost", 0) > 50000
    ]
    high_cost_conditions.sort(key=lambda x: x["net_cost"], reverse=True)

    high_cost_summary = []
    for c in high_cost_conditions:
        high_cost_summary.append({
            "conditionId": c["conditionId"],
            "slug": c["slug"],
            "status": c["status"],
            "buy_cost": round(c["buy_cost"], 2),
            "buy_shares": round(c["buy_shares"], 2),
            "redeem_usdc": round(c["redeem_usdc"], 2),
            "merge_usdc": round(c["merge_usdc"], 2),
            "sell_proceeds": round(c["sell_proceeds"], 2),
            "net_cost": round(c["net_cost"], 2),
            "total_payout": round(c["total_payout"], 2),
            "pnl": round(c["pnl"], 2),
            "roi_pct": round(c["roi_pct"], 2),
            "outcome_bought": c["outcome_bought"],
            "avg_buy_price": round(c["avg_buy_price"], 4),
        })

    # =========================================================================
    # PART 2: WIN conditions with negative P&L
    # =========================================================================
    win_negative_pnl = [
        c for c in conditions
        if c.get("status") == "WIN" and c.get("pnl", 0) < 0
    ]
    win_negative_pnl.sort(key=lambda x: x["pnl"])  # most negative first

    # For the top 5 most negative, do deep trade analysis
    top5_negative = win_negative_pnl[:5]

    detailed_analysis = []
    for c in top5_negative:
        cid = c["conditionId"]
        trades = trades_by_cid.get(cid, [])

        # Group trades by outcome
        by_outcome: dict[str, dict] = defaultdict(lambda: {
            "buy_count": 0,
            "sell_count": 0,
            "buy_usdc_total": 0.0,
            "sell_usdc_total": 0.0,
            "buy_shares_total": 0.0,
            "sell_shares_total": 0.0,
            "buy_prices": [],
            "sell_prices": [],
            "outcome_indices": set(),
        })

        for t in trades:
            outcome = t.get("outcome", "UNKNOWN")
            side = t.get("side", "")
            usdc = float(t.get("usdcSize", 0))
            shares = float(t.get("size", 0))
            price = float(t.get("price", 0))
            oi = t.get("outcomeIndex")

            rec = by_outcome[outcome]
            rec["outcome_indices"].add(oi)

            if side == "BUY":
                rec["buy_count"] += 1
                rec["buy_usdc_total"] += usdc
                rec["buy_shares_total"] += shares
                rec["buy_prices"].append(price)
            elif side == "SELL":
                rec["sell_count"] += 1
                rec["sell_usdc_total"] += usdc
                rec["sell_shares_total"] += shares
                rec["sell_prices"].append(price)

        outcome_details = {}
        for outcome, rec in by_outcome.items():
            outcome_details[outcome] = {
                "outcome_indices": sorted(rec["outcome_indices"]),
                "buy_count": rec["buy_count"],
                "sell_count": rec["sell_count"],
                "buy_usdc_total": round(rec["buy_usdc_total"], 2),
                "sell_usdc_total": round(rec["sell_usdc_total"], 2),
                "buy_shares_total": round(rec["buy_shares_total"], 2),
                "sell_shares_total": round(rec["sell_shares_total"], 2),
                "buy_price_min": round(min(rec["buy_prices"]), 6) if rec["buy_prices"] else None,
                "buy_price_max": round(max(rec["buy_prices"]), 6) if rec["buy_prices"] else None,
                "buy_price_avg": round(
                    sum(rec["buy_prices"]) / len(rec["buy_prices"]), 6
                ) if rec["buy_prices"] else None,
                "sell_price_min": round(min(rec["sell_prices"]), 6) if rec["sell_prices"] else None,
                "sell_price_max": round(max(rec["sell_prices"]), 6) if rec["sell_prices"] else None,
            }

        # Compute total buy cost and total sell proceeds from raw trades
        total_buy_from_trades = sum(
            float(t["usdcSize"]) for t in trades if t.get("side") == "BUY"
        )
        total_sell_from_trades = sum(
            float(t["usdcSize"]) for t in trades if t.get("side") == "SELL"
        )
        total_buy_shares_from_trades = sum(
            float(t["size"]) for t in trades if t.get("side") == "BUY"
        )

        # Determine which outcome won (redeemed)
        # The winning outcome's shares redeem at $1 each
        # If redeem_usdc > 0, that's the payout for the winning outcome shares
        num_outcomes_traded = len(by_outcome)
        all_outcomes = list(by_outcome.keys())

        # Check: did they buy both sides?
        bought_both_sides = sum(
            1 for rec in by_outcome.values() if rec["buy_count"] > 0
        ) > 1

        # Determine the winning outcome
        # redeem_usdc = number of shares of winning outcome held at settlement
        # If they bought both sides, only winning side shares redeem
        winning_outcome = None
        losing_outcome_cost = 0.0

        if bought_both_sides and len(all_outcomes) == 2:
            # The redeem amount tells us which side won
            # redeem = shares of winning outcome that were NOT merged
            # The trader loses the cost of the losing side's shares
            for outcome, rec in by_outcome.items():
                net_shares = rec["buy_shares_total"] - rec["sell_shares_total"]
                if abs(net_shares - c["redeem_usdc"]) < 1.0 or (
                    c["redeem_usdc"] > 0 and net_shares > 0
                ):
                    pass  # This might be the winner

        # P&L formula verification
        pnl_formula_check = {
            "buy_cost": round(c["buy_cost"], 2),
            "sell_proceeds": round(c["sell_proceeds"], 2),
            "net_cost_formula": "buy_cost - sell_proceeds",
            "net_cost": round(c["buy_cost"] - c["sell_proceeds"], 2),
            "redeem_usdc": round(c["redeem_usdc"], 2),
            "merge_usdc": round(c["merge_usdc"], 2),
            "total_payout_formula": "redeem_usdc + merge_usdc",
            "total_payout": round(c["redeem_usdc"] + c["merge_usdc"], 2),
            "pnl_formula": "total_payout - net_cost",
            "pnl": round((c["redeem_usdc"] + c["merge_usdc"]) - (c["buy_cost"] - c["sell_proceeds"]), 2),
            "pnl_matches_stored": abs(
                ((c["redeem_usdc"] + c["merge_usdc"]) - (c["buy_cost"] - c["sell_proceeds"])) - c["pnl"]
            ) < 0.01,
        }

        # Root cause analysis
        root_cause = []

        if bought_both_sides:
            # Calculate cost of each side
            side_costs = {}
            for outcome, rec in by_outcome.items():
                net_buy = rec["buy_usdc_total"] - rec["sell_usdc_total"]
                side_costs[outcome] = net_buy

            # The losing side's cost is pure loss
            # The winning side redeems at $1/share
            # If total cost of both sides > redemption, that's why P&L is negative

            root_cause.append(
                f"BOTH_SIDES_TRADED: Bought {len(all_outcomes)} outcomes: {all_outcomes}. "
                f"Only one side redeems at settlement. The cost of the losing side "
                f"is a pure loss that may exceed the winning side's profit."
            )

            for outcome, cost in side_costs.items():
                root_cause.append(
                    f"  {outcome}: net_cost=${cost:.2f}"
                )

        # Check if they overpaid (bought at high prices)
        total_shares_held = total_buy_shares_from_trades - c.get("sell_shares", 0) - c.get("merge_shares", 0)
        effective_buy_price = c["buy_cost"] / c["buy_shares"] if c["buy_shares"] > 0 else 0

        if effective_buy_price > 0.5:
            root_cause.append(
                f"HIGH_AVG_BUY_PRICE: Average buy price = {effective_buy_price:.4f} "
                f"(>50c). Bought at expensive prices reducing margin."
            )

        if c["merge_usdc"] > 0:
            root_cause.append(
                f"PARTIAL_MERGE: Merged {c['merge_shares']:.0f} shares for "
                f"${c['merge_usdc']:.2f}. Merge returns $1/pair but "
                f"total pair cost may exceed $1."
            )

        if not root_cause:
            root_cause.append("UNKNOWN: Further investigation needed.")

        detailed_analysis.append({
            "conditionId": cid,
            "slug": c["slug"],
            "title": c["title"],
            "status": c["status"],
            "pnl": round(c["pnl"], 2),
            "buy_cost": round(c["buy_cost"], 2),
            "sell_proceeds": round(c["sell_proceeds"], 2),
            "net_cost": round(c["net_cost"], 2),
            "redeem_usdc": round(c["redeem_usdc"], 2),
            "merge_usdc": round(c["merge_usdc"], 2),
            "total_payout": round(c["total_payout"], 2),
            "buy_shares": round(c["buy_shares"], 2),
            "avg_buy_price": round(c["avg_buy_price"], 4),
            "outcome_bought_label": c["outcome_bought"],
            "total_trades": len(trades),
            "num_outcomes_traded": num_outcomes_traded,
            "bought_both_sides": bought_both_sides,
            "outcome_details": outcome_details,
            "pnl_formula_verification": pnl_formula_check,
            "root_cause": root_cause,
        })

    # =========================================================================
    # PART 3: P&L formula verification across ALL WIN conditions
    # =========================================================================
    formula_issues = []
    for c in conditions:
        if c.get("status") != "WIN":
            continue
        computed_pnl = (c["redeem_usdc"] + c["merge_usdc"]) - (c["buy_cost"] - c["sell_proceeds"])
        if abs(computed_pnl - c["pnl"]) > 0.01:
            formula_issues.append({
                "conditionId": c["conditionId"],
                "slug": c["slug"],
                "stored_pnl": round(c["pnl"], 2),
                "computed_pnl": round(computed_pnl, 2),
                "difference": round(computed_pnl - c["pnl"], 2),
            })

    # =========================================================================
    # PART 4: Summary statistics for WIN conditions with negative P&L
    # =========================================================================
    # Check how many traded both sides
    both_sides_count = 0
    single_side_count = 0
    for c in win_negative_pnl:
        cid = c["conditionId"]
        trades = trades_by_cid.get(cid, [])
        outcomes_bought = set()
        for t in trades:
            if t.get("side") == "BUY":
                outcomes_bought.add(t.get("outcome", ""))
        if len(outcomes_bought) > 1:
            both_sides_count += 1
        else:
            single_side_count += 1

    # For single-side losers, investigate why
    single_side_negative_details = []
    for c in win_negative_pnl:
        cid = c["conditionId"]
        trades = trades_by_cid.get(cid, [])
        outcomes_bought = set()
        for t in trades:
            if t.get("side") == "BUY":
                outcomes_bought.add(t.get("outcome", ""))
        if len(outcomes_bought) == 1:
            # Single side buyer but still negative P&L on WIN?
            # This means they bought more than they redeemed
            # net_cost > redeem_usdc + merge_usdc
            single_side_negative_details.append({
                "conditionId": cid,
                "slug": c["slug"],
                "pnl": round(c["pnl"], 2),
                "buy_cost": round(c["buy_cost"], 2),
                "sell_proceeds": round(c["sell_proceeds"], 2),
                "net_cost": round(c["net_cost"], 2),
                "redeem_usdc": round(c["redeem_usdc"], 2),
                "merge_usdc": round(c["merge_usdc"], 2),
                "buy_shares": round(c["buy_shares"], 2),
                "sell_shares": round(c.get("sell_shares", 0), 2),
                "redeem_shares": round(c.get("redeem_shares", 0), 2),
                "merge_shares": round(c.get("merge_shares", 0), 2),
                "outcome_bought": c["outcome_bought"],
                "avg_buy_price": round(c["avg_buy_price"], 4),
                "explanation": (
                    f"Net cost ${c['buy_cost'] - c['sell_proceeds']:.2f} > "
                    f"total payout ${c['redeem_usdc'] + c['merge_usdc']:.2f}. "
                    f"Bought {c['buy_shares']:.0f} shares at avg {c['avg_buy_price']:.4f}, "
                    f"redeemed {c.get('redeem_shares', 0):.0f} shares. "
                    f"Lost shares may have been sold at a loss or merged below cost."
                ),
            })

    # =========================================================================
    # PART 5: All WIN negative P&L - compact list
    # =========================================================================
    all_win_negative = []
    for c in win_negative_pnl:
        cid = c["conditionId"]
        trades = trades_by_cid.get(cid, [])
        outcomes_bought = set()
        outcomes_sold = set()
        for t in trades:
            if t.get("side") == "BUY":
                outcomes_bought.add(t.get("outcome", ""))
            elif t.get("side") == "SELL":
                outcomes_sold.add(t.get("outcome", ""))

        all_win_negative.append({
            "conditionId": cid,
            "slug": c["slug"],
            "pnl": round(c["pnl"], 2),
            "net_cost": round(c["net_cost"], 2),
            "redeem_usdc": round(c["redeem_usdc"], 2),
            "merge_usdc": round(c["merge_usdc"], 2),
            "outcomes_bought": sorted(outcomes_bought),
            "outcomes_sold": sorted(outcomes_sold),
            "bought_both_sides": len(outcomes_bought) > 1,
            "avg_buy_price": round(c["avg_buy_price"], 4),
        })

    # =========================================================================
    # Assemble final output
    # =========================================================================
    result = {
        "part1_high_cost_conditions": {
            "description": "All conditions with net_cost > $50,000",
            "count": len(high_cost_summary),
            "conditions": high_cost_summary,
        },
        "part2_win_negative_pnl_summary": {
            "description": "WIN conditions with negative P&L",
            "total_count": len(win_negative_pnl),
            "total_negative_pnl": round(sum(c["pnl"] for c in win_negative_pnl), 2),
            "both_sides_traded_count": both_sides_count,
            "single_side_only_count": single_side_count,
            "all_win_negative": all_win_negative,
        },
        "part3_top5_negative_pnl_deep_analysis": {
            "description": "Detailed trade-level analysis of top 5 WIN conditions with most negative P&L",
            "conditions": detailed_analysis,
        },
        "part4_pnl_formula_verification": {
            "description": "Verify P&L formula: pnl = (redeem_usdc + merge_usdc) - (buy_cost - sell_proceeds)",
            "formula": "pnl = (redeem_usdc + merge_usdc) - (buy_cost - sell_proceeds)",
            "issues_found": len(formula_issues),
            "issues": formula_issues,
            "conclusion": (
                "P&L formula is correct and consistent across all WIN conditions."
                if len(formula_issues) == 0
                else f"Found {len(formula_issues)} conditions with P&L formula mismatch."
            ),
        },
        "part5_single_side_negative_win": {
            "description": "WIN conditions with negative P&L that only bought ONE side (not both)",
            "count": len(single_side_negative_details),
            "conditions": single_side_negative_details,
        },
        "conclusion": {
            "primary_cause": (
                "The primary reason WIN conditions can have negative P&L is that "
                "the trader bought BOTH sides of the same conditionId (e.g., 'Cubs' AND 'Rockies'). "
                "At settlement, only the winning side's shares redeem at $1/share. "
                "The cost of the losing side's shares is a pure loss. "
                "If the total cost of both sides exceeds the winning side's redemption value, "
                "the net P&L is negative despite having a 'WIN' (redeem > 0)."
            ),
            "secondary_causes": [
                "Buying at high prices (avg > 0.50) reduces the profit margin on winning shares.",
                "Merge operations (collapsing complementary positions) return $1/pair but "
                "if the pair was assembled at >$1 total cost, the merge itself is a loss.",
                "The 'WIN' status only indicates that a redemption occurred (redeem_usdc > 0), "
                "NOT that the overall position was profitable.",
            ],
            "both_sides_pct": (
                f"{both_sides_count}/{len(win_negative_pnl)} "
                f"({both_sides_count/len(win_negative_pnl)*100:.1f}%)"
                if win_negative_pnl else "N/A"
            ),
        },
    }

    json.dump(result, sys.stdout, indent=2, ensure_ascii=False)
    print()  # trailing newline


if __name__ == "__main__":
    main()
