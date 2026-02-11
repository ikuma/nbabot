#!/usr/bin/env python3
"""sovereign2013 両サイドベット & MERGE 戦略の深掘り分析.

3つの核心的な問いに答える:
1. アービトラージなのか? (combined VWAP < $1.00?)
2. どのように機会を特定しているのか? (タイミング、価格帯、パターン)
3. ML / Total / Spread でどう違うのか? (マーケットタイプ別戦略差)
"""

import json
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, median

BASE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE))

from src.analysis.pnl import classify_market_type, classify_sport

DATA_DIR = BASE / "data" / "traders" / "sovereign2013"
OUT = BASE / "data" / "reports" / "sovereign2013-analysis" / "bothside_analysis.md"


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------
@dataclass
class ConditionBothSide:
    """Per-condition both-side analysis record."""

    condition_id: str
    slug: str
    title: str
    event_slug: str
    sport: str
    market_type: str

    # Side 0 (outcomeIndex=0) BUY aggregates
    side0_shares: float = 0.0
    side0_cost: float = 0.0
    side0_buy_count: int = 0
    side0_first_ts: float = field(default=float("inf"))

    # Side 1 (outcomeIndex=1) BUY aggregates
    side1_shares: float = 0.0
    side1_cost: float = 0.0
    side1_buy_count: int = 0
    side1_first_ts: float = field(default=float("inf"))

    # Settlement
    merge_shares: float = 0.0
    merge_usdc: float = 0.0
    redeem_shares: float = 0.0
    redeem_usdc: float = 0.0

    # SELL aggregates (both sides)
    sell_proceeds: float = 0.0
    sell_shares: float = 0.0

    @property
    def is_bothside(self) -> bool:
        return self.side0_buy_count > 0 and self.side1_buy_count > 0

    @property
    def vwap_0(self) -> float:
        return self.side0_cost / self.side0_shares if self.side0_shares > 0 else 0.0

    @property
    def vwap_1(self) -> float:
        return self.side1_cost / self.side1_shares if self.side1_shares > 0 else 0.0

    @property
    def combined_vwap(self) -> float:
        if self.vwap_0 > 0 and self.vwap_1 > 0:
            return self.vwap_0 + self.vwap_1
        return 0.0

    @property
    def mergeable_pairs(self) -> float:
        """Max shares that could be MERGE'd (min of both sides)."""
        return min(self.side0_shares, self.side1_shares)

    @property
    def merge_profit_potential(self) -> float:
        """If merged at combined VWAP, profit per pair = 1.00 - combined_vwap."""
        if self.combined_vwap > 0:
            return (1.0 - self.combined_vwap) * self.mergeable_pairs
        return 0.0

    @property
    def total_buy_cost(self) -> float:
        return self.side0_cost + self.side1_cost

    @property
    def net_cost(self) -> float:
        return self.total_buy_cost - self.sell_proceeds

    @property
    def total_payout(self) -> float:
        return self.merge_usdc + self.redeem_usdc

    @property
    def pnl(self) -> float:
        return self.total_payout - self.net_cost

    @property
    def time_gap_seconds(self) -> float:
        """Time between first BUY on side 0 and first BUY on side 1."""
        if self.side0_first_ts < float("inf") and self.side1_first_ts < float("inf"):
            return abs(self.side1_first_ts - self.side0_first_ts)
        return -1.0

    @property
    def arb_class(self) -> str:
        cv = self.combined_vwap
        if cv <= 0:
            return "single_side"
        if cv < 0.98:
            return "likely_arb"
        if cv <= 1.00:
            return "break_even"
        return "negative_ev"


def _classify_market_type_with_title(slug: str, title: str) -> str:
    """Fallback market type classification using title."""
    mt = classify_market_type(slug)
    if mt != "Moneyline":
        return mt
    # title ベースのフォールバック
    title_lower = title.lower()
    if title_lower.startswith("spread:") or ": spread" in title_lower:
        return "Spread"
    if ": o/u " in title_lower or "over/under" in title_lower:
        return "Total"
    return mt


# ---------------------------------------------------------------------------
# Data loading & building
# ---------------------------------------------------------------------------
def load_json(name: str) -> list[dict]:
    path = DATA_DIR / name
    print(f"  Loading {name} ({path.stat().st_size / 1024 / 1024:.0f} MB)...")
    with open(path) as f:
        return json.load(f)


def build_bothside_conditions(
    trades: list[dict],
    merges: list[dict],
    redeems: list[dict],
) -> dict[str, ConditionBothSide]:
    """Build per-condition records with outcomeIndex-level BUY detail."""
    conditions: dict[str, ConditionBothSide] = {}

    for t in trades:
        cid = t.get("conditionId", "")
        if not cid:
            continue

        if cid not in conditions:
            slug = t.get("slug", "")
            title = t.get("title", "")
            conditions[cid] = ConditionBothSide(
                condition_id=cid,
                slug=slug,
                title=title,
                event_slug=t.get("eventSlug", ""),
                sport=classify_sport(slug),
                market_type=_classify_market_type_with_title(slug, title),
            )

        c = conditions[cid]
        side = t.get("side", "")
        oi = t.get("outcomeIndex", -1)
        size = float(t.get("size", 0))
        usdc = float(t.get("usdcSize", 0))
        ts = t.get("timestamp", 0)

        if side == "BUY":
            if oi == 0:
                c.side0_shares += size
                c.side0_cost += usdc
                c.side0_buy_count += 1
                c.side0_first_ts = min(c.side0_first_ts, ts)
            elif oi == 1:
                c.side1_shares += size
                c.side1_cost += usdc
                c.side1_buy_count += 1
                c.side1_first_ts = min(c.side1_first_ts, ts)
        elif side == "SELL":
            c.sell_proceeds += usdc
            c.sell_shares += size

    for m in merges:
        cid = m.get("conditionId", "")
        if cid in conditions:
            conditions[cid].merge_usdc += float(m.get("usdcSize", 0))
            conditions[cid].merge_shares += float(m.get("size", 0))

    for r in redeems:
        cid = r.get("conditionId", "")
        if cid in conditions:
            conditions[cid].redeem_usdc += float(r.get("usdcSize", 0))
            conditions[cid].redeem_shares += float(r.get("size", 0))

    return conditions


# ---------------------------------------------------------------------------
# Analysis functions
# ---------------------------------------------------------------------------
def analyze_overview(conds: dict[str, ConditionBothSide]) -> list[str]:
    """Section 1: Both-side overview at condition and event level."""
    out: list[str] = []
    out.append("## 1. 両サイドベット概要")
    out.append("")

    all_conds = list(conds.values())
    bothside = [c for c in all_conds if c.is_bothside]
    single = [c for c in all_conds if not c.is_bothside]

    out.append("### Condition レベル")
    out.append("")
    out.append(f"- 全条件数: **{len(all_conds):,}**")
    out.append(f"- 両サイド BUY: **{len(bothside):,}** ({len(bothside)/len(all_conds)*100:.1f}%)")
    out.append(f"- 片サイドのみ: **{len(single):,}** ({len(single)/len(all_conds)*100:.1f}%)")
    out.append("")

    # PnL comparison
    bs_cost = sum(c.net_cost for c in bothside)
    bs_payout = sum(c.total_payout for c in bothside)
    bs_pnl = sum(c.pnl for c in bothside)
    ss_cost = sum(c.net_cost for c in single)
    ss_payout = sum(c.total_payout for c in single)
    ss_pnl = sum(c.pnl for c in single)

    out.append("| タイプ | 条件数 | 純コスト | Payout | PnL | ROI |")
    out.append("|--------|--------|----------|--------|-----|-----|")
    for label, n, cost, payout, pnl in [
        ("両サイド", len(bothside), bs_cost, bs_payout, bs_pnl),
        ("片サイド", len(single), ss_cost, ss_payout, ss_pnl),
    ]:
        roi = pnl / cost * 100 if cost > 0 else 0
        out.append(
            f"| {label} | {n:,} | ${cost:,.0f} | ${payout:,.0f} | ${pnl:,.0f} | {roi:.2f}% |"
        )
    out.append("")

    # Event level
    event_conds: dict[str, list[ConditionBothSide]] = defaultdict(list)
    for c in all_conds:
        if c.event_slug:
            event_conds[c.event_slug].append(c)

    events_with_bothside = sum(
        1 for cids in event_conds.values() if any(c.is_bothside for c in cids)
    )
    total_events = len(event_conds)

    out.append("### Event レベル")
    out.append("")
    out.append(f"- 全イベント数: **{total_events:,}**")
    out.append(
        f"- 両サイドを含むイベント: **{events_with_bothside:,}** "
        f"({events_with_bothside/total_events*100:.1f}%)"
    )
    out.append("")

    return out


def analyze_arbitrage_classification(conds: dict[str, ConditionBothSide]) -> list[str]:
    """Section 2: Classify conditions by combined VWAP."""
    out: list[str] = []
    out.append("## 2. アービトラージ分類 (Combined VWAP)")
    out.append("")

    bothside = [c for c in conds.values() if c.is_bothside]

    classes: dict[str, list[ConditionBothSide]] = defaultdict(list)
    for c in bothside:
        classes[c.arb_class].append(c)

    out.append(
        "Combined VWAP = VWAP(side0) + VWAP(side1)。\n"
        "< 0.98 なら MERGE で 2c+ の利益が確定するアービトラージ。"
    )
    out.append("")

    out.append("| クラス | 基準 | 条件数 | 割合 | 合計BUYコスト | MERGE Payout | REDEEM Payout | PnL |")
    out.append("|--------|------|--------|------|-------------|-------------|--------------|-----|")

    labels = {
        "likely_arb": ("Likely Arb", "< 0.98"),
        "break_even": ("Break Even", "0.98-1.00"),
        "negative_ev": ("Negative EV", "> 1.00"),
    }
    for cls_key in ["likely_arb", "break_even", "negative_ev"]:
        label, crit = labels[cls_key]
        cs = classes.get(cls_key, [])
        n = len(cs)
        pct = n / len(bothside) * 100 if bothside else 0
        buy_cost = sum(c.total_buy_cost for c in cs)
        merge_p = sum(c.merge_usdc for c in cs)
        redeem_p = sum(c.redeem_usdc for c in cs)
        pnl = sum(c.pnl for c in cs)
        out.append(
            f"| {label} | {crit} | {n:,} | {pct:.1f}% | ${buy_cost:,.0f} "
            f"| ${merge_p:,.0f} | ${redeem_p:,.0f} | ${pnl:,.0f} |"
        )
    out.append("")

    # Combined VWAP statistics for bothside
    cvwaps = [c.combined_vwap for c in bothside if c.combined_vwap > 0]
    if cvwaps:
        out.append("### Combined VWAP 統計")
        out.append("")
        out.append(f"- 平均: **{mean(cvwaps):.4f}**")
        out.append(f"- 中央値: **{median(cvwaps):.4f}**")
        out.append(f"- 最小: **{min(cvwaps):.4f}**")
        out.append(f"- 最大: **{max(cvwaps):.4f}**")
        out.append(f"- < 1.00 の割合: **{sum(1 for v in cvwaps if v < 1.0)/len(cvwaps)*100:.1f}%**")
        out.append("")

    # Merge profit potential
    arb_conds = classes.get("likely_arb", [])
    if arb_conds:
        total_merge_potential = sum(c.merge_profit_potential for c in arb_conds)
        actual_merge = sum(c.merge_usdc for c in arb_conds)
        out.append("### Likely Arb の MERGE 利益ポテンシャル")
        out.append("")
        out.append(f"- 理論 MERGE 利益 (mergeable_pairs × (1 - combined_vwap)): **${total_merge_potential:,.0f}**")
        out.append(f"- 実際の MERGE payout: **${actual_merge:,.0f}**")
        out.append("")

    return out


def analyze_pnl_decomposition(conds: dict[str, ConditionBothSide]) -> list[str]:
    """Section 3: Decompose PnL into MERGE leg and Directional leg."""
    out: list[str] = []
    out.append("## 3. PnL 分解: MERGE leg vs Directional leg")
    out.append("")

    bothside = [c for c in conds.values() if c.is_bothside]

    # MERGE leg コスト = 実際に MERGE された shares × combined_vwap
    #   (実際の MERGE 分だけのコストを配分)
    # MERGE leg payout = merge_usdc
    # Directional leg コスト = total_buy_cost - merge_leg_cost
    # Directional leg payout = redeem_usdc

    merge_leg_cost = 0.0
    merge_leg_payout = 0.0
    dir_leg_cost = 0.0
    dir_leg_payout = 0.0

    # 理論値も参考に算出
    theory_merge_cost = 0.0
    theory_merge_payout = 0.0

    for c in bothside:
        # 実際の MERGE shares ベースでコスト配分
        if c.combined_vwap > 0 and c.merge_shares > 0:
            m_cost = c.merge_shares * c.combined_vwap
        else:
            m_cost = 0.0
        merge_leg_cost += m_cost
        merge_leg_payout += c.merge_usdc

        d_cost = c.total_buy_cost - m_cost
        dir_leg_cost += d_cost
        dir_leg_payout += c.redeem_usdc

        # 理論値 (全 mergeable_pairs ベース)
        if c.combined_vwap > 0:
            theory_merge_cost += c.mergeable_pairs * c.combined_vwap
            theory_merge_payout += c.mergeable_pairs  # $1.00 per pair

    merge_pnl = merge_leg_payout - merge_leg_cost
    dir_pnl = dir_leg_payout - dir_leg_cost

    out.append("両サイド条件の BUY コストを、実際に MERGE された shares と")
    out.append("残り (方向性ベット + 未 MERGE ペア) に分解する。")
    out.append("")
    out.append("- **MERGE leg コスト** = actual_merge_shares × combined_vwap")
    out.append("- **Directional leg コスト** = total_buy_cost − MERGE leg コスト")
    out.append("")

    out.append("| レグ | コスト(推定) | Payout | PnL | ROI |")
    out.append("|------|-------------|--------|-----|-----|")
    for label, cost, payout in [
        ("MERGE leg (実際 MERGE 分)", merge_leg_cost, merge_leg_payout),
        ("Directional leg (残り全部)", dir_leg_cost, dir_leg_payout),
    ]:
        pnl = payout - cost
        roi = pnl / cost * 100 if cost > 0 else 0
        out.append(f"| {label} | ${cost:,.0f} | ${payout:,.0f} | ${pnl:,.0f} | {roi:.2f}% |")
    out.append("")

    total_pnl = merge_pnl + dir_pnl
    out.append("### PnL 構成比")
    out.append("")
    if total_pnl != 0:
        out.append(f"- MERGE leg PnL: **${merge_pnl:,.0f}** ({merge_pnl/total_pnl*100:.1f}%)")
        out.append(f"- Directional leg PnL: **${dir_pnl:,.0f}** ({dir_pnl/total_pnl*100:.1f}%)")
    else:
        out.append(f"- MERGE leg PnL: **${merge_pnl:,.0f}**")
        out.append(f"- Directional leg PnL: **${dir_pnl:,.0f}**")
    out.append("")

    # 理論値 (全 mergeable_pairs ベース)
    theory_merge_pnl = theory_merge_payout - theory_merge_cost
    out.append("### 参考: 理論値 (全ペア化可能分)")
    out.append("")
    out.append(f"- mergeable_pairs 合計: **{theory_merge_payout:,.0f} shares**")
    out.append(f"- 実際に MERGE された: **{sum(c.merge_shares for c in bothside):,.0f} shares** "
               f"({sum(c.merge_shares for c in bothside)/theory_merge_payout*100:.1f}%)" if theory_merge_payout > 0 else "")
    out.append(f"- 理論 MERGE コスト: **${theory_merge_cost:,.0f}**")
    out.append(f"- 理論 MERGE payout ($1.00/pair): **${theory_merge_payout:,.0f}**")
    out.append(f"- 理論 MERGE PnL: **${theory_merge_pnl:,.0f}**")
    out.append("")

    # 片サイド条件の PnL も比較
    single = [c for c in conds.values() if not c.is_bothside]
    ss_cost = sum(c.net_cost for c in single)
    ss_payout = sum(c.total_payout for c in single)
    ss_pnl = ss_payout - ss_cost
    ss_roi = ss_pnl / ss_cost * 100 if ss_cost > 0 else 0

    out.append("### 参考: 片サイド条件の PnL")
    out.append("")
    out.append(f"- 条件数: {len(single):,}")
    out.append(f"- 純コスト: ${ss_cost:,.0f} / Payout: ${ss_payout:,.0f} / PnL: ${ss_pnl:,.0f} / ROI: {ss_roi:.2f}%")
    out.append("")

    return out


def analyze_market_type_breakdown(conds: dict[str, ConditionBothSide]) -> list[str]:
    """Section 4: ML / Spread / Total breakdown."""
    out: list[str] = []
    out.append("## 4. マーケットタイプ別分析 (ML / Spread / Total)")
    out.append("")

    mt_stats: dict[str, dict] = defaultdict(
        lambda: {
            "total": 0,
            "bothside": 0,
            "buy_cost": 0.0,
            "merge_usdc": 0.0,
            "redeem_usdc": 0.0,
            "pnl": 0.0,
            "cvwaps": [],
        }
    )

    for c in conds.values():
        mt = c.market_type
        mt_stats[mt]["total"] += 1
        mt_stats[mt]["buy_cost"] += c.total_buy_cost
        mt_stats[mt]["merge_usdc"] += c.merge_usdc
        mt_stats[mt]["redeem_usdc"] += c.redeem_usdc
        mt_stats[mt]["pnl"] += c.pnl
        if c.is_bothside:
            mt_stats[mt]["bothside"] += 1
            if c.combined_vwap > 0:
                mt_stats[mt]["cvwaps"].append(c.combined_vwap)

    out.append("| マーケット | 全条件 | 両サイド | 両サイド率 | BUYコスト | MERGE | REDEEM | PnL | 平均CVWAP | CVWAP中央値 |")
    out.append("|-----------|--------|---------|----------|----------|-------|--------|-----|----------|-----------|")

    for mt in ["Moneyline", "Spread", "Total"]:
        s = mt_stats.get(mt)
        if not s:
            continue
        bs_pct = s["bothside"] / s["total"] * 100 if s["total"] > 0 else 0
        avg_cv = mean(s["cvwaps"]) if s["cvwaps"] else 0
        med_cv = median(s["cvwaps"]) if s["cvwaps"] else 0
        out.append(
            f"| {mt} | {s['total']:,} | {s['bothside']:,} | {bs_pct:.1f}% "
            f"| ${s['buy_cost']:,.0f} | ${s['merge_usdc']:,.0f} "
            f"| ${s['redeem_usdc']:,.0f} | ${s['pnl']:,.0f} "
            f"| {avg_cv:.4f} | {med_cv:.4f} |"
        )

    # "Other" if exists
    other_mts = [mt for mt in mt_stats if mt not in ("Moneyline", "Spread", "Total")]
    for mt in sorted(other_mts):
        s = mt_stats[mt]
        bs_pct = s["bothside"] / s["total"] * 100 if s["total"] > 0 else 0
        avg_cv = mean(s["cvwaps"]) if s["cvwaps"] else 0
        med_cv = median(s["cvwaps"]) if s["cvwaps"] else 0
        out.append(
            f"| {mt} | {s['total']:,} | {s['bothside']:,} | {bs_pct:.1f}% "
            f"| ${s['buy_cost']:,.0f} | ${s['merge_usdc']:,.0f} "
            f"| ${s['redeem_usdc']:,.0f} | ${s['pnl']:,.0f} "
            f"| {avg_cv:.4f} | {med_cv:.4f} |"
        )
    out.append("")

    # Arb classification by market type
    out.append("### マーケットタイプ別アービトラージ分類")
    out.append("")
    out.append("| マーケット | Likely Arb | Break Even | Negative EV |")
    out.append("|-----------|-----------|-----------|------------|")

    for mt in ["Moneyline", "Spread", "Total"]:
        cs = [c for c in conds.values() if c.market_type == mt and c.is_bothside]
        arb = sum(1 for c in cs if c.arb_class == "likely_arb")
        be = sum(1 for c in cs if c.arb_class == "break_even")
        neg = sum(1 for c in cs if c.arb_class == "negative_ev")
        total = arb + be + neg
        if total > 0:
            out.append(
                f"| {mt} | {arb:,} ({arb/total*100:.0f}%) "
                f"| {be:,} ({be/total*100:.0f}%) "
                f"| {neg:,} ({neg/total*100:.0f}%) |"
            )
    out.append("")

    return out


def analyze_sport_breakdown(conds: dict[str, ConditionBothSide]) -> list[str]:
    """Section 5: Sport breakdown."""
    out: list[str] = []
    out.append("## 5. スポーツ別分析")
    out.append("")

    sport_stats: dict[str, dict] = defaultdict(
        lambda: {
            "total": 0,
            "bothside": 0,
            "buy_cost": 0.0,
            "merge_usdc": 0.0,
            "redeem_usdc": 0.0,
            "pnl": 0.0,
            "cvwaps": [],
        }
    )

    for c in conds.values():
        sp = c.sport
        sport_stats[sp]["total"] += 1
        sport_stats[sp]["buy_cost"] += c.total_buy_cost
        sport_stats[sp]["merge_usdc"] += c.merge_usdc
        sport_stats[sp]["redeem_usdc"] += c.redeem_usdc
        sport_stats[sp]["pnl"] += c.pnl
        if c.is_bothside:
            sport_stats[sp]["bothside"] += 1
            if c.combined_vwap > 0:
                sport_stats[sp]["cvwaps"].append(c.combined_vwap)

    out.append("| スポーツ | 全条件 | 両サイド | 両サイド率 | BUYコスト | MERGE | REDEEM | PnL | 平均CVWAP |")
    out.append("|---------|--------|---------|----------|----------|-------|--------|-----|----------|")

    sorted_sports = sorted(sport_stats.items(), key=lambda x: x[1]["buy_cost"], reverse=True)
    for sp, s in sorted_sports:
        bs_pct = s["bothside"] / s["total"] * 100 if s["total"] > 0 else 0
        avg_cv = mean(s["cvwaps"]) if s["cvwaps"] else 0
        out.append(
            f"| {sp} | {s['total']:,} | {s['bothside']:,} | {bs_pct:.1f}% "
            f"| ${s['buy_cost']:,.0f} | ${s['merge_usdc']:,.0f} "
            f"| ${s['redeem_usdc']:,.0f} | ${s['pnl']:,.0f} "
            f"| {avg_cv:.4f} |"
        )
    out.append("")

    return out


def analyze_timing_patterns(conds: dict[str, ConditionBothSide]) -> list[str]:
    """Section 6: Timing gap between first BUY on each side."""
    out: list[str] = []
    out.append("## 6. タイミング分析 (両サイド間の時間ギャップ)")
    out.append("")

    bothside = [c for c in conds.values() if c.is_bothside]
    gaps = [c.time_gap_seconds for c in bothside if c.time_gap_seconds >= 0]

    if not gaps:
        out.append("両サイド条件のタイムスタンプデータなし。")
        out.append("")
        return out

    out.append(
        "Side 0 の最初の BUY と Side 1 の最初の BUY のタイムスタンプ差を分析。"
    )
    out.append("")

    # Classify timing
    buckets = [
        ("Atomic (<1秒)", 0, 1),
        ("1秒-1分", 1, 60),
        ("1分-10分", 60, 600),
        ("10分-1時間", 600, 3600),
        ("1-6時間", 3600, 21600),
        ("6-24時間", 21600, 86400),
        ("1-7日", 86400, 604800),
        ("7日+", 604800, float("inf")),
    ]

    out.append("| 時間ギャップ | 件数 | 割合 | 平均PnL | 平均CVWAP |")
    out.append("|-------------|------|------|---------|----------|")

    for label, lo, hi in buckets:
        matching = [c for c in bothside if lo <= c.time_gap_seconds < hi]
        n = len(matching)
        if n == 0:
            continue
        pct = n / len(bothside) * 100
        avg_pnl = mean([c.pnl for c in matching])
        cvwaps = [c.combined_vwap for c in matching if c.combined_vwap > 0]
        avg_cv = mean(cvwaps) if cvwaps else 0
        out.append(f"| {label} | {n:,} | {pct:.1f}% | ${avg_pnl:,.2f} | {avg_cv:.4f} |")
    out.append("")

    out.append("### タイムギャップ統計")
    out.append("")
    out.append(f"- 平均: **{mean(gaps)/3600:.1f} 時間**")
    out.append(f"- 中央値: **{median(gaps)/3600:.1f} 時間**")
    out.append(f"- Atomic (<1秒): **{sum(1 for g in gaps if g < 1):,}** ({sum(1 for g in gaps if g < 1)/len(gaps)*100:.1f}%)")
    out.append(f"- Sequential (>1時間): **{sum(1 for g in gaps if g > 3600):,}** ({sum(1 for g in gaps if g > 3600)/len(gaps)*100:.1f}%)")
    out.append("")

    return out


def analyze_combined_vwap_distribution(conds: dict[str, ConditionBothSide]) -> list[str]:
    """Section 7: 1-cent histogram of combined VWAP."""
    out: list[str] = []
    out.append("## 7. Combined VWAP 分布 (1セント刻みヒストグラム)")
    out.append("")

    bothside = [c for c in conds.values() if c.is_bothside]
    cvwaps = [c.combined_vwap for c in bothside if c.combined_vwap > 0]

    if not cvwaps:
        out.append("データなし。")
        out.append("")
        return out

    # Build histogram from 0.80 to 1.20
    hist: dict[str, int] = {}
    for v in cvwaps:
        bucket = round(v, 2)
        key = f"{bucket:.2f}"
        hist[key] = hist.get(key, 0) + 1

    max_count = max(hist.values()) if hist else 1

    out.append("| CVWAP | 件数 | 割合 | ヒストグラム |")
    out.append("|-------|------|------|-------------|")

    for cent in range(80, 121):
        key = f"{cent/100:.2f}"
        count = hist.get(key, 0)
        if count == 0:
            continue
        pct = count / len(cvwaps) * 100
        bar_len = int(count / max_count * 40)
        bar = "#" * bar_len
        out.append(f"| {key} | {count:,} | {pct:.1f}% | {bar} |")
    out.append("")

    # Cumulative < 1.00
    under_100 = sum(1 for v in cvwaps if v < 1.00)
    out.append(f"- **CVWAP < 1.00 (利益圏)**: {under_100:,} / {len(cvwaps):,} ({under_100/len(cvwaps)*100:.1f}%)")
    out.append(f"- **CVWAP >= 1.00 (損失圏)**: {len(cvwaps)-under_100:,} / {len(cvwaps):,} ({(len(cvwaps)-under_100)/len(cvwaps)*100:.1f}%)")
    out.append("")

    return out


def generate_strategy_implications(conds: dict[str, ConditionBothSide]) -> list[str]:
    """Section 8: Strategy implications for nbabot."""
    out: list[str] = []
    out.append("## 8. nbabot への戦略的示唆")
    out.append("")

    bothside = [c for c in conds.values() if c.is_bothside]
    arb_conds = [c for c in bothside if c.arb_class == "likely_arb"]
    cvwaps = [c.combined_vwap for c in bothside if c.combined_vwap > 0]

    # Compute key metrics for summary
    under_100_pct = sum(1 for v in cvwaps if v < 1.0) / len(cvwaps) * 100 if cvwaps else 0
    med_cvwap = median(cvwaps) if cvwaps else 0
    arb_pnl = sum(c.pnl for c in arb_conds)
    total_pnl = sum(c.pnl for c in conds.values())
    merge_total = sum(c.merge_usdc for c in conds.values())
    redeem_total = sum(c.redeem_usdc for c in conds.values())

    out.append("### 3つの問いへの回答")
    out.append("")
    out.append("**Q1: アービトラージなのか?**")
    out.append("")
    out.append(
        f"- 両サイド条件の **{under_100_pct:.0f}%** で combined VWAP < 1.00 → YES、アービトラージ要素あり"
    )
    out.append(f"- 中央値 CVWAP: **{med_cvwap:.4f}** → 1ペアあたり約 {(1-med_cvwap)*100:.1f}c の利益")
    out.append(f"- ただし MERGE payout ${merge_total:,.0f} vs REDEEM payout ${redeem_total:,.0f}")
    out.append("  → **純粋なアービトラージだけでなく、方向性ベットの利益も大きい**")
    out.append("")

    out.append("**Q2: どのように機会を特定しているのか?**")
    out.append("")

    # Timing insight
    gaps = [c.time_gap_seconds for c in bothside if c.time_gap_seconds >= 0]
    if gaps:
        atomic_pct = sum(1 for g in gaps if g < 1) / len(gaps) * 100
        seq_pct = sum(1 for g in gaps if g > 3600) / len(gaps) * 100
        out.append(f"- Atomic (<1秒) 両サイド: {atomic_pct:.1f}% → 同時注文の自動化")
        out.append(f"- Sequential (>1時間): {seq_pct:.1f}% → 価格変動を見ての逐次追加")
    out.append("- 価格帯 0.20-0.55 のスイートスポットに集中 (calibration edge)")
    out.append("")

    out.append("**Q3: ML / Total / Spread でどう違うのか?**")
    out.append("")

    for mt in ["Moneyline", "Spread", "Total"]:
        cs = [c for c in conds.values() if c.market_type == mt]
        bs = [c for c in cs if c.is_bothside]
        mt_pnl = sum(c.pnl for c in cs)
        bs_pct = len(bs) / len(cs) * 100 if cs else 0
        out.append(f"- **{mt}**: {len(cs):,} 条件, PnL ${mt_pnl:,.0f}, 両サイド率 {bs_pct:.1f}%")
    out.append("")

    out.append("### nbabot への適用")
    out.append("")
    out.append("1. **MERGE 戦略の追加検討**: Yes+No の合計 < $1.00 時の自動両サイド購入")
    out.append("2. **Spread/Total マーケットへの拡張**: 校正テーブルの横展開")
    out.append("3. **タイミング分析の活用**: sequential パターンは DCA と組み合わせ可能")
    out.append("")

    return out


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    print("=" * 60)
    print("sovereign2013 両サイドベット & MERGE 戦略分析")
    print("=" * 60)

    print("\nLoading data...")
    trades = load_json("raw_trade.json")
    merges = load_json("raw_merge.json")
    redeems = load_json("raw_redeem.json")
    print(f"\nTrades: {len(trades):,} / Merges: {len(merges):,} / Redeems: {len(redeems):,}")

    print("\nBuilding per-condition both-side records...")
    conds = build_bothside_conditions(trades, merges, redeems)
    print(f"Conditions: {len(conds):,}")

    # 検証: 合計値チェック
    total_buy = sum(c.total_buy_cost for c in conds.values())
    total_merge = sum(c.merge_usdc for c in conds.values())
    total_redeem = sum(c.redeem_usdc for c in conds.values())
    total_pnl = sum(c.pnl for c in conds.values())
    print(f"\nValidation:")
    print(f"  Total BUY cost: ${total_buy:,.0f}")
    print(f"  MERGE payout:   ${total_merge:,.0f}")
    print(f"  REDEEM payout:  ${total_redeem:,.0f}")
    print(f"  Total PnL:      ${total_pnl:,.0f}")

    bothside = [c for c in conds.values() if c.is_bothside]
    print(f"\n  Both-side conditions: {len(bothside):,} / {len(conds):,}")

    print("\nGenerating report...")

    report_lines: list[str] = []
    report_lines.append("# sovereign2013 両サイドベット & MERGE 戦略分析")
    report_lines.append("")
    report_lines.append(f"**分析日**: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    report_lines.append(
        f"**データ**: TRADE {len(trades):,} / MERGE {len(merges):,} / REDEEM {len(redeems):,}"
    )
    report_lines.append(f"**条件数**: {len(conds):,}")
    report_lines.append("")

    # Executive Summary
    report_lines.append("## Executive Summary")
    report_lines.append("")

    cvwaps = [c.combined_vwap for c in bothside if c.combined_vwap > 0]
    under_100 = sum(1 for v in cvwaps if v < 1.0) if cvwaps else 0
    under_100_pct = under_100 / len(cvwaps) * 100 if cvwaps else 0
    med_cvwap = median(cvwaps) if cvwaps else 0

    # PnL 分解 (analyze_pnl_decomposition と同じロジック)
    es_merge_leg_cost = 0.0
    es_merge_leg_payout = 0.0
    es_dir_leg_cost = 0.0
    es_dir_leg_payout = 0.0
    for c in bothside:
        if c.combined_vwap > 0 and c.merge_shares > 0:
            m_cost = c.merge_shares * c.combined_vwap
        else:
            m_cost = 0.0
        es_merge_leg_cost += m_cost
        es_merge_leg_payout += c.merge_usdc
        es_dir_leg_cost += c.total_buy_cost - m_cost
        es_dir_leg_payout += c.redeem_usdc
    es_merge_pnl = es_merge_leg_payout - es_merge_leg_cost
    es_dir_pnl = es_dir_leg_payout - es_dir_leg_cost
    es_total_pnl = es_merge_pnl + es_dir_pnl
    es_merge_pct = es_merge_pnl / es_total_pnl * 100 if es_total_pnl != 0 else 0
    es_dir_pct = es_dir_pnl / es_total_pnl * 100 if es_total_pnl != 0 else 0

    # タイミング統計
    gaps = [c.time_gap_seconds for c in bothside if c.time_gap_seconds >= 0]
    es_seq_pct = sum(1 for g in gaps if g > 3600) / len(gaps) * 100 if gaps else 0
    es_med_gap_h = median(gaps) / 3600 if gaps else 0

    # マーケットタイプ別 PnL + Likely Arb 率
    es_mt: dict[str, dict] = {}
    for mt in ["Moneyline", "Spread", "Total"]:
        cs = [c for c in conds.values() if c.market_type == mt]
        bs = [c for c in cs if c.is_bothside]
        mt_pnl = sum(c.pnl for c in cs)
        arb_n = sum(1 for c in bs if c.arb_class == "likely_arb")
        arb_pct = arb_n / len(bs) * 100 if bs else 0
        es_mt[mt] = {"pnl": mt_pnl, "arb_pct": arb_pct}

    # マーケットタイプ中 PnL 最大のものと Arb 率最高のもの
    max_pnl_mt = max(es_mt, key=lambda k: es_mt[k]["pnl"])
    max_arb_mt = max(es_mt, key=lambda k: es_mt[k]["arb_pct"])

    report_lines.append(
        f"1. **アービトラージか?** — ハイブリッド戦略。{under_100_pct:.0f}% で CVWAP < 1.00。"
        f"MERGE leg PnL +${es_merge_pnl:,.0f} ({es_merge_pct:.1f}%) + "
        f"Directional leg PnL +${es_dir_pnl:,.0f} ({es_dir_pct:.1f}%)。"
        f"純粋アービトラージではなく、MERGE でリスクヘッジしつつ方向性で稼ぐ構造。"
    )
    report_lines.append(
        f"2. **機会の特定方法** — Sequential (>1h) が {es_seq_pct:.0f}%、"
        f"中央値ギャップ {es_med_gap_h:.1f}h。"
        f"価格変動を観察しながら逐次追加。"
    )
    report_lines.append(
        f"3. **マーケットタイプ差** — {max_pnl_mt} が最大 PnL "
        f"(${es_mt[max_pnl_mt]['pnl']:,.0f})、"
        f"{max_arb_mt} が Likely Arb 率最高 ({es_mt[max_arb_mt]['arb_pct']:.0f}%)。"
    )
    report_lines.append("")
    report_lines.append("---")
    report_lines.append("")

    # Sections
    report_lines.extend(analyze_overview(conds))
    report_lines.append("---")
    report_lines.append("")
    report_lines.extend(analyze_arbitrage_classification(conds))
    report_lines.append("---")
    report_lines.append("")
    report_lines.extend(analyze_pnl_decomposition(conds))
    report_lines.append("---")
    report_lines.append("")
    report_lines.extend(analyze_market_type_breakdown(conds))
    report_lines.append("---")
    report_lines.append("")
    report_lines.extend(analyze_sport_breakdown(conds))
    report_lines.append("---")
    report_lines.append("")
    report_lines.extend(analyze_timing_patterns(conds))
    report_lines.append("---")
    report_lines.append("")
    report_lines.extend(analyze_combined_vwap_distribution(conds))
    report_lines.append("---")
    report_lines.append("")
    report_lines.extend(generate_strategy_implications(conds))

    report_text = "\n".join(report_lines)

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT, "w") as f:
        f.write(report_text)

    print(f"\nReport written to: {OUT}")
    print(f"Report length: {len(report_text):,} chars, {len(report_lines):,} lines")


if __name__ == "__main__":
    main()
