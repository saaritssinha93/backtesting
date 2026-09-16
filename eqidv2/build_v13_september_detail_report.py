"""Build a September-only execution audit for the completed V13 replays."""
from __future__ import annotations

from pathlib import Path
import json
import numpy as np
import pandas as pd

ROOT = Path(r"C:\TradingData\eqidv2\fno_oi\strategy_research")
OUT = ROOT / "v13_september_asof_20260911"
SEP = "2026-09"

STOCK_ARMS = {
    "V13.5 stock control (unconstrained per-trade capital model)": (
        ROOT / "v13_corrected_v5/higher_frequency/fno_v13_corrected_v5_higher_frequency_trades.csv",
        "filled", "net_profit_rupees", "capital_per_entry_rupees", "exposure_per_entry_rupees", None,
    ),
    "V13.6 stock portfolio (₹300k, 3 slots)": (
        ROOT / "v13_corrected_v6/asof_20260911_300k_3slot/fno_v13_v6_portfolio_trades.csv",
        "portfolio_executed", "portfolio_net_profit_rupees", "portfolio_trade_capital_rupees", "portfolio_trade_exposure_rupees",
        ("portfolio_reserved_capital_before_rupees", "portfolio_gross_exposure_before_rupees", "portfolio_open_risk_before_rupees"),
    ),
    "V13.7 stock 180-minute shadow (₹300k, 3 slots)": (
        ROOT / "v13_corrected_v7/asof_20260911_180m_300k_3slot/v13_v7_time180_shadow__portfolio_trades.csv",
        "portfolio_executed", "portfolio_net_profit_rupees", "portfolio_trade_capital_rupees", "portfolio_trade_exposure_rupees",
        ("portfolio_reserved_capital_before_rupees", "portfolio_gross_exposure_before_rupees", "portfolio_open_risk_before_rupees"),
    ),
}
OPTIONS_ARMS = {
    "V13.5 options — full lot at T1 (3 lots)": ROOT / "v13_corrected_v5/options_backtest/fno_v13_v5_options_backtest_trades.csv",
    "V13.6 options — execution-safe full lot at T1 (3 lots)": ROOT / "v13_corrected_v6_options/asof_20260911_native_atm_3lots/fno_v13_v6_options_trades.csv",
}

def money(value: float) -> str:
    return f"₹{value:,.2f}"

def pct(value: float) -> str:
    return f"{value:.2f}%"

def num(value: float) -> str:
    return f"{value:.2f}"

def md_table(frame: pd.DataFrame) -> str:
    if frame.empty:
        return "_No executed trades._"
    return frame.to_markdown(index=False)

def time_range(frame: pd.DataFrame, column: str) -> str:
    x = pd.to_datetime(frame[column], errors="coerce").dropna()
    return "—" if x.empty else f"{x.min():%H:%M}–{x.max():%H:%M}"

def pf(frame: pd.DataFrame, pnl: str) -> float:
    positive = frame.loc[frame[pnl] > 0, pnl].sum()
    loss = -frame.loc[frame[pnl] < 0, pnl].sum()
    return np.inf if loss == 0 and positive > 0 else (positive / loss if loss else 0.0)

def stock_report(title: str, spec: tuple, slug: str) -> tuple[str, pd.DataFrame]:
    path, flag, pnl, capital, exposure, capacity = spec
    raw = pd.read_csv(path)
    frame = raw.loc[(raw["day"].astype(str).str.startswith(SEP)) & (raw[flag].astype(str).str.lower() == "true")].copy()
    for col in (pnl, capital, exposure, "cost_rupees", "holding_minutes", "net_return_on_capital_pct"):
        if col in frame:
            frame[col] = pd.to_numeric(frame[col], errors="coerce").fillna(0.0)
    daily = []
    for day, x in frame.groupby("day", sort=True):
        net = x[pnl].sum(); deployed = x[capital].sum()
        row = {
            "Day": day, "Trades": len(x), "Long": int((x.side == "LONG").sum()), "Short": int((x.side == "SHORT").sum()),
            "Wins/Losses": f"{int((x[pnl] > 0).sum())}/{int((x[pnl] < 0).sum())}",
            "Capital deployed": money(deployed), "Gross exposure traded": money(x[exposure].sum()),
            "Net P&L": money(net), "P&L / deployed capital": pct(100 * net / deployed) if deployed else "—",
            "PF": "∞" if np.isinf(pf(x, pnl)) else num(pf(x, pnl)), "Avg trade": money(net / len(x)),
            "Entry window": time_range(x, "entry_ts"), "Exit window": time_range(x, "exit_ts"),
            "Avg hold (min)": num(x.holding_minutes.mean()),
            "Exit outcomes": "; ".join(f"{k}: {v}" for k, v in x.exit_reason.value_counts().items()),
        }
        if capacity:
            reserved, gross, risk = capacity
            row["Peak reserved capital"] = money((x[reserved] + x[capital]).max())
            row["Peak open exposure"] = money((x[gross] + x[exposure]).max())
            row["Peak initial risk"] = money((x[risk] + x.get("portfolio_trade_initial_risk_rupees", 0)).max())
        daily.append(row)
    detail_cols = ["day", "tradingsymbol", "side", "setup_id", "entry_ts", "entry_price", "exit_ts", "exit_price", "holding_minutes", "exit_reason", "net_return_pct", "net_return_on_capital_pct", capital, exposure, pnl]
    detail = frame[[c for c in detail_cols if c in frame]].sort_values(["day", "entry_ts", "tradingsymbol"])
    detail.to_csv(OUT / f"{slug}_executed_trades.csv", index=False)
    total_cap = frame[capital].sum(); total_net = frame[pnl].sum()
    header = f"## {title}\n\n"
    header += f"Executed September trades: **{len(frame)}** | total deployed capital: **{money(total_cap)}** | total gross exposure traded: **{money(frame[exposure].sum())}** | net P&L: **{money(total_net)}** | P&L/deployed capital: **{pct(100*total_net/total_cap)}** | PF: **{'∞' if np.isinf(pf(frame,pnl)) else num(pf(frame,pnl))}**.\n\n"
    return header + md_table(pd.DataFrame(daily)) + f"\n\nDetailed executions: `{slug}_executed_trades.csv`\n", detail

def options_report(title: str, path: Path, slug: str) -> str:
    raw = pd.read_csv(path)
    frame = raw.loc[(raw.day.astype(str).str.startswith(SEP)) & (raw.execution_status == "EXECUTED") & (raw.exit_variant == "OPTION_FULL_LOT_AT_T1")].copy()
    for col in ("entry_premium_outlay_rupees", "net_pnl_rupees", "gross_pnl_rupees", "cost_proxy_rupees", "holding_minutes", "net_return_pct"):
        frame[col] = pd.to_numeric(frame[col], errors="coerce").fillna(0.0)
    rows = []
    for day, x in frame.groupby("day", sort=True):
        outlay = x.entry_premium_outlay_rupees.sum(); net = x.net_pnl_rupees.sum()
        rows.append({
            "Day": day, "Trades": len(x), "CE / PE": f"{int((x.option_type=='CE').sum())} / {int((x.option_type=='PE').sum())}",
            "Underlying L / S": f"{int((x.equity_side=='LONG').sum())} / {int((x.equity_side=='SHORT').sum())}",
            "Premium paid (capital)": money(outlay), "Net P&L": money(net), "P&L / premium": pct(100*net/outlay) if outlay else "—",
            "PF": "∞" if np.isinf(pf(x, "net_pnl_rupees")) else num(pf(x, "net_pnl_rupees")), "Avg trade": money(net/len(x)),
            "Entry window": time_range(x, "entry_ts"), "Exit window": time_range(x, "exit_ts"), "Avg hold (min)": num(x.holding_minutes.mean()),
            "Exit outcomes": "; ".join(f"{k}: {v}" for k,v in x.exit_reason.value_counts().items()),
        })
    detail_cols = ["day", "equity_symbol", "equity_side", "option_tradingsymbol", "option_type", "option_strike", "lot_size", "option_lots", "quantity", "entry_ts", "entry_premium", "entry_premium_outlay_rupees", "exit_ts", "exit_premium", "holding_minutes", "exit_reason", "gross_pnl_rupees", "cost_proxy_rupees", "net_pnl_rupees", "net_return_pct"]
    detail = frame[detail_cols].sort_values(["day", "entry_ts", "equity_symbol"])
    detail.to_csv(OUT / f"{slug}_executed_option_trades.csv", index=False)
    outlay = frame.entry_premium_outlay_rupees.sum(); net = frame.net_pnl_rupees.sum()
    text = f"## {title}\n\nExecuted September options: **{len(frame)}** | premium paid (the full cash capital for these long-option positions; no separate short-option margin): **{money(outlay)}** | net P&L: **{money(net)}** | P&L/premium: **{pct(100*net/outlay)}** | PF: **{'∞' if np.isinf(pf(frame,'net_pnl_rupees')) else num(pf(frame,'net_pnl_rupees'))}**.\n\n"
    return text + md_table(pd.DataFrame(rows)) + f"\n\nDetailed executions: `{slug}_executed_option_trades.csv`\n"

def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    sections = ["# V13 September 2026 Detailed Stock and Options Audit — as of 11 September\n", "## Scope and definitions\n\nThis audit is cut off at the last completed trading session before 12 September: **Friday, 11 September 2026**. Stock cash and futures-OI inputs are complete through that date, and the stock tables cover 1–11 September. The locally verified option-premium package ends on **7 September**; its tables therefore cover 1, 2, 3, 4 and 7 September only. No option result is fabricated for 8–11 September. `Capital deployed` is the sum of fresh per-trade capital; it is not compounded. V13.6/V13.7 show causal peak reserved capital, exposure and initial risk. For long options, premium paid is the cash capital at risk; there is no additional short-option margin. PF is gross profits divided by absolute gross losses, after stated costs.\n"]
    for index, (title, spec) in enumerate(STOCK_ARMS.items(), 5):
        section, _ = stock_report(title, spec, f"v{index}_stock")
        sections.append(section)
    for index, (title, path) in enumerate(OPTIONS_ARMS.items(), 5):
        sections.append(options_report(title, path, f"v{index}_options"))
    v8 = pd.read_csv(ROOT / "v13_corrected_v8_feature_shadow/asof_20260911/fno_v13_v8_causal_features.csv")
    sept = v8.loc[v8.day.astype(str).str.startswith(SEP)]
    sections.append("## V13.8 causal futures-feature shadow\n\nV13.8 does **not** generate a separate executable order, margin, entry or exit ledger. It only labels the V13.5 outcomes after causal futures features are calculated, so it cannot supply a valid separate capital/P&L trade report. September feature states: `" + json.dumps(sept.v8_feature_status.value_counts().to_dict()) + "`.\n")
    (OUT / "V13_SEPTEMBER_2026_DETAILED_AUDIT.md").write_text("\n".join(sections), encoding="utf-8")
    print(OUT / "V13_SEPTEMBER_2026_DETAILED_AUDIT.md")

if __name__ == "__main__":
    main()
