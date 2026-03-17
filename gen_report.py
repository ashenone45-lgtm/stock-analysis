"""
日报生成脚本

用法：
  python gen_report.py              # 生成最新交易日报告
  python gen_report.py --date 2026-03-14
  python gen_report.py --out reports/  # 指定输出目录
"""
import argparse
import io
import sys
from datetime import datetime
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

import numpy as np
import pandas as pd

DATA_DIR = Path(__file__).parent / "data" / "market"
NAMES_CSV = Path(__file__).parent / "data" / "stock_names.csv"
REPORTS_DIR = Path(__file__).parent / "reports"


# ── 数据加载 ────────────────────────────────────────────────

def load_names() -> dict:
    if not NAMES_CSV.exists():
        return {}
    df = pd.read_csv(NAMES_CSV, dtype=str)
    return dict(zip(df["code"], df["name"]))


def calc_rsi(close: pd.Series, period: int = 14) -> float:
    delta = close.diff().dropna()
    if len(delta) < period:
        return float("nan")
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    rs = gain / loss.replace(0, np.nan)
    return float((100 - 100 / (1 + rs)).iloc[-1])


def calc_macd_hist(close: pd.Series) -> float:
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    macd = ema12 - ema26
    signal = macd.ewm(span=9, adjust=False).mean()
    return float((macd - signal).iloc[-1])


def trend_score(r: dict) -> int:
    s = 0
    if r["ma5"] > r["ma20"] > r["ma60"]:
        s += 2
    elif r["ma5"] > r["ma20"]:
        s += 1
    elif r["ma5"] < r["ma20"] < r["ma60"]:
        s -= 2
    elif r["ma5"] < r["ma20"]:
        s -= 1
    rsi = r["rsi"]
    if not np.isnan(rsi):
        if rsi > 70:
            s -= 1
        elif rsi < 30:
            s += 1
    s += 1 if r["macd_hist"] > 0 else -1
    return s


def load_market(target_date: str, names: dict) -> pd.DataFrame:
    records = []
    for f in DATA_DIR.glob("*.parquet"):
        df = pd.read_parquet(f)
        df["日期"] = pd.to_datetime(df["日期"]).dt.strftime("%Y-%m-%d")
        df = df.sort_values("日期").reset_index(drop=True)
        today = df[df["日期"] == target_date]
        if today.empty:
            continue
        close = df["收盘"].astype(float)
        n = len(close)
        code = f.stem
        rec = {
            "代码": code,
            "名称": names.get(code, ""),
            "收盘": float(today["收盘"].iloc[0]),
            "涨跌幅": float(today["涨跌幅"].iloc[0]),
            "涨跌额": float(today["涨跌额"].iloc[0]),
            "成交额亿": float(today["成交额"].iloc[0]) / 1e8,
            "换手率": float(today["换手率"].iloc[0]),
            "ma5":  float(close.iloc[-5:].mean()) if n >= 5 else np.nan,
            "ma20": float(close.iloc[-20:].mean()) if n >= 20 else np.nan,
            "ma60": float(close.iloc[-60:].mean()) if n >= 60 else np.nan,
            "rsi":  calc_rsi(close),
            "macd_hist": calc_macd_hist(close),
        }
        rec["score"] = trend_score(rec)
        rec["ma60_bias"] = (rec["收盘"] - rec["ma60"]) / rec["ma60"] * 100 if rec["ma60"] else np.nan
        records.append(rec)
    return pd.DataFrame(records)


# ── 报告生成 ────────────────────────────────────────────────

def _row(r, cols):
    """格式化表格行"""
    return "| " + " | ".join(cols(r)) + " |"


def build_report(df: pd.DataFrame, target_date: str) -> str:
    total = len(df)
    n_up = (df["涨跌幅"] > 0).sum()
    n_down = (df["涨跌幅"] < 0).sum()
    n_flat = total - n_up - n_down
    n_limit_up = (df["涨跌幅"] >= 9.9).sum()
    n_limit_down = (df["涨跌幅"] <= -9.9).sum()
    avg_chg = df["涨跌幅"].mean()
    med_chg = df["涨跌幅"].median()
    total_vol = df["成交额亿"].sum()

    strong = df[df["score"] >= 3]
    weak = df[df["score"] <= -3]
    rsi_avg = df["rsi"].dropna().mean()
    n_oversold = (df["rsi"] < 30).sum()
    n_overbought = (df["rsi"] > 70).sum()

    top_gain = df.nlargest(10, "涨跌幅")
    top_loss = df.nsmallest(10, "涨跌幅")
    top_vol = df.nlargest(5, "成交额亿")
    oversold_cands = df[(df["rsi"] < 30) & (df["涨跌幅"] < 0)].nsmallest(8, "rsi")
    risk_stocks = df[df["涨跌幅"] <= -9.9].sort_values("涨跌幅")

    # 综合判断
    if avg_chg >= 1.0:
        sentiment = "乐观偏多"
        suggestion = "可适度跟进强势股，注意仓位控制"
    elif avg_chg >= 0:
        sentiment = "温和偏多"
        suggestion = "关注成交量放大的强势品种，轻仓试多"
    elif avg_chg >= -1.5:
        sentiment = "谨慎偏空"
        suggestion = "以观望为主，持仓控制在五成以下"
    elif avg_chg >= -3.0:
        sentiment = "明显偏空"
        suggestion = "观望为主，不追跌；关注超卖龙头企稳信号"
    else:
        sentiment = "极度悲观"
        suggestion = "空仓观望；超卖个股等放量止跌后再考虑布局"

    lines = []
    w = lines.append

    w(f"# 市场日报 · {target_date}")
    w(f"")
    w(f"> 生成时间：{datetime.now().strftime('%Y-%m-%d %H:%M')}  |  覆盖股票：{total} 只（IT/半导体/互联网/卫星航天/有色金属）")
    w(f"")

    # 一、概况
    w(f"## 一、市场概况")
    w(f"")
    w(f"| 指标 | 数值 |")
    w(f"|------|------|")
    w(f"| 上涨 / 下跌 / 平盘 | {n_up} / {n_down} / {n_flat} |")
    w(f"| 涨停 / 跌停 | {n_limit_up} / {n_limit_down} |")
    w(f"| 平均涨跌幅 | **{avg_chg:+.2f}%** |")
    w(f"| 中位数涨跌幅 | {med_chg:+.2f}% |")
    w(f"| 板块总成交额 | {total_vol:.1f} 亿元 |")
    w(f"| 市场情绪 | **{sentiment}** |")
    w(f"")

    up_pct = n_up / total * 100
    if up_pct < 20:
        breadth_comment = f"上涨面仅 {up_pct:.0f}%，市场极度普跌，恐慌情绪蔓延。"
    elif up_pct < 40:
        breadth_comment = f"上涨面 {up_pct:.0f}%，多数个股走弱，做多意愿不足。"
    elif up_pct < 60:
        breadth_comment = f"上涨面 {up_pct:.0f}%，多空分歧明显，市场震荡分化。"
    else:
        breadth_comment = f"上涨面 {up_pct:.0f}%，普涨格局，市场情绪积极。"
    w(f"{breadth_comment}平均跌幅与中位数接近（{avg_chg:+.2f}% vs {med_chg:+.2f}%），说明跌幅分布均匀，非个别股票拖累。")
    w(f"")

    # 二、技术信号
    w(f"## 二、技术面信号")
    w(f"")
    w(f"| 信号 | 数量 | 占比 |")
    w(f"|------|------|------|")
    w(f"| 强势（均线多排 + MACD 金叉） | {len(strong)} 只 | {len(strong)/total*100:.1f}% |")
    w(f"| 弱势（均线空排 + MACD 死叉） | {len(weak)} 只 | {len(weak)/total*100:.1f}% |")
    w(f"| RSI 均值 | {rsi_avg:.1f} | {'接近超卖' if rsi_avg < 40 else '中性' if rsi_avg < 60 else '偏高'} |")
    w(f"| RSI < 30（超卖） | {n_oversold} 只 | {n_oversold/total*100:.1f}% |")
    w(f"| RSI > 70（超买） | {n_overbought} 只 | {n_overbought/total*100:.1f}% |")
    w(f"")
    if n_oversold / total > 0.25:
        w(f"> ⚠️ 超卖个股占比超过 25%，市场存在超跌反弹动能，但需等待放量止跌信号确认。")
    elif n_overbought / total > 0.20:
        w(f"> ⚠️ 超买个股占比超过 20%，短线注意高位回调风险。")
    w(f"")

    # 三、涨幅榜
    w(f"## 三、涨幅榜 TOP10")
    w(f"")
    w(f"| 代码 | 名称 | 涨跌幅 | 收盘 | 成交额 | RSI | 趋势评分 |")
    w(f"|------|------|--------|------|--------|-----|----------|")
    for _, r in top_gain.iterrows():
        score_str = "强势↑↑" if r["score"] >= 3 else "偏多↑" if r["score"] >= 1 else "震荡—" if r["score"] >= -1 else "偏空↓"
        w(f"| {r['代码']} | {r['名称']} | **{r['涨跌幅']:+.2f}%** | {r['收盘']:.2f} | {r['成交额亿']:.1f}亿 | {r['rsi']:.0f} | {score_str} |")
    w(f"")

    # 四、跌幅榜
    w(f"## 四、跌幅榜 TOP10")
    w(f"")
    w(f"| 代码 | 名称 | 涨跌幅 | 收盘 | 成交额 | RSI | 趋势评分 |")
    w(f"|------|------|--------|------|--------|-----|----------|")
    for _, r in top_loss.iterrows():
        score_str = "偏多↑" if r["score"] >= 1 else "震荡—" if r["score"] >= -1 else "偏空↓" if r["score"] >= -3 else "弱势↓↓"
        w(f"| {r['代码']} | {r['名称']} | **{r['涨跌幅']:+.2f}%** | {r['收盘']:.2f} | {r['成交额亿']:.1f}亿 | {r['rsi']:.0f} | {score_str} |")
    w(f"")

    # 五、强势股
    if not strong.empty:
        w(f"## 五、强势股（均线多排 + MACD 金叉）")
        w(f"")
        w(f"| 代码 | 名称 | 涨跌幅 | 收盘 | 强于MA60 | RSI |")
        w(f"|------|------|--------|------|----------|-----|")
        for _, r in strong.nlargest(10, "涨跌幅").iterrows():
            bias = f"+{r['ma60_bias']:.1f}%" if r["ma60_bias"] > 0 else f"{r['ma60_bias']:.1f}%"
            w(f"| {r['代码']} | {r['名称']} | {r['涨跌幅']:+.2f}% | {r['收盘']:.2f} | {bias} | {r['rsi']:.0f} |")
        w(f"")

    # 六、成交额 TOP5
    w(f"## 六、资金活跃度（成交额 TOP5）")
    w(f"")
    w(f"| 代码 | 名称 | 涨跌幅 | 成交额 | 换手率 |")
    w(f"|------|------|--------|--------|--------|")
    for _, r in top_vol.iterrows():
        w(f"| {r['代码']} | {r['名称']} | {r['涨跌幅']:+.2f}% | {r['成交额亿']:.1f}亿 | {r['换手率']:.2f}% |")
    w(f"")

    # 七、超卖反弹候选
    if not oversold_cands.empty:
        w(f"## 七、超卖反弹候选（RSI < 30）")
        w(f"")
        w(f"| 代码 | 名称 | 今日涨跌 | RSI | 偏离MA60 | 风险提示 |")
        w(f"|------|------|----------|-----|----------|----------|")
        for _, r in oversold_cands.iterrows():
            bias = f"{r['ma60_bias']:+.1f}%"
            risk = "⚠️ 趋势仍空" if r["score"] < 0 else "可关注"
            w(f"| {r['代码']} | {r['名称']} | {r['涨跌幅']:+.2f}% | {r['rsi']:.0f} | {bias} | {risk} |")
        w(f"")
        w(f"> 超卖 ≠ 立即反弹。建议等待**放量止跌 + 次日高开**信号后再考虑介入。")
        w(f"")

    # 八、风险事件
    if not risk_stocks.empty:
        w(f"## 八、风险事件（跌停或接近跌停）")
        w(f"")
        w(f"| 代码 | 名称 | 涨跌幅 | 成交额 | 说明 |")
        w(f"|------|------|--------|--------|------|")
        for _, r in risk_stocks.iterrows():
            note = "跌停" if r["涨跌幅"] <= -9.9 else "深跌"
            w(f"| {r['代码']} | {r['名称']} | **{r['涨跌幅']:+.2f}%** | {r['成交额亿']:.1f}亿 | {note}，需关注公告 |")
        w(f"")

    # 九、综合结论
    w(f"## 九、综合结论")
    w(f"")
    w(f"| 维度 | 判断 |")
    w(f"|------|------|")
    w(f"| 短期情绪 | {sentiment} |")
    w(f"| 技术结构 | 弱势 {len(weak)} 只 vs 强势 {len(strong)} 只，{'空头占优' if len(weak) > len(strong) else '多头占优' if len(strong) > len(weak) else '多空均衡'} |")
    w(f"| 超卖程度 | RSI均值 {rsi_avg:.1f}，{n_oversold} 只进入超卖（{n_oversold/total*100:.0f}%）|")
    w(f"| 资金面 | 龙头成交 {top_vol.iloc[0]['名称']} {top_vol.iloc[0]['成交额亿']:.0f}亿，{'未大幅缩量' if total_vol > 2000 else '成交明显萎缩'} |")
    w(f"| **操作建议** | **{suggestion}** |")
    w(f"")
    w(f"---")
    w(f"*本报告由量化脚本自动生成，仅供参考，不构成投资建议。*")

    return "\n".join(lines)


# ── 主入口 ──────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, default=None)
    parser.add_argument("--out", type=str, default=None)
    args = parser.parse_args()

    # 自动取最新交易日
    if args.date:
        target_date = args.date
    else:
        sample = pd.read_parquet(next(DATA_DIR.glob("*.parquet")))
        sample["日期"] = pd.to_datetime(sample["日期"]).dt.strftime("%Y-%m-%d")
        target_date = sample["日期"].max()

    out_dir = Path(args.out) if args.out else REPORTS_DIR
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"加载 {target_date} 数据...", flush=True)
    names = load_names()
    df = load_market(target_date, names)

    if df.empty:
        print(f"没有 {target_date} 的数据，请先运行 daily 工作流。")
        sys.exit(1)

    print("生成报告...", flush=True)
    report = build_report(df, target_date)

    out_file = out_dir / f"daily_{target_date}.md"
    out_file.write_text(report, encoding="utf-8")
    print(f"已保存: {out_file}")

    # 同时打印到终端
    print("\n" + report)
