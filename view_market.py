"""
市场概览与趋势扫描

用法：
  python view_market.py              # 今日涨跌榜 + 趋势信号（全部股票）
  python view_market.py --top 20     # 只显示涨幅/跌幅前N名
  python view_market.py --date 2026-03-14  # 指定日期
"""
import argparse
import io
import sys
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import numpy as np
import pandas as pd

DATA_DIR = Path(__file__).parent / "data" / "market"
NAMES_CSV = Path(__file__).parent / "data" / "stock_names.csv"

matplotlib.rcParams["font.sans-serif"] = ["Microsoft YaHei", "SimHei"]
matplotlib.rcParams["font.family"] = "sans-serif"
matplotlib.rcParams["axes.unicode_minus"] = False


def load_names() -> dict:
    """加载股票代码→名称映射，文件不存在时返回空字典。"""
    if not NAMES_CSV.exists():
        return {}
    df = pd.read_csv(NAMES_CSV, dtype=str)
    return dict(zip(df["code"], df["name"]))


# ── 技术指标计算 ────────────────────────────────────────────

def calc_rsi(close: pd.Series, period: int = 14) -> float:
    delta = close.diff().dropna()
    if len(delta) < period:
        return float("nan")
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    rs = gain / loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    return round(float(rsi.iloc[-1]), 1)


def calc_macd(close: pd.Series):
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    macd = ema12 - ema26
    signal = macd.ewm(span=9, adjust=False).mean()
    hist = macd - signal
    return float(macd.iloc[-1]), float(signal.iloc[-1]), float(hist.iloc[-1])


def trend_signal(row: dict) -> str:
    """综合 MA + RSI + MACD 输出趋势信号。"""
    score = 0
    reasons = []

    # MA 多头排列
    if row["ma5"] > row["ma20"] > row["ma60"]:
        score += 2
        reasons.append("均线多排")
    elif row["ma5"] > row["ma20"]:
        score += 1
        reasons.append("短线向上")
    elif row["ma5"] < row["ma20"] < row["ma60"]:
        score -= 2
        reasons.append("均线空排")
    elif row["ma5"] < row["ma20"]:
        score -= 1
        reasons.append("短线向下")

    # RSI
    rsi = row["rsi"]
    if not np.isnan(rsi):
        if rsi > 70:
            score -= 1
            reasons.append(f"RSI超买{rsi:.0f}")
        elif rsi < 30:
            score += 1
            reasons.append(f"RSI超卖{rsi:.0f}")

    # MACD 金死叉
    if row["macd_hist"] > 0:
        score += 1
        reasons.append("MACD金叉")
    else:
        score -= 1
        reasons.append("MACD死叉")

    # 价格位置（相对MA60）
    price_vs_ma60 = (row["收盘"] - row["ma60"]) / row["ma60"] * 100
    if price_vs_ma60 > 5:
        reasons.append(f"强于MA60 +{price_vs_ma60:.1f}%")
    elif price_vs_ma60 < -5:
        reasons.append(f"弱于MA60 {price_vs_ma60:.1f}%")

    if score >= 3:
        label = "强势↑↑"
    elif score >= 1:
        label = "偏多↑"
    elif score <= -3:
        label = "弱势↓↓"
    elif score <= -1:
        label = "偏空↓"
    else:
        label = "震荡—"

    return label, score, " | ".join(reasons[:3])


# ── 数据加载 ────────────────────────────────────────────────

def load_all(target_date: str) -> pd.DataFrame:
    names = load_names()
    records = []
    for f in DATA_DIR.glob("*.parquet"):
        df = pd.read_parquet(f)
        df["日期"] = pd.to_datetime(df["日期"]).dt.strftime("%Y-%m-%d")
        df = df.sort_values("日期").reset_index(drop=True)

        today_rows = df[df["日期"] == target_date]
        if today_rows.empty:
            continue

        close = df["收盘"].astype(float)
        n = len(close)

        code = f.stem
        rec = {
            "代码": code,
            "名称": names.get(code, ""),
            "日期": target_date,
            "开盘": float(today_rows["开盘"].iloc[0]),
            "收盘": float(today_rows["收盘"].iloc[0]),
            "涨跌幅": float(today_rows["涨跌幅"].iloc[0]),
            "涨跌额": float(today_rows["涨跌额"].iloc[0]),
            "成交额": float(today_rows["成交额"].iloc[0]) / 1e8,  # 亿元
            "换手率": float(today_rows["换手率"].iloc[0]),
            "ma5":  round(float(close.iloc[-5:].mean()), 2) if n >= 5 else float("nan"),
            "ma20": round(float(close.iloc[-20:].mean()), 2) if n >= 20 else float("nan"),
            "ma60": round(float(close.iloc[-60:].mean()), 2) if n >= 60 else float("nan"),
            "rsi":  calc_rsi(close),
        }
        macd, signal, hist = calc_macd(close)
        rec["macd"] = round(macd, 4)
        rec["macd_signal"] = round(signal, 4)
        rec["macd_hist"] = round(hist, 4)

        label, score, reason = trend_signal(rec)
        rec["趋势"] = label
        rec["分数"] = score
        rec["信号"] = reason

        records.append(rec)

    return pd.DataFrame(records)


# ── 可视化 ──────────────────────────────────────────────────

def plot_summary(df: pd.DataFrame, target_date: str) -> Path:
    gainers = df.nlargest(15, "涨跌幅")
    losers = df.nsmallest(15, "涨跌幅")

    # 趋势分布
    trend_counts = df["趋势"].value_counts()
    order = ["强势↑↑", "偏多↑", "震荡—", "偏空↓", "弱势↓↓"]
    trend_counts = trend_counts.reindex([o for o in order if o in trend_counts.index])

    fig = plt.figure(figsize=(18, 14))
    fig.suptitle(f"市场概览  {target_date}  （{len(df)} 只股票）", fontsize=16, fontweight="bold", y=0.98)
    gs = gridspec.GridSpec(3, 2, figure=fig, hspace=0.45, wspace=0.3)

    def label(row):
        name = row["名称"]
        return f"{row['代码']} {name}" if name else row["代码"]

    # 涨幅榜
    ax1 = fig.add_subplot(gs[0, 0])
    g_labels = gainers.apply(label, axis=1).tolist()[::-1]
    bars = ax1.barh(g_labels, gainers["涨跌幅"].tolist()[::-1], color="#e74c3c")
    ax1.set_title("涨幅榜 TOP15", fontsize=11)
    ax1.set_xlabel("涨跌幅 (%)")
    ax1.tick_params(axis="y", labelsize=8)
    for bar, val in zip(bars, gainers["涨跌幅"].tolist()[::-1]):
        ax1.text(bar.get_width() + 0.05, bar.get_y() + bar.get_height() / 2,
                 f"{val:+.2f}%", va="center", fontsize=7)

    # 跌幅榜
    ax2 = fig.add_subplot(gs[0, 1])
    l_labels = losers.apply(label, axis=1).tolist()
    bars2 = ax2.barh(l_labels, losers["涨跌幅"].tolist(), color="#2ecc71")
    ax2.set_title("跌幅榜 TOP15", fontsize=11)
    ax2.set_xlabel("涨跌幅 (%)")
    ax2.tick_params(axis="y", labelsize=8)
    for bar, val in zip(bars2, losers["涨跌幅"].tolist()):
        ax2.text(bar.get_width() - 0.05, bar.get_y() + bar.get_height() / 2,
                 f"{val:+.2f}%", va="center", ha="right", fontsize=7)

    # 趋势分布饼图
    ax3 = fig.add_subplot(gs[1, 0])
    colors_pie = ["#e74c3c", "#e67e22", "#95a5a6", "#3498db", "#2ecc71"]
    wedges, texts, autotexts = ax3.pie(
        trend_counts.values,
        labels=trend_counts.index,
        autopct="%1.0f%%",
        colors=colors_pie[:len(trend_counts)],
        startangle=90,
    )
    ax3.set_title("趋势信号分布", fontsize=11)

    # 涨跌幅分布直方图
    ax4 = fig.add_subplot(gs[1, 1])
    pct = df["涨跌幅"].dropna()
    bins = np.linspace(pct.quantile(0.02), pct.quantile(0.98), 40)
    n_pos = (pct > 0).sum()
    n_neg = (pct < 0).sum()
    n_flat = (pct == 0).sum()
    ax4.hist(pct[pct > 0], bins=bins, color="#e74c3c", alpha=0.7, label=f"上涨 {n_pos}")
    ax4.hist(pct[pct < 0], bins=bins, color="#2ecc71", alpha=0.7, label=f"下跌 {n_neg}")
    ax4.hist(pct[pct == 0], bins=bins, color="#95a5a6", alpha=0.7, label=f"平盘 {n_flat}")
    ax4.axvline(0, color="black", linewidth=0.8, linestyle="--")
    ax4.set_title("涨跌幅分布", fontsize=11)
    ax4.set_xlabel("涨跌幅 (%)")
    ax4.set_ylabel("股票数")
    ax4.legend(fontsize=8)

    # RSI 分布
    ax5 = fig.add_subplot(gs[2, 0])
    rsi_vals = df["rsi"].dropna()
    ax5.hist(rsi_vals, bins=30, color="#9b59b6", alpha=0.8)
    ax5.axvline(30, color="green", linestyle="--", linewidth=1, label="超卖 30")
    ax5.axvline(70, color="red", linestyle="--", linewidth=1, label="超买 70")
    ax5.set_title(f"RSI 分布  (均值 {rsi_vals.mean():.1f})", fontsize=11)
    ax5.set_xlabel("RSI")
    ax5.set_ylabel("股票数")
    ax5.legend(fontsize=8)

    # 强势股 TOP10 文字表
    ax6 = fig.add_subplot(gs[2, 1])
    ax6.axis("off")
    strong = df[df["分数"] >= 3].nlargest(10, "涨跌幅")[["代码", "名称", "收盘", "涨跌幅", "rsi", "趋势"]]
    if not strong.empty:
        table_data = [[r["代码"], r["名称"], f"{r['收盘']:.2f}", f"{r['涨跌幅']:+.2f}%",
                       f"{r['rsi']:.0f}", r["趋势"]] for _, r in strong.iterrows()]
        tbl = ax6.table(
            cellText=table_data,
            colLabels=["代码", "名称", "收盘", "涨跌幅", "RSI", "趋势"],
            loc="center", cellLoc="center",
        )
        tbl.auto_set_font_size(False)
        tbl.set_fontsize(9)
        tbl.scale(1, 1.4)
        ax6.set_title("强势股（多头排列+MACD金叉）", fontsize=11)
    else:
        ax6.text(0.5, 0.5, "暂无强势股信号", ha="center", va="center", fontsize=12)
        ax6.set_title("强势股", fontsize=11)

    out = Path(__file__).parent / f"market_{target_date}.png"
    fig.savefig(str(out), dpi=120, bbox_inches="tight")
    plt.close(fig)
    return out


# ── 终端输出 ────────────────────────────────────────────────

def print_table(df: pd.DataFrame, top: int) -> None:
    up = df[df["涨跌幅"] > 0].nlargest(top, "涨跌幅")
    down = df[df["涨跌幅"] < 0].nsmallest(top, "涨跌幅")

    total = len(df)
    n_up = (df["涨跌幅"] > 0).sum()
    n_down = (df["涨跌幅"] < 0).sum()
    n_flat = total - n_up - n_down
    avg = df["涨跌幅"].mean()

    print(f"\n{'='*60}")
    print(f"  市场概览  {df['日期'].iloc[0]}  共 {total} 只")
    print(f"  上涨 {n_up} | 下跌 {n_down} | 平盘 {n_flat} | 平均 {avg:+.2f}%")
    print(f"{'='*60}")

    def fmt(r):
        name = r["名称"] if r["名称"] else "----"
        return f"  {r['代码']} {name:<6} {r['收盘']:>7.2f} {r['涨跌幅']:>+7.2f}% " \
               f"{r['rsi']:>6.0f} {r['成交额']:>10.2f} {r['趋势']:6} {r['信号']}"

    print(f"\n【涨幅榜 TOP{top}】")
    print(f"  {'代码':6} {'名称':<6} {'收盘':>7} {'涨跌幅':>8} {'RSI':>6} {'成交额(亿)':>10} {'趋势':6} 信号")
    print(f"  {'-'*76}")
    for _, r in up.iterrows():
        print(fmt(r))

    print(f"\n【跌幅榜 TOP{top}】")
    print(f"  {'代码':6} {'名称':<6} {'收盘':>7} {'涨跌幅':>8} {'RSI':>6} {'成交额(亿)':>10} {'趋势':6} 信号")
    print(f"  {'-'*76}")
    for _, r in down.iterrows():
        print(fmt(r))

    # 强势信号
    strong = df[df["分数"] >= 3].sort_values("涨跌幅", ascending=False)
    weak = df[df["分数"] <= -3].sort_values("涨跌幅")
    print(f"\n【强势信号（多头排列+MACD金叉）: {len(strong)} 只】")
    for _, r in strong.head(10).iterrows():
        name = r["名称"] if r["名称"] else ""
        print(f"  {r['代码']} {name:<6}  {r['收盘']:.2f}  {r['涨跌幅']:+.2f}%  {r['信号']}")
    print(f"\n【弱势信号（空头排列+MACD死叉）: {len(weak)} 只】")
    for _, r in weak.head(10).iterrows():
        name = r["名称"] if r["名称"] else ""
        print(f"  {r['代码']} {name:<6}  {r['收盘']:.2f}  {r['涨跌幅']:+.2f}%  {r['信号']}")


# ── 主入口 ──────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--top", type=int, default=10, help="涨跌榜显示数量")
    parser.add_argument("--date", type=str, default=None, help="指定日期 YYYY-MM-DD")
    args = parser.parse_args()

    # 自动取最新交易日
    if args.date:
        target_date = args.date
    else:
        sample = pd.read_parquet(next(DATA_DIR.glob("*.parquet")))
        sample["日期"] = pd.to_datetime(sample["日期"]).dt.strftime("%Y-%m-%d")
        target_date = sample["日期"].max()

    print(f"加载 {target_date} 数据中...", flush=True)
    df = load_all(target_date)

    if df.empty:
        print(f"没有找到 {target_date} 的数据，请先运行 daily 工作流。")
        sys.exit(1)

    print_table(df, args.top)

    print(f"\n生成图表中...", flush=True)
    out = plot_summary(df, target_date)
    print(f"已保存: {out}")

    import subprocess
    subprocess.Popen(["start", "", str(out)], shell=True)
