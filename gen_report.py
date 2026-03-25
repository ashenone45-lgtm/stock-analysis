"""
日报生成脚本

用法：
  python gen_report.py              # 生成最新交易日报告（Markdown + HTML）
  python gen_report.py --date 2026-03-14
  python gen_report.py --out reports/  # 指定输出目录
  python gen_report.py --no-html       # 只生成 Markdown，跳过 HTML
"""
import argparse
import io
import sys
from datetime import datetime
from pathlib import Path

if getattr(sys.stdout, "encoding", "").lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
if getattr(sys.stderr, "encoding", "").lower() != "utf-8":
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

import numpy as np
import pandas as pd
from tqdm import tqdm

from crawler.config import INDUSTRY_BOARDS
from crawler.report_utils import load_financial_snapshot, prepare_report_context, sector_table as _sector_table

DATA_DIR = Path(__file__).parent / "data" / "market"
NAMES_CSV = Path(__file__).parent / "data" / "stock_names.csv"
INDUSTRIES_CSV = Path(__file__).parent / "data" / "stock_industries.csv"
REPORTS_DIR = Path(__file__).parent / "reports"


# ── 数据加载 ────────────────────────────────────────────────

def load_names() -> dict:
    if not NAMES_CSV.exists():
        return {}
    df = pd.read_csv(NAMES_CSV, dtype=str)
    return dict(zip(df["code"], df["name"]))


def load_industry_map() -> dict:
    """读取 data/stock_industries.csv，返回 {code: industry_label}"""
    if not INDUSTRIES_CSV.exists():
        return {}
    df = pd.read_csv(INDUSTRIES_CSV, dtype=str)
    return dict(zip(df["代码"], df["行业"]))


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
    industry_map = load_industry_map()
    records = []
    parquet_files = list(DATA_DIR.glob("*.parquet"))
    for f in tqdm(parquet_files, desc="加载行情数据", unit="只"):
        df = pd.read_parquet(f)
        df["日期"] = pd.to_datetime(df["日期"]).dt.strftime("%Y-%m-%d")
        df = df.sort_values("日期").reset_index(drop=True)
        today = df[df["日期"] == target_date]
        if today.empty:
            continue
        close = df["收盘"].astype(float)
        n = len(close)
        code = f.stem

        # 量能计算
        vol_series = df["成交量"].astype(float) if "成交量" in df.columns else None
        vol5 = float(vol_series.iloc[-5:].mean()) if vol_series is not None and n >= 5 else np.nan
        vol_today = float(today["成交量"].iloc[0]) if vol_series is not None else np.nan
        vol_ratio = vol_today / vol5 if vol5 > 0 and not np.isnan(vol5) else np.nan

        rec = {
            "代码": code,
            "名称": names.get(code, ""),
            "行业": industry_map.get(code, "其他"),
            "收盘": float(today["收盘"].iloc[0]),
            "涨跌幅": float(today["涨跌幅"].iloc[0]),
            "涨跌额": float(today["涨跌额"].iloc[0]),
            "成交额亿": float(today["成交额"].iloc[0]) / 1e8,
            "换手率": float(today["换手率"].iloc[0]),
            "vol5": vol5,
            "vol_ratio": vol_ratio,
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


def _glossary_section() -> str:
    """返回"看不懂先读这里"的指标说明 Markdown 文本"""
    lines = [
        "## 📖 指标说明（看不懂先读这里）",
        "",
        "| 术语 | 白话解释 |",
        "|------|----------|",
        "| **涨跌幅** | 今天比昨天涨了/跌了多少百分比。+10% 为涨停，-10% 为跌停 |",
        "| **RSI 热度**（0~100） | 衡量股票冷热的指标。**>70 = 涨太快，短期可能回调；<30 = 跌太深，可能出现反弹机会** |",
        "| **量比** | 今日成交量 ÷ 近5日平均成交量。**>2 = 放量（大量资金涌入）；<0.5 = 缩量（市场冷清）；≈1 = 正常** |",
        "| **换手率** | 今天全部股份中有多少比例发生了买卖，越高说明交投越活跃 |",
        "| **均线向上排列** | 5日均线 > 20日均线 > 60日均线，短期涨得比长期快，说明上涨趋势健康 |",
        "| **动能向上（MACD金叉）** | 短期涨势超过长期涨势，是技术派常用的买入信号 |",
        "| **动能向下（MACD死叉）** | 短期涨势跌破长期涨势，是技术派常用的卖出信号 |",
        "| **距60日均线** | 当前价格比过去60个交易日平均价高/低多少。正值=涨破均线，负值=跌破均线 |",
        "| **强势/偏多/震荡/偏空/弱势** | 综合均线+RSI+动能的趋势判断，强势最看涨，弱势最看跌 |",
        "| **营收增速** | 最新季报营业收入 vs 约一年前同期，正值=营收在增长 |",
        "| **负债率** | 总负债 ÷ 总资产，越高说明借债越多，一般超过70%需注意风险 |",
        "",
        "> 本报告仅做信息整理，不构成投资建议。股市有风险，入市需谨慎。",
        "",
    ]
    return "\n".join(lines)


def build_report(df: pd.DataFrame, target_date: str) -> str:
    industries_desc = " / ".join(INDUSTRY_BOARDS.keys())

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
    sector_df = _sector_table(df)

    # 板块平均量比
    avg_vol_ratio = df["vol_ratio"].dropna().mean()

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

    # 情绪 emoji 映射
    sentiment_emoji = {
        "乐观偏多": "🚀", "温和偏多": "📈",
        "谨慎偏空": "😐", "明显偏空": "📉", "极度悲观": "🆘",
    }.get(sentiment, "📊")
    chg_emoji = "🔴" if avg_chg >= 0 else "🟢"   # A股惯例

    w(f"# 📊 市场日报 · {target_date}")
    w(f"")
    w(f"> 生成时间：{datetime.now().strftime('%Y-%m-%d %H:%M')}  ·  覆盖股票 **{total}** 只（{industries_desc}）")
    w(f"")

    # ── 今日速览 ──
    w(f"## 🎯 今日速览")
    w(f"")
    w(f"> {sentiment_emoji} 情绪：**{sentiment}** &nbsp;｜&nbsp; {chg_emoji} 均涨：**{avg_chg:+.2f}%** &nbsp;｜&nbsp; 💰 成交：**{total_vol:.1f} 亿**")
    w(f">")
    w(f"> 📈 上涨 **{n_up}** 只 &nbsp;·&nbsp; 📉 下跌 **{n_down}** 只 &nbsp;·&nbsp; ➖ 平盘 **{n_flat}** 只 &nbsp;｜&nbsp; 🔴 涨停 **{n_limit_up}** 只 &nbsp;·&nbsp; 🟢 跌停 **{n_limit_down}** 只")
    if not sector_df.empty:
        top_sec = sector_df.iloc[0]
        bot_sec = sector_df.iloc[-1]
        w(f">")
        w(f"> 🏆 领涨板块：**{top_sec['行业']}** {top_sec['平均涨跌']:+.2f}% &nbsp;｜&nbsp; 🔻 拖累板块：**{bot_sec['行业']}** {bot_sec['平均涨跌']:+.2f}%")
    w(f"")
    w(f"---")
    w(f"")
    w(_glossary_section())

    # 一、概况
    w(f"## 一、市场概况 📊")
    w(f"")
    w(f"| 指标 | 数值 |")
    w(f"|------|------|")
    w(f"| 上涨 / 下跌 / 平盘 | {n_up} / {n_down} / {n_flat} |")
    w(f"| 涨停 / 跌停 | {n_limit_up} / {n_limit_down} |")
    w(f"| 平均涨跌幅 | **{avg_chg:+.2f}%** |")
    w(f"| 中位数涨跌幅 | {med_chg:+.2f}% |")
    w(f"| 板块总成交额 | {total_vol:.1f} 亿元 |")
    w(f"| 板块平均量比（今日/近5日） | {avg_vol_ratio:.2f} |")
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
    w(f"## 二、技术面信号 🔬")
    w(f"")
    w(f"| 信号 | 数量 | 占比 |")
    w(f"|------|------|------|")
    w(f"| 强势（均线向上排列 + 动能向上） | {len(strong)} 只 | {len(strong)/total*100:.1f}% |")
    w(f"| 弱势（均线向下排列 + 动能向下） | {len(weak)} 只 | {len(weak)/total*100:.1f}% |")
    w(f"| 市场平均热度（RSI） | {rsi_avg:.1f} | {'偏冷，跌得较多' if rsi_avg < 40 else '温度正常' if rsi_avg < 60 else '偏热，涨得较多'} |")
    w(f"| 热度极低（RSI<30，跌得过深，可能反弹） | {n_oversold} 只 | {n_oversold/total*100:.1f}% |")
    w(f"| 热度极高（RSI>70，涨得过快，注意风险） | {n_overbought} 只 | {n_overbought/total*100:.1f}% |")
    w(f"")
    if n_oversold / total > 0.25:
        w(f"> ⚠️ 超过1/4的股票跌得很深（RSI<30），整体存在反弹动能，但需等到出现**放量止跌**的信号再行动。")
    elif n_overbought / total > 0.20:
        w(f"> ⚠️ 超过1/5的股票涨幅过快（RSI>70），短线注意高位回调风险，不要追涨。")
    w(f"")

    # 三、板块表现
    w(f"## 三、板块表现 🏭")
    w(f"")
    if not sector_df.empty:
        w(f"| 行业 | 平均涨跌 | 成交额 | 上涨/下跌 | 强势股 | 平均量比 |")
        w(f"|------|----------|--------|-----------|--------|----------|")
        for _, r in sector_df.iterrows():
            vr = f"{r['平均量比']:.2f}" if not np.isnan(r["平均量比"]) else "—"
            w(f"| {r['行业']} | **{r['平均涨跌']:+.2f}%** | {r['成交额亿']:.1f}亿 | {int(r['上涨数'])}/{int(r['下跌数'])} | {int(r['强势数'])} 只 | {vr} |")
        w(f"")
        top_sec = sector_df.iloc[0]
        bot_sec = sector_df.iloc[-1]
        w(f"> 领涨板块：**{top_sec['行业']}** {top_sec['平均涨跌']:+.2f}%  |  拖累板块：**{bot_sec['行业']}** {bot_sec['平均涨跌']:+.2f}%")
    else:
        w(f"> 暂无行业映射数据，请先运行 `build_stock_pool()` 生成 `data/stock_industries.csv`。")
    w(f"")

    # 三b、板块个股详情
    if not sector_df.empty:
        w(f"## 三b、板块个股详情 🔎")
        w(f"")
        w(f"> 每个板块列出当日涨幅最高的 **3 只**和跌幅最深的 **2 只**，快速定位板块内强弱分化。")
        w(f"")
        for _, sec_row in sector_df.iterrows():
            industry = sec_row["行业"]
            sub = df[df["行业"] == industry].copy()
            if sub.empty:
                continue
            top3 = sub.nlargest(3, "涨跌幅")
            bot2 = sub.nsmallest(2, "涨跌幅")
            highlight = pd.concat([top3, bot2]).drop_duplicates("代码")
            avg_sign = "▲" if sec_row["平均涨跌"] >= 0 else "▼"
            w(f"### {industry} &nbsp; {avg_sign}{sec_row['平均涨跌']:+.2f}% &nbsp; （{int(sec_row['上涨数'])}涨/{int(sec_row['下跌数'])}跌，共{int(sec_row['上涨数']+sec_row['下跌数'])}只）")
            w(f"")
            w(f"| 代码 | 名称 | 涨跌幅 | 收盘价 | 热度RSI | 趋势 |")
            w(f"|------|------|--------|--------|---------|------|")
            for _, r in highlight.iterrows():
                score_str = "强势↑↑" if r["score"] >= 3 else "偏多↑" if r["score"] >= 1 else "震荡—" if r["score"] >= -1 else "偏空↓" if r["score"] >= -3 else "弱势↓↓"
                rsi_note = f"{r['rsi']:.0f}" if not np.isnan(r["rsi"]) else "—"
                w(f"| {r['代码']} | {r['名称']} | **{r['涨跌幅']:+.2f}%** | {r['收盘']:.2f} | {rsi_note} | {score_str} |")
            w(f"")
    w(f"")

    # 四、涨幅榜
    w(f"## 四、涨幅榜 TOP10 🚀")
    w(f"")
    w(f"| 代码 | 名称 | 涨跌幅 | 收盘价 | 成交额 | 热度(RSI) | 趋势判断 |")
    w(f"|------|------|--------|--------|--------|-----------|----------|")
    for _, r in top_gain.iterrows():
        score_str = "强势" if r["score"] >= 3 else "偏多" if r["score"] >= 1 else "震荡" if r["score"] >= -1 else "偏空"
        rsi_note = f"{r['rsi']:.0f}{'🔥' if r['rsi'] > 70 else ''}"
        w(f"| {r['代码']} | {r['名称']} | **{r['涨跌幅']:+.2f}%** | {r['收盘']:.2f} | {r['成交额亿']:.1f}亿 | {rsi_note} | {score_str} |")
    w(f"")

    # 五、跌幅榜
    w(f"## 五、跌幅榜 TOP10 📉")
    w(f"")
    w(f"| 代码 | 名称 | 涨跌幅 | 收盘价 | 成交额 | 热度(RSI) | 趋势判断 |")
    w(f"|------|------|--------|--------|--------|-----------|----------|")
    for _, r in top_loss.iterrows():
        score_str = "偏多" if r["score"] >= 1 else "震荡" if r["score"] >= -1 else "偏空" if r["score"] >= -3 else "弱势"
        rsi_note = f"{r['rsi']:.0f}{'❄️' if r['rsi'] < 30 else ''}"
        w(f"| {r['代码']} | {r['名称']} | **{r['涨跌幅']:+.2f}%** | {r['收盘']:.2f} | {r['成交额亿']:.1f}亿 | {rsi_note} | {score_str} |")
    w(f"")

    # 六、强势股（含财务数据）
    if not strong.empty:
        strong_top = strong.nlargest(10, "涨跌幅")
        fin_data = load_financial_snapshot(list(strong_top["代码"]))
        w(f"## 六、强势股 ⚡（均线向上排列 + 动能向上）")
        w(f"")
        w(f"> 这些股票同时满足：短中长均线向上排列（趋势健康）+ 动能指标向上（买入信号），是当前市场中技术面最强的品种。")
        w(f"")
        w(f"| 代码 | 名称 | 涨跌幅 | 收盘价 | 距60日均线 | 热度(RSI) | 营收增速 |")
        w(f"|------|------|--------|--------|------------|-----------|----------|")
        for _, r in strong_top.iterrows():
            bias = f"+{r['ma60_bias']:.1f}%" if r["ma60_bias"] > 0 else f"{r['ma60_bias']:.1f}%"
            fin = fin_data.get(r["代码"], {})
            rev_yoy = f"{fin['rev_yoy']:+.1f}%" if "rev_yoy" in fin else "—"
            w(f"| {r['代码']} | {r['名称']} | {r['涨跌幅']:+.2f}% | {r['收盘']:.2f} | {bias} | {r['rsi']:.0f} | {rev_yoy} |")
        w(f"")

    # 七、资金活跃度（含量比）
    w(f"## 七、资金活跃度 💰（成交额 TOP5）")
    w(f"")
    w(f"> **量比** = 今日成交量 ÷ 近5日均量。放量(>2x)说明大量资金涌入；缩量(<0.5x)说明市场冷清。")
    w(f"")
    w(f"| 代码 | 名称 | 涨跌幅 | 成交额 | 换手率 | 量比 | 资金状态 |")
    w(f"|------|------|--------|--------|--------|------|----------|")
    for _, r in top_vol.iterrows():
        vr = r["vol_ratio"]
        if np.isnan(vr):
            vr_str, vr_status = "—", "—"
        elif vr > 2:
            vr_str, vr_status = f"{vr:.2f}", "放量"
        elif vr < 0.5:
            vr_str, vr_status = f"{vr:.2f}", "缩量"
        else:
            vr_str, vr_status = f"{vr:.2f}", "正常"
        w(f"| {r['代码']} | {r['名称']} | {r['涨跌幅']:+.2f}% | {r['成交额亿']:.1f}亿 | {r['换手率']:.2f}% | {vr_str} | {vr_status} |")
    w(f"")

    # 八、超跌候选（含负债率）
    if not oversold_cands.empty:
        fin_data2 = load_financial_snapshot(list(oversold_cands["代码"]))
        w(f"## 八、超跌候选 🔍（热度极低，可能存在反弹机会）")
        w(f"")
        w(f"> 这些股票 RSI < 30，意味着跌得比较深，历史上此类情况常出现反弹。但**跌深不等于一定反弹**，建议等到出现**当天放量 + 第二天高开**的信号再考虑。")
        w(f"")
        w(f"| 代码 | 名称 | 今日涨跌 | 热度(RSI) | 距60日均线 | 负债率 | 提示 |")
        w(f"|------|------|----------|-----------|------------|--------|------|")
        for _, r in oversold_cands.iterrows():
            bias = f"{r['ma60_bias']:+.1f}%"
            risk = "⚠️ 趋势仍向下" if r["score"] < 0 else "可观察"
            fin = fin_data2.get(r["代码"], {})
            debt = f"{fin['debt_ratio']:.1f}%" if "debt_ratio" in fin else "—"
            w(f"| {r['代码']} | {r['名称']} | {r['涨跌幅']:+.2f}% | {r['rsi']:.0f} | {bias} | {debt} | {risk} |")
        w(f"")

    # 九、风险事件
    if not risk_stocks.empty:
        w(f"## 九、风险事件 ⚠️（跌停或接近跌停）")
        w(f"")
        w(f"| 代码 | 名称 | 涨跌幅 | 成交额 | 说明 |")
        w(f"|------|------|--------|--------|------|")
        for _, r in risk_stocks.iterrows():
            note = "跌停" if r["涨跌幅"] <= -9.9 else "深跌"
            w(f"| {r['代码']} | {r['名称']} | **{r['涨跌幅']:+.2f}%** | {r['成交额亿']:.1f}亿 | {note}，需关注公告 |")
        w(f"")

    # 十、综合结论
    w(f"## 十、综合结论 📋")
    w(f"")
    w(f"| 维度 | 判断 |")
    w(f"|------|------|")
    w(f"| 短期情绪 | {sentiment} |")
    w(f"| 趋势结构 | 弱势 {len(weak)} 只 vs 强势 {len(strong)} 只，{'空头偏多' if len(weak) > len(strong) else '多头偏多' if len(strong) > len(weak) else '多空均衡'} |")
    w(f"| 整体热度 | 市场平均RSI {rsi_avg:.1f}，{n_oversold} 只跌得过深（RSI<30，占{n_oversold/total*100:.0f}%）|")
    w(f"| 资金面 | 龙头成交 {top_vol.iloc[0]['名称']} {top_vol.iloc[0]['成交额亿']:.0f}亿，{'未大幅缩量' if total_vol > 2000 else '成交明显萎缩'} |")
    if not sector_df.empty:
        top_sec = sector_df.iloc[0]
        w(f"| 板块领涨 | {top_sec['行业']} {top_sec['平均涨跌']:+.2f}% |")
    w(f"| **操作建议** | **{suggestion}** |")
    w(f"")
    w(f"---")
    w(f"*本报告由量化脚本自动生成，仅供参考，不构成投资建议。*")

    return "\n".join(lines)


# ── 工具函数 ─────────────────────────────────────────────────

def _available_dates() -> list[str]:
    """扫描 parquet 文件，返回所有有数据的交易日列表（升序）。"""
    seen: set[str] = set()
    for f in DATA_DIR.glob("*.parquet"):
        try:
            tmp = pd.read_parquet(f, columns=["日期"])
            tmp["日期"] = pd.to_datetime(tmp["日期"]).dt.strftime("%Y-%m-%d")
            seen.update(tmp["日期"].tolist())
        except Exception:
            pass
    return sorted(seen)


def _generate_one(target_date: str, out_dir: Path, no_html: bool, open_after: bool) -> bool:
    """生成单日报告。返回 True 表示成功。"""
    print(f"\n{'='*50}", flush=True)
    print(f"生成 {target_date} 报告...", flush=True)
    names = load_names()
    df = load_market(target_date, names)
    if df.empty:
        print(f"[!] 没有 {target_date} 的数据，跳过。")
        return False

    report = build_report(df, target_date)
    md_file = out_dir / f"daily_{target_date}.md"
    md_file.write_text(report, encoding="utf-8")
    print(f"已保存: {md_file}")

    if not no_html:
        from report_html import build_html
        from gen_index import update_manifest_and_index, get_prev_date
        manifest_path = out_dir / "manifest.json"
        prev_date = get_prev_date(manifest_path, before_date=target_date)
        html_file = out_dir / f"daily_{target_date}.html"
        build_html(df, target_date, html_file, prev_date=prev_date)
        print(f"已保存: {html_file}")
        ctx = prepare_report_context(df)
        update_manifest_and_index(
            ctx=ctx,
            target_date=target_date,
            reports_dir=out_dir,
            index_path=Path(__file__).parent / "index.html",
        )
        if open_after:
            import os
            os.startfile(str(html_file))
    return True


# ── 主入口 ──────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="生成 A股日报（Markdown + HTML）")
    parser.add_argument("--date", type=str, default=None,
                        help="指定单个日期，如 2026-03-24")
    parser.add_argument("--dates", type=str, nargs="+", metavar="DATE",
                        help="指定多个日期，如 --dates 2026-03-18 2026-03-19 2026-03-24")
    parser.add_argument("--date-range", type=str, nargs=2, metavar=("START", "END"),
                        help="指定日期范围，如 --date-range 2026-03-01 2026-03-24（自动取范围内有数据的交易日）")
    parser.add_argument("--out", type=str, default=None)
    parser.add_argument("--no-html", action="store_true", help="只生成 Markdown，跳过 HTML")
    parser.add_argument("--require-today", action="store_true",
                        help="若数据日期不是今天则报错退出（用于定时任务校验）")
    args = parser.parse_args()

    out_dir = Path(args.out) if args.out else REPORTS_DIR
    out_dir.mkdir(parents=True, exist_ok=True)

    # ── 确定要生成的日期列表 ──
    if args.dates:
        target_dates = sorted(set(args.dates))
    elif args.date_range:
        start, end = args.date_range
        all_dates = _available_dates()
        target_dates = [d for d in all_dates if start <= d <= end]
        if not target_dates:
            print(f"范围 {start} ~ {end} 内无可用数据。")
            sys.exit(1)
        print(f"范围 {start} ~ {end} 内找到 {len(target_dates)} 个交易日：{target_dates}")
    elif args.date:
        target_dates = [args.date]
    else:
        # 自动取最新交易日
        files = list(DATA_DIR.glob("*.parquet"))
        dates = []
        for f in files[:20]:
            try:
                tmp = pd.read_parquet(f, columns=["日期"])
                tmp["日期"] = pd.to_datetime(tmp["日期"]).dt.strftime("%Y-%m-%d")
                dates.append(tmp["日期"].max())
            except Exception:
                pass
        latest = max(dates) if dates else None
        if not latest:
            print("无法读取行情数据，请先运行 daily 工作流。")
            sys.exit(1)
        target_dates = [latest]

    # ── 单日模式：保留原有日期检查逻辑 ──
    if len(target_dates) == 1:
        today_str = datetime.now().strftime("%Y-%m-%d")
        if target_dates[0] == today_str:
            print(f"[✓] 数据日期：{target_dates[0]}（今天）", flush=True)
        else:
            print(f"[!] 注意：将使用 {target_dates[0]} 的数据。", flush=True)
        if args.require_today and target_dates[0] != today_str:
            sys.exit(1)

    # ── 逐日生成 ──
    ok, fail = 0, 0
    for i, d in enumerate(target_dates):
        open_browser = sys.stdout.isatty() and len(target_dates) == 1
        if _generate_one(d, out_dir, args.no_html, open_after=open_browser):
            ok += 1
        else:
            fail += 1

    if len(target_dates) > 1:
        print(f"\n完成：成功 {ok} 份，跳过 {fail} 份。", flush=True)
