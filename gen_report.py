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


# ── PDF 生成 ─────────────────────────────────────────────────

def _register_cn_font():
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont
    font_candidates = [
        (r"C:\Windows\Fonts\msyh.ttc", 0),   # Microsoft YaHei
        (r"C:\Windows\Fonts\msyhbd.ttc", 0),
        (r"C:\Windows\Fonts\simhei.ttf", None),
    ]
    for path, idx in font_candidates:
        if Path(path).exists():
            kwargs = {"subfontIndex": idx} if idx is not None else {}
            pdfmetrics.registerFont(TTFont("CnFont", path, **kwargs))
            return "CnFont"
    raise RuntimeError("未找到可用的中文字体文件")


def build_pdf(df: pd.DataFrame, target_date: str, out_path: Path) -> None:
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.lib.units import mm
    from reportlab.platypus import (
        HRFlowable, PageBreak, Paragraph, SimpleDocTemplate, Spacer, Table,
        TableStyle,
    )

    font = _register_cn_font()
    W, H = A4

    # ── 样式 ──
    def sty(name, **kw):
        return ParagraphStyle(name, fontName=font, **kw)

    S = {
        "title":   sty("title",   fontSize=20, leading=28, spaceAfter=4, textColor=colors.HexColor("#1a1a2e")),
        "meta":    sty("meta",    fontSize=9,  leading=14, textColor=colors.grey),
        "h1":      sty("h1",      fontSize=13, leading=20, spaceBefore=14, spaceAfter=4,
                        textColor=colors.HexColor("#2c3e50"), borderPadding=(0,0,2,0)),
        "body":    sty("body",    fontSize=9,  leading=15, textColor=colors.HexColor("#333333")),
        "caption": sty("caption", fontSize=8,  leading=12, textColor=colors.grey, spaceAfter=6),
        "warn":    sty("warn",    fontSize=8,  leading=13, textColor=colors.HexColor("#c0392b"),
                        borderColor=colors.HexColor("#e74c3c"), borderWidth=1,
                        borderPadding=4, backColor=colors.HexColor("#fdf0ef")),
        "tip":     sty("tip",     fontSize=8,  leading=13, textColor=colors.HexColor("#1a5276"),
                        borderColor=colors.HexColor("#2980b9"), borderWidth=1,
                        borderPadding=4, backColor=colors.HexColor("#eaf4fb")),
    }

    # ── 表格通用样式 ──
    TBL_HDR = colors.HexColor("#2c3e50")
    TBL_ALT = colors.HexColor("#f2f4f4")

    def tbl_style(n_rows, highlight_col=None, up_col=None, down_col=None, col_data=None):
        base = [
            ("FONTNAME",    (0, 0), (-1, -1), font),
            ("FONTSIZE",    (0, 0), (-1,  0), 8.5),
            ("FONTSIZE",    (0, 1), (-1, -1), 8),
            ("BACKGROUND",  (0, 0), (-1,  0), TBL_HDR),
            ("TEXTCOLOR",   (0, 0), (-1,  0), colors.white),
            ("ALIGN",       (0, 0), (-1, -1), "CENTER"),
            ("VALIGN",      (0, 0), (-1, -1), "MIDDLE"),
            ("ROWBACKGROUND", (0, 1), (-1, -1), [colors.white, TBL_ALT]),
            ("GRID",        (0, 0), (-1, -1), 0.3, colors.HexColor("#cccccc")),
            ("TOPPADDING",  (0, 0), (-1, -1), 4),
            ("BOTTOMPADDING",(0,0), (-1, -1), 4),
        ]
        # 涨跌幅列染色
        if up_col is not None and col_data is not None:
            for i, val in enumerate(col_data, start=1):
                try:
                    v = float(str(val).replace("%", "").replace("+", ""))
                    clr = colors.HexColor("#fdecea") if v > 0 else colors.HexColor("#eafaf1") if v < 0 else colors.white
                    base.append(("BACKGROUND", (up_col, i), (up_col, i), clr))
                except Exception:
                    pass
        return TableStyle(base)

    def section(story, title):
        story.append(Spacer(1, 6))
        story.append(Paragraph(title, S["h1"]))
        story.append(HRFlowable(width="100%", thickness=0.5, color=colors.HexColor("#2c3e50"), spaceAfter=4))

    def kv_table(rows):
        data = [[Paragraph(k, S["body"]), Paragraph(str(v), S["body"])] for k, v in rows]
        t = Table(data, colWidths=[55*mm, W - 30*mm - 55*mm])
        t.setStyle(tbl_style(len(data)))
        return t

    def stock_table(story, headers, rows, chg_col_idx=None):
        hdr = [Paragraph(h, ParagraphStyle("th", fontName=font, fontSize=8.5,
                          textColor=colors.white, alignment=1)) for h in headers]
        body_rows = []
        chg_vals = []
        for r in rows:
            body_rows.append([Paragraph(str(c), S["body"]) for c in r])
            if chg_col_idx is not None:
                chg_vals.append(r[chg_col_idx])
        n_cols = len(headers)
        col_w = (W - 30*mm) / n_cols
        t = Table([hdr] + body_rows, colWidths=[col_w]*n_cols, repeatRows=1)
        t.setStyle(tbl_style(len(body_rows), up_col=chg_col_idx, col_data=chg_vals if chg_vals else None))
        story.append(t)
        story.append(Spacer(1, 4))

    # ── 数据准备 ──
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

    if avg_chg >= 1.0:
        sentiment, suggestion = "乐观偏多", "可适度跟进强势股，注意仓位控制"
    elif avg_chg >= 0:
        sentiment, suggestion = "温和偏多", "关注成交量放大的强势品种，轻仓试多"
    elif avg_chg >= -1.5:
        sentiment, suggestion = "谨慎偏空", "以观望为主，持仓控制在五成以下"
    elif avg_chg >= -3.0:
        sentiment, suggestion = "明显偏空", "观望为主，不追跌；关注超卖龙头企稳信号"
    else:
        sentiment, suggestion = "极度悲观", "空仓观望；超卖个股等放量止跌后再考虑布局"

    # ── 构建文档 ──
    doc = SimpleDocTemplate(
        str(out_path), pagesize=A4,
        leftMargin=15*mm, rightMargin=15*mm,
        topMargin=15*mm, bottomMargin=15*mm,
    )
    story = []

    # 封面
    story.append(Spacer(1, 10*mm))
    story.append(Paragraph(f"市场日报", S["title"]))
    story.append(Paragraph(target_date, ParagraphStyle("date", fontName=font, fontSize=15,
                            textColor=colors.HexColor("#e74c3c"), spaceAfter=2)))
    story.append(Paragraph(
        f"生成时间：{datetime.now().strftime('%Y-%m-%d %H:%M')}　|　"
        f"覆盖股票：{total} 只（IT / 半导体 / 互联网 / 卫星航天 / 有色金属）",
        S["meta"]))
    story.append(HRFlowable(width="100%", thickness=1, color=colors.HexColor("#2c3e50"), spaceBefore=6, spaceAfter=10))

    # 一、市场概况
    section(story, "一、市场概况")
    story.append(kv_table([
        ("上涨 / 下跌 / 平盘", f"{n_up} / {n_down} / {n_flat}"),
        ("涨停 / 跌停", f"{n_limit_up} / {n_limit_down}"),
        ("平均涨跌幅", f"{avg_chg:+.2f}%"),
        ("中位数涨跌幅", f"{med_chg:+.2f}%"),
        ("板块总成交额", f"{total_vol:.1f} 亿元"),
        ("市场情绪", sentiment),
    ]))
    story.append(Spacer(1, 4))
    up_pct = n_up / total * 100
    if up_pct < 20:
        comment = f"上涨面仅 {up_pct:.0f}%，市场极度普跌，恐慌情绪蔓延。"
    elif up_pct < 40:
        comment = f"上涨面 {up_pct:.0f}%，多数个股走弱，做多意愿不足。"
    elif up_pct < 60:
        comment = f"上涨面 {up_pct:.0f}%，多空分歧明显，市场震荡分化。"
    else:
        comment = f"上涨面 {up_pct:.0f}%，普涨格局，市场情绪积极。"
    story.append(Paragraph(f"{comment}平均跌幅与中位数接近（{avg_chg:+.2f}% vs {med_chg:+.2f}%），跌幅分布均匀。", S["body"]))

    # 二、技术面信号
    section(story, "二、技术面信号")
    story.append(kv_table([
        ("强势（均线多排 + MACD金叉）", f"{len(strong)} 只  ({len(strong)/total*100:.1f}%)"),
        ("弱势（均线空排 + MACD死叉）", f"{len(weak)} 只  ({len(weak)/total*100:.1f}%)"),
        ("RSI 均值", f"{rsi_avg:.1f}  ({'接近超卖' if rsi_avg < 40 else '中性' if rsi_avg < 60 else '偏高'})"),
        ("RSI < 30（超卖）", f"{n_oversold} 只  ({n_oversold/total*100:.1f}%)"),
        ("RSI > 70（超买）", f"{n_overbought} 只  ({n_overbought/total*100:.1f}%)"),
    ]))
    if n_oversold / total > 0.25:
        story.append(Spacer(1, 4))
        story.append(Paragraph("超卖个股占比超过 25%，市场存在超跌反弹动能，但需等待放量止跌信号确认。", S["warn"]))

    # 三、涨幅榜
    section(story, "三、涨幅榜 TOP10")
    hdrs = ["代码", "名称", "涨跌幅", "收盘", "成交额", "RSI", "趋势"]
    rows = []
    for _, r in top_gain.iterrows():
        sc = r["score"]
        sig = "强势" if sc >= 3 else "偏多" if sc >= 1 else "震荡" if sc >= -1 else "偏空"
        rows.append([r["代码"], r["名称"], f"{r['涨跌幅']:+.2f}%", f"{r['收盘']:.2f}",
                     f"{r['成交额亿']:.1f}亿", f"{r['rsi']:.0f}", sig])
    stock_table(story, hdrs, rows, chg_col_idx=2)

    # 四、跌幅榜
    section(story, "四、跌幅榜 TOP10")
    rows = []
    for _, r in top_loss.iterrows():
        sc = r["score"]
        sig = "偏多" if sc >= 1 else "震荡" if sc >= -1 else "偏空" if sc >= -3 else "弱势"
        rows.append([r["代码"], r["名称"], f"{r['涨跌幅']:+.2f}%", f"{r['收盘']:.2f}",
                     f"{r['成交额亿']:.1f}亿", f"{r['rsi']:.0f}", sig])
    stock_table(story, hdrs, rows, chg_col_idx=2)

    # 五、强势股
    if not strong.empty:
        section(story, "五、强势股（均线多排 + MACD金叉）")
        hdrs5 = ["代码", "名称", "涨跌幅", "收盘", "强于MA60", "RSI"]
        rows5 = []
        for _, r in strong.nlargest(10, "涨跌幅").iterrows():
            bias = f"+{r['ma60_bias']:.1f}%" if r["ma60_bias"] > 0 else f"{r['ma60_bias']:.1f}%"
            rows5.append([r["代码"], r["名称"], f"{r['涨跌幅']:+.2f}%",
                          f"{r['收盘']:.2f}", bias, f"{r['rsi']:.0f}"])
        stock_table(story, hdrs5, rows5, chg_col_idx=2)

    # 六、资金活跃度
    section(story, "六、资金活跃度（成交额 TOP5）")
    hdrs6 = ["代码", "名称", "涨跌幅", "成交额", "换手率"]
    rows6 = [[r["代码"], r["名称"], f"{r['涨跌幅']:+.2f}%",
              f"{r['成交额亿']:.1f}亿", f"{r['换手率']:.2f}%"] for _, r in top_vol.iterrows()]
    stock_table(story, hdrs6, rows6, chg_col_idx=2)

    # 七、超卖反弹候选
    if not oversold_cands.empty:
        section(story, "七、超卖反弹候选（RSI < 30）")
        hdrs7 = ["代码", "名称", "今日涨跌", "RSI", "偏离MA60", "风险提示"]
        rows7 = []
        for _, r in oversold_cands.iterrows():
            risk = "趋势仍空" if r["score"] < 0 else "可关注"
            rows7.append([r["代码"], r["名称"], f"{r['涨跌幅']:+.2f}%",
                          f"{r['rsi']:.0f}", f"{r['ma60_bias']:+.1f}%", risk])
        stock_table(story, hdrs7, rows7, chg_col_idx=2)
        story.append(Paragraph("超卖 ≠ 立即反弹。建议等待放量止跌 + 次日高开信号后再考虑介入。", S["tip"]))

    # 八、风险事件
    if not risk_stocks.empty:
        section(story, "八、风险事件（跌停）")
        hdrs8 = ["代码", "名称", "涨跌幅", "成交额", "说明"]
        rows8 = [[r["代码"], r["名称"], f"{r['涨跌幅']:+.2f}%",
                  f"{r['成交额亿']:.1f}亿", "跌停，需关注公告"] for _, r in risk_stocks.iterrows()]
        stock_table(story, hdrs8, rows8, chg_col_idx=2)

    # 九、综合结论
    section(story, "九、综合结论")
    strong_weak = "空头占优" if len(weak) > len(strong) else "多头占优" if len(strong) > len(weak) else "多空均衡"
    vol_comment = "未大幅缩量" if total_vol > 2000 else "成交明显萎缩"
    story.append(kv_table([
        ("短期情绪", sentiment),
        ("技术结构", f"弱势 {len(weak)} 只 vs 强势 {len(strong)} 只，{strong_weak}"),
        ("超卖程度", f"RSI均值 {rsi_avg:.1f}，{n_oversold} 只进入超卖（{n_oversold/total*100:.0f}%）"),
        ("资金面", f"龙头 {top_vol.iloc[0]['名称']} 成交 {top_vol.iloc[0]['成交额亿']:.0f}亿，{vol_comment}"),
        ("操作建议", suggestion),
    ]))
    story.append(Spacer(1, 10))
    story.append(HRFlowable(width="100%", thickness=0.5, color=colors.grey, spaceAfter=4))
    story.append(Paragraph("本报告由量化脚本自动生成，仅供参考，不构成投资建议。", S["caption"]))

    doc.build(story)


# ── 主入口 ──────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, default=None)
    parser.add_argument("--out", type=str, default=None)
    parser.add_argument("--no-pdf", action="store_true", help="只生成 Markdown，跳过 PDF")
    args = parser.parse_args()

    # 自动取最新交易日（多取几个文件防止单文件数据缺失）
    if args.date:
        target_date = args.date
    else:
        files = list(DATA_DIR.glob("*.parquet"))
        dates = []
        for f in files[:20]:
            try:
                tmp = pd.read_parquet(f, columns=["日期"])
                tmp["日期"] = pd.to_datetime(tmp["日期"]).dt.strftime("%Y-%m-%d")
                dates.append(tmp["日期"].max())
            except Exception:
                pass
        target_date = max(dates) if dates else None
        if not target_date:
            print("无法读取行情数据，请先运行 daily 工作流。")
            sys.exit(1)

    out_dir = Path(args.out) if args.out else REPORTS_DIR
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"加载 {target_date} 数据...", flush=True)
    names = load_names()
    df = load_market(target_date, names)

    if df.empty:
        print(f"没有 {target_date} 的数据，请先运行 daily 工作流。")
        sys.exit(1)

    print("生成 Markdown 报告...", flush=True)
    report = build_report(df, target_date)
    md_file = out_dir / f"daily_{target_date}.md"
    md_file.write_text(report, encoding="utf-8")
    print(f"已保存: {md_file}")

    if not args.no_pdf:
        print("生成 PDF 报告...", flush=True)
        pdf_file = out_dir / f"daily_{target_date}.pdf"
        build_pdf(df, target_date, pdf_file)
        print(f"已保存: {pdf_file}")
        import subprocess
        subprocess.Popen(["start", "", str(pdf_file)], shell=True)
