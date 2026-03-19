"""
PDF 报告生成模块

由 gen_report.py 调用：
    from report_pdf import build_pdf
    build_pdf(df, target_date, out_path)
"""
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd


def _register_cn_font():
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont
    font_candidates = [
        (r"C:\Windows\Fonts\msyh.ttc", 0),
        (r"C:\Windows\Fonts\msyhbd.ttc", 0),
        (r"C:\Windows\Fonts\simhei.ttf", None),
    ]
    for path, idx in font_candidates:
        if Path(path).exists():
            kwargs = {"subfontIndex": idx} if idx is not None else {}
            pdfmetrics.registerFont(TTFont("CnFont", path, **kwargs))
            return "CnFont"
    raise RuntimeError("未找到可用的中文字体文件")


def _draw_sector_chart(sector_df, font_name, total_width):
    """绘制板块涨跌横向柱状图，返回 ReportLab Drawing 对象"""
    from reportlab.graphics.shapes import Drawing, Rect, String, Line
    from reportlab.lib.colors import HexColor

    n = len(sector_df)
    if n == 0:
        return None

    bar_h = 15
    gap = 7
    label_w = 58   # 行业名称区域宽度（points）
    val_w = 40     # 数值标签区域宽度
    bar_area = total_width - label_w - val_w
    chart_h = n * (bar_h + gap) + gap * 2

    d = Drawing(total_width, chart_h)
    max_abs = max(abs(sector_df["平均涨跌"]).max(), 0.1)
    mid_x = label_w + bar_area / 2

    # 背景斑马纹
    for i in range(n):
        y = chart_h - gap - (i + 1) * (bar_h + gap)
        if i % 2 == 0:
            d.add(Rect(0, y - gap / 2, total_width, bar_h + gap,
                       fillColor=HexColor("#f9f9f9"), strokeColor=None))

    # 中心轴
    d.add(Line(mid_x, 0, mid_x, chart_h,
               strokeColor=HexColor("#aaaaaa"), strokeWidth=0.6))

    for i, (_, row) in enumerate(sector_df.iterrows()):
        y = chart_h - gap - (i + 1) * (bar_h + gap)
        val = row["平均涨跌"]
        bar_w = abs(val) / max_abs * (bar_area / 2 - 4)

        # A股惯例：红涨绿跌
        bar_color = HexColor("#e74c3c") if val >= 0 else HexColor("#27ae60")

        if val >= 0:
            d.add(Rect(mid_x, y + 2, max(bar_w, 1), bar_h - 4,
                       fillColor=bar_color, strokeColor=None))
        else:
            d.add(Rect(mid_x - bar_w, y + 2, max(bar_w, 1), bar_h - 4,
                       fillColor=bar_color, strokeColor=None))

        # 行业标签（右对齐）
        d.add(String(label_w - 4, y + bar_h / 2 - 4, row["行业"],
                     fontSize=8, fontName=font_name, textAnchor="end",
                     fillColor=HexColor("#333333")))

        # 数值标签
        pct_str = f"{val:+.2f}%"
        val_color = HexColor("#b03030") if val >= 0 else HexColor("#1a7a3a")
        if val >= 0:
            d.add(String(mid_x + bar_w + 4, y + bar_h / 2 - 4, pct_str,
                         fontSize=8, fontName=font_name, textAnchor="start",
                         fillColor=val_color))
        else:
            d.add(String(mid_x - bar_w - 4, y + bar_h / 2 - 4, pct_str,
                         fontSize=8, fontName=font_name, textAnchor="end",
                         fillColor=val_color))

    return d


def build_pdf(df: pd.DataFrame, target_date: str, out_path: Path) -> None:
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.lib.units import mm
    from reportlab.platypus import (
        HRFlowable, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle,
    )

    font = _register_cn_font()
    W, H = A4
    BODY_W = W - 30 * mm  # 正文宽度

    # ── 颜色常量 ──
    C = {
        "navy":    colors.HexColor("#1a1a2e"),
        "red":     colors.HexColor("#e74c3c"),
        "green":   colors.HexColor("#27ae60"),
        "blue":    colors.HexColor("#2980b9"),
        "orange":  colors.HexColor("#e67e22"),
        "purple":  colors.HexColor("#8e44ad"),
        "grey":    colors.HexColor("#7f8c8d"),
        "light":   colors.HexColor("#f8f9fa"),
        "alt":     colors.HexColor("#eef1f5"),
        "white":   colors.white,
        "hdr_bg":  colors.HexColor("#2c3e50"),
        "red_bg":  colors.HexColor("#fdecea"),
        "grn_bg":  colors.HexColor("#eafaf1"),
    }

    # ── 样式工厂 ──
    def sty(name, **kw):
        return ParagraphStyle(name, fontName=font, **kw)

    S = {
        "h1":      sty("h1",   fontSize=11, leading=16, textColor=C["navy"]),
        "body":    sty("body", fontSize=9,  leading=15, textColor=colors.HexColor("#333333")),
        "small":   sty("sm",   fontSize=8,  leading=12, textColor=C["grey"]),
        "caption": sty("cap",  fontSize=7.5,leading=11, textColor=C["grey"]),
        "warn":    sty("warn", fontSize=8,  leading=13, textColor=C["red"],
                       borderColor=C["red"], borderWidth=1, borderPadding=4,
                       backColor=C["red_bg"]),
        "tip":     sty("tip",  fontSize=8,  leading=13, textColor=colors.HexColor("#1a5276"),
                       borderColor=C["blue"], borderWidth=1, borderPadding=4,
                       backColor=colors.HexColor("#eaf4fb")),
    }

    # ── 章节标题（彩色左边框 + 浅色背景）──
    def section(story, title, accent_color=None):
        clr = accent_color or C["red"]
        story.append(Spacer(1, 8))
        accent_w = 4   # 左边彩色条宽度
        row = [[
            Paragraph("", S["body"]),
            Paragraph(title, sty(f"h_{title[:4]}", fontSize=11, leading=16,
                                  textColor=C["navy"])),
        ]]
        t = Table(row, colWidths=[accent_w, BODY_W - accent_w], rowHeights=[16])
        t.setStyle(TableStyle([
            ("BACKGROUND",     (0, 0), (0, -1), clr),
            ("BACKGROUND",     (1, 0), (1, -1), C["alt"]),
            ("LEFTPADDING",    (0, 0), (0, -1), 0),   # 彩色条无内边距
            ("RIGHTPADDING",   (0, 0), (0, -1), 0),
            ("LEFTPADDING",    (1, 0), (1, -1), 8),
            ("TOPPADDING",     (0, 0), (-1, -1), 3),
            ("BOTTOMPADDING",  (0, 0), (-1, -1), 3),
            ("FONTNAME",       (0, 0), (-1, -1), font),
        ]))
        story.append(t)
        story.append(Spacer(1, 4))

    # ── KV 键值表 ──
    def kv_table(rows, key_col_w=58*mm):
        hdr_bg  = C["hdr_bg"]
        alt_bg  = C["alt"]
        data = []
        styles = [
            ("FONTNAME",       (0, 0), (-1, -1), font),
            ("FONTSIZE",       (0, 0), (-1, -1), 8.5),
            ("TOPPADDING",     (0, 0), (-1, -1), 4),
            ("BOTTOMPADDING",  (0, 0), (-1, -1), 4),
            ("GRID",           (0, 0), (-1, -1), 0.3, colors.HexColor("#dddddd")),
            ("BACKGROUND",     (0, 0), (0, -1), colors.HexColor("#ecf0f1")),
            ("TEXTCOLOR",      (0, 0), (0, -1), C["navy"]),
            ("ROWBACKGROUND",  (0, 0), (-1, -1), [C["white"], alt_bg]),
        ]
        for k, v in rows:
            data.append([Paragraph(k, S["body"]), Paragraph(str(v), S["body"])])
        t = Table(data, colWidths=[key_col_w, BODY_W - key_col_w])
        t.setStyle(TableStyle(styles))
        return t

    # ── 彩色指标卡片行 ──
    def metric_cards(story, items):
        """items = [(label, value_str, text_color), ...]"""
        n = len(items)
        cw = BODY_W / n
        label_row = [Paragraph(lab, sty(f"ml{i}", fontSize=7.5, textColor=C["grey"], alignment=1))
                     for i, (lab, _, _) in enumerate(items)]
        value_row = [Paragraph(val, sty(f"mv{i}", fontSize=15, textColor=clr, alignment=1))
                     for i, (_, val, clr) in enumerate(items)]
        t = Table([label_row, value_row], colWidths=[cw] * n, rowHeights=[7 * mm, 11 * mm])
        t.setStyle(TableStyle([
            ("FONTNAME",      (0, 0), (-1, -1), font),
            ("TOPPADDING",    (0, 0), (-1, -1), 3),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
            ("BACKGROUND",    (0, 0), (-1, -1), C["light"]),
            ("INNERGRID",     (0, 0), (-1, -1), 0.5, colors.HexColor("#dddddd")),
            ("BOX",           (0, 0), (-1, -1), 0.8, colors.HexColor("#cccccc")),
        ]))
        story.append(t)
        story.append(Spacer(1, 6))

    # ── 股票数据表 ──
    def stock_table(story, headers, rows, chg_col_idx=None):
        th_style = sty("th", fontSize=8, textColor=C["white"], alignment=1)
        hdr = [Paragraph(h, th_style) for h in headers]
        body_rows, chg_vals = [], []
        for r in rows:
            body_rows.append([Paragraph(str(c), S["body"]) for c in r])
            if chg_col_idx is not None:
                chg_vals.append(r[chg_col_idx])
        n_cols = len(headers)
        col_w = BODY_W / n_cols
        base = [
            ("FONTNAME",       (0, 0), (-1, -1), font),
            ("FONTSIZE",       (0, 0), (-1,  0), 8.5),
            ("FONTSIZE",       (0, 1), (-1, -1), 8),
            ("BACKGROUND",     (0, 0), (-1,  0), C["hdr_bg"]),
            ("TEXTCOLOR",      (0, 0), (-1,  0), C["white"]),
            ("ALIGN",          (0, 0), (-1, -1), "CENTER"),
            ("VALIGN",         (0, 0), (-1, -1), "MIDDLE"),
            ("ROWBACKGROUND",  (0, 1), (-1, -1), [C["white"], C["alt"]]),
            ("GRID",           (0, 0), (-1, -1), 0.3, colors.HexColor("#cccccc")),
            ("TOPPADDING",     (0, 0), (-1, -1), 4),
            ("BOTTOMPADDING",  (0, 0), (-1, -1), 4),
        ]
        if chg_col_idx is not None and chg_vals:
            for i, val in enumerate(chg_vals, start=1):
                try:
                    v = float(str(val).replace("%", "").replace("+", ""))
                    bg = C["red_bg"] if v > 0 else C["grn_bg"] if v < 0 else C["white"]
                    base.append(("BACKGROUND", (chg_col_idx, i), (chg_col_idx, i), bg))
                except Exception:
                    pass
        t = Table([hdr] + body_rows, colWidths=[col_w] * n_cols, repeatRows=1)
        t.setStyle(TableStyle(base))
        story.append(t)
        story.append(Spacer(1, 4))

    # ── 数据准备 ──
    from crawler.config import INDUSTRY_BOARDS
    from crawler.report_utils import prepare_report_context
    industries_desc = " / ".join(INDUSTRY_BOARDS.keys())

    ctx           = prepare_report_context(df)
    total         = ctx["total"]
    n_up          = ctx["n_up"];          n_down       = ctx["n_down"];  n_flat = ctx["n_flat"]
    n_limit_up    = ctx["n_limit_up"];    n_limit_down = ctx["n_limit_down"]
    avg_chg       = ctx["avg_chg"];       med_chg      = ctx["med_chg"]
    total_vol     = ctx["total_vol"];     avg_vol_ratio = ctx["avg_vol_ratio"]
    strong        = ctx["strong"];        weak         = ctx["weak"]
    rsi_avg       = ctx["rsi_avg"];       n_oversold   = ctx["n_oversold"]
    n_overbought  = ctx["n_overbought"]
    top_gain      = ctx["top10_gain"];    top_loss     = ctx["top10_loss"]
    top_vol       = ctx["top5_vol"];      oversold_cands = ctx["oversold_c"]
    risk_stocks   = ctx["risk_stocks"];   sector_df    = ctx["sector_df"]
    fin_strong    = ctx["fin_strong"];    fin_oversold = ctx["fin_oversold"]
    sentiment     = ctx["sentiment"];     suggestion   = ctx["suggestion"]
    strong_weak   = ctx["strong_weak"]
    chg_color     = C["red"] if avg_chg >= 0 else C["green"]

    # ── 构建文档 ──
    doc = SimpleDocTemplate(
        str(out_path), pagesize=A4,
        leftMargin=15 * mm, rightMargin=15 * mm,
        topMargin=15 * mm, bottomMargin=15 * mm,
    )
    story = []

    # ── 封面：深色标题横幅 ──
    def _banner(text, bg, fg=colors.white, font_size=20, row_h=22*mm, pad=6*mm):
        p = Paragraph(text, sty(f"bn{text[:3]}", fontSize=font_size, leading=font_size+4,
                                 textColor=fg, alignment=1))
        t = Table([[p]], colWidths=[BODY_W], rowHeights=[row_h])
        t.setStyle(TableStyle([
            ("BACKGROUND",    (0, 0), (-1, -1), bg),
            ("TOPPADDING",    (0, 0), (-1, -1), pad),
            ("BOTTOMPADDING", (0, 0), (-1, -1), pad),
        ]))
        return t

    story.append(Spacer(1, 8 * mm))
    story.append(_banner("A 股市场日报", C["navy"], font_size=22, row_h=20*mm, pad=5*mm))
    story.append(_banner(target_date, C["red"], font_size=14, row_h=9*mm, pad=2*mm))
    story.append(Spacer(1, 4))
    story.append(Paragraph(
        f"生成时间：{datetime.now().strftime('%Y-%m-%d %H:%M')}　｜　覆盖：{total} 只股票",
        sty("meta", fontSize=8.5, textColor=C["grey"], alignment=1)
    ))
    story.append(Spacer(1, 5))

    # 封面指标卡
    avg_sign = "+" if avg_chg >= 0 else ""
    metric_cards(story, [
        ("上涨",   f"{n_up} 只",              C["red"]),
        ("下跌",   f"{n_down} 只",            C["green"]),
        ("平均涨跌", f"{avg_sign}{avg_chg:.2f}%", chg_color),
        ("总成交额", f"{total_vol:.0f} 亿",   C["blue"]),
        ("市场情绪", sentiment,               C["orange"] if avg_chg >= 0 else C["grey"]),
    ])
    story.append(HRFlowable(width="100%", thickness=0.8, color=C["navy"], spaceAfter=8))

    # ── 一、市场概况 ──
    section(story, "一、市场概况", C["navy"])
    story.append(kv_table([
        ("上涨 / 下跌 / 平盘",    f"{n_up} / {n_down} / {n_flat}"),
        ("涨停 / 跌停",           f"{n_limit_up} / {n_limit_down}"),
        ("平均涨跌幅",             f"{avg_chg:+.2f}%"),
        ("中位数涨跌幅",           f"{med_chg:+.2f}%"),
        ("板块总成交额",           f"{total_vol:.1f} 亿元"),
        ("板块平均量比（今日/近5日）", f"{avg_vol_ratio:.2f}"),
        ("市场情绪",              sentiment),
    ]))
    story.append(Spacer(1, 4))
    up_pct = n_up / total * 100
    if up_pct < 20:
        breadth = f"上涨面仅 {up_pct:.0f}%，市场极度普跌，恐慌情绪蔓延。"
    elif up_pct < 40:
        breadth = f"上涨面 {up_pct:.0f}%，多数个股走弱，做多意愿不足。"
    elif up_pct < 60:
        breadth = f"上涨面 {up_pct:.0f}%，多空分歧明显，市场震荡分化。"
    else:
        breadth = f"上涨面 {up_pct:.0f}%，普涨格局，市场情绪积极。"
    story.append(Paragraph(
        f"{breadth}平均涨幅与中位数接近（{avg_chg:+.2f}% vs {med_chg:+.2f}%），跌幅分布均匀。",
        S["body"]
    ))

    # ── 二、技术面信号 ──
    section(story, "二、技术面信号", C["blue"])
    story.append(kv_table([
        ("强势（均线向上排列 + 动能向上）",     f"{len(strong)} 只  ({len(strong)/total*100:.1f}%)"),
        ("弱势（均线向下排列 + 动能向下）",     f"{len(weak)} 只  ({len(weak)/total*100:.1f}%)"),
        ("市场平均热度（RSI）",               f"{rsi_avg:.1f}  ({'偏冷，跌得较多' if rsi_avg < 40 else '温度正常' if rsi_avg < 60 else '偏热，涨得较多'})"),
        ("热度极低 RSI<30（跌得过深，可能反弹）", f"{n_oversold} 只  ({n_oversold/total*100:.1f}%)"),
        ("热度极高 RSI>70（涨得过快，注意风险）", f"{n_overbought} 只  ({n_overbought/total*100:.1f}%)"),
    ]))
    if n_oversold / total > 0.25:
        story.append(Spacer(1, 4))
        story.append(Paragraph(
            "超过1/4的股票跌得很深（RSI<30），整体存在反弹动能，但需等到出现放量止跌的信号再行动。",
            S["warn"]
        ))

    # ── 三、板块表现（含柱状图）──
    section(story, "三、板块表现", C["purple"])
    if not sector_df.empty:
        sec_hdrs = ["行业", "平均涨跌", "成交额", "上涨/下跌", "强势股", "平均量比"]
        sec_rows = []
        for _, r in sector_df.iterrows():
            vr = f"{r['平均量比']:.2f}" if not np.isnan(r["平均量比"]) else "—"
            sec_rows.append([
                r["行业"],
                f"{r['平均涨跌']:+.2f}%",
                f"{r['成交额亿']:.1f}亿",
                f"{int(r['上涨数'])}/{int(r['下跌数'])}",
                f"{int(r['强势数'])}只",
                vr,
            ])
        stock_table(story, sec_hdrs, sec_rows, chg_col_idx=1)

        # 板块柱状图
        chart = _draw_sector_chart(sector_df, font, BODY_W)
        if chart is not None:
            story.append(Spacer(1, 4))
            story.append(chart)
            story.append(Spacer(1, 2))
            story.append(Paragraph("▲ 柱状图：红色=上涨，绿色=下跌（A股惯例）", S["caption"]))

        top_sec = sector_df.iloc[0]
        bot_sec = sector_df.iloc[-1]
        story.append(Paragraph(
            f"领涨板块：{top_sec['行业']} {top_sec['平均涨跌']:+.2f}%  ｜  拖累板块：{bot_sec['行业']} {bot_sec['平均涨跌']:+.2f}%",
            S["tip"]
        ))
    else:
        story.append(Paragraph(
            "暂无行业映射数据，请先运行 build_stock_pool() 生成 data/stock_industries.csv。",
            S["body"]
        ))

    # ── 四、涨幅榜 ──
    section(story, "四、涨幅榜 TOP10", C["red"])
    hdrs = ["代码", "名称", "涨跌幅", "收盘价", "成交额", "热度(RSI)", "趋势判断"]
    rows = []
    for _, r in top_gain.iterrows():
        sc = r["score"]
        sig = "强势" if sc >= 3 else "偏多" if sc >= 1 else "震荡" if sc >= -1 else "偏空"
        rsi_note = f"{r['rsi']:.0f}" + (" 🔥" if r["rsi"] > 70 else "")
        rows.append([r["代码"], r["名称"], f"{r['涨跌幅']:+.2f}%", f"{r['收盘']:.2f}",
                     f"{r['成交额亿']:.1f}亿", rsi_note, sig])
    stock_table(story, hdrs, rows, chg_col_idx=2)

    # ── 五、跌幅榜 ──
    section(story, "五、跌幅榜 TOP10", C["green"])
    rows = []
    for _, r in top_loss.iterrows():
        sc = r["score"]
        sig = "偏多" if sc >= 1 else "震荡" if sc >= -1 else "偏空" if sc >= -3 else "弱势"
        rsi_note = f"{r['rsi']:.0f}" + (" ❄️" if r["rsi"] < 30 else "")
        rows.append([r["代码"], r["名称"], f"{r['涨跌幅']:+.2f}%", f"{r['收盘']:.2f}",
                     f"{r['成交额亿']:.1f}亿", rsi_note, sig])
    stock_table(story, hdrs, rows, chg_col_idx=2)

    # ── 六、强势股 ──
    if not strong.empty:
        strong_top = ctx["strong_top"]
        fin_data = fin_strong
        section(story, "六、强势股（均线向上排列 + 动能向上）", C["orange"])
        hdrs6 = ["代码", "名称", "涨跌幅", "收盘价", "距60日均线", "热度(RSI)", "营收增速"]
        rows6 = []
        for _, r in strong_top.iterrows():
            bias = f"+{r['ma60_bias']:.1f}%" if r["ma60_bias"] > 0 else f"{r['ma60_bias']:.1f}%"
            fin = fin_data.get(r["代码"], {})
            rev_yoy = f"{fin['rev_yoy']:+.1f}%" if "rev_yoy" in fin else "—"
            rows6.append([r["代码"], r["名称"], f"{r['涨跌幅']:+.2f}%",
                          f"{r['收盘']:.2f}", bias, f"{r['rsi']:.0f}", rev_yoy])
        stock_table(story, hdrs6, rows6, chg_col_idx=2)

    # ── 七、资金活跃度 ──
    section(story, "七、资金活跃度（成交额 TOP5）", C["blue"])
    hdrs7 = ["代码", "名称", "涨跌幅", "成交额", "换手率", "量比", "资金状态"]
    rows7 = []
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
        rows7.append([r["代码"], r["名称"], f"{r['涨跌幅']:+.2f}%",
                      f"{r['成交额亿']:.1f}亿", f"{r['换手率']:.2f}%", vr_str, vr_status])
    stock_table(story, hdrs7, rows7, chg_col_idx=2)

    # ── 八、超跌候选 ──
    if not oversold_cands.empty:
        fin_data2 = fin_oversold
        section(story, "八、超跌候选（热度极低，可能存在反弹机会）", C["grey"])
        hdrs8 = ["代码", "名称", "今日涨跌", "热度(RSI)", "距60日均线", "负债率", "提示"]
        rows8 = []
        for _, r in oversold_cands.iterrows():
            risk = "趋势仍向下" if r["score"] < 0 else "可观察"
            fin = fin_data2.get(r["代码"], {})
            debt = f"{fin['debt_ratio']:.1f}%" if "debt_ratio" in fin else "—"
            rows8.append([r["代码"], r["名称"], f"{r['涨跌幅']:+.2f}%",
                          f"{r['rsi']:.0f}", f"{r['ma60_bias']:+.1f}%", debt, risk])
        stock_table(story, hdrs8, rows8, chg_col_idx=2)
        story.append(Paragraph(
            "跌得深不等于一定反弹。建议等到出现当天放量 + 第二天高开的信号再考虑介入。",
            S["tip"]
        ))

    # ── 九、风险事件 ──
    if not risk_stocks.empty:
        section(story, "九、风险事件（跌停）", C["red"])
        hdrs9 = ["代码", "名称", "涨跌幅", "成交额", "说明"]
        rows9 = [[r["代码"], r["名称"], f"{r['涨跌幅']:+.2f}%",
                  f"{r['成交额亿']:.1f}亿", "跌停，需关注公告"] for _, r in risk_stocks.iterrows()]
        stock_table(story, hdrs9, rows9, chg_col_idx=2)

    # ── 十、综合结论 ──
    section(story, "十、综合结论", C["navy"])
    # strong_weak 已由 prepare_report_context 统一计算
    vol_comment = "未大幅缩量" if total_vol > 2000 else "成交明显萎缩"
    conclusion_rows = [
        ("短期情绪", sentiment),
        ("趋势结构", f"弱势 {len(weak)} 只 vs 强势 {len(strong)} 只，{strong_weak}"),
        ("整体热度", f"市场平均RSI {rsi_avg:.1f}，{n_oversold} 只跌得过深（{n_oversold/total*100:.0f}%）"),
        ("资金面",   f"龙头 {top_vol.iloc[0]['名称']} 成交 {top_vol.iloc[0]['成交额亿']:.0f}亿，{vol_comment}"),
    ]
    if not sector_df.empty:
        top_sec = sector_df.iloc[0]
        conclusion_rows.append(("板块领涨", f"{top_sec['行业']} {top_sec['平均涨跌']:+.2f}%"))
    conclusion_rows.append(("操作建议", suggestion))
    story.append(kv_table(conclusion_rows))

    story.append(Spacer(1, 10))
    story.append(HRFlowable(width="100%", thickness=0.5, color=C["grey"], spaceAfter=4))
    story.append(Paragraph(
        "本报告由量化脚本自动生成，仅供参考，不构成投资建议。", S["caption"]
    ))

    doc.build(story)
