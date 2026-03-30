"""
HTML 报告生成器

生成独立 HTML 文件，内嵌所有 CSS，无外部依赖，可直接在浏览器打开。
"""

import numpy as np
import pandas as pd
from html import escape as _e
from pathlib import Path

from crawler.report_utils import (
    load_financial_snapshot as _load_financial_snapshot,
    prepare_report_context,
    sector_table as _sector_table,
)

# ── CSS ──────────────────────────────────────────────────────────────────────

_CSS = """
:root {
  --up:#e53935; --down:#2e7d32; --navy:#1a237e;
  --bg:#f0f2f5; --card-bg:#fff; --text:#212121; --border:#e0e0e0;
}
* { box-sizing:border-box; margin:0; padding:0; }
body { font-family:-apple-system,"PingFang SC","Microsoft YaHei",sans-serif;
       background:var(--bg); color:var(--text); font-size:14px; }

/* ── Header ── */
.hdr { background:linear-gradient(135deg,#0d47a1,#1a237e); color:#fff; padding:20px 32px 24px; }
.hdr h1 { font-size:22px; letter-spacing:2px; margin-bottom:4px; }
.hdr .sub { font-size:12px; opacity:.75; margin-bottom:0; }
.hdr-divider { border:none; border-top:1px solid rgba(255,255,255,.18); margin:14px 0; }
.cards { display:flex; gap:8px; flex-wrap:wrap; }
.card { background:rgba(255,255,255,.12); border:1px solid rgba(255,255,255,.2);
        border-radius:6px; padding:8px 14px; min-width:88px; }
.card .lbl { font-size:11px; opacity:.75; margin-bottom:3px; }
.card .val { font-size:16px; font-weight:700; }
.card .val.up { color:#ff8a80; }
.card .val.dn { color:#69f0ae; }
.card .val.ntr { color:#fff9c4; }

/* ── Layout ── */
.content { max-width:1100px; margin:0 auto; padding:16px; }
.two-col { display:grid; grid-template-columns:1fr 1fr; gap:16px; }
@media(max-width:720px){ .two-col { grid-template-columns:1fr; } }

/* ── Section ── */
.sec { background:#fff; border-radius:8px;
       box-shadow:0 1px 4px rgba(0,0,0,.1); margin-bottom:16px; overflow:hidden; }
.sec-t { padding:9px 16px; font-size:14px; font-weight:700; color:#fff; }
.sec-b { padding:14px 16px; overflow-x:auto; }

/* section title colors */
.c1{background:#1565c0} .c2{background:#283593} .c3{background:#4527a0}
.c4{background:#6a1b9a} .c5{background:#880e4f} .c6{background:#b71c1c}
.c7{background:#e65100} .c8{background:#1b5e20} .c9{background:#37474f}
.c10{background:#263238}

/* ── Table ── */
table { width:100%; border-collapse:collapse; font-size:13px; }
th { background:#37474f; color:#fff; padding:7px 10px; text-align:left; white-space:nowrap; }
td { padding:6px 10px; border-bottom:1px solid #f0f0f0; vertical-align:middle; }
tr:nth-child(even) td { background:#fafafa; }
tr:hover td { background:#e3f2fd; }

/* ── Color ── */
.up { color:var(--up); font-weight:600; }
.dn { color:var(--down); font-weight:600; }

/* ── Tags ── */
.tag { display:inline-block; padding:2px 7px; border-radius:3px; font-size:11px; }
.t-strong{background:#ffebee;color:#c62828} .t-bull{background:#fff3e0;color:#e65100}
.t-neut{background:#f5f5f5;color:#616161}   .t-bear{background:#e8f5e9;color:#2e7d32}
.t-weak{background:#e0f2f1;color:#00695c}

/* ── Callout ── */
.callout { background:#e8eaf6; border-left:4px solid #3f51b5;
           padding:10px 14px; border-radius:0 6px 6px 0;
           font-size:13px; line-height:1.8; color:#283593; margin-bottom:12px; }
.callout b { color:#0d47a1; }
.callout.warn { background:#fff8e1; border-left-color:#ffa000; color:#6d4c00; }
.callout.warn b { color:#e65100; }
.callout.info { background:#e3f2fd; border-left-color:#1e88e5; color:#0d47a1; }

/* ── Alert ── */
.alert { background:#fff8e1; border-left:4px solid #ffa000;
         padding:8px 14px; margin-bottom:12px; border-radius:0 6px 6px 0;
         font-weight:700; color:#e65100; font-size:14px; }

/* ── Today card ── */
.today { background:#e8eaf6; border-radius:6px; padding:12px 16px; line-height:2; font-size:14px; }
.today b { color:#1a237e; }

/* ── Sector bar chart ── */
.bar-chart { margin-bottom:10px; }
.bar-row { display:flex; align-items:center; gap:8px; margin:5px 0; }
.bar-nm { width:68px; font-size:12px; text-align:right; color:#333;
          white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
.bar-ax { flex:1; position:relative; height:20px; background:#f5f5f5; border-radius:3px; }
.bar-cx { position:absolute; top:0; bottom:0; left:50%; width:1px; background:#bbb; }
.bar-fl { position:absolute; top:2px; bottom:2px; border-radius:2px; }
.bar-vl { width:52px; font-size:12px; font-weight:600; }

/* ── Sector detail ── */
.sd-block { margin-bottom:18px; }
.sd-block:last-child { margin-bottom:0; }
.sec-detail-hdr { padding:6px 12px; border-radius:4px 4px 0 0;
                  font-weight:700; font-size:13px; }
.sec-detail-hdr.up { background:#ffebee; color:#c62828; border-left:3px solid var(--up); }
.sec-detail-hdr.dn { background:#e8f5e9; color:#1b5e20; border-left:3px solid var(--down); }
.sd-sub { font-weight:400; font-size:12px; opacity:.8; }

/* ── Sector tags ── */
.sector-tags { display:flex; flex-wrap:wrap; gap:6px; }
.stag { display:inline-flex; align-items:center; gap:5px; padding:4px 11px;
        border-radius:4px; font-size:12px; font-weight:600;
        background:rgba(255,255,255,.13); border:1px solid rgba(255,255,255,.22);
        color:#fff; white-space:nowrap; }
.stag .sv { font-size:12px; font-weight:700; }
.stag.sup .sv { color:#ff8a80; }
.stag.sdn .sv { color:#69f0ae; }

/* ── Nav ── */
.nav { position:sticky; top:0; z-index:100;
       background:rgba(26,35,126,.97); backdrop-filter:blur(6px);
       border-bottom:1px solid rgba(255,255,255,.1);
       padding:0 16px; overflow-x:auto; white-space:nowrap; }
.nav a { display:inline-block; color:rgba(255,255,255,.8); text-decoration:none;
         font-size:12px; padding:9px 10px; border-bottom:2px solid transparent;
         transition:color .15s, border-color .15s; }
.nav a:hover { color:#fff; border-bottom-color:#90caf9; }

/* ── Hist-bar（报告间导航） ── */
.hist-bar { background:rgba(13,71,161,.9); padding:5px 16px;
            display:flex; align-items:center; gap:10px; font-size:12px; }
.hist-bar a { color:rgba(255,255,255,.85); text-decoration:none;
              padding:2px 8px; border-radius:3px;
              border:1px solid rgba(255,255,255,.2); }
.hist-bar a:hover { background:rgba(255,255,255,.1); }
.hist-bar .spacer { flex:1; }
.hist-bar .hist-date { color:rgba(255,255,255,.5); font-size:11px; }

/* ── Misc ── */
.note { margin-top:10px; color:#555; font-size:13px; line-height:1.7; }
.footer { text-align:center; color:#9e9e9e; font-size:12px; padding:20px 16px; }
"""

# ── 辅助函数 ──────────────────────────────────────────────────────────────────

def _chg_cls(v: float) -> str:
    if isinstance(v, float) and np.isnan(v):
        return ""
    return "up" if v >= 0 else "dn"


def _chg_str(v: float) -> str:
    if isinstance(v, float) and np.isnan(v):
        return "—"
    return f"+{v:.2f}%" if v >= 0 else f"{v:.2f}%"


def _score_tag(score: int) -> str:
    if score >= 3:
        return '<span class="tag t-strong">强势↑↑</span>'
    if score >= 1:
        return '<span class="tag t-bull">偏多↑</span>'
    if score >= -1:
        return '<span class="tag t-neut">震荡—</span>'
    if score >= -3:
        return '<span class="tag t-bear">偏空↓</span>'
    return '<span class="tag t-weak">弱势↓↓</span>'


def _td_chg(v: float) -> str:
    cls = _chg_cls(v)
    return f'<td class="{cls}">{_chg_str(v)}</td>'


def _table(headers: list, rows: list) -> str:
    """生成 HTML 表格。rows 中每个 cell 可以是字符串或已含 <td> 的字符串。"""
    ths = "".join(f"<th>{_e(h)}</th>" for h in headers)
    trs = []
    for row in rows:
        tds = ""
        for c in row:
            s = str(c)
            if s.startswith("<td"):
                tds += s
            else:
                tds += f"<td>{_e(s)}</td>"
        trs.append(f"<tr>{tds}</tr>")
    return f"<table><thead><tr>{ths}</tr></thead><tbody>{''.join(trs)}</tbody></table>"


def _section(title: str, body: str, cls: str = "c1", anchor: str = "") -> str:
    id_attr = f' id="{anchor}"' if anchor else ""
    return (f'<div class="sec"{id_attr}>'
            f'<div class="sec-t {cls}">{title}</div>'
            f'<div class="sec-b">{body}</div>'
            f'</div>')


def _bar_row(name: str, val: float, max_abs: float) -> str:
    pct = min(abs(val) / max_abs * 50, 50) if max_abs > 0 else 0
    if val >= 0:
        style = f"left:50%;width:{pct:.1f}%;background:var(--up);"
        cls = "up"
    else:
        style = f"right:50%;width:{pct:.1f}%;background:var(--down);"
        cls = "dn"
    return (f'<div class="bar-row">'
            f'<div class="bar-nm">{_e(name)}</div>'
            f'<div class="bar-ax"><div class="bar-cx"></div>'
            f'<div class="bar-fl" style="{style}"></div></div>'
            f'<div class="bar-vl {cls}">{_chg_str(val)}</div>'
            f'</div>')


# ── 章节构建函数 ──────────────────────────────────────────────────────────────

def _build_header(target_date: str, total: int, sector_df: pd.DataFrame,
                  n_up: int, n_down: int, n_flat: int, n_lu: int, n_ld: int,
                  avg_chg: float, avg_cls: str, total_vol: float,
                  sentiment: str, se_emoji: str, market: str = "a") -> str:
    now_str = pd.Timestamp.now().strftime("%H:%M")
    market_label = {"a": "A股", "hk": "港股", "us": "美股"}.get(market, "美股")
    if not sector_df.empty:
        tags = "".join(
            f'<span class="stag {"sup" if r["平均涨跌"] >= 0 else "sdn"}">'
            f'{_e(r["行业"])} <span class="sv">{_chg_str(r["平均涨跌"])}</span></span>'
            for _, r in sector_df.iterrows()
        )
        sector_tags = f'<div class="sector-tags">{tags}</div>'
    else:
        sector_tags = ""
    return f"""<div class="hdr">
  <h1>📊 {market_label}市场日报 &middot; {_e(target_date)}</h1>
  <div class="sub">覆盖 {total} 只股票 &middot; {len(sector_df) if not sector_df.empty else 0} 个板块 &middot; 生成于 {now_str}</div>
  {sector_tags}
  <hr class="hdr-divider">
  <div class="cards">
    <div class="card"><div class="lbl">上涨</div><div class="val up">{n_up}</div></div>
    <div class="card"><div class="lbl">下跌</div><div class="val dn">{n_down}</div></div>
    <div class="card"><div class="lbl">平盘</div><div class="val ntr">{n_flat}</div></div>
    <div class="card"><div class="lbl">均涨幅</div><div class="val {avg_cls}">{_chg_str(avg_chg)}</div></div>
    <div class="card"><div class="lbl">成交额</div><div class="val ntr">{total_vol:.0f}亿</div></div>
    <div class="card"><div class="lbl">涨停/跌停</div><div class="val ntr">{n_lu}/{n_ld}</div></div>
    <div class="card"><div class="lbl">市场情绪</div><div class="val ntr">{_e(se_emoji + sentiment)}</div></div>
  </div>
</div>"""


def _build_overview_html(sector_df, sentiment, se_emoji, avg_chg, avg_cls,
                         total_vol, n_up, n_down, n_flat, n_lu, n_ld) -> tuple[str, str]:
    """返回 (alerts_html, today_html)"""
    alerts = ""
    if n_lu >= 5:
        alerts += f'<div class="alert">⚠️ 今日涨停 {n_lu} 只，市场情绪高涨，注意追高风险</div>'
    if n_ld >= 3:
        alerts += f'<div class="alert">⚠️ 今日跌停 {n_ld} 只，市场恐慌情绪明显</div>'
    top_sec_str = ""
    if not sector_df.empty:
        ts, bs = sector_df.iloc[0], sector_df.iloc[-1]
        top_sec_str = (f"<br>🏆 领涨：<b>{_e(ts['行业'])}</b> "
                       f"<span class='{_chg_cls(ts['平均涨跌'])}'>{_chg_str(ts['平均涨跌'])}</span>"
                       f" &nbsp;｜&nbsp; 🔻 拖累：<b>{_e(bs['行业'])}</b> "
                       f"<span class='{_chg_cls(bs['平均涨跌'])}'>{_chg_str(bs['平均涨跌'])}</span>")
    today_html = (f'<div class="today">'
                  f'{_e(se_emoji)} 情绪：<b>{_e(sentiment)}</b> &nbsp;｜&nbsp; '
                  f'均涨：<b class="{avg_cls}">{_chg_str(avg_chg)}</b> &nbsp;｜&nbsp; '
                  f'成交：<b>{total_vol:.1f}亿</b><br>'
                  f'📈 上涨 <b>{n_up}</b> 只 &nbsp;·&nbsp; '
                  f'📉 下跌 <b>{n_down}</b> 只 &nbsp;·&nbsp; '
                  f'➖ 平盘 <b>{n_flat}</b> 只 &nbsp;｜&nbsp; '
                  f'涨停 <b>{n_lu}</b> 只 &nbsp;·&nbsp; 跌停 <b>{n_ld}</b> 只'
                  f'{top_sec_str}</div>')
    return alerts, today_html


def _build_sector_html(sector_df: pd.DataFrame) -> str:
    if sector_df.empty:
        return "<p>暂无行业数据，请先运行 build_stock_pool()</p>"
    max_abs = max(sector_df["平均涨跌"].abs().max(), 0.01)
    bars = "".join(_bar_row(r["行业"], r["平均涨跌"], max_abs) for _, r in sector_df.iterrows())
    sec_rows = []
    for _, r in sector_df.iterrows():
        vr = f"{r['平均量比']:.2f}" if not np.isnan(r["平均量比"]) else "—"
        chg_cls = _chg_cls(r["平均涨跌"])
        sec_rows.append([r["行业"],
                         f'<td class="{chg_cls}"><b>{_chg_str(r["平均涨跌"])}</b></td>',
                         f"{r['成交额亿']:.1f}亿",
                         f"{int(r['上涨数'])}/{int(r['下跌数'])}",
                         f"{int(r['强势数'])}只", vr])
    ts, bs = sector_df.iloc[0], sector_df.iloc[-1]
    callout = (f'<div class="callout">'
               f'🏆 领涨：<b>{_e(ts["行业"])}</b> <span class="{_chg_cls(ts["平均涨跌"])}">{_chg_str(ts["平均涨跌"])}</span>'
               f'　·　成交 {ts["成交额亿"]:.1f}亿，{int(ts["上涨数"])}涨/{int(ts["下跌数"])}跌'
               f'　　🔻 拖累：<b>{_e(bs["行业"])}</b> <span class="{_chg_cls(bs["平均涨跌"])}">{_chg_str(bs["平均涨跌"])}</span>'
               f'　·　成交 {bs["成交额亿"]:.1f}亿，{int(bs["上涨数"])}涨/{int(bs["下跌数"])}跌</div>')
    return callout + f'<div class="bar-chart">{bars}</div>' + _table(["行业", "平均涨跌", "成交额", "涨/跌", "强势", "量比"], sec_rows)


def _build_sector_detail_html(df: pd.DataFrame, sector_df: pd.DataFrame) -> str:
    if sector_df.empty:
        return "<p>暂无数据</p>"
    parts = ['<div class="callout info">每个板块列出当日<b>涨幅最高 3 只</b>和<b>跌幅最深 2 只</b>，'
             '快速定位板块内强弱分化。RSI&gt;70 表示短期涨得偏快；RSI&lt;30 表示短期跌得偏深。</div>']
    for _, sec_row in sector_df.iterrows():
        industry = sec_row["行业"]
        sub = df[df["行业"] == industry]
        if sub.empty:
            continue
        highlight = pd.concat([sub.nlargest(3, "涨跌幅"), sub.nsmallest(2, "涨跌幅")]).drop_duplicates("代码")
        avg_v = sec_row["平均涨跌"]
        hdr_cls = "sec-detail-hdr up" if avg_v >= 0 else "sec-detail-hdr dn"
        rows = [[r["代码"], r["名称"], _td_chg(r["涨跌幅"]), f"{r['收盘']:.2f}",
                 f"{r['rsi']:.0f}" if not np.isnan(r["rsi"]) else "—",
                 f"<td>{_score_tag(int(r['score']))}</td>"]
                for _, r in highlight.iterrows()]
        n_total = int(sec_row["上涨数"] + sec_row["下跌数"])
        parts.append(
            f'<div class="sd-block"><div class="{hdr_cls}">'
            f'{_e(industry)} &nbsp; {"▲" if avg_v >= 0 else "▼"}{_chg_str(avg_v)}'
            f' &nbsp;<span class="sd-sub">{int(sec_row["上涨数"])}涨/{int(sec_row["下跌数"])}跌，共{n_total}只</span>'
            f'</div>{_table(["代码", "名称", "涨跌幅", "收盘", "热度RSI", "趋势"], rows)}</div>'
        )
    return "".join(parts)


def _build_rankboard(top10_gain: pd.DataFrame, top10_loss: pd.DataFrame) -> tuple[str, str]:
    """返回 (gain_table_html, loss_table_html)"""
    def rows(sub, hot_emoji, hot_thresh, above):
        result = []
        for _, r in sub.iterrows():
            rsi_s = f"{r['rsi']:.0f}{hot_emoji if (r['rsi'] > hot_thresh if above else r['rsi'] < hot_thresh) else ''}"
            result.append([r["代码"], r["名称"], _td_chg(r["涨跌幅"]),
                           f"{r['收盘']:.2f}", f"{r['成交额亿']:.1f}亿", rsi_s,
                           f"<td>{_score_tag(int(r['score']))}</td>"])
        return result
    hdrs = ["代码", "名称", "涨跌幅", "收盘", "成交额", "热度RSI", "趋势"]
    return (_table(hdrs, rows(top10_gain, "🔥", 70, True)),
            _table(hdrs, rows(top10_loss, "❄️", 30, False)))


def _build_strong_html(strong_top: pd.DataFrame, fin: dict) -> str:
    callout = ('<div class="callout">这些股票同时满足：<b>短中长均线向上排列</b>（5日&gt;20日&gt;60日，趋势健康）'
               ' + <b>动能向上（MACD金叉）</b>（买入信号），是当前市场中技术面最强的品种。'
               '「距60日均线」为正值表示已涨破均线，数值越大说明强势持续越久，但也要留意追高风险。</div>')
    if strong_top.empty:
        return callout + "<p style='margin-top:10px'>今日无强势股（评分≥3）</p>"
    rows = []
    for _, r in strong_top.iterrows():
        bias = r["ma60_bias"]
        bias_str = f"+{bias:.1f}%" if not np.isnan(bias) and bias >= 0 else (f"{bias:.1f}%" if not np.isnan(bias) else "—")
        bias_cls = _chg_cls(bias) if not np.isnan(bias) else ""
        rev = f"{fin.get(r['代码'], {}).get('rev_yoy', float('nan')):+.1f}%" if "rev_yoy" in fin.get(r["代码"], {}) else "—"
        rows.append([r["代码"], r["名称"], _td_chg(r["涨跌幅"]), f"{r['收盘']:.2f}",
                     f'<td class="{bias_cls}">{_e(bias_str)}</td>', f"{r['rsi']:.0f}", rev])
    return callout + _table(["代码", "名称", "涨跌幅", "收盘", "距60日均线", "热度RSI", "营收增速"], rows)


def _build_vol_html(top5_vol: pd.DataFrame) -> str:
    callout = ('<div class="callout info"><b>量比</b> = 今日成交量 ÷ 近5日平均成交量。'
               '&gt;2 表示放量（大量资金涌入）；&lt;0.5 表示缩量（市场冷清）；≈1 属正常水平。'
               '高换手率 + 放量上涨，通常是主力资金积极介入的信号。</div>')
    rows = []
    for _, r in top5_vol.iterrows():
        vr = r["vol_ratio"]
        vr_str = "—" if np.isnan(vr) else f"{vr:.2f}"
        vs = "—" if np.isnan(vr) else ("放量🔥" if vr > 2 else "缩量" if vr < 0.5 else "正常")
        rows.append([r["代码"], r["名称"], _td_chg(r["涨跌幅"]),
                     f"{r['成交额亿']:.1f}亿", f"{r['换手率']:.2f}%", vr_str, vs])
    return callout + _table(["代码", "名称", "涨跌幅", "成交额", "换手率", "量比", "状态"], rows)


def _build_oversold_html(oversold_c: pd.DataFrame, fin: dict) -> str:
    callout = ('<div class="callout warn">这些股票 RSI &lt; 30，意味着跌得比较深，历史上此类情况常出现反弹。'
               '但 <b>跌深不等于一定反弹</b>，建议等到出现「<b>当天放量 + 第二天高开</b>」的信号再考虑介入。'
               '趋势仍向下（评分为负）的股票风险更高，需谨慎。</div>')
    if oversold_c.empty:
        return callout + "<p style='margin-top:10px'>今日无超跌候选（RSI&lt;30 且当日下跌）</p>"
    rows = []
    for _, r in oversold_c.iterrows():
        f_data = fin.get(r["代码"], {})
        bias = r["ma60_bias"]
        rows.append([r["代码"], r["名称"], _td_chg(r["涨跌幅"]),
                     f"{r['rsi']:.0f}", f"{bias:+.1f}%" if not np.isnan(bias) else "—",
                     f"{f_data['debt_ratio']:.1f}%" if "debt_ratio" in f_data else "—",
                     "⚠️趋势向下" if r["score"] < 0 else "可观察"])
    return callout + _table(["代码", "名称", "今日涨跌", "热度RSI", "距60日均线", "负债率", "提示"], rows)


def _build_market_overview(n_up, n_down, n_flat, n_lu, n_ld, avg_chg, avg_cls,
                           med_chg, total_vol, avg_vr, sentiment) -> str:
    up_pct = n_up / (n_up + n_down + n_flat) * 100
    if up_pct < 20:
        breadth = f"上涨面仅{up_pct:.0f}%，市场极度普跌。"
    elif up_pct < 40:
        breadth = f"上涨面{up_pct:.0f}%，多数个股走弱。"
    elif up_pct < 60:
        breadth = f"上涨面{up_pct:.0f}%，多空分歧明显。"
    else:
        breadth = f"上涨面{up_pct:.0f}%，普涨格局，市场情绪积极。"
    diff = abs(avg_chg - med_chg)
    med_note = (f"平均（{_chg_str(avg_chg)}）与中位数（{_chg_str(med_chg)}）相差 {diff:.1f}%，"
                f"少数极端个股拉偏了均值，中位数更能反映大多数股票的真实状态。"
                if diff > 1.5 else
                f"平均（{_chg_str(avg_chg)}）与中位数（{_chg_str(med_chg)}）接近，"
                f"涨跌分布较为均匀，没有明显的极端个股拉偏。")
    return (_table(["指标", "数值"], [
        ["上涨/下跌/平盘", f"{n_up}/{n_down}/{n_flat}"],
        ["涨停/跌停", f"{n_lu}/{n_ld}"],
        ["平均涨跌幅", f'<td class="{avg_cls}"><b>{_chg_str(avg_chg)}</b></td>'],
        ["中位数涨跌幅", _chg_str(med_chg)],
        ["总成交额", f"{total_vol:.1f}亿"],
        ["板块平均量比", f"{avg_vr:.2f}"],
        ["市场情绪", f"<td><b>{_e(sentiment)}</b></td>"],
    ]) + f'<p class="note">{_e(breadth)}</p><p class="note" style="margin-top:6px">{med_note}</p>',
            breadth)


def _build_tech_html(strong, weak, total, rsi_avg, n_ov, n_ob) -> str:
    rsi_desc = "偏冷（跌得较多）" if rsi_avg < 40 else ("温度正常" if rsi_avg < 60 else "偏热（涨得较多）")
    note = ""
    if n_ov / total > 0.25:
        note = '<p class="note">⚠️ 超过1/4的股票RSI&lt;30（跌得很深），整体存在反弹动能，需等到放量止跌信号后再行动。</p>'
    elif n_ob / total > 0.20:
        note = '<p class="note">⚠️ 超过1/5的股票RSI&gt;70（涨幅过快），短线注意高位回调风险，不要追涨。</p>'
    return _table(["信号", "数量", "占比"], [
        ["强势（均线向上+动能向上）", f"{len(strong)}只", f"{len(strong)/total*100:.1f}%"],
        ["弱势（均线向下+动能向下）", f"{len(weak)}只", f"{len(weak)/total*100:.1f}%"],
        ["市场平均热度(RSI)", f"{rsi_avg:.1f}", rsi_desc],
        ["热度极低 RSI<30（跌得过深）", f"{n_ov}只", f"{n_ov/total*100:.1f}%"],
        ["热度极高 RSI>70（涨得过快）", f"{n_ob}只", f"{n_ob/total*100:.1f}%"],
    ]) + note


def _build_conclusion_html(sentiment, suggestion, breadth, strong, weak, strong_weak,
                           rsi_avg, n_ov, total, top5_vol, total_vol, sector_df) -> str:
    vol_status = "未大幅缩量，资金参与度尚可" if total_vol > 2000 else "成交明显萎缩，市场观望情绪浓"
    rsi_status = "市场整体偏冷，超跌较多" if rsi_avg < 40 else ("热度正常" if rsi_avg < 60 else "市场整体偏热，追高需谨慎")
    structure = ("空头占优，下跌趋势明显" if len(weak) > len(strong) * 1.5
                 else "多头占优，上涨动能较强" if len(strong) > len(weak) * 1.5
                 else "多空力量相当，市场处于震荡阶段")
    top_vol_name = top5_vol.iloc[0]["名称"] if not top5_vol.empty else ""
    top_vol_val  = float(top5_vol.iloc[0]["成交额亿"]) if not top5_vol.empty else 0
    top_sec_line = "—"
    sector_line  = ""
    if not sector_df.empty:
        ts = sector_df.iloc[0]
        top_sec_line = f"{_e(ts['行业'])} {_chg_str(ts['平均涨跌'])}"
        sector_line = f'板块领涨 <b>{_e(sector_df.iloc[0]["行业"])}</b>，拖累 <b>{_e(sector_df.iloc[-1]["行业"])}</b>。'
    summary = (f'<div class="callout" style="margin-bottom:14px">'
               f'今日市场 <b>{_e(sentiment)}</b>，{_e(breadth)}'
               f'强势股 {len(strong)} 只 vs 弱势股 {len(weak)} 只，{_e(structure)}。'
               f'RSI均值 {rsi_avg:.1f}，{_e(rsi_status)}。{_e(vol_status)}（总成交 {total_vol:.0f} 亿）。'
               f'{sector_line}'
               f'<br><b style="color:#0d47a1">操作建议：{_e(suggestion)}</b></div>')
    return summary + _table(["维度", "判断"], [
        ["短期情绪", _e(sentiment)],
        ["趋势结构", f"弱势{len(weak)}只 vs 强势{len(strong)}只，{_e(strong_weak)}"],
        ["整体热度", f"平均RSI {rsi_avg:.1f}，超跌{n_ov}只（占{n_ov/total*100:.0f}%）"],
        ["资金面", f"龙头成交 {_e(top_vol_name)} {top_vol_val:.0f}亿，{_e(vol_status)}"],
        ["板块领涨", top_sec_line],
        ["操作建议", f'<td><b>{_e(suggestion)}</b></td>'],
    ])


_GLOSS_HTML = _table(["术语", "白话解释"], [
    ["涨跌幅", "今天比昨天涨了/跌了多少。+10%为涨停，-10%为跌停"],
    ["RSI热度（0~100）", "衡量股票冷热：>70=涨太快可能回调；<30=跌太深可能反弹"],
    ["量比", "今日成交量÷近5日均量。>2=放量（大资金涌入）；<0.5=缩量（冷清）"],
    ["换手率", "今日全部股份中发生买卖的比例，越高说明交投越活跃"],
    ["均线向上排列", "5日>20日>60日均线，短期涨得比长期快，上涨趋势健康"],
    ["动能向上（MACD金叉）", "短期涨势超过长期，技术派常用买入信号"],
    ["动能向下（MACD死叉）", "短期涨势跌破长期，技术派常用卖出信号"],
    ["距60日均线", "当前价格比过去60日均价高/低多少。正=涨破均线，负=跌破均线"],
    ["营收增速", "最新季报营收 vs 约一年前同期，正值=营收在增长"],
    ["负债率", "总负债÷总资产，超过70%需注意风险"],
]) + '<p class="note" style="margin-top:12px">⚠️ 本报告仅做信息整理，不构成投资建议。股市有风险，入市需谨慎。</p>'


# ── 主函数 ────────────────────────────────────────────────────────────────────

def build_html(df: pd.DataFrame, target_date: str, out_path, prev_date: "str | None" = None, market: str = "a") -> None:
    """生成独立 HTML 报告并写入 out_path。

    prev_date: 上一份报告的日期字符串（YYYY-MM-DD），用于 hist-bar 导航链接。
    market: "a" 表示 A 股，"hk" 表示港股。
    """
    ctx         = prepare_report_context(df)
    total       = ctx["total"]
    n_up        = ctx["n_up"];       n_down      = ctx["n_down"];   n_flat      = ctx["n_flat"]
    n_lu        = ctx["n_limit_up"]; n_ld        = ctx["n_limit_down"]
    avg_chg     = ctx["avg_chg"];    med_chg     = ctx["med_chg"]
    total_vol   = ctx["total_vol"];  avg_vr      = ctx["avg_vol_ratio"]
    rsi_avg     = ctx["rsi_avg"];    n_ov        = ctx["n_oversold"]; n_ob = ctx["n_overbought"]
    strong      = ctx["strong"];     weak        = ctx["weak"]
    strong_top  = ctx["strong_top"]; sector_df   = ctx["sector_df"]
    top10_gain  = ctx["top10_gain"]; top10_loss  = ctx["top10_loss"]
    top5_vol    = ctx["top5_vol"];   oversold_c  = ctx["oversold_c"]
    risk_stocks = ctx["risk_stocks"]
    fin1        = ctx["fin_strong"]; fin2        = ctx["fin_oversold"]
    sentiment   = ctx["sentiment"];  suggestion  = ctx["suggestion"]
    se_emoji    = ctx["se_emoji"];   strong_weak = ctx["strong_weak"]
    avg_cls     = "up" if avg_chg >= 0 else "dn"

    # ── 构建各章节 ──
    header                  = _build_header(target_date, total, sector_df, n_up, n_down, n_flat, n_lu, n_ld, avg_chg, avg_cls, total_vol, sentiment, se_emoji, market=market)
    alerts, today_html      = _build_overview_html(sector_df, sentiment, se_emoji, avg_chg, avg_cls, total_vol, n_up, n_down, n_flat, n_lu, n_ld)
    sector_html             = _build_sector_html(sector_df)
    sector_detail_html      = _build_sector_detail_html(df, sector_df)
    gain_table, loss_table  = _build_rankboard(top10_gain, top10_loss)
    strong_html             = _build_strong_html(strong_top, fin1)
    vol_table               = _build_vol_html(top5_vol)
    oversold_html           = _build_oversold_html(oversold_c, fin2)
    overview_html, breadth  = _build_market_overview(n_up, n_down, n_flat, n_lu, n_ld, avg_chg, avg_cls, med_chg, total_vol, avg_vr, sentiment)
    tech_html               = _build_tech_html(strong, weak, total, rsi_avg, n_ov, n_ob)
    concl_html              = _build_conclusion_html(sentiment, suggestion, breadth, strong, weak, strong_weak, rsi_avg, n_ov, total, top5_vol, total_vol, sector_df)

    risk_section = (_section("⚠️ 风险事件（跌停）",
                             _table(["代码", "名称", "涨跌幅", "成交额", "说明"],
                                    [[r["代码"], r["名称"], _td_chg(r["涨跌幅"]),
                                      f"{r['成交额亿']:.1f}亿", "跌停，需关注公告"]
                                     for _, r in risk_stocks.iterrows()]), "c6", "s-risk")
                    if not risk_stocks.empty else "")

    nav_items = [("#s-overview","🎯 速览"), ("#s-sector","🏭 板块"), ("#s-detail","🔎 个股详情"),
                 ("#s-gain","🚀 涨幅榜"), ("#s-loss","📉 跌幅榜"), ("#s-strong","⚡ 强势股"),
                 ("#s-vol","💰 资金"), ("#s-oversold","🔍 超跌"), ("#s-summary","📊 概况"),
                 ("#s-tech","🔬 技术"), ("#s-concl","📋 结论"), ("#s-gloss","📖 说明")]
    if not risk_stocks.empty:
        nav_items.insert(8, ("#s-risk", "⚠️ 风险"))
    nav_html = '<nav class="nav">' + "".join(f'<a href="{h}">{l}</a>' for h, l in nav_items) + "</nav>"

    _market_label_map = {"a": "A股", "hk": "港股", "us": "美股"}
    archive_link = f"../../index_{market}.html"
    file_prefix = {"hk": "hk_", "us": "us_"}.get(market, "daily_")
    prev_link = ""
    if prev_date:
        prev_link = f'<a href="{file_prefix}{prev_date}.html">‹ {prev_date}</a>'
    hist_bar = (f'<div class="hist-bar">'
                f'<a href="{archive_link}">← 存档</a>'
                f'{prev_link}'
                f'<div class="spacer"></div>'
                f'<span class="hist-date">{_e(target_date)}</span>'
                f'</div>')

    body = f"""{hist_bar}{nav_html}
<div class="content">
{alerts}
{_section("🎯 今日速览", today_html, "c1", "s-overview")}
{_section("🏭 板块表现", sector_html, "c2", "s-sector")}
{_section("🔎 板块个股详情", sector_detail_html, "c2", "s-detail")}
<div class="two-col">
  {_section("🚀 涨幅榜 TOP10", gain_table, "c3", "s-gain")}
  {_section("📉 跌幅榜 TOP10", loss_table, "c4", "s-loss")}
</div>
{_section("⚡ 强势股（均线向上排列 + 动能向上）", strong_html, "c5", "s-strong")}
{_section("💰 资金活跃度 TOP5", vol_table, "c7", "s-vol")}
{_section("🔍 超跌候选（RSI&lt;30，可能存在反弹机会）", oversold_html, "c8", "s-oversold")}
{risk_section}
<div class="two-col">
  {_section("📊 市场概况", overview_html, "c9", "s-summary")}
  {_section("🔬 技术面信号", tech_html, "c9", "s-tech")}
</div>
{_section("📋 综合结论", concl_html, "c10", "s-concl")}
{_section("📖 指标说明（看不懂先读这里）", _GLOSS_HTML, "c2", "s-gloss")}
<div class="footer">本报告由量化脚本自动生成，仅供参考，不构成投资建议。</div>
</div>"""

    Path(out_path).write_text(f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{_market_label_map.get(market, "美股")}日报 {_e(target_date)}</title>
<style>{_CSS}</style>
</head>
<body>
{header}
{body}
</body>
</html>""", encoding="utf-8")
