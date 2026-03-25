"""
历史报告存档工具

- 维护 reports/manifest.json（报告元数据库）
- 生成 index.html（存档首页，按月分组卡片列表）

对外接口：
  update_manifest_and_index(ctx, target_date, reports_dir, index_path)
  get_prev_date(manifest_path) -> str | None

命令行运行：扫描 reports/*.html，回填 manifest，重新生成 index.html
"""
import io
import json
import re
import sys
from collections import defaultdict
from datetime import datetime
from html import escape as _e
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

_WEEKDAY_CN = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]

REPORTS_DIR = Path(__file__).parent / "reports"
INDEX_PATH = Path(__file__).parent / "index.html"


# ── manifest 读写 ─────────────────────────────────────────────────────────────

def _load_manifest(manifest_path: Path) -> dict:
    if manifest_path.exists():
        try:
            return json.loads(manifest_path.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"generated_at": "", "entries": []}


def _save_manifest(data: dict, manifest_path: Path) -> None:
    data["generated_at"] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    manifest_path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def get_prev_date(manifest_path: Path, before_date: "str | None" = None) -> "str | None":
    """返回 manifest 中严格早于 before_date 的最新一条条目日期。

    before_date 为 None 时返回最新条目日期。
    entries 按降序排列，直接找第一个满足条件的条目即可。
    """
    data = _load_manifest(manifest_path)
    entries = data.get("entries", [])
    for entry in entries:
        if before_date is None or entry["date"] < before_date:
            return entry["date"]
    return None


def _safe_float(v, ndigits: int = 2):
    """将浮点值转为 JSON 可序列化的值，NaN/Inf → None。"""
    try:
        import math
        f = float(v)
        return None if math.isnan(f) or math.isinf(f) else round(f, ndigits)
    except (TypeError, ValueError):
        return None


def _ctx_to_entry(ctx: dict, target_date: str) -> dict:
    sector_df = ctx.get("sector_df")
    top_sector = top_sector_chg = bot_sector = bot_sector_chg = None
    if sector_df is not None and not sector_df.empty:
        top_sector = str(sector_df.iloc[0]["行业"])
        top_sector_chg = _safe_float(sector_df.iloc[0]["平均涨跌"])
        bot_sector = str(sector_df.iloc[-1]["行业"])
        bot_sector_chg = _safe_float(sector_df.iloc[-1]["平均涨跌"])
    return {
        "date": target_date,
        "sentiment": ctx.get("sentiment", "—"),
        "se_emoji": ctx.get("se_emoji", "📊"),
        "avg_chg": _safe_float(ctx.get("avg_chg")),
        "n_up": int(ctx.get("n_up", 0)),
        "n_down": int(ctx.get("n_down", 0)),
        "n_flat": int(ctx.get("n_flat", 0)),
        "n_limit_up": int(ctx.get("n_limit_up", 0)),
        "n_limit_down": int(ctx.get("n_limit_down", 0)),
        "total_vol": _safe_float(ctx.get("total_vol"), ndigits=1),
        "total": int(ctx.get("total", 0)),
        "top_sector": top_sector,
        "top_sector_chg": top_sector_chg,
        "bot_sector": bot_sector,
        "bot_sector_chg": bot_sector_chg,
    }


def update_manifest_and_index(
    ctx: dict,
    target_date: str,
    reports_dir: Path,
    index_path: Path,
) -> None:
    """将 ctx 中的统计量写入 manifest.json，并重新生成 index.html。"""
    manifest_path = reports_dir / "manifest.json"
    data = _load_manifest(manifest_path)
    entries = data.get("entries", [])

    new_entry = _ctx_to_entry(ctx, target_date)
    entries = [e for e in entries if e["date"] != target_date]
    entries.append(new_entry)
    entries.sort(key=lambda e: e["date"], reverse=True)
    data["entries"] = entries

    _save_manifest(data, manifest_path)
    _build_index_html(data, reports_dir, index_path)
    print(f"[✓] manifest 已更新（共 {len(entries)} 条），index.html 已重新生成")


# ── index.html 生成 ───────────────────────────────────────────────────────────

_INDEX_CSS = """
:root {
  --up:#e53935; --down:#2e7d32; --navy:#1a237e;
  --bg:#f0f2f5; --card-bg:#fff; --text:#212121; --border:#e0e0e0;
}
* { box-sizing:border-box; margin:0; padding:0; }
body { font-family:-apple-system,"PingFang SC","Microsoft YaHei",sans-serif;
       background:var(--bg); color:var(--text); font-size:14px; }

.hdr { background:linear-gradient(135deg,#0d47a1,#1a237e); color:#fff; padding:20px 32px 24px; }
.hdr h1 { font-size:22px; letter-spacing:2px; margin-bottom:4px; }
.hdr .sub { font-size:12px; opacity:.75; }
.hdr-meta { margin-top:12px; display:flex; gap:16px; flex-wrap:wrap; }
.hdr-badge { background:rgba(255,255,255,.12); border:1px solid rgba(255,255,255,.2);
             border-radius:6px; padding:6px 14px; font-size:13px; }
.hdr-badge b { font-size:18px; font-weight:700; }

.content { max-width:960px; margin:0 auto; padding:16px; }

.month-group { margin-bottom:24px; }
.month-label { font-size:16px; font-weight:700; color:#1a237e;
               padding:8px 0 8px; border-bottom:2px solid #1a237e; margin-bottom:12px; }
.card-grid { display:grid; grid-template-columns:repeat(auto-fill,minmax(280px,1fr)); gap:12px; }

.card { background:#fff; border-radius:8px; box-shadow:0 1px 4px rgba(0,0,0,.1);
        padding:14px 16px; display:flex; flex-direction:column; gap:6px;
        border-top:3px solid transparent; transition:box-shadow .15s; }
.card:hover { box-shadow:0 4px 12px rgba(0,0,0,.15); }
.card.up-card { border-top-color:var(--up); }
.card.dn-card { border-top-color:var(--down); }
.card.ntr-card { border-top-color:#9e9e9e; }

.card-date { display:flex; align-items:center; gap:8px; }
.card-date .date-main { font-size:15px; font-weight:700; color:#212121; }
.card-date .weekday { font-size:11px; color:#9e9e9e; }
.sentiment-badge { display:inline-block; padding:2px 8px; border-radius:3px;
                   font-size:11px; font-weight:600; }
.sb-up { background:#ffebee; color:#c62828; }
.sb-dn { background:#e8f5e9; color:#1b5e20; }
.sb-ntr { background:#f5f5f5; color:#616161; }

.card-chg { font-size:24px; font-weight:700; }
.chg-up { color:var(--up); }
.chg-dn { color:var(--down); }
.chg-ntr { color:#9e9e9e; }

.card-pills { display:flex; gap:6px; flex-wrap:wrap; }
.pill { display:inline-block; padding:2px 7px; border-radius:3px; font-size:11px; }
.pill-up { background:#ffebee; color:#c62828; }
.pill-dn { background:#e8f5e9; color:#1b5e20; }
.pill-ntr { background:#f5f5f5; color:#757575; }

.card-vol { font-size:12px; color:#757575; }
.card-sectors { font-size:12px; color:#555; line-height:1.6; }
.card-sectors .sec-up { color:var(--up); font-weight:600; }
.card-sectors .sec-dn { color:var(--down); font-weight:600; }

.card-btn { margin-top:4px; }
.card-btn a { display:inline-block; background:#1565c0; color:#fff;
              padding:5px 14px; border-radius:4px; text-decoration:none;
              font-size:12px; font-weight:600; }
.card-btn a:hover { background:#0d47a1; }

.footer { text-align:center; color:#9e9e9e; font-size:12px; padding:20px 16px; }
"""


def _chg_str(v) -> str:
    if v is None:
        return "—"
    return f"+{v:.2f}%" if v >= 0 else f"{v:.2f}%"


def _card_html(entry: dict, reports_dir: Path) -> str:
    date = entry["date"]
    try:
        dt = datetime.strptime(date, "%Y-%m-%d")
        weekday = _WEEKDAY_CN[dt.weekday()]
    except Exception:
        weekday = ""

    avg_chg = entry.get("avg_chg")
    sentiment = entry.get("sentiment", "—")
    se_emoji = entry.get("se_emoji", "📊")

    if avg_chg is None:
        card_cls, chg_cls, sb_cls = "ntr-card", "chg-ntr", "sb-ntr"
    elif avg_chg >= 0:
        card_cls, chg_cls, sb_cls = "up-card", "chg-up", "sb-up"
    else:
        card_cls, chg_cls, sb_cls = "dn-card", "chg-dn", "sb-dn"

    n_up = entry.get("n_up", 0)
    n_down = entry.get("n_down", 0)
    n_flat = entry.get("n_flat", 0)
    n_lu = entry.get("n_limit_up", 0)
    total_vol = entry.get("total_vol")

    top_sector = entry.get("top_sector")
    top_sector_chg = entry.get("top_sector_chg")
    bot_sector = entry.get("bot_sector")
    bot_sector_chg = entry.get("bot_sector_chg")

    sector_html = ""
    if top_sector:
        sector_html = (
            f'<div class="card-sectors">'
            f'领涨 <span class="sec-up">{_e(top_sector)} {_chg_str(top_sector_chg)}</span>'
            f' &nbsp;·&nbsp; 拖累 <span class="sec-dn">{_e(bot_sector or "—")} {_chg_str(bot_sector_chg)}</span>'
            f'</div>'
        )

    report_filename = f"daily_{date}.html"
    report_exists = (reports_dir / report_filename).exists()
    btn_html = ""
    if report_exists:
        btn_html = (
            f'<div class="card-btn">'
            f'<a href="reports/{_e(report_filename)}">查看报告 →</a>'
            f'</div>'
        )

    vol_str = f"{total_vol:.0f}亿" if total_vol else "—"
    lu_str = f" · 涨停{n_lu}只" if n_lu else ""

    return (
        f'<div class="card {card_cls}">'
        f'<div class="card-date">'
        f'<span class="date-main">{_e(date)}</span>'
        f'<span class="weekday">{weekday}</span>'
        f'<span class="sentiment-badge {sb_cls}">{_e(se_emoji + sentiment)}</span>'
        f'</div>'
        f'<div class="card-chg {chg_cls}">{_chg_str(avg_chg)}</div>'
        f'<div class="card-pills">'
        f'<span class="pill pill-up">涨 {n_up}</span>'
        f'<span class="pill pill-dn">跌 {n_down}</span>'
        f'<span class="pill pill-ntr">平 {n_flat}</span>'
        f'</div>'
        f'<div class="card-vol">成交额 {vol_str}{lu_str}</div>'
        f'{sector_html}'
        f'{btn_html}'
        f'</div>'
    )


def _build_index_html(data: dict, reports_dir: Path, index_path: Path) -> None:
    entries = data.get("entries", [])
    total_count = len(entries)

    if entries:
        latest = entries[0]["date"]
        oldest = entries[-1]["date"]
        span_str = f"{oldest} ~ {latest}"
    else:
        span_str = "暂无报告"

    by_month: dict[str, list] = defaultdict(list)
    for entry in entries:
        month_key = entry["date"][:7]
        by_month[month_key].append(entry)

    month_blocks = []
    for month_key in sorted(by_month.keys(), reverse=True):
        cards = "".join(_card_html(e, reports_dir) for e in by_month[month_key])
        month_blocks.append(
            f'<div class="month-group">'
            f'<div class="month-label">{_e(month_key)}</div>'
            f'<div class="card-grid">{cards}</div>'
            f'</div>'
        )

    blocks_html = (
        "\n".join(month_blocks)
        if month_blocks
        else "<p style='color:#9e9e9e;padding:20px 0'>暂无报告记录</p>"
    )
    gen_time = datetime.now().strftime("%Y-%m-%d %H:%M")

    html = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>A股日报 · 历史存档</title>
<style>{_INDEX_CSS}</style>
</head>
<body>
<div class="hdr">
  <h1>📊 A股日报 · 历史存档</h1>
  <div class="sub">自动生成 · 申万行业覆盖 · 每日更新</div>
  <div class="hdr-meta">
    <div class="hdr-badge">共 <b>{total_count}</b> 份报告</div>
    <div class="hdr-badge">时间跨度 <b>{_e(span_str)}</b></div>
    <div class="hdr-badge">更新于 <b>{gen_time}</b></div>
  </div>
</div>
<div class="content">
{blocks_html}
<div class="footer">本报告由量化脚本自动生成，仅供参考，不构成投资建议。</div>
</div>
</body>
</html>"""
    index_path.write_text(html, encoding="utf-8")


# ── 命令行入口：回填历史 ──────────────────────────────────────────────────────

def _backfill(reports_dir: Path, index_path: Path) -> None:
    """扫描 reports/*.html，补全缺失的 manifest 条目。"""
    manifest_path = reports_dir / "manifest.json"
    data = _load_manifest(manifest_path)
    existing_dates = {e["date"] for e in data.get("entries", [])}

    html_files = sorted(reports_dir.glob("daily_????-??-??.html"))
    print(f"发现 {len(html_files)} 个报告文件")

    for html_file in html_files:
        m = re.search(r"daily_(\d{4}-\d{2}-\d{2})\.html", html_file.name)
        if not m:
            continue
        date = m.group(1)
        if date in existing_dates:
            print(f"  [跳过] {date}（已有记录）")
            continue

        ctx = None
        try:
            # gen_report.load_market 聚合当日全部股票数据，再计算 ctx
            from gen_report import load_market, load_names
            from crawler.report_utils import prepare_report_context

            names = load_names()
            df_day = load_market(date, names)
            if not df_day.empty:
                ctx = prepare_report_context(df_day)
        except Exception as exc:
            print(f"    (加载行情失败：{exc})")

        if ctx:
            entry = _ctx_to_entry(ctx, date)
            avg_str = f"{entry['avg_chg']:+.2f}%" if entry["avg_chg"] is not None else "NaN"
            print(f"  [计算] {date}：{entry['sentiment']} avg={avg_str}")
        else:
            entry = {
                "date": date,
                "sentiment": "—",
                "se_emoji": "📊",
                "avg_chg": None,
                "n_up": 0,
                "n_down": 0,
                "n_flat": 0,
                "n_limit_up": 0,
                "n_limit_down": 0,
                "total_vol": None,
                "total": 0,
                "top_sector": None,
                "top_sector_chg": None,
                "bot_sector": None,
                "bot_sector_chg": None,
            }
            print(f"  [占位] {date}（无行情数据）")

        data.setdefault("entries", []).append(entry)
        existing_dates.add(date)

    data["entries"].sort(key=lambda e: e["date"], reverse=True)
    _save_manifest(data, manifest_path)
    _build_index_html(data, reports_dir, index_path)
    print(f"\n[✓] manifest 已保存：{manifest_path}")
    print(f"[✓] index.html 已生成：{index_path}")


if __name__ == "__main__":
    _backfill(REPORTS_DIR, INDEX_PATH)
