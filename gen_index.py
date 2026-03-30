"""
历史报告存档工具

- 维护 reports/{a,hk}/manifest.json（报告元数据库）
- 生成 index_a.html / index_hk.html（市场存档首页，按月分组卡片列表）
- 生成 index.html（双市场落地页）

对外接口：
  update_manifest_and_index(ctx, target_date, reports_dir, index_path, market)
  get_prev_date(manifest_path) -> str | None
  build_root_index(root_path)

命令行运行：扫描报告目录，回填 manifest，重新生成存档页
"""
import io
import json
import re
import sys
from collections import defaultdict
from datetime import datetime
from html import escape as _e
from pathlib import Path

if getattr(sys.stdout, "encoding", "").lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
if getattr(sys.stderr, "encoding", "").lower() != "utf-8":
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

_WEEKDAY_CN = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]

_ROOT = Path(__file__).parent
REPORTS_DIR = _ROOT / "reports" / "a"
INDEX_PATH = _ROOT / "index_a.html"


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
    market: str = "a",
) -> None:
    """将 ctx 中的统计量写入 manifest.json，并重新生成存档页。"""
    manifest_path = reports_dir / "manifest.json"
    data = _load_manifest(manifest_path)
    entries = data.get("entries", [])

    new_entry = _ctx_to_entry(ctx, target_date)
    entries = [e for e in entries if e["date"] != target_date]
    entries.append(new_entry)
    entries.sort(key=lambda e: e["date"], reverse=True)
    data["entries"] = entries

    _save_manifest(data, manifest_path)
    _build_index_html(data, reports_dir, index_path, market=market)
    # 同时刷新根落地页
    root_index = index_path.parent / "index.html"
    build_root_index(root_index)
    print(f"[✓] manifest 已更新（共 {len(entries)} 条），{index_path.name} 已重新生成")


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


def _card_html(entry: dict, reports_dir: Path, market: str = "a") -> str:
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
            f'领涨 <span class="{"sec-up" if top_sector_chg is not None and top_sector_chg >= 0 else "sec-dn"}">{_e(top_sector)} {_chg_str(top_sector_chg)}</span>'
            f' &nbsp;·&nbsp; 拖累 <span class="{"sec-up" if bot_sector_chg is not None and bot_sector_chg >= 0 else "sec-dn"}">{_e(bot_sector or "—")} {_chg_str(bot_sector_chg)}</span>'
            f'</div>'
        )

    sub_dir = market if market in ("hk", "us") else "a"
    file_prefix = {"hk": "hk_", "us": "us_"}.get(market, "daily_")
    report_filename = f"{file_prefix}{date}.html"
    report_exists = (reports_dir / report_filename).exists()
    btn_html = ""
    if report_exists:
        btn_html = (
            f'<div class="card-btn">'
            f'<a href="reports/{sub_dir}/{_e(report_filename)}">查看报告 →</a>'
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


def _build_index_html(data: dict, reports_dir: Path, index_path: Path, market: str = "a") -> None:
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
        cards = "".join(_card_html(e, reports_dir, market=market) for e in by_month[month_key])
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
    market_label = {"a": "A股", "hk": "港股", "us": "美股"}.get(market, "美股")
    market_sub = {"a": "申万行业覆盖", "hk": "港股核心板块", "us": "NYSE · NASDAQ · ETF指数"}.get(market, "NYSE · NASDAQ · ETF指数")

    html = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{market_label}日报 · 历史存档</title>
<style>{_INDEX_CSS}</style>
</head>
<body>
<div class="hdr">
  <h1>📊 {market_label}日报 · 历史存档</h1>
  <div class="sub">自动生成 · {market_sub} · 每日更新</div>
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


# ── 根落地页 ───────────────────────────────────────────────────────────────────

_ROOT_CSS = """
:root { --navy:#1a237e; --bg:#f0f2f5; }
* { box-sizing:border-box; margin:0; padding:0; }
body { font-family:-apple-system,"PingFang SC","Microsoft YaHei",sans-serif;
       background:var(--bg); color:#212121; min-height:100vh;
       display:flex; flex-direction:column; align-items:center; justify-content:center; }
.root-wrap { text-align:center; padding:32px 16px; }
.root-title { font-size:28px; font-weight:700; color:var(--navy); margin-bottom:8px; letter-spacing:2px; }
.root-sub { font-size:14px; color:#757575; margin-bottom:40px; }
.market-btns { display:flex; gap:24px; flex-wrap:wrap; justify-content:center; }
.market-btn { display:flex; flex-direction:column; align-items:center; justify-content:center;
              width:200px; height:140px; border-radius:12px; text-decoration:none;
              transition:transform .15s, box-shadow .15s; box-shadow:0 2px 8px rgba(0,0,0,.12); }
.market-btn:hover { transform:translateY(-4px); box-shadow:0 8px 24px rgba(0,0,0,.18); }
.btn-a { background:linear-gradient(135deg,#0d47a1,#1a237e); }
.btn-hk { background:linear-gradient(135deg,#b71c1c,#7f0000); }
.btn-us { background:linear-gradient(135deg,#1b5e20,#2e7d32); }
.market-btn .icon { font-size:36px; margin-bottom:10px; }
.market-btn .label { font-size:18px; font-weight:700; color:#fff; letter-spacing:1px; }
.market-btn .desc { font-size:12px; color:rgba(255,255,255,.75); margin-top:4px; }
.footer { margin-top:40px; font-size:12px; color:#9e9e9e; }
"""


def build_root_index(root_path: Path) -> None:
    """生成根目录 index.html 双市场落地页。"""
    gen_time = datetime.now().strftime("%Y-%m-%d %H:%M")
    html = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>股票日报 · 选择市场</title>
<style>{_ROOT_CSS}</style>
</head>
<body>
<div class="root-wrap">
  <div class="root-title">📊 股票日报</div>
  <div class="root-sub">自动生成 · 每日更新 · 更新于 {gen_time}</div>
  <div class="market-btns">
    <a class="market-btn btn-a" href="index_a.html">
      <span class="icon">🇨🇳</span>
      <span class="label">A 股日报</span>
      <span class="desc">申万行业 · 沪深两市</span>
    </a>
    <a class="market-btn btn-hk" href="index_hk.html">
      <span class="icon">🇭🇰</span>
      <span class="label">港股日报</span>
      <span class="desc">核心板块 · 港交所</span>
    </a>
    <a class="market-btn btn-us" href="index_us.html">
      <span class="icon">🇺🇸</span>
      <span class="label">美 股 日 报</span>
      <span class="desc">NYSE · NASDAQ · ETF指数</span>
    </a>
  </div>
  <div class="footer">本报告由量化脚本自动生成，仅供参考，不构成投资建议。</div>
</div>
</body>
</html>"""
    root_path.write_text(html, encoding="utf-8")
    print(f"[✓] 根落地页已生成：{root_path}")


# ── 命令行入口：回填历史 ──────────────────────────────────────────────────────

def _backfill(reports_dir: Path, index_path: Path, market: str = "a") -> None:
    """扫描报告目录 HTML，补全缺失的 manifest 条目。"""
    manifest_path = reports_dir / "manifest.json"
    data = _load_manifest(manifest_path)
    existing_dates = {e["date"] for e in data.get("entries", [])}

    file_prefix = {"hk": "hk_", "us": "us_"}.get(market, "daily_")
    pattern = f"{file_prefix}????-??-??.html"
    date_re = rf"{re.escape(file_prefix)}(\d{{4}}-\d{{2}}-\d{{2}})\.html"
    html_files = sorted(reports_dir.glob(pattern))
    print(f"发现 {len(html_files)} 个报告文件")

    for html_file in html_files:
        m = re.search(date_re, html_file.name)
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
    _build_index_html(data, reports_dir, index_path, market=market)
    print(f"\n[✓] manifest 已保存：{manifest_path}")
    print(f"[✓] {index_path.name} 已生成：{index_path}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="回填 manifest，重新生成存档页")
    parser.add_argument("--market", choices=["a", "hk", "us", "all"], default="all",
                        help="市场：a=A股，hk=港股，us=美股，all=三个市场（默认）")
    args = parser.parse_args()

    _market_label = {"a": "A股", "hk": "港股", "us": "美股"}
    markets = ["a", "hk", "us"] if args.market == "all" else [args.market]
    for mkt in markets:
        r_dir = _ROOT / "reports" / mkt
        i_path = _ROOT / f"index_{mkt}.html"
        r_dir.mkdir(parents=True, exist_ok=True)
        print(f"\n=== 处理 {_market_label.get(mkt, mkt)} ===")
        _backfill(r_dir, i_path, market=mkt)

    build_root_index(_ROOT / "index.html")
