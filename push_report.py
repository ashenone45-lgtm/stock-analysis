"""
推送日报到飞书/钉钉 Webhook

用法：
  python push_report.py              # 推送最新交易日报告
  python push_report.py --date 2026-03-14
  python push_report.py --dry-run    # 只打印消息内容，不实际发送

配置：
  在项目根目录创建 .env 文件：
    FEISHU_WEBHOOK=https://open.feishu.cn/open-apis/bot/v2/hook/xxxx
    DINGTALK_WEBHOOK=https://oapi.dingtalk.com/robot/send?access_token=xxxx
  只需填写其中一个，两个都填则同时推送。
"""

import argparse
import json
import os
import ssl
import sys
import urllib.request
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

from gen_report import load_market, load_names

DATA_DIR = Path(__file__).parent / "data" / "market"
REPORTS_DIR = Path(__file__).parent / "reports"
ENV_FILE = Path(__file__).parent / ".env"


def load_env() -> dict:
    env = {}
    if not ENV_FILE.exists():
        return env
    for line in ENV_FILE.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" in line:
            key, _, val = line.partition("=")
            env[key.strip()] = val.strip()
    return env


def http_post(url: str, payload: dict) -> dict:
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json; charset=utf-8"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _make_upload_opener() -> urllib.request.OpenerDirector:
    """创建带代理的 opener（用于上传到境外服务）。

    设置 SKIP_SSL_VERIFY=1 可跳过证书验证（仅用于代理导致证书链问题的情况）。
    """
    env = load_env()
    proxy = (
        env.get("UPLOAD_PROXY")
        or os.environ.get("HTTPS_PROXY")
        or os.environ.get("HTTP_PROXY")
        or "http://127.0.0.1:7890"  # 默认本地代理
    )
    ctx = ssl.create_default_context()
    if os.environ.get("SKIP_SSL_VERIFY") == "1":
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    handlers: list = [urllib.request.HTTPSHandler(context=ctx)]
    if proxy:
        handlers.append(urllib.request.ProxyHandler({"http": proxy, "https": proxy}))
    return urllib.request.build_opener(*handlers)


def _multipart_body(field: str, filename: str, data: bytes) -> tuple[bytes, str]:
    """构造 multipart/form-data 请求体，返回 (body, boundary)"""
    import uuid
    boundary = uuid.uuid4().hex
    body = (
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="{field}"; filename="{filename}"\r\n'
        f"Content-Type: application/octet-stream\r\n\r\n"
    ).encode() + data + f"\r\n--{boundary}--\r\n".encode()
    return body, boundary


_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"



def _try_transfer_sh(path: Path, data: bytes, opener: urllib.request.OpenerDirector) -> str | None:
    req = urllib.request.Request(
        f"https://transfer.sh/{path.name}",
        data=data,
        headers={"Content-Type": "application/octet-stream", "User-Agent": _UA},
        method="PUT",
    )
    with opener.open(req, timeout=30) as resp:
        result = resp.read().decode().strip()
    if not result.startswith("http"):
        raise ValueError(f"非预期响应: {result[:200]}")
    return result


def _try_litterbox(path: Path, data: bytes, opener: urllib.request.OpenerDirector) -> str | None:
    """catbox.moe 临时存储，72小时有效"""
    body, boundary = _multipart_body("fileToUpload", path.name, data)
    # 追加 reqtype 和 time 字段
    extra = (
        f"--{boundary}\r\nContent-Disposition: form-data; name=\"reqtype\"\r\n\r\nfileupload\r\n"
        f"--{boundary}\r\nContent-Disposition: form-data; name=\"time\"\r\n\r\n72h\r\n"
    ).encode()
    # 重新组装：extra + file part
    file_part = (
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="fileToUpload"; filename="{path.name}"\r\n'
        f"Content-Type: application/octet-stream\r\n\r\n"
    ).encode() + data + f"\r\n--{boundary}--\r\n".encode()
    body = extra + file_part
    req = urllib.request.Request(
        "https://litterbox.catbox.moe/resources/internals/api.php",
        data=body,
        headers={
            "Content-Type": f"multipart/form-data; boundary={boundary}",
            "User-Agent": _UA,
        },
        method="POST",
    )
    with opener.open(req, timeout=30) as resp:
        result = resp.read().decode().strip()
    if not result.startswith("http"):
        raise ValueError(f"非预期响应: {result[:200]}")
    return result


def upload_pdf(pdf_path: str) -> str | None:
    """上传 PDF，依次尝试 0x0.st → transfer.sh（走本地代理），返回公网链接；失败返回 None"""
    path = Path(pdf_path)
    if not path.exists():
        return None
    with open(path, "rb") as f:
        data = f.read()
    opener = _make_upload_opener()
    for name, fn in [("litterbox", _try_litterbox), ("transfer.sh", _try_transfer_sh)]:
        try:
            url = fn(path, data, opener)
            if url and url.startswith("http"):
                print(f"PDF 已上传至 {name}: {url}", flush=True)
                return url
        except Exception as e:
            print(f"{name} 上传失败，尝试下一个: {e}", file=sys.stderr)
    print("PDF 所有上传渠道均失败，跳过链接。", file=sys.stderr)
    return None


def build_report_data(df: pd.DataFrame, target_date: str) -> dict:
    """提取完整报告数据供消息构建使用"""
    total = len(df)
    n_up = int((df["涨跌幅"] > 0).sum())
    n_down = int((df["涨跌幅"] < 0).sum())
    n_flat = total - n_up - n_down
    n_limit_up = int((df["涨跌幅"] >= 9.9).sum())
    n_limit_down = int((df["涨跌幅"] <= -9.9).sum())
    avg_chg = float(df["涨跌幅"].mean())
    med_chg = float(df["涨跌幅"].median())
    total_vol = float(df["成交额亿"].sum())

    strong = df[df["score"] >= 3]
    weak = df[df["score"] <= -3]
    rsi_avg = float(df["rsi"].dropna().mean())
    n_oversold = int((df["rsi"] < 30).sum())
    n_overbought = int((df["rsi"] > 70).sum())

    top10_gain = df.nlargest(10, "涨跌幅")
    top10_loss = df.nsmallest(10, "涨跌幅")
    top5_vol = df.nlargest(5, "成交额亿")
    strong_stocks = strong.nlargest(10, "涨跌幅")
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

    strong_weak = (
        "空头占优" if len(weak) > len(strong)
        else "多头占优" if len(strong) > len(weak)
        else "多空均衡"
    )

    def score_label(sc):
        if sc >= 3:
            return "强势↑↑"
        if sc >= 1:
            return "偏多↑"
        if sc >= -1:
            return "震荡—"
        if sc >= -3:
            return "偏空↓"
        return "弱势↓↓"

    def to_rows(sub_df, cols):
        return [
            {c: sub_df[c].iloc[i] for c in cols}
            for i in range(len(sub_df))
        ]

    return {
        "date": target_date,
        "total": total,
        "n_up": n_up,
        "n_down": n_down,
        "n_flat": n_flat,
        "n_limit_up": n_limit_up,
        "n_limit_down": n_limit_down,
        "avg_chg": avg_chg,
        "med_chg": med_chg,
        "total_vol": total_vol,
        "sentiment": sentiment,
        "suggestion": suggestion,
        "n_strong": len(strong),
        "n_weak": len(weak),
        "rsi_avg": rsi_avg,
        "n_oversold": n_oversold,
        "n_overbought": n_overbought,
        "strong_weak": strong_weak,
        "top10_gain": top10_gain,
        "top10_loss": top10_loss,
        "top5_vol": top5_vol,
        "strong_stocks": strong_stocks,
        "oversold_cands": oversold_cands,
        "risk_stocks": risk_stocks,
        "score_label": score_label,
        "pdf_path": str(REPORTS_DIR / f"daily_{target_date}.pdf"),
        "pdf_exists": (REPORTS_DIR / f"daily_{target_date}.pdf").exists(),
        "pdf_url": None,  # 由主入口上传后填入
    }


# ── 飞书消息构建 ──────────────────────────────────────────────

def _txt(text: str) -> dict:
    return {"tag": "text", "text": text}


def _row(*parts: str) -> list:
    return [_txt(p) for p in parts]


def build_feishu_payload(d: dict) -> dict:
    """构建飞书富文本消息体（摘要风格）"""
    avg_sign = "+" if d["avg_chg"] >= 0 else ""

    def gain_line(r):
        sign = "+" if r["涨跌幅"] >= 0 else ""
        return [_txt(f"  {r['代码']} {r['名称']}  {sign}{r['涨跌幅']:.2f}%")]

    strong_codes = list(d["strong_stocks"]["代码"].values[:8])
    strong_text = "、".join(strong_codes) if strong_codes else "无"
    oversold_codes = list(d["oversold_cands"]["代码"].values[:5])
    oversold_text = "、".join(oversold_codes) if oversold_codes else "无"

    content = [
        [_txt(f"市场概况: {d['total']}只  |  上涨 {d['n_up']}  |  下跌 {d['n_down']}  |  均涨 {avg_sign}{d['avg_chg']:.2f}%  |  情绪: {d['sentiment']}")],
        [_txt(" ")],
        [_txt("🔴 涨幅榜 Top5")],
        *[gain_line(r) for _, r in d["top10_gain"].head(5).iterrows()],
        [_txt(" ")],
        [_txt("🟢 跌幅榜 Top5")],
        *[gain_line(r) for _, r in d["top10_loss"].head(5).iterrows()],
        [_txt(" ")],
        [_txt(f"⚡ 强势信号 (评分≥3): {strong_text}")],
        [_txt(f"📉 超卖候选 (RSI<30): {oversold_text}")],
        [_txt(" ")],
    ]

    if d.get("pdf_url"):
        content.append([_txt("📄 完整报告: "), {"tag": "a", "text": "点击下载PDF", "href": d["pdf_url"]}])
    else:
        content.append([_txt(f"📄 完整报告: {d['pdf_path']}")])

    return {
        "msg_type": "post",
        "content": {
            "post": {
                "zh_cn": {
                    "title": f"📊 A股日报 {d['date']}",
                    "content": content,
                }
            }
        },
    }


# ── 钉钉消息构建 ──────────────────────────────────────────────

def build_dingtalk_payload(d: dict) -> dict:
    """构建钉钉 Markdown 消息体（摘要风格）"""
    avg_sign = "+" if d["avg_chg"] >= 0 else ""

    def gain_line(r):
        sign = "+" if r["涨跌幅"] >= 0 else ""
        return f"- {r['代码']} {r['名称']}  **{sign}{r['涨跌幅']:.2f}%**"

    top5_gain_md = "\n".join(gain_line(r) for _, r in d["top10_gain"].head(5).iterrows())
    top5_loss_md = "\n".join(gain_line(r) for _, r in d["top10_loss"].head(5).iterrows())

    strong_codes = list(d["strong_stocks"]["代码"].values[:8])
    strong_text = "、".join(strong_codes) if strong_codes else "无"
    oversold_codes = list(d["oversold_cands"]["代码"].values[:5])
    oversold_text = "、".join(oversold_codes) if oversold_codes else "无"

    pdf_line = (
        f"📄 [完整PDF报告]({d['pdf_url']})"
        if d.get("pdf_url")
        else f"📄 完整报告: {d['pdf_path']}"
    )

    text = f"""## 📊 A股日报 {d["date"]}

**市场概况:** {d["total"]}只 | 上涨 {d["n_up"]} | 下跌 {d["n_down"]} | 均涨 {avg_sign}{d["avg_chg"]:.2f}% | 情绪: **{d["sentiment"]}**

#### 🔴 涨幅榜 Top5
{top5_gain_md}

#### 🟢 跌幅榜 Top5
{top5_loss_md}

⚡ **强势信号(≥3分):** {strong_text}

📉 **超卖候选(RSI<30):** {oversold_text}

{pdf_line}
"""

    return {
        "msgtype": "markdown",
        "markdown": {
            "title": f"A股日报 {d['date']}",
            "text": text,
        },
        "at": {"isAtAll": False},
    }


# ── 推送函数 ──────────────────────────────────────────────────

def push_feishu(webhook_url: str, payload: dict, dry_run: bool) -> bool:
    if dry_run:
        print("=== [飞书] 消息预览 ===")
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return True
    try:
        result = http_post(webhook_url, payload)
        if result.get("code") == 0:
            print(f"飞书推送成功: {result}")
            return True
        else:
            print(f"飞书推送失败: {result}", file=sys.stderr)
            return False
    except Exception as e:
        print(f"飞书推送异常: {e}", file=sys.stderr)
        return False


def push_dingtalk(webhook_url: str, payload: dict, dry_run: bool) -> bool:
    if dry_run:
        print("=== [钉钉] 消息预览 ===")
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return True
    try:
        result = http_post(webhook_url, payload)
        if result.get("errcode") == 0:
            print(f"钉钉推送成功: {result}")
            return True
        else:
            print(f"钉钉推送失败: {result}", file=sys.stderr)
            return False
    except Exception as e:
        print(f"钉钉推送异常: {e}", file=sys.stderr)
        return False


# ── 主入口 ────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, default=None)
    parser.add_argument("--dry-run", action="store_true", help="只打印消息内容，不实际发送")
    args = parser.parse_args()

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
        if not dates:
            print("无法读取行情数据，请先运行 daily 工作流。")
            sys.exit(1)
        target_date = max(dates)

    print(f"加载 {target_date} 数据...", flush=True)
    names = load_names()
    df = load_market(target_date, names)

    if df.empty:
        print(f"没有 {target_date} 的数据，请先运行 daily 工作流。")
        sys.exit(1)

    data = build_report_data(df, target_date)

    if data["pdf_exists"] and not args.dry_run:
        print("上传 PDF...", flush=True)
        data["pdf_url"] = upload_pdf(data["pdf_path"])

    env = load_env()
    feishu_url = env.get("FEISHU_WEBHOOK", "")
    dingtalk_url = env.get("DINGTALK_WEBHOOK", "")

    if not feishu_url and not dingtalk_url and not args.dry_run:
        print("未配置 Webhook URL，请在 .env 文件中设置 FEISHU_WEBHOOK 或 DINGTALK_WEBHOOK。")
        print("可参考 .env.example 文件。")
        sys.exit(1)

    success = False

    if feishu_url or args.dry_run:
        payload = build_feishu_payload(data)
        ok = push_feishu(feishu_url, payload, args.dry_run)
        success = success or ok

    if dingtalk_url or (args.dry_run and not feishu_url):
        payload = build_dingtalk_payload(data)
        ok = push_dingtalk(dingtalk_url, payload, args.dry_run)
        success = success or ok

    if not success and not args.dry_run:
        sys.exit(1)
