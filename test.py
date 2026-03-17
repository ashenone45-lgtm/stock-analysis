# test.py — 手动端到端验证脚本
"""
运行方式：python test.py
使用少量代表性股票验证各模块，不触发全量爬取。
"""
import io
import logging
import os
import sys

# Fix Chinese garbled text on Windows GBK terminals
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# 跨行业代表：平安银行(深)、招商银行(沪)、宁德时代(深)、中国卫星(沪)、韦尔股份(沪)
SAMPLE_SYMBOLS = ["000001", "600036", "300750", "601881", "603501"]


def test_stock_pool():
    print("\n── Test 1: 股票池构建 ──")
    from crawler.stock_pool import build_stock_pool
    pool = build_stock_pool()
    assert len(pool) > 0, "股票池为空！"
    for code in pool[:5]:
        assert len(code) == 6 and code.isdigit(), f"代码格式错误: {code}"
    print(f"[OK] 股票池: {len(pool)} 只 | 前5: {pool[:5]}")
    return pool


def test_market_history():
    print("\n── Test 2: 行情历史（5只样本股）──")
    from crawler.fetchers.market import fetch_history
    from crawler.storage.parquet import read
    for symbol in SAMPLE_SYMBOLS:
        fetch_history(symbol)
        df = read(f"data/market/{symbol}.parquet")
        assert len(df) > 100, f"[{symbol}] 数据行数不足: {len(df)}"
        size_kb = os.path.getsize(f"data/market/{symbol}.parquet") / 1024
        print(f"  [OK] {symbol}: {len(df)} 行 | {df['日期'].min()} ~ {df['日期'].max()} | {size_kb:.1f} KB")


def test_daily_spot():
    print("\n── Test 3: 每日行情快照 ──")
    from crawler.fetchers.market import fetch_daily_spot
    from crawler.storage.parquet import read
    from datetime import date
    today = str(date.today())
    fetch_daily_spot(SAMPLE_SYMBOLS)
    for symbol in SAMPLE_SYMBOLS[:2]:
        df = read(f"data/market/{symbol}.parquet")
        today_rows = df[df["日期"].astype(str).str.startswith(today)]
        flag = "[OK]" if len(today_rows) > 0 else "[WARN]"
        print(f"  {flag} {symbol}: 今日数据 {len(today_rows)} 行（非交易日可为0）")


def test_news():
    print("\n── Test 4: 公告数据 ──")
    from crawler.fetchers.news import fetch_daily_news
    from datetime import date
    fetch_daily_news(SAMPLE_SYMBOLS)
    today_str = date.today().strftime("%Y-%m-%d")
    path = f"data/news/{today_str}.parquet"
    if os.path.exists(path):
        from crawler.storage.parquet import read
        df = read(path)
        print(f"  [OK] 今日公告: {len(df)} 条 -> {path}")
    else:
        print(f"  [WARN] 今日无公告（非交易日或暂无公告）")


def test_financial():
    print("\n── Test 5: 财务报表（招商银行 600036）──")
    from crawler.fetchers.financial import fetch_financial
    symbol = "600036"
    fetch_financial(symbol)
    files = [f for f in os.listdir("data/financial") if f.startswith(symbol)]
    assert len(files) >= 1, f"[{symbol}] 未生成财务文件"
    print(f"  [OK] {symbol} 财务文件: {files}")


def test_idempotency():
    print("\n── Test 6: 存储幂等性验证 ──")
    from crawler.fetchers.market import fetch_history
    from crawler.storage.parquet import read
    symbol = "000001"
    fetch_history(symbol)  # 第一次写入
    df1 = read(f"data/market/{symbol}.parquet")
    fetch_history(symbol)  # 重跑，不应产生重复行
    df2 = read(f"data/market/{symbol}.parquet")
    assert len(df1) == len(df2), f"重跑后行数变化: {len(df1)} → {len(df2)}"
    print(f"  [OK] 幂等OK: 两次运行行数一致 ({len(df2)} 行)")


if __name__ == "__main__":
    print("=" * 55)
    print("Stock Crawler 端到端验证")
    print("=" * 55)

    test_stock_pool()
    test_market_history()
    test_daily_spot()
    test_news()
    test_financial()
    test_idempotency()

    print("\n" + "=" * 55)
    print("[OK] 所有验证通过")
    print("=" * 55)
