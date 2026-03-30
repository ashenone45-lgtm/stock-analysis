"""Stock pool builder from industry boards."""
import logging
from pathlib import Path

import akshare as ak
import pandas as pd

from crawler.config import ETF_BOARDS, INDUSTRY_BOARDS
from crawler.utils import retry_with_backoff

logger = logging.getLogger(__name__)

INDUSTRIES_CSV = Path(__file__).parent.parent / "data" / "stock_industries.csv"
NAMES_CSV = Path(__file__).parent.parent / "data" / "stock_names.csv"


def build_stock_pool() -> list[str]:
    """按行业板块拉取成分股，合并去重，返回股票代码列表。

    副作用：将 {code: industry_label} 写入 data/stock_industries.csv。

    Returns:
        去重排序后的股票代码列表，如 ["000001", "600036", ...]
    """
    all_symbols: set[str] = set()
    industry_map: dict[str, str] = {}  # code -> label

    for label, sw_codes in INDUSTRY_BOARDS.items():
        for sw_code in sw_codes:
            try:
                df = retry_with_backoff(
                    lambda c=sw_code: ak.sw_index_third_cons(symbol=c),
                    stock_code=f"sw:{sw_code}",
                )
            except Exception as e:
                logger.error("[%s] %s: fetch failed: %s", label, sw_code, e)
                continue

            if df is None or df.empty:
                logger.warning("[%s] %s: returned empty.", label, sw_code)
                continue

            # 申万成分股代码格式为 "688234.SH"，取前6位即纯数字代码
            code_col = next((c for c in df.columns if "代码" in c), df.columns[1])
            symbols = df[code_col].astype(str).str[:6].tolist()
            for code in symbols:
                # 已有映射的代码保持不变（首次遇到的行业标签优先）
                if code not in industry_map:
                    industry_map[code] = label
            all_symbols.update(symbols)
            logger.info("[%s] %s: %d stocks.", label, sw_code, len(symbols))

    # 追加 ETF_BOARDS（手动维护，不经过申万行业 API）
    etf_names: dict[str, str] = {}  # code -> name
    for label, stocks in ETF_BOARDS.items():
        for code, name in stocks:
            if code not in industry_map:
                industry_map[code] = label
            all_symbols.add(code)
            etf_names[code] = name
    logger.info("ETF/指数手动追加: %d 只", len(etf_names))

    # 将 ETF 名称合并写入 stock_names.csv（仅补充缺失条目，不覆盖已有数据）
    if etf_names and NAMES_CSV.exists():
        names_df = pd.read_csv(NAMES_CSV, dtype=str)
        existing_codes = set(names_df["code"])
        new_rows = [
            {"code": code, "name": name}
            for code, name in etf_names.items()
            if code not in existing_codes
        ]
        if new_rows:
            names_df = pd.concat([names_df, pd.DataFrame(new_rows)], ignore_index=True)
            names_df.to_csv(NAMES_CSV, index=False, encoding="utf-8")
            logger.info("已向 stock_names.csv 追加 %d 条 ETF 名称", len(new_rows))

    result = sorted(all_symbols)
    logger.info("Stock pool built: %d unique stocks.", len(result))

    # 持久化行业映射
    if industry_map:
        INDUSTRIES_CSV.parent.mkdir(parents=True, exist_ok=True)
        mapping_df = pd.DataFrame(
            [{"代码": code, "行业": label} for code, label in sorted(industry_map.items())]
        )
        mapping_df.to_csv(INDUSTRIES_CSV, index=False, encoding="utf-8-sig")
        logger.info("行业映射已保存: %s (%d 只)", INDUSTRIES_CSV, len(mapping_df))

    return result
