"""Stock pool builder from industry boards."""
import logging

import akshare as ak

from crawler.config import INDUSTRY_BOARDS
from crawler.utils import retry_with_backoff

logger = logging.getLogger(__name__)


def build_stock_pool() -> list[str]:
    """按行业板块拉取成分股，合并去重，返回股票代码列表。

    Returns:
        去重排序后的股票代码列表，如 ["000001", "600036", ...]
    """
    all_symbols: set[str] = set()

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
            all_symbols.update(symbols)
            logger.info("[%s] %s: %d stocks.", label, sw_code, len(symbols))

    result = sorted(all_symbols)
    logger.info("Stock pool built: %d unique stocks.", len(result))
    return result
