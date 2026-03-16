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

    for label, board_name in INDUSTRY_BOARDS.items():
        try:
            df = retry_with_backoff(
                lambda bn=board_name: ak.stock_board_industry_cons_em(symbol=bn),
                stock_code=f"board:{label}",
            )
        except Exception as e:
            logger.error("[%s] %s: fetch failed: %s", label, board_name, e)
            continue

        if df is None or df.empty:
            logger.warning("[%s] %s: returned empty.", label, board_name)
            continue

        # 寻找代码列（东方财富通常为 "代码"）
        code_col = next((c for c in df.columns if "代码" in c), df.columns[0])
        symbols = df[code_col].astype(str).str.zfill(6).tolist()
        all_symbols.update(symbols)
        logger.info("[%s] %s: %d stocks.", label, board_name, len(symbols))

    result = sorted(all_symbols)
    logger.info("Stock pool built: %d unique stocks.", len(result))
    return result
