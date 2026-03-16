"""
三个工作流编排入口。

使用方式：
  python -m crawler.workflow init        # 历史初始化（一次性）
  python -m crawler.workflow daily       # 每日增量（收盘后）
  python -m crawler.workflow financial   # 季度财务（每季度首次）

在 Claude Code 中也可通过 ruflo MCP 工具调用：
  mcp__ruflo__terminal_execute: python -m crawler.workflow daily
  mcp__ruflo__workflow_create: 注册定时触发
"""
import logging
import sys

from crawler.fetchers.financial import fetch_financial_batch
from crawler.fetchers.market import fetch_daily_spot, fetch_history_batch
from crawler.fetchers.news import fetch_daily_news
from crawler.stock_pool import build_stock_pool

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def init_history() -> None:
    """历史初始化：构建股票池 → 并发拉取近3年日K。"""
    logger.info("=== 历史初始化工作流开始 ===")
    symbols = build_stock_pool()
    logger.info("股票池: %d 只", len(symbols))
    fetch_history_batch(symbols)
    logger.info("=== 历史初始化完成 ===")


def daily_update() -> None:
    """每日增量：拉取当日行情快照 + 公告。"""
    logger.info("=== 每日增量工作流开始 ===")
    symbols = build_stock_pool()
    fetch_daily_spot(symbols)
    fetch_daily_news(symbols)
    logger.info("=== 每日增量完成 ===")


def quarterly_financial() -> None:
    """季度财务：并发拉取三张财务报表。"""
    logger.info("=== 季度财务工作流开始 ===")
    symbols = build_stock_pool()
    fetch_financial_batch(symbols)
    logger.info("=== 季度财务完成 ===")


_COMMANDS = {
    "init":      init_history,
    "daily":     daily_update,
    "financial": quarterly_financial,
}

if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else None
    if cmd not in _COMMANDS:
        print(f"Usage: python -m crawler.workflow [{'|'.join(_COMMANDS)}]")
        sys.exit(1)
    _COMMANDS[cmd]()
