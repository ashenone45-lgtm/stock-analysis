# crawler/config.py
from pathlib import Path

# ── 项目根目录 ─────────────────────────────────────────────
BASE_DIR = Path(__file__).parent.parent

# ── 数据存储路径 ───────────────────────────────────────────
DATA_DIR = BASE_DIR / "data"
MARKET_DIR = DATA_DIR / "market"
FINANCIAL_DIR = DATA_DIR / "financial"
NEWS_DIR = DATA_DIR / "news"
LOG_FILE = BASE_DIR / "logs" / "errors.log"

# ── 历史数据参数 ───────────────────────────────────────────
HISTORY_YEARS = 3  # 拉取近 N 年历史

# ── 行业板块映射（东方财富板块名称） ─────────────────────────
# key: 内部标签；value: akshare stock_board_industry_cons_em() 接受的板块名称
INDUSTRY_BOARDS = {
    "IT服务":   "软件开发",
    "互联网":   "互联网服务",
    "半导体":   "半导体",
    "卫星航天": "航天航空",
    "有色金属": "有色金属",
}

# ── 并发参数 ───────────────────────────────────────────────
MAX_WORKERS = 8  # 并发爬取线程数
