# crawler/us_config.py
"""美股板块→股票映射（手动维护，含普通股 + ETF）"""

# {板块标签: [(代码, 名称), ...]}
US_STOCK_POOL: dict[str, list[tuple[str, str]]] = {

    # ── 科技（30只）───────────────────────────────────────────────────────────
    "科技": [
        ("AAPL",  "苹果"),        ("MSFT",  "微软"),        ("GOOGL", "谷歌"),
        ("AMZN",  "亚马逊"),      ("META",  "Meta"),        ("NVDA",  "英伟达"),
        ("TSLA",  "特斯拉"),      ("AMD",   "超微半导体"),   ("INTC",  "英特尔"),
        ("AVGO",  "博通"),        ("QCOM",  "高通"),        ("ORCL",  "甲骨文"),
        ("CRM",   "Salesforce"), ("ADBE",  "Adobe"),       ("AMAT",  "应用材料"),
        ("NOW",   "ServiceNow"), ("SNOW",  "Snowflake"),   ("PLTR",  "Palantir"),
        ("UBER",  "优步"),        ("LYFT",  "Lyft"),        ("ABNB",  "Airbnb"),
        ("SHOP",  "Shopify"),    ("SQ",    "Block"),       ("PYPL",  "PayPal"),
        ("NFLX",  "奈飞"),        ("SPOT",  "Spotify"),    ("TWLO",  "Twilio"),
        ("MU",    "美光科技"),    ("LRCX",  "泛林集团"),    ("KLAC",  "科磊半导体"),
    ],

    # ── 金融（25只）───────────────────────────────────────────────────────────
    "金融": [
        ("JPM",  "摩根大通"),    ("BAC",  "美国银行"),    ("GS",   "高盛"),
        ("MS",   "摩根士丹利"),  ("WFC",  "富国银行"),    ("C",    "花旗"),
        ("V",    "Visa"),       ("MA",   "万事达"),       ("AXP",  "美国运通"),
        ("SCHW", "嘉信理财"),   ("BLK",  "贝莱德"),       ("USB",  "合众银行"),
        ("PNC",  "PNC金融"),    ("TFC",  "Truist金融"),   ("COF",  "第一资本"),
        ("DFS",  "Discover"),   ("SYF",  "同步金融"),     ("ALLY", "Ally金融"),
        ("ICE",  "洲际交易所"), ("CME",  "芝加哥商交所"), ("SPGI", "标普全球"),
        ("MCO",  "穆迪"),       ("MSCI", "MSCI"),         ("FIS",  "FIS金融"),
        ("FISV", "Fiserv"),
    ],

    # ── 医疗（22只）───────────────────────────────────────────────────────────
    "医疗": [
        ("JNJ",  "强生"),          ("UNH",  "联合健康"),    ("PFE",  "辉瑞"),
        ("MRNA", "莫德纳"),        ("ABBV", "艾伯维"),      ("LLY",  "礼来"),
        ("MRK",  "默克"),          ("BMY",  "百时美施贵宝"), ("TMO",  "赛默飞"),
        ("ABT",  "雅培"),          ("MDT",  "美敦力"),      ("SYK",  "史赛克"),
        ("ISRG", "直觉外科"),      ("BSX",  "波士顿科学"),  ("EW",   "爱德华兹"),
        ("GILD", "吉利德"),        ("REGN", "再生元"),      ("VRTX", "顶点制药"),
        ("BIIB", "百健"),          ("AMGN", "安进"),        ("HCA",  "HCA医疗"),
        ("CVS",  "CVS健康"),
    ],

    # ── 能源（18只）───────────────────────────────────────────────────────────
    "能源": [
        ("XOM",  "埃克森美孚"),  ("CVX",  "雪佛龙"),    ("SLB",  "斯伦贝谢"),
        ("COP",  "康菲石油"),    ("EOG",  "EOG资源"),   ("PSX",  "飞利浦66"),
        ("MPC",  "马拉松石油"),  ("VLO",  "瓦莱罗能源"), ("PXD",  "先锋自然资源"),
        ("OXY",  "西方石油"),    ("HAL",  "哈里伯顿"),   ("BKR",  "贝克休斯"),
        ("DVN",  "德文能源"),    ("FANG", "钻石背能源"), ("APA",  "APA公司"),
        ("MRO",  "马拉松油气"),  ("HES",  "赫斯"),       ("CTRA", "科特拉能源"),
    ],

    # ── 消费（25只）───────────────────────────────────────────────────────────
    "消费": [
        ("WMT",  "沃尔玛"),    ("COST", "好市多"),    ("MCD",  "麦当劳"),
        ("SBUX", "星巴克"),    ("NKE",  "耐克"),      ("PG",   "宝洁"),
        ("KO",   "可口可乐"),  ("PEP",  "百事可乐"),  ("HD",   "家得宝"),
        ("LOW",  "劳氏"),      ("TGT",  "塔吉特"),    ("AMZN", "亚马逊"),  # 消费+科技跨列，取首次
        ("CVS",  "CVS健康"),   ("DG",   "达乐"),      ("DLTR", "Dollar Tree"),
        ("YUM",  "百胜餐饮"),  ("CMG",  "Chipotle"), ("DPZ",  "达美乐披萨"),
        ("DKNG", "DraftKings"),("WYNN", "永利度假"),  ("MGM",  "美高梅"),
        ("LVS",  "拉斯维加斯金沙"), ("F",  "福特"),   ("GM",   "通用汽车"),
        ("RIVN", "Rivian"),
    ],

    # ── 中概股（20只）─────────────────────────────────────────────────────────
    "中概股": [
        ("BABA", "阿里巴巴"),  ("PDD",  "拼多多"),    ("JD",   "京东"),
        ("BIDU", "百度"),      ("NTES", "网易"),      ("TME",  "腾讯音乐"),
        ("BILI", "哔哩哔哩"),  ("IQ",   "爱奇艺"),    ("EDU",  "新东方"),
        ("TAL",  "好未来"),    ("VIPS", "唯品会"),    ("MOMO", "挚文集团"),
        ("ZH",   "知乎"),      ("TIGR", "老虎证券"),  ("FUTU", "富途控股"),
        ("LI",   "理想汽车"),  ("NIO",  "蔚来"),      ("XPEV", "小鹏汽车"),
        ("ZK",   "中科云网"),  ("CANG", "苍穹物流"),
    ],

    # ── 指数ETF（12只）────────────────────────────────────────────────────────
    "指数ETF": [
        ("SPY",  "标普500ETF"),      ("QQQ",  "纳斯达克100ETF"),
        ("DIA",  "道琼斯ETF"),       ("IWM",  "罗素2000ETF"),
        ("VTI",  "全市场ETF"),       ("VOO",  "先锋标普500ETF"),
        ("IVV",  "iShares标普500"),  ("MDY",  "中盘股ETF"),
        ("IJR",  "小盘股ETF"),       ("VEA",  "发达市场ETF"),
        ("EEM",  "新兴市场ETF"),     ("ACWI", "全球股市ETF"),
    ],

    # ── 行业ETF（16只）────────────────────────────────────────────────────────
    "行业ETF": [
        ("XLK",  "科技行业ETF"),     ("XLF",  "金融行业ETF"),
        ("XLE",  "能源行业ETF"),     ("XLV",  "医疗行业ETF"),
        ("XLY",  "非必需消费ETF"),   ("XLP",  "必需消费ETF"),
        ("XLI",  "工业行业ETF"),     ("XLU",  "公用事业ETF"),
        ("XLRE", "房地产ETF"),       ("XLB",  "材料行业ETF"),
        ("ARKK", "ARK创新ETF"),      ("ARKW", "ARK下一代互联网ETF"),
        ("SOXX", "半导体ETF"),       ("CQQQ", "中国科技ETF"),
        ("KWEB", "中国互联网ETF"),   ("MCHI", "MSCI中国ETF"),
    ],

    # ── 商品/债券ETF（12只）───────────────────────────────────────────────────
    "商品ETF": [
        ("GLD",  "黄金ETF"),         ("SLV",  "白银ETF"),
        ("GDX",  "黄金矿业ETF"),     ("USO",  "原油ETF"),
        ("TLT",  "长期国债ETF"),     ("SHY",  "短期国债ETF"),
        ("HYG",  "高收益债ETF"),     ("LQD",  "投资级债券ETF"),
        ("BND",  "全债券市场ETF"),   ("PDBC", "多元商品ETF"),
        ("DBC",  "商品指数ETF"),     ("IAU",  "黄金ETF-iShares"),
    ],
}


def get_all_codes() -> list[str]:
    """返回所有美股代码列表（去重）"""
    seen: set[str] = set()
    result: list[str] = []
    for stocks in US_STOCK_POOL.values():
        for code, _ in stocks:
            if code not in seen:
                seen.add(code)
                result.append(code)
    return result


def get_industry_map() -> dict[str, str]:
    """返回 {code: 板块标签}（跨列股票取第一个板块）"""
    result: dict[str, str] = {}
    for industry, stocks in US_STOCK_POOL.items():
        for code, _ in stocks:
            if code not in result:
                result[code] = industry
    return result
