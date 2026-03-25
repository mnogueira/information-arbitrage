from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from information_arbitrage.env import get_bool, get_csv, get_env, get_float, get_int, get_json


DEFAULT_RSS_FEEDS = [
    "https://feeds.reuters.com/reuters/topNews",
    "http://feeds.bbci.co.uk/news/world/rss.xml",
    "https://www.aljazeera.com/xml/rss/all.xml",
    "https://news.google.com/rss/search?q=iran+oil+war+ceasefire",
    "https://www.alarabiya.net/.mrss/en.xml",
    "https://www.aa.com.tr/en/rss/default?cat=world",
    "https://www.arabnews.com/rss.xml",
    "https://gulfnews.com/rss",
    "https://www.dawn.com/feeds/home",
    "https://feeds.bbci.co.uk/mundo/rss.xml",
]

DEFAULT_POLYMARKET_KEYWORDS = [
    "iran",
    "oil",
    "geopolitical",
    "ceasefire",
    "hormuz",
    "middle east",
]

DEFAULT_IB_NEWS_PROVIDERS = [
    "BZ",
    "BRFG",
    "BRFUPDN",
    "DJ-GM",
    "DJ-N",
    "DJ-RT",
    "DJ-RTG",
    "DJ-RTE",
    "DJ-RTA",
    "DJNL",
]


@dataclass(slots=True)
class Settings:
    base_dir: Path
    data_dir: Path
    docs_dir: Path
    db_path: Path
    confidence_threshold: float = 0.6
    max_headline_age_seconds: int = 300
    stale_similarity_window_minutes: int = 30
    confirmation_window_minutes: int = 5
    max_open_positions: int = 5
    daily_loss_limit_fraction: float = 0.02
    account_equity: float = 100_000.0
    headline_claim_batch_size: int = 25
    trader_poll_interval_seconds: float = 1.0
    rss_poll_interval_seconds: float = 30.0
    polymarket_poll_interval_seconds: float = 15.0
    ib_news_poll_interval_seconds: float = 10.0
    ib_price_poll_interval_seconds: float = 1.0
    enable_google_trends_collector: bool = False
    openai_api_key: str | None = None
    openai_model: str = "gpt-5-mini"
    openai_reasoning_effort: str = "low"
    ib_data_host: str = "127.0.0.1"
    ib_data_port: int = 4001
    ib_data_client_id: int = 41
    ib_data_account: str | None = None
    ib_exec_host: str = "127.0.0.1"
    ib_exec_port: int = 4002
    ib_exec_client_id: int = 42
    ib_exec_account: str | None = None
    ib_news_provider_codes: list[str] = field(default_factory=lambda: list(DEFAULT_IB_NEWS_PROVIDERS))
    ib_news_watch_symbols: list[str] = field(
        default_factory=lambda: ["CL", "BRN", "BZ", "PBR", "VALE", "ITUB", "EWZ", "GC", "EURUSD"]
    )
    ib_contract_months: dict[str, str] = field(default_factory=dict)
    mt5_path: str = r"C:\Program Files\MetaTrader 5 Terminal\terminal64.exe"
    mt5_login: int | None = None
    mt5_server: str | None = None
    mt5_password: str | None = None
    rss_feeds: list[str] = field(default_factory=lambda: list(DEFAULT_RSS_FEEDS))
    polymarket_keywords: list[str] = field(default_factory=lambda: list(DEFAULT_POLYMARKET_KEYWORDS))
    polymarket_limit: int = 200
    telegram_bot_token: str | None = None
    telegram_chat_id: str | None = None
    simulate_only: bool = False

    @classmethod
    def from_env(cls, base_dir: str | Path | None = None) -> "Settings":
        base = Path(base_dir or Path.cwd()).resolve()
        data_dir = base / "data"
        docs_dir = base / "docs"
        data_dir.mkdir(parents=True, exist_ok=True)

        return cls(
            base_dir=base,
            data_dir=data_dir,
            docs_dir=docs_dir,
            db_path=Path(get_env("IA_DB_PATH", str(data_dir / "information_arbitrage.duckdb"))),
            confidence_threshold=get_float("IA_CONFIDENCE_THRESHOLD", 0.6),
            max_headline_age_seconds=get_int("IA_MAX_HEADLINE_AGE_SECONDS", 300),
            stale_similarity_window_minutes=get_int("IA_STALE_SIMILARITY_WINDOW_MINUTES", 30),
            confirmation_window_minutes=get_int("IA_CONFIRMATION_WINDOW_MINUTES", 5),
            max_open_positions=get_int("IA_MAX_OPEN_POSITIONS", 5),
            daily_loss_limit_fraction=get_float("IA_DAILY_LOSS_LIMIT_FRACTION", 0.02),
            account_equity=get_float("IA_ACCOUNT_EQUITY", 100_000.0),
            headline_claim_batch_size=get_int("IA_HEADLINE_CLAIM_BATCH_SIZE", 25),
            trader_poll_interval_seconds=get_float("IA_TRADER_POLL_INTERVAL_SECONDS", 1.0),
            rss_poll_interval_seconds=get_float("IA_RSS_POLL_INTERVAL_SECONDS", 30.0),
            polymarket_poll_interval_seconds=get_float("IA_POLYMARKET_POLL_INTERVAL_SECONDS", 15.0),
            ib_news_poll_interval_seconds=get_float("IA_IB_NEWS_POLL_INTERVAL_SECONDS", 10.0),
            ib_price_poll_interval_seconds=get_float("IA_IB_PRICE_POLL_INTERVAL_SECONDS", 1.0),
            enable_google_trends_collector=get_bool("IA_ENABLE_GOOGLE_TRENDS", False),
            openai_api_key=get_env("OPENAI_API_KEY"),
            openai_model=get_env("OPENAI_MODEL", "gpt-5-mini") or "gpt-5-mini",
            openai_reasoning_effort=get_env("OPENAI_REASONING_EFFORT", "low") or "low",
            ib_data_host=get_env("IB_DATA_HOST", "127.0.0.1") or "127.0.0.1",
            ib_data_port=get_int("IB_DATA_PORT", 4001),
            ib_data_client_id=get_int("IB_DATA_CLIENT_ID", 41),
            ib_data_account=get_env("IB_DATA_ACCOUNT", "U4212647"),
            ib_exec_host=get_env("IB_EXEC_HOST", "127.0.0.1") or "127.0.0.1",
            ib_exec_port=get_int("IB_EXEC_PORT", 4002),
            ib_exec_client_id=get_int("IB_EXEC_CLIENT_ID", 42),
            ib_exec_account=get_env("IB_EXEC_ACCOUNT", "DUP391965"),
            ib_news_provider_codes=get_csv("IB_NEWS_PROVIDER_CODES", list(DEFAULT_IB_NEWS_PROVIDERS)),
            ib_news_watch_symbols=get_csv(
                "IB_NEWS_WATCH_SYMBOLS",
                ["CL", "BRN", "BZ", "PBR", "VALE", "ITUB", "EWZ", "GC", "EURUSD"],
            ),
            ib_contract_months=get_json("IB_CONTRACT_MONTHS", {}),
            mt5_path=get_env("MT5_PATH", r"C:\Program Files\MetaTrader 5 Terminal\terminal64.exe")
            or r"C:\Program Files\MetaTrader 5 Terminal\terminal64.exe",
            mt5_login=int(get_env("MT5_LOGIN", "53981091")) if get_env("MT5_LOGIN", "53981091") else None,
            mt5_server=get_env("MT5_SERVER", "XPMT5-DEMO"),
            mt5_password=get_env("MT5_PASSWORD"),
            rss_feeds=get_csv("IA_RSS_FEEDS", list(DEFAULT_RSS_FEEDS)),
            polymarket_keywords=get_csv("IA_POLYMARKET_KEYWORDS", list(DEFAULT_POLYMARKET_KEYWORDS)),
            polymarket_limit=get_int("IA_POLYMARKET_LIMIT", 200),
            telegram_bot_token=get_env("TELEGRAM_BOT_TOKEN"),
            telegram_chat_id=get_env("TELEGRAM_CHAT_ID"),
            simulate_only=get_bool("IA_SIMULATE_ONLY", False),
        )
