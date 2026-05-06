# pipeline/constants.py
#
# Single source of truth for all hardcoded values in the pipeline.
# Operator-configurable values live in config.py; physics-of-the-system
# values (costs, delays, protocol constants) live here.

# --- API cost per call (USD) ---
API_COSTS: dict[str, float] = {
    "serper_producer": 0.001,     # DNS-miss path: Serper called in producer
    "serper_dispatcher": 0.001,   # fallback: Serper called in dispatcher after patterns exhausted
    "zuhal": 0.0005,              # rescue backend — runs after both SMTP backends return invalid
}

# --- Exponential backoff parameters (base_seconds, max_seconds) per service ---
SERVICE_BACKOFF: dict[str, tuple[float, float]] = {
    "dns": (0.5, 8.0),
    "serper": (1.0, 32.0),
    "zuhal": (1.0, 64.0),
    "bbops": (1.0, 60.0),
    "racknerd": (1.0, 32.0),
}

# --- DNS ---
DNS_TLDS: tuple[str, ...] = (".com", ".net", ".org", ".us", ".info")
DNS_CONCURRENCY_DEFAULT: int = 100
DOMAIN_STEM_MIN_LENGTH: int = 5
MAX_WITHOUT_CANDIDATES: int = 3

# --- Racknerd / direct SMTP ---
RACKNERD_SMTP_TIMEOUT_S: float = 15.0
RACKNERD_MX_CACHE_TTL_S: int = 3600       # 1 hour MX resolution cache
RACKNERD_MX_MAX_HOSTS: int = 3            # probe up to N MX hosts per domain
RACKNERD_SPAMHAUS_WINDOW_S: int = 60      # sliding window for block detection
RACKNERD_SPAMHAUS_THRESHOLD: int = 100    # blocks in window before cooldown
RACKNERD_SPAMHAUS_COOLDOWN_S: float = 300.0  # seconds to pause all SMTP on cooldown

# --- SSH tunnel ---
TUNNEL_READY_RETRIES: int = 20
TUNNEL_READY_INTERVAL_S: float = 0.5
TUNNEL_STOP_TIMEOUT_S: float = 3.0
TUNNEL_BACKOFF_START_S: float = 2.0
TUNNEL_BACKOFF_MAX_S: float = 60.0

# --- Dispatcher ---
DISPATCH_POLL_MAX_INTERVAL_S: int = 30
DISPATCH_POLL_EMPTY_BACKOFF_THRESHOLD: int = 3

# --- Fallback domain blocklist ---
# Known directory/aggregator domains that should never be used as a business domain.
# This list grows at runtime via ProducerWorker._fallback_blocklist when a domain
# is seen as first-organic fallback for 2+ different businesses in the same run.
FALLBACK_DOMAIN_BLOCKLIST: frozenset[str] = frozenset({
    # Generic business directories
    "bbb.org", "yelp.com", "yellowpages.com", "manta.com",
    "bizapedia.com", "opencorporates.com", "corporationwiki.com",
    "dnb.com", "zoominfo.com", "bizstanding.com", "bizjournals.com",
    "inc.com", "bloomberg.com",
    # Nonprofit/charity aggregators — emit generic info@/support@ addresses
    "greatnonprofits.org", "guidestar.org", "candid.org", "charitynavigator.org",
    "idealist.org", "volunteermatch.org", "nonprofit.cores.org",
    # Social / people-search
    "linkedin.com", "facebook.com", "instagram.com", "twitter.com", "x.com",
    "whitepages.com", "spokeo.com", "intelius.com",
    # Maps / generic web
    "google.com", "mapquest.com",
})

