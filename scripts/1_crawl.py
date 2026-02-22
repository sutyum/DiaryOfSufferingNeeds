import os
import sys
import hashlib
import sqlite3
import time
from firecrawl import FirecrawlApp
from pathlib import Path
from tenacity import retry, stop_after_attempt, wait_exponential
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
import re

SOURCES = [
    {
        "name": "Science for ME - Patient Experiences",
        "url": "https://www.s4me.info/forums/patient-experiences-and-stories.18/",
        "queries": ["patient stories", "threads", "experiences"],
        "allow_tokens": ["threads", "experiences", "stories", "patients"],
        "prefer_tokens": ["/threads/"],
        "deny_tokens": ["news-in-brief", "whats-new", "dismiss-notice", "/help/"],
        "hard_deny_tokens": ["news-in-brief", "whats-new", "dismiss-notice"],
        "enabled": False,
        "disabled_reason": "Login wall for anonymous users (verified 2026-02-22).",
    },
    {
        "name": "Science for ME - ME/CFS Coping & Management",
        "url": "https://www.s4me.info/forums/me-cfs-coping-management.8/",
        "queries": ["coping", "thread", "management"],
        "allow_tokens": ["threads", "coping", "management", "me-cfs"],
        "prefer_tokens": ["/threads/"],
        "deny_tokens": ["news-in-brief", "whats-new", "dismiss-notice", "/help/"],
        "hard_deny_tokens": ["news-in-brief", "whats-new", "dismiss-notice"],
        "enabled": False,
        "disabled_reason": "404 forum endpoint (verified 2026-02-22).",
    },
    {
        "name": "Phoenix Rising - Disease Onset & Progression",
        "url": "https://forums.phoenixrising.me/forums/disease-onset-and-progression.254/",
        "queries": ["patient story", "disease onset", "progression"],
        "allow_tokens": ["threads", "disease-onset", "progression", "patient", "story"],
        "prefer_tokens": ["/threads/"],
        "max_urls": 300,
    },
    {
        "name": "Phoenix Rising - Symptoms & Treatments",
        "url": "https://forums.phoenixrising.me/forums/symptoms.31/",
        "queries": ["symptoms", "treatments", "thread"],
        "allow_tokens": ["threads", "symptom", "pain", "dysautonomia", "sleep", "neurological"],
        "prefer_tokens": ["/threads/"],
        "max_urls": 250,
    },
    {
        "name": "Phoenix Rising - Living With ME/CFS",
        "url": "https://forums.phoenixrising.me/forums/living-with-me-cfs.108/",
        "queries": ["living with me/cfs", "daily life", "disability"],
        "allow_tokens": ["threads", "living-with-me-cfs", "disability", "lifestyle", "daily"],
        "prefer_tokens": ["/threads/"],
        "max_urls": 200,
    },
    {
        "name": "Healthtalk.org - Chronic Pain",
        "url": "https://healthtalk.org/chronic-pain/overview",
        "queries": ["experiences", "lived experience", "story"],
        "allow_tokens": ["chronic-pain", "experiences", "living-with", "interview"],
        "prefer_tokens": ["/experiences/", "/interview/"],
        "required_tokens": ["chronic-pain"],
        "min_score": 12,
        "max_urls": 200,
    },
    {
        "name": "Healthtalk.org - Long COVID",
        "url": "https://healthtalk.org/long-covid/overview",
        "queries": ["experiences", "lived experience", "story"],
        "allow_tokens": ["long-covid", "experiences", "living-with", "interview"],
        "prefer_tokens": ["/experiences/", "/interview/"],
        "required_tokens": ["long-covid"],
        "min_score": 12,
        "max_urls": 200,
    },
    {
        "name": "Dysautonomia International - Patient Stories",
        "url": "http://www.dysautonomiainternational.org/page.php?ID=14",
        "queries": ["patient stories", "story", "personal experience"],
        "allow_tokens": ["patient", "story", "dysautonomia", "page.php"],
        "prefer_tokens": ["page.php?id=14", "story"],
        "min_score": 12,
        "max_urls": 120,
        "enabled": False,
        "disabled_reason": "Current endpoint maps mostly organizational pages, not patient narratives (verified 2026-02-22).",
    },
    {
        "name": "Surviving Antidepressants - Introductions and Updates",
        "url": "https://www.survivingantidepressants.org/forum/3-introductions-and-updates/",
        "queries": ["topic", "introduction", "experience"],
        "allow_tokens": ["topic", "introductions", "updates"],
        "prefer_tokens": ["/topic/"],
    },
    {
        "name": "Ehlers-Danlos Society - Our Stories",
        "url": "https://www.ehlers-danlos.com/our-stories/",
        "queries": ["our stories", "patient stories", "journey"],
        "allow_tokens": ["our-stories", "story", "patient"],
        "prefer_tokens": ["/story/"],
    },
]

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "public_data" / "scraped"
REJECTED_DIR = BASE_DIR / "public_data" / "scraped_rejected"
DB_PATH = BASE_DIR / "public_data" / "crawl_state.db"
MAX_RETRIES = int(os.environ.get("CRAWL_MAX_RETRIES", "3"))
MAP_DELAY_SECONDS = float(os.environ.get("CRAWL_MAP_DELAY_SECONDS", "15"))
CHUNK_DELAY_SECONDS = float(os.environ.get("CRAWL_CHUNK_DELAY_SECONDS", "2"))
MAP_LIMIT_PER_CALL = int(os.environ.get("CRAWL_MAP_LIMIT_PER_CALL", "2000"))
MAX_URLS_PER_SOURCE = int(os.environ.get("CRAWL_MAX_URLS_PER_SOURCE", "1200"))
MIN_RELEVANCE_SCORE = int(os.environ.get("CRAWL_MIN_RELEVANCE_SCORE", "6"))
MIN_FALLBACK_HIGH_SIGNAL_LINKS = int(os.environ.get("CRAWL_MIN_FALLBACK_HIGH_SIGNAL_LINKS", "3"))
MIN_MARKDOWN_CHARS = int(os.environ.get("CRAWL_MIN_MARKDOWN_CHARS", "250"))
MIN_MARKDOWN_CHARS_FALLBACK = int(os.environ.get("CRAWL_MIN_MARKDOWN_CHARS_FALLBACK", "120"))
PROCESSING_STALE_HOURS = int(os.environ.get("CRAWL_PROCESSING_STALE_HOURS", "2"))
MAX_SOURCES = int(os.environ.get("CRAWL_MAX_SOURCES", "0"))
QUARANTINE_SUSPECT_OUTPUTS = os.environ.get("CRAWL_QUARANTINE_SUSPECT_OUTPUTS", "1").lower() not in {"0", "false", "no"}

HIGH_SIGNAL_URL_TOKENS = (
    "patient",
    "patients",
    "story",
    "stories",
    "journey",
    "experience",
    "experiences",
    "diagnosis",
    "symptom",
    "symptoms",
    "treatment",
    "treatments",
    "long-covid",
    "me-cfs",
    "chronic-pain",
    "fibromyalgia",
    "dysautonomia",
)

LOW_SIGNAL_URL_TOKENS = (
    "login",
    "logout",
    "register",
    "signup",
    "sign-up",
    "privacy",
    "terms",
    "cookies",
    "contact",
    "about-us",
    "members",
    "member",
    "profile",
    "search",
    "tag/",
    "/tags",
    "/feed",
    "/rss",
    "wp-json",
    "wp-admin",
    "calendar",
    "whats-new",
    "dismiss-notice",
    "lost-password",
    "announcement",
    "news-in-brief",
    "/account/",
    "/help/",
    "/members/",
)

TRACKING_QUERY_KEYS = {
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_term",
    "utm_content",
    "fbclid",
    "gclid",
    "ref",
    "source",
}

BINARY_FILE_EXTENSIONS = {
    ".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg", ".pdf",
    ".zip", ".rar", ".7z", ".mp4", ".mp3", ".avi", ".mov",
    ".doc", ".docx", ".ppt", ".pptx", ".xls", ".xlsx",
}

INTERSTITIAL_HARD_MARKERS = (
    "access denied",
    "verify you are human",
    "checking if the site connection is secure",
    "just a moment",
)

INTERSTITIAL_SOFT_MARKERS = (
    "enable javascript",
    "captcha",
    "cloudflare",
    "bot protection",
)

ERROR_PAGE_MARKERS = (
    "oops! we ran into some problems",
    "the requested forum could not be found",
    "page not found",
    "404 not found",
)

AUTH_WALL_HARD_MARKERS = (
    "you must be logged-in to do that",
    "log in | science for me",
    "# log in",
)

AUTH_WALL_SOFT_MARKERS = (
    "log in",
    "register",
    "forgot your password",
    "stay logged in",
    "you must be logged-in",
)

DIRECTORY_CONTENT_MARKERS = (
    "topics in this forum",
    "sort by",
    "new posts",
    "search forums",
    "install the app",
    "first page",
    "last page",
    "page 1 of",
)

class FirecrawlConnectivityError(RuntimeError):
    pass

def _source_host(url: str) -> str:
    return urlparse(url).netloc.lower()

def _is_story_like_url(url: str, source: Dict[str, Any]) -> bool:
    url_lower = url.lower()
    default_story_tokens = (
        "/threads/",
        "/thread/",
        "/topic/",
        "/topics/",
        "/story/",
        "/stories/",
        "/journey/",
        "/interview/",
    )
    if any(token in url_lower for token in default_story_tokens):
        return True

    source_tokens = source.get("prefer_tokens", [])
    return any(isinstance(token, str) and token.lower() in url_lower for token in source_tokens)

def _is_forum_listing_url(url: str) -> bool:
    path = urlparse(url).path.lower()
    return any(token in path for token in ("/forums/", "/forum/", "/categories/"))

def _allow_seed_fallback(source: Dict[str, Any]) -> bool:
    configured = source.get("allow_seed_fallback")
    if isinstance(configured, bool):
        return configured
    return not _is_forum_listing_url(source.get("url", ""))

def _source_is_enabled(source: Dict[str, Any]) -> bool:
    configured = source.get("enabled")
    if configured is None:
        return True
    return bool(configured)

def _source_min_score(source: Dict[str, Any]) -> int:
    configured = source.get("min_score")
    if isinstance(configured, int) and configured > 0:
        return configured
    return MIN_RELEVANCE_SCORE

def _extract_links(map_result: Any) -> List[str]:
    if isinstance(map_result, dict):
        links = map_result.get("links", [])
    else:
        links = getattr(map_result, "links", [])
    if not isinstance(links, list):
        return []
    return [url for url in links if isinstance(url, str)]

def _extract_markdown(scrape_result: Any) -> str:
    if isinstance(scrape_result, dict):
        markdown = scrape_result.get("markdown", "")
    else:
        markdown = getattr(scrape_result, "markdown", "")
    return markdown if isinstance(markdown, str) else ""

def _extract_source_url_from_markdown(markdown_content: str) -> str:
    match = re.search(r"<!-- SOURCE_URL:\s*(.*?)\s*-->", markdown_content)
    if not match:
        return ""
    extracted = match.group(1).strip()
    return _canonicalize_url(extracted) or extracted

def _canonicalize_url(url: str) -> str:
    if not isinstance(url, str):
        return ""
    candidate = url.strip()
    if not candidate:
        return ""

    parsed = urlparse(candidate)
    if parsed.scheme.lower() not in {"http", "https"} or not parsed.netloc:
        return ""

    normalized_path = parsed.path
    # Collapse common forum pagination/post anchors back to canonical thread/topic URL.
    normalized_path = re.sub(r"/post-\d+/?$", "", normalized_path)
    normalized_path = re.sub(r"/latest/?$", "", normalized_path)
    normalized_path = re.sub(r"/page-\d+/?$", "", normalized_path)
    normalized_path = re.sub(r"/page/\d+/?$", "", normalized_path)

    query_pairs = parse_qsl(parsed.query, keep_blank_values=True)
    kept_query = [(k, v) for k, v in query_pairs if k.lower() not in TRACKING_QUERY_KEYS]
    if "/threads/" in normalized_path or "/topic/" in normalized_path or "/topics/" in normalized_path:
        kept_query = [(k, v) for k, v in kept_query if k.lower() not in {"page", "p"}]
    if normalized_path not in {"", "/"}:
        normalized_path = normalized_path.rstrip("/")
    normalized = parsed._replace(
        scheme=parsed.scheme.lower(),
        netloc=parsed.netloc.lower(),
        path=normalized_path,
        query=urlencode(kept_query, doseq=True),
        fragment="",
    )
    canonical = urlunparse(normalized)
    return canonical

def _has_binary_extension(url: str) -> bool:
    path = urlparse(url).path.lower()
    return any(path.endswith(ext) for ext in BINARY_FILE_EXTENSIONS)

def _score_url(url: str, source: Dict[str, Any]) -> int:
    parsed = urlparse(url)
    path = parsed.path.lower()
    url_lower = url.lower()
    score = 0

    hard_deny_tokens = source.get("hard_deny_tokens", [])
    if any(isinstance(token, str) and token.lower() in url_lower for token in hard_deny_tokens):
        return -100

    source_host = _source_host(source["url"])
    if parsed.netloc.lower() == source_host:
        score += 4

    if _has_binary_extension(url):
        return -100

    is_story_like = _is_story_like_url(url, source)
    if is_story_like:
        score += 20

    if "/threads/" in path or "/topic/" in path:
        score += 12
    if "/forums/" in path or "/forum/" in path:
        score += 4
        if not is_story_like:
            score -= 12

    if "/page/" in path or "page-" in path or "page=" in parsed.query.lower():
        if not is_story_like:
            score -= 10

    high_signal_hits = sum(1 for token in HIGH_SIGNAL_URL_TOKENS if token in url_lower)
    score += min(24, high_signal_hits * 3)

    low_signal_hits = sum(1 for token in LOW_SIGNAL_URL_TOKENS if token in url_lower)
    score -= min(40, low_signal_hits * 8)

    if url == _canonicalize_url(source["url"]) and not is_story_like:
        score -= 2

    allow_tokens = source.get("allow_tokens", [])
    deny_tokens = source.get("deny_tokens", [])
    score += sum(4 for token in allow_tokens if token.lower() in url_lower)
    score -= sum(10 for token in deny_tokens if token.lower() in url_lower)

    if any(marker in parsed.query.lower() for marker in ("sort", "advancedsearchform", "view", "filter")):
        score -= 8

    if parsed.query and "?" in url and not any(key in parsed.query.lower() for key in ("id=", "page=", "p=")):
        score -= 4

    return score

def _rank_candidate_urls(raw_urls: Iterable[str], source: Dict[str, Any]) -> List[Tuple[str, int]]:
    seed_host = _source_host(source["url"])
    ranked: Dict[str, int] = {}
    required_tokens = [
        token.strip().lower()
        for token in source.get("required_tokens", [])
        if isinstance(token, str) and token.strip()
    ]
    min_score = _source_min_score(source)

    for raw_url in raw_urls:
        normalized = _canonicalize_url(raw_url)
        if not normalized:
            continue

        parsed_host = _source_host(normalized)
        if parsed_host != seed_host:
            continue
        if required_tokens and not any(token in normalized.lower() for token in required_tokens):
            continue

        score = _score_url(normalized, source)
        current = ranked.get(normalized)
        if current is None or score > current:
            ranked[normalized] = score

    filtered = [(url, score) for url, score in ranked.items() if score >= min_score]
    filtered.sort(key=lambda item: (-item[1], item[0]))
    return filtered

def _source_queries(source: Dict[str, Any]) -> List[Optional[str]]:
    queries = [None]
    for query in source.get("queries", []):
        if isinstance(query, str) and query.strip():
            queries.append(query.strip())
    return queries

def _min_markdown_chars_for_url(url: str) -> int:
    url_lower = url.lower()
    if any(token in url_lower for token in ("thread", "topic", "story", "journey", "experience")):
        return MIN_MARKDOWN_CHARS
    return MIN_MARKDOWN_CHARS_FALLBACK

def _looks_like_interstitial(markdown_content: str) -> bool:
    content = markdown_content.lower()
    if any(marker in content for marker in INTERSTITIAL_HARD_MARKERS):
        return True

    soft_hits = sum(1 for marker in INTERSTITIAL_SOFT_MARKERS if marker in content)
    short_page = len(content) < 8000
    marker_dense_header = sum(1 for marker in INTERSTITIAL_SOFT_MARKERS if marker in content[:2000]) >= 2
    return soft_hits >= 2 and (short_page or marker_dense_header)

def _looks_like_error_page(markdown_content: str) -> bool:
    content = markdown_content.lower()
    return any(marker in content for marker in ERROR_PAGE_MARKERS)

def _looks_like_auth_wall(markdown_content: str) -> bool:
    content = markdown_content.lower()
    if any(marker in content for marker in AUTH_WALL_HARD_MARKERS):
        return True

    soft_hits = sum(1 for marker in AUTH_WALL_SOFT_MARKERS if marker in content)
    short_page = len(content) < 18000
    heading_login = bool(re.search(r"(^|\n)#+\s*log in\b", content))
    return heading_login or (soft_hits >= 3 and short_page and "xenforo" in content)

def _looks_like_directory_page(markdown_content: str, url: str) -> bool:
    content_lower = markdown_content.lower()
    lines = [line.strip() for line in markdown_content.splitlines() if line.strip()]
    if len(lines) < 35:
        return False

    linkish_lines = 0
    narrative_lines = 0
    for line in lines:
        if line.startswith("- [") or line.startswith("[") or line.startswith("|"):
            linkish_lines += 1
            continue
        if len(line) > 100 and not re.search(r"https?://", line):
            narrative_lines += 1

    ratio = linkish_lines / len(lines)
    url_lower = url.lower()
    listing_context = (
        any(token in url_lower for token in ("/forums/", "/forum/", "/categories/", "/page/", "page-"))
        or any(token in content_lower for token in DIRECTORY_CONTENT_MARKERS)
    )
    return listing_context and ratio > 0.58 and narrative_lines < 6

def _is_suspect_markdown_content(markdown_content: str, url_hint: str = "") -> bool:
    return (
        _looks_like_interstitial(markdown_content)
        or _looks_like_auth_wall(markdown_content)
        or _looks_like_error_page(markdown_content)
        or _looks_like_directory_page(markdown_content, url_hint)
    )

def _output_path_for_url(url: str) -> Path:
    url_hash = hashlib.md5(url.encode("utf-8")).hexdigest()
    return DATA_DIR / f"{url_hash}.md"

def _move_to_rejected(file_path: Path) -> Optional[Path]:
    if not file_path.exists():
        return None

    REJECTED_DIR.mkdir(parents=True, exist_ok=True)
    target_path = REJECTED_DIR / file_path.name
    if target_path.exists():
        stem = file_path.stem
        suffix = file_path.suffix
        index = 1
        while target_path.exists():
            target_path = REJECTED_DIR / f"{stem}.q{index}{suffix}"
            index += 1
    file_path.replace(target_path)
    return target_path

def apply_disabled_source_filters(conn: sqlite3.Connection) -> int:
    enabled_names = {source.get("name") for source in SOURCES if _source_is_enabled(source)}
    if not enabled_names:
        return 0

    cursor = conn.cursor()
    cursor.execute("SELECT url, source_name, status, retry_count FROM urls")
    rows = cursor.fetchall()

    updated = 0
    for url, source_name, status, retry_count in rows:
        if source_name in enabled_names:
            continue

        if status != "FAILED" or int(retry_count or 0) < MAX_RETRIES:
            conn.execute(
                "UPDATE urls SET status = 'FAILED', retry_count = ?, processed_at = CURRENT_TIMESTAMP WHERE url = ?",
                (MAX_RETRIES, url),
            )
            updated += 1

        _move_to_rejected(_output_path_for_url(url))

    return updated

def quarantine_suspect_outputs(conn: sqlite3.Connection) -> int:
    if not DATA_DIR.exists():
        return 0

    moved = 0

    for file_path in sorted(DATA_DIR.glob("*.md")):
        try:
            content = file_path.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue

        source_url = _extract_source_url_from_markdown(content)
        url_hint = source_url or ""
        if not _is_suspect_markdown_content(content, url_hint):
            continue

        moved_path = _move_to_rejected(file_path)
        if moved_path is None:
            continue
        moved += 1

        if source_url:
            conn.execute(
                "UPDATE urls SET status = 'FAILED', retry_count = ?, processed_at = CURRENT_TIMESTAMP WHERE url = ?",
                (MAX_RETRIES, source_url),
            )

    return moved

def _status_priority(status: str) -> int:
    order = {
        "PENDING": 4,
        "PROCESSING": 3,
        "FAILED": 2,
        "COMPLETED": 1,
    }
    return order.get(status, 0)

def normalize_existing_urls(conn: sqlite3.Connection) -> int:
    cursor = conn.cursor()
    cursor.execute(
        "SELECT url, source_name, status, discovered_at, processed_at, retry_count, priority FROM urls"
    )
    rows = cursor.fetchall()
    if not rows:
        return 0

    aggregated: Dict[str, Dict[str, Any]] = {}
    for url, source_name, status, discovered_at, processed_at, retry_count, priority in rows:
        canonical = _canonicalize_url(url) or url
        existing = aggregated.get(canonical)
        candidate = {
            "url": canonical,
            "source_name": source_name,
            "status": status,
            "discovered_at": discovered_at,
            "processed_at": processed_at,
            "retry_count": int(retry_count or 0),
            "priority": int(priority or 0),
        }

        if existing is None:
            aggregated[canonical] = candidate
            continue

        if _status_priority(candidate["status"]) > _status_priority(existing["status"]):
            existing["status"] = candidate["status"]
        if candidate["source_name"] and not existing["source_name"]:
            existing["source_name"] = candidate["source_name"]
        existing["retry_count"] = max(existing["retry_count"], candidate["retry_count"])
        existing["priority"] = max(existing["priority"], candidate["priority"])

        if candidate["discovered_at"] and (
            not existing["discovered_at"] or candidate["discovered_at"] < existing["discovered_at"]
        ):
            existing["discovered_at"] = candidate["discovered_at"]
        if candidate["processed_at"] and (
            not existing["processed_at"] or candidate["processed_at"] > existing["processed_at"]
        ):
            existing["processed_at"] = candidate["processed_at"]

    had_noncanonical = any((_canonicalize_url(row[0]) or row[0]) != row[0] for row in rows)
    had_duplicates = len(aggregated) < len(rows)
    if not had_noncanonical and not had_duplicates:
        return 0

    conn.execute("DELETE FROM urls")
    for record in aggregated.values():
        conn.execute(
            '''
            INSERT INTO urls (url, source_name, status, discovered_at, processed_at, retry_count, priority)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                record["url"],
                record["source_name"],
                record["status"],
                record["discovered_at"],
                record["processed_at"],
                record["retry_count"],
                record["priority"],
            ),
        )
    return len(rows) - len(aggregated)

def apply_hard_deny_filters(conn: sqlite3.Connection) -> int:
    source_index = {source.get("name"): source for source in SOURCES}
    cursor = conn.cursor()
    cursor.execute("SELECT url, source_name, status, retry_count FROM urls")
    rows = cursor.fetchall()

    updated = 0
    for url, source_name, status, retry_count in rows:
        source = source_index.get(source_name)
        if not source:
            continue
        hard_deny_tokens = source.get("hard_deny_tokens", [])
        if not hard_deny_tokens:
            continue
        url_lower = url.lower()
        if any(isinstance(token, str) and token.lower() in url_lower for token in hard_deny_tokens):
            if status != "FAILED" or int(retry_count or 0) < MAX_RETRIES:
                conn.execute(
                    "UPDATE urls SET status = 'FAILED', retry_count = ?, processed_at = CURRENT_TIMESTAMP WHERE url = ?",
                    (MAX_RETRIES, url),
                )
                updated += 1

    return updated

def apply_seed_fallback_filters(conn: sqlite3.Connection) -> int:
    blocked_seed_urls = {
        seed
        for source in SOURCES
        for seed in [_canonicalize_url(source.get("url", ""))]
        if seed and not _allow_seed_fallback(source)
    }
    if not blocked_seed_urls:
        return 0

    cursor = conn.cursor()
    cursor.execute("SELECT url, status, retry_count FROM urls")
    rows = cursor.fetchall()

    updated = 0
    for url, status, retry_count in rows:
        canonical = _canonicalize_url(url) or url
        if canonical not in blocked_seed_urls:
            continue
        if status != "FAILED" or int(retry_count or 0) < MAX_RETRIES:
            conn.execute(
                "UPDATE urls SET status = 'FAILED', retry_count = ?, processed_at = CURRENT_TIMESTAMP WHERE url = ?",
                (MAX_RETRIES, url),
            )
            updated += 1
    return updated

def ensure_directories():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    REJECTED_DIR.mkdir(parents=True, exist_ok=True)
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

def init_db():
    """Initialize the SQLite KV store with WAL mode and robust tracking schemas."""
    conn = sqlite3.connect(DB_PATH, isolation_level=None)
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('''
        CREATE TABLE IF NOT EXISTS urls (
            url TEXT PRIMARY KEY,
            source_name TEXT,
            status TEXT DEFAULT 'PENDING',  -- PENDING, PROCESSING, COMPLETED, FAILED
            discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            processed_at TIMESTAMP,
            retry_count INTEGER DEFAULT 0,
            priority INTEGER DEFAULT 0
        )
    ''')
    try:
        # Just in case we are modifying an existing populated DB
        conn.execute('ALTER TABLE urls ADD COLUMN retry_count INTEGER DEFAULT 0')
    except sqlite3.OperationalError:
        pass # Column already exists
    try:
        conn.execute('ALTER TABLE urls ADD COLUMN priority INTEGER DEFAULT 0')
    except sqlite3.OperationalError:
        pass
    conn.execute('CREATE INDEX IF NOT EXISTS idx_urls_status_processed_at ON urls (status, processed_at)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_urls_status_priority ON urls (status, priority DESC, discovered_at ASC)')
    merged = normalize_existing_urls(conn)
    if merged > 0:
        print(f"Normalized URL table: merged {merged} duplicate/non-canonical rows.")
    filtered = apply_hard_deny_filters(conn)
    if filtered > 0:
        print(f"Applied hard-deny filters to {filtered} rows.")
    disabled_filtered = apply_disabled_source_filters(conn)
    if disabled_filtered > 0:
        print(f"Applied disabled-source filters to {disabled_filtered} rows.")
    seed_filtered = apply_seed_fallback_filters(conn)
    if seed_filtered > 0:
        print(f"Applied seed-fallback filters to {seed_filtered} rows.")
    if QUARANTINE_SUSPECT_OUTPUTS:
        moved = quarantine_suspect_outputs(conn)
        if moved > 0:
            print(f"Quarantined {moved} suspect markdown files to {REJECTED_DIR}.")
    conn.close()

def add_urls_to_db(urls: Sequence[Any], source_name: str) -> Tuple[int, int]:
    """Add newly mapped URLs to the database. Returns (added_count, priority_updates)."""
    conn = sqlite3.connect(DB_PATH, isolation_level=None)
    conn.execute('PRAGMA journal_mode=WAL')
    cursor = conn.cursor()
    added_count = 0
    updated_priority_count = 0

    for item in urls:
        if isinstance(item, tuple):
            url, priority = item
        else:
            url, priority = item, MIN_RELEVANCE_SCORE

        if not isinstance(url, str) or not url.startswith('http'):
            continue
        if not isinstance(priority, int):
            priority = MIN_RELEVANCE_SCORE

        try:
            cursor.execute('SELECT status, retry_count, priority FROM urls WHERE url = ?', (url,))
            existing = cursor.fetchone()
            if existing is None:
                conn.execute(
                    'INSERT INTO urls (url, source_name, status, priority) VALUES (?, ?, ?, ?)',
                    (url, source_name, 'PENDING', priority)
                )
                added_count += 1
                continue

            status, retry_count, existing_priority = existing
            should_update_priority = isinstance(existing_priority, int) and priority > existing_priority
            should_requeue_failed = status == 'FAILED' and retry_count < MAX_RETRIES

            if should_update_priority or should_requeue_failed:
                next_status = 'PENDING' if should_requeue_failed else status
                next_priority = priority if should_update_priority else existing_priority
                conn.execute(
                    'UPDATE urls SET source_name = ?, status = ?, priority = ? WHERE url = ?',
                    (source_name, next_status, next_priority, url)
                )
                if should_update_priority:
                    updated_priority_count += 1
        except sqlite3.Error as e:
            print(f"⚠️ Failed DB insert/update for {url}: {e}")
    conn.close()
    return added_count, updated_priority_count

def get_pending_chunk(chunk_size=100):
    """
    Fetch a chunk of PENDING URLs to scrape.
    Atomically marks them as PROCESSING to ensure multi-writer concurrent safety.
    Recovers URLs that were stuck in PROCESSING for over 2 hours.
    """
    conn = sqlite3.connect(DB_PATH, isolation_level=None)
    conn.execute('PRAGMA journal_mode=WAL')
    cursor = conn.cursor()
    transaction_started = False
    try:
        conn.execute('BEGIN IMMEDIATE') # Lock for claiming to avoid race conditions
        transaction_started = True
        cursor.execute('''
            SELECT url FROM urls 
            WHERE (status = 'PENDING' OR (status = 'PROCESSING' AND datetime(processed_at) < datetime('now', ?)))
              AND retry_count < ?
            ORDER BY priority DESC, discovered_at ASC
            LIMIT ?
        ''', (f'-{PROCESSING_STALE_HOURS} hour', MAX_RETRIES, chunk_size))
        rows = cursor.fetchall()
        urls = [r[0] for r in rows]
        
        if urls:
            placeholders = ','.join(['?'] * len(urls))
            cursor.execute(f'''
                UPDATE urls 
                SET status = 'PROCESSING', processed_at = CURRENT_TIMESTAMP 
                WHERE url IN ({placeholders})
            ''', urls)
            
        conn.execute('COMMIT')
        return urls
    except Exception:
        if transaction_started:
            conn.execute('ROLLBACK')
        raise
    finally:
        conn.close()

def mark_status(url, status, increment_retry=False):
    """Mark a URL status globally. Bumps retry_count if specified."""
    conn = sqlite3.connect(DB_PATH, isolation_level=None)
    conn.execute('PRAGMA journal_mode=WAL')
    if increment_retry:
        conn.execute(
            'UPDATE urls SET status = ?, processed_at = CURRENT_TIMESTAMP, retry_count = retry_count + 1 WHERE url = ?',
            (status, url)
        )
    else:
        conn.execute(
            'UPDATE urls SET status = ?, processed_at = CURRENT_TIMESTAMP WHERE url = ?',
            (status, url)
        )
    conn.close()

def handle_failure(url, error_context=""):
    """Handle extraction failures gracefully, checking retry upper limits."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('SELECT retry_count FROM urls WHERE url = ?', (url,))
    row = cursor.fetchone()
    conn.close()
    
    current_retries = row[0] if row else 0
    
    next_retry = current_retries + 1
    if next_retry >= MAX_RETRIES:
        mark_status(url, 'FAILED', increment_retry=True)
        print(f" ❌ PERMANENT FAIL [{next_retry}/{MAX_RETRIES}]: {url} ({error_context})")
    else:
        # Requeue for a future attempt
        mark_status(url, 'PENDING', increment_retry=True)
        print(f" ⚠️ REQUEUED [{next_retry}/{MAX_RETRIES}]: {url} ({error_context})")

@retry(stop=stop_after_attempt(4), wait=wait_exponential(multiplier=1, min=3, max=12))
def scrape_with_retry(app, url, only_main_content=True):
    """Scrape a URL with exponential backoff on failure (SDK level resiliency)."""
    return app.scrape(
        url, 
        formats=['markdown'], 
        only_main_content=only_main_content
    )

@retry(stop=stop_after_attempt(2), wait=wait_exponential(multiplier=1, min=2, max=6))
def map_with_retry(app, seed_url: str, search_query: Optional[str] = None):
    kwargs: Dict[str, Any] = {"limit": MAP_LIMIT_PER_CALL}
    if search_query:
        kwargs["search"] = search_query
    return app.map(seed_url, **kwargs)

@retry(stop=stop_after_attempt(2), wait=wait_exponential(multiplier=1, min=2, max=6))
def scrape_links_with_retry(app, url: str):
    return app.scrape(url, formats=["links"], only_main_content=False)

def _print_top_ranked_urls(ranked_urls: Sequence[Tuple[str, int]], sample_size: int = 5):
    if not ranked_urls:
        return
    print("  Top ranked candidate URLs:")
    for url, score in ranked_urls[:sample_size]:
        print(f"   - [{score:02d}] {url}")

def _source_map_budget(source: Dict[str, Any]) -> int:
    configured = source.get("max_urls")
    if isinstance(configured, int) and configured > 0:
        return configured
    return MAX_URLS_PER_SOURCE

def map_single_source(app, source: Dict[str, Any]) -> Tuple[int, int]:
    seed_url = source["url"]
    name = source["name"]
    queries = _source_queries(source)

    discovered: List[str] = [seed_url]
    total_raw = 0
    failed_calls = 0
    failure_messages: List[str] = []

    print(f"Mapping source: {name} | Root URL: {seed_url}")
    for idx, query in enumerate(queries):
        label = "seed scan" if query is None else f"query='{query}'"
        try:
            map_result = map_with_retry(app, seed_url, search_query=query)
            links = _extract_links(map_result)
            total_raw += len(links)
            discovered.extend(links)
            print(f" ↳ {label}: discovered {len(links)} links.")
        except Exception as e:
            failed_calls += 1
            failure_messages.append(str(e))
            print(f" ↳ {label}: failed ({e})")

        if idx < len(queries) - 1:
            print(f"   Sleeping {MAP_DELAY_SECONDS:.1f}s to respect API limits...")
            time.sleep(MAP_DELAY_SECONDS)

    if total_raw == 0:
        try:
            scrape_links_result = scrape_links_with_retry(app, seed_url)
            fallback_links = _extract_links(scrape_links_result)
            fallback_ranked = _rank_candidate_urls(fallback_links, source)
            if len(fallback_ranked) >= MIN_FALLBACK_HIGH_SIGNAL_LINKS:
                discovered.extend([url for url, _score in fallback_ranked])
                total_raw += len(fallback_links)
                print(
                    f" ↳ scrape-link fallback: discovered {len(fallback_links)} links, "
                    f"accepted {len(fallback_ranked)} high-signal links."
                )
            else:
                print(
                    f" ↳ scrape-link fallback discovered {len(fallback_links)} links but only "
                    f"{len(fallback_ranked)} passed relevance; ignoring fallback set."
                )
        except Exception as e:
            print(f" ↳ scrape-link fallback failed ({e})")

    ranked_urls = _rank_candidate_urls(discovered, source)
    budget = _source_map_budget(source)
    selected_urls = ranked_urls[:budget]
    source_min_score = _source_min_score(source)

    if total_raw == 0 and failed_calls == len(queries):
        network_like_failures = (
            "ConnectionError",
            "NameResolutionError",
            "Max retries exceeded",
            "Failed to establish a new connection",
        )
        if failure_messages and all(any(marker in msg for marker in network_like_failures) for msg in failure_messages):
            raise FirecrawlConnectivityError(
                f"Connectivity to Firecrawl failed while mapping '{name}'."
            )

    if not selected_urls:
        canonical_seed = _canonicalize_url(seed_url)
        if canonical_seed and _allow_seed_fallback(source):
            selected_urls = [(canonical_seed, MIN_RELEVANCE_SCORE)]
            print(" ↳ No URLs passed relevance filter; keeping canonical seed as fallback.")
        else:
            print(" ↳ No URLs passed relevance filter; skipping canonical seed fallback for forum-like source.")

    added, updated = add_urls_to_db(selected_urls, name)

    print(
        f" ↳ Kept {len(selected_urls)} high-signal URLs "
        f"(from {total_raw} raw links, threshold={source_min_score})."
    )
    print(f" ↳ DB updates: {added} new URLs, {updated} priority upgrades.")
    _print_top_ranked_urls(selected_urls)
    return added, len(selected_urls)

def map_sources(app):
    """Phase 1: Discover URLs deeper in the index/seed forums."""
    print("--- Phase 1: Mapping Target Forums ---")
    total_added = 0
    total_selected = 0
    configured_sources = SOURCES[:MAX_SOURCES] if MAX_SOURCES > 0 else SOURCES
    active_sources = [source for source in configured_sources if _source_is_enabled(source)]
    disabled_sources = [source for source in configured_sources if not _source_is_enabled(source)]

    for source in disabled_sources:
        reason = source.get("disabled_reason")
        if isinstance(reason, str) and reason.strip():
            print(f"Skipping disabled source: {source['name']} ({reason.strip()})")
        else:
            print(f"Skipping disabled source: {source['name']}")

    if not active_sources:
        print("No enabled sources configured for mapping.")
        return

    for index, source in enumerate(active_sources):
        added, selected = map_single_source(app, source)
        total_added += added
        total_selected += selected
        if index < len(active_sources) - 1:
            print(f"Sleeping {MAP_DELAY_SECONDS:.1f}s before next source...")
            time.sleep(MAP_DELAY_SECONDS)

    print(
        f"Mapping complete: selected {total_selected} high-signal URLs "
        f"across sources ({total_added} new DB rows)."
    )

def process_chunk(app, url):
    """Process a single URL inside a ThreadPoolExecutor."""
    output_path = _output_path_for_url(url)
    
    # Check filesystem cache just in case DB is out of sync or we are restarting
    if output_path.exists() and output_path.stat().st_size > 0:
        cached_content = output_path.read_text(encoding='utf-8', errors='ignore')
        cached_source_url = _extract_source_url_from_markdown(cached_content) or url
        if not _is_suspect_markdown_content(cached_content, cached_source_url):
            mark_status(url, 'COMPLETED')
            return True

        quarantined_path = _move_to_rejected(output_path)
        if quarantined_path is not None:
            print(f" ⚠️ Quarantined suspect cached file for {url}: {quarantined_path.name}")
        
    try:
        scrape_result = scrape_with_retry(app, url, only_main_content=True)
        markdown_content = _extract_markdown(scrape_result).strip()
        min_chars = _min_markdown_chars_for_url(url)

        if len(markdown_content) < min_chars:
            fallback_result = scrape_with_retry(app, url, only_main_content=False)
            fallback_markdown = _extract_markdown(fallback_result).strip()
            if len(fallback_markdown) > len(markdown_content):
                markdown_content = fallback_markdown

        if markdown_content and len(markdown_content) >= min_chars:
            if _is_suspect_markdown_content(markdown_content, url):
                handle_failure(url, error_context="Low-signal/interstitial/auth/error content detected.")
                return False
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(f"<!-- SOURCE_URL: {url} -->\n\n{markdown_content}\n")
            mark_status(url, 'COMPLETED')
            return True
        else:
            handle_failure(
                url,
                error_context=f"Insufficient markdown extracted ({len(markdown_content)} chars, needed {min_chars}).",
            )
            return False
    except Exception as e:
        handle_failure(url, error_context=str(e))
        return False

def scrape_phase(app, chunk_size=100, max_workers=10):
    """Phase 2: Scrape PENDING URLs concurrently using a ThreadPoolExecutor."""
    print(f"\n--- Phase 2: Scraping URLs (Chunk Size: {chunk_size}, Workers: {max_workers}) ---")
    
    while True:
        pending_urls = get_pending_chunk(chunk_size)
        if not pending_urls:
            print("No PENDING URLs left in the database. Scrape Phase finished.")
            break
            
        print(f"\nProcessing chunk of {len(pending_urls)} URLs...")
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_url = {executor.submit(process_chunk, app, url): url for url in pending_urls}
            
            for future in as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    success = future.result()
                    if success:
                        print(f" ✅ Scraped: {url}")
                except Exception as e:
                    handle_failure(url, error_context=f"Thread exception: {e}")
                    print(f" ⚠️ Thread exception for {url}: {e}")
                    
        print(f"Chunk completed. Backing off slightly before checking next batch...")
        time.sleep(CHUNK_DELAY_SECONDS)

def _maybe_load_env_file(env_path: Path):
    if not env_path.exists():
        return
    try:
        lines = env_path.read_text(encoding='utf-8').splitlines()
    except Exception as e:
        print(f"Warning: failed to read {env_path}: {e}")
        return

    for raw in lines:
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export "):].strip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value

def audit_crawl_outputs():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT status, COUNT(*) FROM urls GROUP BY status ORDER BY status")
    status_counts = cursor.fetchall()
    conn.close()

    files = sorted(DATA_DIR.glob("*.md"))
    suspect_files: List[str] = []
    for file_path in files:
        content = file_path.read_text(encoding='utf-8', errors='ignore')
        source_url = _extract_source_url_from_markdown(content)
        if _is_suspect_markdown_content(content, source_url):
            suspect_files.append(file_path.name)

    print("\n--- Crawl Audit ---")
    if status_counts:
        print("DB status counts:")
        for status, count in status_counts:
            print(f" - {status}: {count}")
    else:
        print("DB status counts: (none)")
    print(f"Scraped markdown files: {len(files)}")
    print(f"Suspect low-signal files: {len(suspect_files)}")
    for file_name in suspect_files[:20]:
        print(f" - {file_name}")
    rejected_files = sorted(REJECTED_DIR.glob("*.md")) if REJECTED_DIR.exists() else []
    print(f"Quarantined markdown files: {len(rejected_files)}")

def preflight_firecrawl_connectivity(app):
    probe_source = next((source for source in SOURCES if _source_is_enabled(source)), None)
    if probe_source is None:
        return
    probe_url = probe_source["url"]
    try:
        app.map(probe_url, limit=1)
    except Exception as e:
        raise FirecrawlConnectivityError(
            f"Firecrawl preflight failed for {probe_url}: {e}"
        ) from e

def main():
    ensure_directories()
    init_db()
    _maybe_load_env_file(BASE_DIR / ".env")

    if "--audit-only" in sys.argv:
        audit_crawl_outputs()
        return

    api_key = os.environ.get("FIRECRAWL_API_KEY")
    if not api_key:
        print("Error: FIRECRAWL_API_KEY environment variable not set. Please export it.")
        return
    
    request_timeout = float(os.environ.get("CRAWL_REQUEST_TIMEOUT", "20"))
    sdk_retries = int(os.environ.get("CRAWL_SDK_MAX_RETRIES", "1"))
    app = FirecrawlApp(api_key=api_key, timeout=request_timeout, max_retries=sdk_retries)

    try:
        if "--skip-map" not in sys.argv:
            preflight_firecrawl_connectivity(app)
            map_sources(app)
        else:
            print("Skipping Mapping Phase (--skip-map provided). Using existing database.")
    except FirecrawlConnectivityError as e:
        print(f"Error: {e}")
        print("Aborting crawl early to avoid burning retries in a network-blocked environment.")
        return
        
    chunk_size = int(os.environ.get("CRAWL_CHUNK_SIZE", "100"))
    max_workers = int(os.environ.get("CRAWL_MAX_WORKERS", "10"))
    print(
        f"Runtime config: chunk_size={chunk_size}, workers={max_workers}, "
        f"min_relevance={MIN_RELEVANCE_SCORE}, min_chars={MIN_MARKDOWN_CHARS}"
    )
    scrape_phase(app, chunk_size=chunk_size, max_workers=max_workers)

if __name__ == "__main__":
    main()
