from __future__ import annotations

import importlib.util
import sqlite3
from pathlib import Path
from types import SimpleNamespace
from typing import Any


def load_module(module_name: str, file_path: Path) -> Any:
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load module from {file_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


ROOT = Path(__file__).resolve().parents[1]
CRAWL_MODULE = load_module("crawl_module", ROOT / "scripts" / "1_crawl.py")


def test_extract_links_supports_dict_and_object() -> None:
    assert CRAWL_MODULE._extract_links({"links": ["https://a.example", "https://b.example"]}) == [
        "https://a.example",
        "https://b.example",
    ]

    obj = SimpleNamespace(links=["https://c.example"])
    assert CRAWL_MODULE._extract_links(obj) == ["https://c.example"]


def test_extract_markdown_supports_dict_and_object() -> None:
    assert CRAWL_MODULE._extract_markdown({"markdown": "hello"}) == "hello"
    assert CRAWL_MODULE._extract_markdown(SimpleNamespace(markdown="world")) == "world"
    assert CRAWL_MODULE._extract_markdown({"markdown": None}) == ""

def test_canonicalize_url_removes_tracking_query() -> None:
    canonical = CRAWL_MODULE._canonicalize_url(
        "https://www.s4me.info/threads/a-story.123/?utm_source=x&utm_medium=y&page=2#fragment"
    )
    assert canonical == "https://www.s4me.info/threads/a-story.123"

def test_canonicalize_url_collapses_thread_pagination_and_post_links() -> None:
    canonical_page = CRAWL_MODULE._canonicalize_url(
        "https://forums.phoenixrising.me/threads/topic-name.12345/page-4"
    )
    canonical_post = CRAWL_MODULE._canonicalize_url(
        "https://forums.phoenixrising.me/threads/topic-name.12345/post-67890"
    )
    assert canonical_page == "https://forums.phoenixrising.me/threads/topic-name.12345"
    assert canonical_post == "https://forums.phoenixrising.me/threads/topic-name.12345"

def test_rank_candidate_urls_prefers_story_pages_and_filters_low_signal() -> None:
    source = CRAWL_MODULE.SOURCES[0]
    ranked = CRAWL_MODULE._rank_candidate_urls(
        [
            "https://www.s4me.info/login/",
            "https://www.s4me.info/members/user.1/",
            "https://www.s4me.info/threads/my-me-cfs-story.555/",
        ],
        source,
    )
    urls = [url for url, _score in ranked]
    assert "https://www.s4me.info/threads/my-me-cfs-story.555" in urls
    assert all("login" not in url for url in urls)
    assert all("/members/" not in url for url in urls)

def test_hard_deny_tokens_force_low_score() -> None:
    source = CRAWL_MODULE.SOURCES[0]
    denied_url = "https://www.s4me.info/threads/news-in-brief-february-2026.48741"
    assert CRAWL_MODULE._score_url(denied_url, source) == -100

def test_rank_candidate_urls_respects_required_tokens() -> None:
    source = next(item for item in CRAWL_MODULE.SOURCES if item["name"] == "Healthtalk.org - Long COVID")
    ranked = CRAWL_MODULE._rank_candidate_urls(
        [
            "https://healthtalk.org/experiences/alopecia/example-entry",
            "https://healthtalk.org/long-covid/living-with-fatigue",
        ],
        source,
    )
    urls = [url for url, _score in ranked]
    assert "https://healthtalk.org/long-covid/living-with-fatigue" in urls
    assert "https://healthtalk.org/experiences/alopecia/example-entry" not in urls

def test_detects_error_page_text() -> None:
    content = """
# Oops! We ran into some problems.
The requested forum could not be found.
"""
    assert CRAWL_MODULE._looks_like_error_page(content) is True

def test_detects_auth_wall_text() -> None:
    content = """
# Log in
You must be logged-in to do that.
Forgot your password?
"""
    assert CRAWL_MODULE._looks_like_auth_wall(content) is True

def test_detects_directory_page_shape() -> None:
    content = "\n".join(
        ["- [New posts](https://example.org/new)"] * 70
        + ["Sort by", "topics in this forum", "Threads 100", "Messages 500"]
    )
    assert CRAWL_MODULE._looks_like_directory_page(
        content, "https://forums.phoenixrising.me/forums/the-patients-story.4/"
    ) is True


def test_get_pending_chunk_marks_urls_processing(tmp_path: Path) -> None:
    db_path = tmp_path / "crawl_state.db"
    data_dir = tmp_path / "scraped"
    rejected_dir = tmp_path / "scraped_rejected"
    CRAWL_MODULE.DB_PATH = db_path
    CRAWL_MODULE.DATA_DIR = data_dir
    CRAWL_MODULE.REJECTED_DIR = rejected_dir

    CRAWL_MODULE.ensure_directories()
    CRAWL_MODULE.init_db()
    CRAWL_MODULE.add_urls_to_db(
        ["https://example.org/a", "https://example.org/b"],
        "test-source",
    )

    claimed = CRAWL_MODULE.get_pending_chunk(chunk_size=1)
    assert len(claimed) == 1

    conn = sqlite3.connect(db_path)
    row = conn.execute("SELECT status FROM urls WHERE url = ?", (claimed[0],)).fetchone()
    conn.close()
    assert row is not None
    assert row[0] == "PROCESSING"


def test_handle_failure_marks_terminal_status(tmp_path: Path) -> None:
    db_path = tmp_path / "crawl_state.db"
    data_dir = tmp_path / "scraped"
    rejected_dir = tmp_path / "scraped_rejected"
    CRAWL_MODULE.DB_PATH = db_path
    CRAWL_MODULE.DATA_DIR = data_dir
    CRAWL_MODULE.REJECTED_DIR = rejected_dir
    CRAWL_MODULE.MAX_RETRIES = 3

    CRAWL_MODULE.ensure_directories()
    CRAWL_MODULE.init_db()

    url = "https://example.org/fail"
    conn = sqlite3.connect(db_path)
    conn.execute(
        "INSERT INTO urls (url, source_name, status, retry_count) VALUES (?, ?, ?, ?)",
        (url, "test-source", "PENDING", 2),
    )
    conn.commit()
    conn.close()

    CRAWL_MODULE.handle_failure(url, error_context="unit-test")

    conn = sqlite3.connect(db_path)
    row = conn.execute("SELECT status, retry_count FROM urls WHERE url = ?", (url,)).fetchone()
    conn.close()
    assert row is not None
    assert row[0] == "FAILED"
    assert row[1] == 3

def test_add_urls_to_db_updates_priority_for_existing_urls(tmp_path: Path) -> None:
    db_path = tmp_path / "crawl_state.db"
    data_dir = tmp_path / "scraped"
    rejected_dir = tmp_path / "scraped_rejected"
    CRAWL_MODULE.DB_PATH = db_path
    CRAWL_MODULE.DATA_DIR = data_dir
    CRAWL_MODULE.REJECTED_DIR = rejected_dir

    CRAWL_MODULE.ensure_directories()
    CRAWL_MODULE.init_db()

    url = "https://example.org/story"
    CRAWL_MODULE.add_urls_to_db([(url, 8)], "source-a")
    _added, updated = CRAWL_MODULE.add_urls_to_db([(url, 20)], "source-b")

    conn = sqlite3.connect(db_path)
    row = conn.execute("SELECT source_name, priority FROM urls WHERE url = ?", (url,)).fetchone()
    conn.close()

    assert updated == 1
    assert row is not None
    assert row[0] == "source-b"
    assert row[1] == 20

def test_normalize_existing_urls_merges_canonical_duplicates(tmp_path: Path) -> None:
    db_path = tmp_path / "crawl_state.db"
    data_dir = tmp_path / "scraped"
    rejected_dir = tmp_path / "scraped_rejected"
    CRAWL_MODULE.DB_PATH = db_path
    CRAWL_MODULE.DATA_DIR = data_dir
    CRAWL_MODULE.REJECTED_DIR = rejected_dir

    CRAWL_MODULE.ensure_directories()
    CRAWL_MODULE.init_db()

    conn = sqlite3.connect(db_path)
    conn.execute(
        "INSERT INTO urls (url, source_name, status, retry_count, priority) VALUES (?, ?, ?, ?, ?)",
        ("https://example.org/threads/story.1/", "source-a", "COMPLETED", 0, 5),
    )
    conn.execute(
        "INSERT INTO urls (url, source_name, status, retry_count, priority) VALUES (?, ?, ?, ?, ?)",
        ("https://example.org/threads/story.1", "source-a", "FAILED", 2, 9),
    )
    merged = CRAWL_MODULE.normalize_existing_urls(conn)
    conn.commit()

    rows = conn.execute(
        "SELECT url, status, retry_count, priority FROM urls WHERE url = ?",
        ("https://example.org/threads/story.1",),
    ).fetchall()
    conn.close()

    assert merged == 1
    assert len(rows) == 1
    assert rows[0][1] == "FAILED"
    assert rows[0][2] == 2
    assert rows[0][3] == 9

def test_apply_hard_deny_filters_marks_existing_rows_failed(tmp_path: Path) -> None:
    db_path = tmp_path / "crawl_state.db"
    data_dir = tmp_path / "scraped"
    rejected_dir = tmp_path / "scraped_rejected"
    CRAWL_MODULE.DB_PATH = db_path
    CRAWL_MODULE.DATA_DIR = data_dir
    CRAWL_MODULE.REJECTED_DIR = rejected_dir

    CRAWL_MODULE.ensure_directories()
    CRAWL_MODULE.init_db()

    denied_url = "https://www.s4me.info/threads/news-in-brief-february-2026.48741"
    conn = sqlite3.connect(db_path)
    conn.execute(
        "INSERT INTO urls (url, source_name, status, retry_count, priority) VALUES (?, ?, ?, ?, ?)",
        (denied_url, "Science for ME - Patient Experiences", "COMPLETED", 0, 20),
    )
    changed = CRAWL_MODULE.apply_hard_deny_filters(conn)
    conn.commit()
    row = conn.execute("SELECT status, retry_count FROM urls WHERE url = ?", (denied_url,)).fetchone()
    conn.close()

    assert changed == 1
    assert row is not None
    assert row[0] == "FAILED"
    assert row[1] == CRAWL_MODULE.MAX_RETRIES

def test_allow_seed_fallback_skips_for_forum_sources() -> None:
    forum_source = {"url": "https://forums.example.org/forums/stories.1/"}
    article_source = {"url": "https://example.org/stories"}
    assert CRAWL_MODULE._allow_seed_fallback(forum_source) is False
    assert CRAWL_MODULE._allow_seed_fallback(article_source) is True

def test_apply_seed_fallback_filters_marks_forum_seed_rows_failed(tmp_path: Path) -> None:
    db_path = tmp_path / "crawl_state.db"
    data_dir = tmp_path / "scraped"
    rejected_dir = tmp_path / "scraped_rejected"
    CRAWL_MODULE.DB_PATH = db_path
    CRAWL_MODULE.DATA_DIR = data_dir
    CRAWL_MODULE.REJECTED_DIR = rejected_dir

    CRAWL_MODULE.ensure_directories()
    CRAWL_MODULE.init_db()

    source = next(item for item in CRAWL_MODULE.SOURCES if item["name"] == "Phoenix Rising - Symptoms & Treatments")
    seed_url = CRAWL_MODULE._canonicalize_url(source["url"])
    conn = sqlite3.connect(db_path)
    conn.execute(
        "INSERT INTO urls (url, source_name, status, retry_count, priority) VALUES (?, ?, ?, ?, ?)",
        (seed_url, source["name"], "COMPLETED", 0, 20),
    )
    changed = CRAWL_MODULE.apply_seed_fallback_filters(conn)
    conn.commit()
    row = conn.execute("SELECT status, retry_count FROM urls WHERE url = ?", (seed_url,)).fetchone()
    conn.close()

    assert changed == 1
    assert row is not None
    assert row[0] == "FAILED"
    assert row[1] == CRAWL_MODULE.MAX_RETRIES
