import os
import sys
import hashlib
import sqlite3
import time
from firecrawl import FirecrawlApp
from pathlib import Path
from tenacity import retry, stop_after_attempt, wait_exponential
from concurrent.futures import ThreadPoolExecutor, as_completed

SOURCES = [
    {"name": "Science for ME - Patient Experiences", "url": "https://www.s4me.info/forums/patient-experiences-and-stories.18/"},
    {"name": "Science for ME - ME/CFS Coping & Management", "url": "https://www.s4me.info/forums/me-cfs-coping-management.8/"},
    {"name": "Phoenix Rising - The Patient's Story", "url": "https://forums.phoenixrising.me/forums/the-patients-story.4/"},
    {"name": "Phoenix Rising - Symptoms & Treatments", "url": "https://forums.phoenixrising.me/forums/me-cfs-symptoms-and-treatments.2/"},
    {"name": "ME Association - Real Life Stories", "url": "https://meassociation.org.uk/real-life-stories/"},
    {"name": "Healthtalk.org - Chronic Pain", "url": "https://healthtalk.org/chronic-pain/overview"},
    {"name": "Healthtalk.org - Long COVID", "url": "https://healthtalk.org/long-covid/overview"},
    {"name": "Dysautonomia International - Patient Stories", "url": "http://www.dysautonomiainternational.org/page.php?ID=14"},
    {"name": "Surviving Antidepressants - Introductions and Updates", "url": "https://www.survivingantidepressants.org/forum/3-introductions-and-updates/"},
    {"name": "Ehlers-Danlos Society - Our Stories", "url": "https://www.ehlers-danlos.com/our-stories/"},
]

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "public_data" / "scraped"
DB_PATH = BASE_DIR / "public_data" / "crawl_state.db"
MAX_RETRIES = 3

def ensure_directories():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
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
            retry_count INTEGER DEFAULT 0
        )
    ''')
    try:
        # Just in case we are modifying an existing populated DB
        conn.execute('ALTER TABLE urls ADD COLUMN retry_count INTEGER DEFAULT 0')
    except sqlite3.OperationalError:
        pass # Column already exists
    conn.close()

def add_urls_to_db(urls, source_name):
    """Add newly mapped URLs to the database."""
    conn = sqlite3.connect(DB_PATH, isolation_level=None)
    conn.execute('PRAGMA journal_mode=WAL')
    added_count = 0
    for url in urls:
        if not url.startswith('http'): continue
        try:
            conn.execute(
                'INSERT INTO urls (url, source_name, status) VALUES (?, ?, ?)',
                (url, source_name, 'PENDING')
            )
            added_count += 1
        except sqlite3.IntegrityError:
            pass # Already exists in DB
    conn.close()
    return added_count

def get_pending_chunk(chunk_size=100):
    """
    Fetch a chunk of PENDING URLs to scrape.
    Atomically marks them as PROCESSING to ensure multi-writer concurrent safety.
    Recovers URLs that were stuck in PROCESSING for over 2 hours.
    """
    conn = sqlite3.connect(DB_PATH, isolation_level=None)
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('BEGIN EXCLUSIVE') # Lock for claiming to avoid race conditions
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT url FROM urls 
        WHERE status = 'PENDING' OR (status = 'PROCESSING' AND datetime(processed_at) < datetime('now', '-2 hour'))
        LIMIT ?
    ''', (chunk_size,))
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
    conn.close()
    return urls

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
    
    if current_retries >= MAX_RETRIES:
        mark_status(url, 'FAILED', increment_retry=True)
        print(f" ❌ PERMANENT FAIL [{current_retries}/{MAX_RETRIES}]: {url} ({error_context})")
    else:
        # Requeue for a future attempt
        mark_status(url, 'PENDING', increment_retry=True)
        print(f" ⚠️ REQUEUED [{current_retries+1}/{MAX_RETRIES}]: {url} ({error_context})")

@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=4, max=10))
def scrape_with_retry(app, url):
    """Scrape a URL with exponential backoff on failure (SDK level resiliency)."""
    return app.scrape(
        url, 
        formats=['markdown'], 
        only_main_content=True
    )

def map_sources(app):
    """Phase 1: Discover URLs deeper in the index/seed forums."""
    print("--- Phase 1: Mapping Target Forums ---")
    for source in SOURCES:
        seed_url = source["url"]
        name = source["name"]
        print(f"Mapping source: {name} | Root URL: {seed_url}")
        try:
            # Map returns a MapData object
            map_result = app.map(seed_url)
            urls = getattr(map_result, 'links', []) if not isinstance(map_result, dict) else map_result.get('links', [])
            
            if urls:
                added = add_urls_to_db(urls, name)
                print(f"↳ Mapped {len(urls)} URLs from {name} ({added} new unique tasks added).")
            else:
                print(f"↳ Warning: No URLs discovered for {seed_url}. Adding root URL as fallback.")
                add_urls_to_db([seed_url], name)
        except Exception as e:
            print(f"↳ Failed to map {seed_url}: {e}")
        
        # small delay to behave nicely
        time.sleep(1)

def process_chunk(app, url):
    """Process a single URL inside a ThreadPoolExecutor."""
    url_hash = hashlib.md5(url.encode('utf-8')).hexdigest()
    output_path = DATA_DIR / f"{url_hash}.md"
    
    # Check filesystem cache just in case DB is out of sync or we are restarting
    if output_path.exists() and output_path.stat().st_size > 0:
        mark_status(url, 'COMPLETED')
        return True
        
    try:
        scrape_result = scrape_with_retry(app, url)
        # Check struct returned by SDK
        markdown_content = scrape_result.get('markdown', '') if isinstance(scrape_result, dict) else getattr(scrape_result, 'markdown', '')
        
        if markdown_content:
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(markdown_content)
            mark_status(url, 'COMPLETED')
            return True
        else:
            handle_failure(url, error_context="No markdown extracted.")
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
                    print(f" ⚠️ Thread exception for {url}: {e}")
                    
        print(f"Chunk completed. Backing off slightly before checking next batch...")
        time.sleep(2)

def main():
    api_key = os.environ.get("FIRECRAWL_API_KEY")
    if not api_key:
        print("Error: FIRECRAWL_API_KEY environment variable not set. Please export it.")
        return

    ensure_directories()
    init_db()
    
    app = FirecrawlApp(api_key=api_key)

    if "--skip-map" not in sys.argv:
        map_sources(app)
    else:
        print("Skipping Mapping Phase (--skip-map provided). Using existing database.")
        
    # Largish batch size configuration (100 links per chunk, 10 concurrent threads)
    scrape_phase(app, chunk_size=100, max_workers=10)

if __name__ == "__main__":
    main()
