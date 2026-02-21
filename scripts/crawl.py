import json
import os
import argparse
import hashlib
from firecrawl import FirecrawlApp
import time
from pathlib import Path
from tenacity import retry, stop_after_attempt, wait_exponential

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

# Use absolute paths relative to the script to bypass MacOS SIP/Sandbox relative path quirks
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "public_data" / "scraped"

def ensure_directories():
    DATA_DIR.mkdir(parents=True, exist_ok=True)

@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=4, max=10))
def scrape_with_retry(app, url):
    """Scrape a URL with exponential backoff on failure."""
    return app.scrape(
        url, 
        formats=['markdown'], 
        only_main_content=True
    )

def crawl_sources():
    api_key = os.environ.get("FIRECRAWL_API_KEY")
    if not api_key:
        print("Error: FIRECRAWL_API_KEY environment variable not set. Please export it.")
        return

    ensure_directories()
    app = FirecrawlApp(api_key=api_key)

    for source in SOURCES:
        url = source.get("url")
        name = source.get("name", "Unknown Source")
        print(f"Crawling source: {name} | URL: {url}")
        
        url_hash = hashlib.md5(url.encode('utf-8')).hexdigest()
        output_path = DATA_DIR / f"{url_hash}.md"

        # Caching: Skip if we already successfully pulled this markdown file
        if output_path.exists() and output_path.stat().st_size > 0:
            print(f"Skipping {url}, already crawled at {output_path}")
            continue

        try:
            print(f"Executing robust scrape for {url}...")
            scrape_result = scrape_with_retry(app, url)
            
            markdown_content = scrape_result.get('markdown', '') if isinstance(scrape_result, dict) else getattr(scrape_result, 'markdown', '')
            
            if markdown_content:
                with open(output_path, 'w', encoding='utf-8') as f:
                    f.write(markdown_content)
                print(f"Successfully saved {len(markdown_content)} characters to {output_path}")
            else:
                print(f"Warning: No markdown extracted from {url}")
                
            time.sleep(2)
            
        except Exception as e:
            print(f"Failed to crawl {url} after retries: {e}")

if __name__ == "__main__":
    crawl_sources()
