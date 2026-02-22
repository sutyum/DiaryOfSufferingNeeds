# A Diary of Suffering Needs (Sufferpedia Explorer)

This project contains a large-scale data ingestion pipeline and a frontend web application designed to securely archive, extract, and cleanly present patient narratives concerning chronic illnesses (ME/CFS, Long COVID, Chronic Pain).

## Architecture Overview

The system is split into two primary domains:

1.  **Data Ingestion Pipeline (Python)**: Uses Firecrawl and highly concurrent threadpools to scrape patient forums, then uses Google's Gemini Flash 3 to parse them strictly into an ontological JSON schema.
2.  **Sufferpedia Explorer (Next.js)**: A frontend React application that renders the massive JSON dataset into a premium, solemn user interface.

### The Pipeline (`/scripts`)

The data pipeline runs in two sequential stages:

#### Stage 1: The Crawler (`1_crawl.py`)
Discovers deep forum thread URLs and systematically downloads them into Markdown using Firecrawl. The crawler uses a concurrent `ThreadPoolExecutor` backed by SQLite (WAL mode) to prevent duplication and handle retries.
```bash
export FIRECRAWL_API_KEY="your_api_key_here"
uv run python scripts/1_crawl.py
```

#### Stage 2: The Parser (`2_parse.py`)
Reads raw `.md` files and uses Gemini to extract structured metadata into `public_data/processed/` JSON.
```bash
export GEMINI_API_KEY="your_api_key_here"
uv run python scripts/2_parse.py
```

### The Frontend (`/web`)

The Next.js Explorer is built primarily with React Server Components. It parses the final JSON payloads directly from the `public_data/processed/` directory at build time, eliminating the need for a secondary database and allowing the application to be deployed immediately on a standalone Virtual Machine.

```bash
cd web
npm install
npm run dev
```

Node.js `>=20.9.0` is required for Next.js 16 builds.

## Quality Checks
Python checks from repo root:
```bash
uv run python -m compileall scripts tests
uv run python -m pytest -q
```

Frontend checks from `web/`:
```bash
npm run lint
npm run build
```

## Technologies Used
- **Crawling/Ingestion**: Firecrawl SDK, SQLite3, `concurrent.futures`, Tenacity
- **LLM Parsing**: `google-genai`, Pydantic (Strict Schema definitions)
- **Frontend**: Next.js (App Router), React, Lucide Icons
- **Environment Management**: `uv` (Python), `npm` (Node)
