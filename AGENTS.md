# Agent Instructions

## Python Environment & Dependencies
This project uses `uv` for all Python dependency management and script execution. 

**Critical Rules for AI Agents:**
1. **Never** use `pip install` or `python -m venv`.
2. **Adding/Removing Packages**: Always use `uv add <package>` or `uv remove <package>`. This will update the `pyproject.toml` correctly.
3. **Running Scripts**: Always execute Python scripts using `uv run`. 
   - Example pipelines: `uv run python scripts/1_crawl.py` or `uv run python scripts/2_parse.py`
   - Example standard use: `uv run python -c "..."`
4. **Environment Syncing**: If dependencies in `pyproject.toml` are manually modified, run `uv sync` to synchronize the `.venv`.

Adhere strictly to `uv` for all Python tasks to ensure consistent and reproducible environments without conflicting cache issues.

## Node.js / Next.js Environment
The frontend explorer is located in the `web/` directory.
1. Always run `npm install`, `npm run build`, and `npm run dev` from *within* the `web/` directory.
2. The UI relies heavily on React Server Components, so avoid injecting client-side event handlers into `page.tsx` unless explicitly marked with `"use client"`.
