# Agent Instructions

## Python Environment & Dependencies
This project uses `uv` for all Python dependency management and script execution. 

**Critical Rules for AI Agents:**
1. **Never** use `pip install` or `python -m venv`.
2. **Adding/Removing Packages**: Always use `uv add <package>` or `uv remove <package>`. This will update the `pyproject.toml` correctly.
3. **Running Scripts**: Always execute Python scripts using `uv run`. 
   - Example to run a script: `uv run scripts/parse.py`
   - Example to run standard python commands: `uv run python -c "..."`
4. **Environment Syncing**: If dependencies in `pyproject.toml` are manually modified, run `uv sync` to synchronize the `.venv`.

Adhere strictly to `uv` for all Python tasks to ensure consistent and reproducible environments without conflicting cache issues.
