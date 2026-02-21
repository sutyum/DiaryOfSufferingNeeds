# Data Extraction Scripts

This directory contains the scripts to extract the deep ontology of suffering from raw patient texts.

## Architecture
We use **Recursive Language Models (RLM)** to process unbounded context. Instead of stuffing raw forum data into an LLM prompt (which causes context degradation), we store the data as a Python variable. An Agent is then given the extraction prompt and can programmatically explore the `context` variable.

*Reference: [Recursive Language Models (arXiv 2512.24601)](https://arxiv.org/abs/2512.24601)*

## Setup

1. Install the `recursive-llm` package (currently requires installing from GitHub):
```bash
uv pip install git+https://github.com/ysz/recursive-llm.git
```

2. Export your LLM Provider API Key. For example:
```bash
export OPENAI_API_KEY="sk-..."
# OR
export ANTHROPIC_API_KEY="sk-ant-..."
```
*(Note: If using Anthropic, you'll need to update `model="gpt-4o"` to `model="claude-3-5-sonnet-20240620"` in `extract.py`)*

## Usage

Run the extraction on the sample stories:
```bash
python scripts/extract.py --input data/raw/sample_stories.json --output data/processed/extracted_stories.json
```
