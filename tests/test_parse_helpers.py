from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import Any


def load_module(module_name: str, file_path: Path) -> Any:
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load module from {file_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


ROOT = Path(__file__).resolve().parents[1]
PARSE_MODULE = load_module("parse_module", ROOT / "scripts" / "2_parse.py")


def make_case() -> dict[str, str]:
    return {
        "condition": "ME/CFS",
        "onset": "Since 2018",
        "threat_to_personhood": "Loss of spontaneity",
        "description": "A patient struggling with energy collapse.",
        "narrative_fragment": "I plan every hour of my day around crashes.",
        "compensatory_rituals": "Strict pacing and rest windows.",
    }


def test_split_markdown_chunks_respect_limit() -> None:
    markdown = ("line\n" * 3000).strip()
    chunks = PARSE_MODULE._split_markdown(markdown, max_chunk_chars=1200)
    assert len(chunks) > 1
    assert all(len(chunk) <= 1200 for chunk in chunks)


def test_extract_json_payload_accepts_fenced_json() -> None:
    raw = """```json
{"cases":[{"condition":"ME/CFS","onset":"Since 2018","threat_to_personhood":"Loss of spontaneity","description":"d","narrative_fragment":"n","compensatory_rituals":"r"}]}
```"""
    payload = PARSE_MODULE._extract_json_payload(raw)
    assert isinstance(payload, dict)
    assert "cases" in payload


def test_parse_model_response_uses_parsed_payload() -> None:
    class DummyResponse:
        parsed = {"cases": [make_case()]}
        text = ""

    parsed = PARSE_MODULE._parse_model_response(DummyResponse())
    assert len(parsed.cases) == 1
    assert parsed.cases[0].condition == "ME/CFS"


def test_parse_model_response_falls_back_to_text_json() -> None:
    class DummyResponse:
        parsed = None
        text = '{"cases":[{"condition":"ME/CFS","onset":"Since 2018","threat_to_personhood":"Loss of spontaneity","description":"d","narrative_fragment":"n","compensatory_rituals":"r"}]}'

    parsed = PARSE_MODULE._parse_model_response(DummyResponse())
    assert len(parsed.cases) == 1
    assert parsed.cases[0].onset == "Since 2018"


def test_dedupe_cases_removes_duplicates() -> None:
    case_a = PARSE_MODULE.WitnessCase(**make_case())
    case_b = PARSE_MODULE.WitnessCase(**make_case())
    deduped = PARSE_MODULE._dedupe_cases([case_a, case_b])
    assert len(deduped) == 1
