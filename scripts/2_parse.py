import os
import json
import re
from pathlib import Path
from google import genai
from google.genai import types
from pydantic import BaseModel, Field, ValidationError
from tenacity import retry, stop_after_attempt, wait_exponential
from typing import Any, Dict, List

# Strict Pydantic ontology for structured JSON output based on The Witness Archive schema
class WitnessCase(BaseModel):
    condition: str = Field(description="The illness or condition accurately described (e.g., 'ME/CFS').")
    onset: str = Field(description="The duration or timeline of the suffering (e.g., 'Year 3', 'Since 2018').")
    threat_to_personhood: str = Field(description="Classify the core loss into a psychological threat (e.g., 'The Loss of Unplanned Time', 'Ambiguous Grief', 'The Erosion of Epistemic Trust').")
    description: str = Field(description="A concise summary of the specific context of this loss.")
    narrative_fragment: str = Field(description="The raw, visceral quoted text describing the friction. Paraphrase slightly to remove exact names/locations but maintain the brutal honesty and texture.")
    compensatory_rituals: str = Field(description="The specific hacks, routines, or workarounds the patient uses, highlighting where current tools fail.")

class WitnessIndex(BaseModel):
    cases: list[WitnessCase]

# Use absolute paths relative to the script to bypass MacOS SIP/Sandbox relative path quirks
BASE_DIR = Path(__file__).parent.parent
INPUT_DIR = BASE_DIR / "public_data" / "scraped"
OUTPUT_DIR = BASE_DIR / "public_data" / "processed"
MODEL_NAME = os.environ.get("GEMINI_MODEL", "gemini-3.0-flash")
MAX_CHUNK_CHARS = 60_000

def _build_prompt(filename: str, markdown_chunk: str, chunk_index: int, chunk_count: int) -> str:
    chunk_context = (
        "This is the entire source."
        if chunk_count == 1
        else f"This is chunk {chunk_index} of {chunk_count} from the source."
    )
    return f"""
You are an expert medical anthropologist and product researcher indexing 'The Witness Archive'.
Your task is to comprehensively analyze the following raw markdown scraped from a patient forum or blog.
You MUST extract EVERY SINGLE deeply visceral 'Case Story of Suffering' you find into the exact JSON schema.

CONTEXT:
- Source Hash: {filename}
- {chunk_context}

CRITICAL INSTRUCTIONS / ETHICS:
- Be EXHAUSTIVE for this chunk.
- Do NOT sanitize the raw human experience. We want the "truth inspiration" to remain intact.
- However, you MUST paraphrase slightly to ensure full anonymization (remove names, specific hospitals, unique identifiers).
- Map the core suffering to a `threat_to_personhood` (e.g., "The Spontaneity Death", "Medical Gaslighting").
- Emphasize hyper-specific frustrations in the `narrative_fragment`.
- Detail the exact `compensatory_rituals` they rely on.
- RETURN ONLY VALID JSON. Do not include markdown formatting or backticks around the json.

RAW SCRAPED MARKDOWN:
{markdown_chunk}
    """

def _split_markdown(markdown: str, max_chunk_chars: int = MAX_CHUNK_CHARS) -> List[str]:
    if max_chunk_chars <= 0:
        raise ValueError("max_chunk_chars must be greater than 0")

    if len(markdown) <= max_chunk_chars:
        return [markdown]

    chunks: List[str] = []
    current_lines: List[str] = []
    current_size = 0

    for line in markdown.splitlines(keepends=True):
        remaining_line = line
        while remaining_line:
            if current_size >= max_chunk_chars and current_lines:
                chunks.append("".join(current_lines))
                current_lines = []
                current_size = 0

            remaining_capacity = max_chunk_chars - current_size
            if remaining_capacity <= 0:
                continue

            segment = remaining_line[:remaining_capacity]
            current_lines.append(segment)
            current_size += len(segment)
            remaining_line = remaining_line[remaining_capacity:]

    if current_lines:
        chunks.append("".join(current_lines))

    return chunks

def _extract_json_payload(result_text: str) -> Dict[str, Any]:
    fenced_match = re.search(r"```(?:json)?\s*(.*?)\s*```", result_text, flags=re.DOTALL | re.IGNORECASE)
    candidate = fenced_match.group(1) if fenced_match else result_text
    data = json.loads(candidate)
    if not isinstance(data, dict):
        raise ValueError("Model response did not decode to a JSON object.")
    return data

def _validate_index_payload(payload: Any) -> WitnessIndex:
    if isinstance(payload, WitnessIndex):
        return payload
    return WitnessIndex.model_validate(payload)

def _parse_model_response(response: Any) -> WitnessIndex:
    parsed = getattr(response, "parsed", None)
    if parsed is not None:
        return _validate_index_payload(parsed)

    try:
        result_text = response.text
    except Exception:
        result_text = ""
    if not isinstance(result_text, str) or not result_text.strip():
        raise ValueError("Model response did not include text or parsed payload.")

    payload = _extract_json_payload(result_text)
    return _validate_index_payload(payload)

def _dedupe_cases(cases: List[WitnessCase]) -> List[WitnessCase]:
    deduped: List[WitnessCase] = []
    seen = set()
    for case in cases:
        key = (
            case.condition.strip(),
            case.onset.strip(),
            case.threat_to_personhood.strip(),
            case.description.strip(),
            case.narrative_fragment.strip(),
            case.compensatory_rituals.strip(),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(case)
    return deduped

def ensure_directories():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=4, max=10))
def parse_with_retry(client, prompt):
    """Call the Gemini API with exponential backoff on failure (rate limits etc)."""
    return client.models.generate_content(
        model=MODEL_NAME,
        contents=prompt,
        config=types.GenerateContentConfig(
            response_mime_type="application/json",
            response_schema=WitnessIndex,
            temperature=0.1, # Keep strict for parsing
        )
    )

def parse_markdown_files():
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        print("Error: GEMINI_API_KEY environment variable not set. Please export it.")
        return

    client = genai.Client(api_key=api_key)
    ensure_directories()
    
    md_files = sorted(INPUT_DIR.glob("*.md"))
    
    if not md_files:
        print(f"No raw markdown files found in {INPUT_DIR}. Please run scripts/1_crawl.py first.")
        return

    for file_path in md_files:
        filename = file_path.name
        output_path = OUTPUT_DIR / filename.replace('.md', '.json')
        
        # Caching: Skip if we already successfully parsed this JSON file
        if output_path.exists() and output_path.stat().st_size > 0:
            print(f"Skipping {filename}, already processed.")
            continue
            
        print(f"Parsing raw markdown from {filename}...")
        
        with open(file_path, 'r', encoding='utf-8') as f:
            raw_markdown = f.read()

        chunks = _split_markdown(raw_markdown)
        collected_cases: List[WitnessCase] = []
        try:
            print(f"Executing robust parsing for {filename} across {len(chunks)} chunk(s)...")
            for index, chunk in enumerate(chunks, start=1):
                prompt = _build_prompt(filename, chunk, index, len(chunks))
                response = parse_with_retry(client, prompt)
                parsed_index = _parse_model_response(response)
                collected_cases.extend(parsed_index.cases)

            deduped_cases = _dedupe_cases(collected_cases)
            output_payload = {"cases": []}
            source_hash = filename.replace('.md', '')
            for case in deduped_cases:
                case_payload = case.model_dump()
                case_payload["source_hash"] = source_hash
                output_payload["cases"].append(case_payload)

            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(output_payload, f, indent=2, ensure_ascii=False)
                
            print(f"Successfully structured {len(output_payload['cases'])} narratives to {output_path}")
            
        except (json.JSONDecodeError, ValidationError, ValueError) as e:
            print(f"Validation error parsing {filename} after retries: {e}")
        except Exception as e:
            print(f"Error parsing {filename} after retries: {e}")

if __name__ == "__main__":
    parse_markdown_files()
