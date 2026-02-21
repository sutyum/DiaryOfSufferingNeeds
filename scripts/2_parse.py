import os
import json
from pathlib import Path
from google import genai
from google.genai import types
from pydantic import BaseModel, Field
from tenacity import retry, stop_after_attempt, wait_exponential

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

def ensure_directories():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=4, max=10))
def parse_with_retry(client, prompt):
    """Call the Gemini API with exponential backoff on failure (rate limits etc)."""
    return client.models.generate_content(
        model='gemini-3.0-flash', # using standard stable flash
        contents=prompt,
        config=types.GenerateContentConfig(
            response_mime_type="application/json",
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
    
    md_files = list(INPUT_DIR.glob("*.md"))
    
    if not md_files:
        print(f"No raw markdown files found in {INPUT_DIR}. Please run crawl.py first.")
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

        prompt = f"""
You are an expert medical anthropologist and product researcher indexing 'The Witness Archive'.
Your task is to comprehensively analyze the following raw markdown scraped from a patient forum or blog.
You MUST extract EVERY SINGLE deeply visceral 'Case Story of Suffering' you find into the exact JSON schema.

CRITICAL INSTRUCTIONS / ETHICS:
- Be EXHAUSTIVE. A single forum thread may contain 10, 20, or even 50 distinct patient narratives. You must extract EVERY UNIQUE STORY as a distinct Case.
- Do NOT sanitize the raw human experience. We want the "truth inspiration" to remain intact.
- However, you MUST paraphrase slightly to ensure full anonymization (remove names, specific hospitals, unique identifiers).
- Map the core suffering to a `threat_to_personhood` (e.g., "The Spontaneity Death", "Medical Gaslighting").
- Emphasize hyper-specific frustrations in the `narrative_fragment`.
- Detail the exact `compensatory_rituals` they rely on.
- RETURN ONLY VALID JSON. Do not include markdown formatting or backticks around the json.

EXPECTED JSON FORMAT:
{{
  "cases": [
    {{
      "condition": "The illness or condition accurately described (e.g., 'ME/CFS').",
      "onset": "The duration or timeline of the suffering (e.g., 'Year 3', 'Since 2018').",
      "threat_to_personhood": "Classify the core loss into a psychological threat (e.g., 'The Loss of Unplanned Time', 'Ambiguous Grief', 'The Erosion of Epistemic Trust').",
      "description": "A concise summary of the specific context of this loss.",
      "narrative_fragment": "The raw, visceral quoted text describing the friction. Paraphrase slightly.",
      "compensatory_rituals": "The specific hacks, routines, or workarounds the patient uses."
    }}
  ]
}}

RAW SCRAPED MARKDOWN (Source Hash: {filename}):
{raw_markdown}
        """
        
        try:
            print(f"Executing robust parsing for {filename}...")
            response = parse_with_retry(client, prompt)
            
            result_text = response.text
            
            with open(output_path, 'w', encoding='utf-8') as f:
                parsed_json = json.loads(result_text)
                
                # Add source linkage meta
                for case in parsed_json.get("cases", []):
                    case["source_hash"] = filename.replace('.md', '')
                    
                json.dump(parsed_json, f, indent=2)
                
            print(f"Successfully structured {len(parsed_json.get('cases', []))} narratives to {output_path}")
            
        except Exception as e:
            print(f"Error parsing {filename} after retries: {e}")

if __name__ == "__main__":
    parse_markdown_files()
