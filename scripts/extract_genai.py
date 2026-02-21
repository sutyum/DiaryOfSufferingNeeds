import os
import json
import argparse
from google import genai
from google.genai import types
from pydantic import BaseModel, Field

# Define our strict Pydantic ontology for structured JSON output based on The Witness Archive schema
class WitnessCase(BaseModel):
    condition: str = Field(description="The illness or condition accurately described (e.g., 'ME/CFS').")
    onset: str = Field(description="The duration or timeline of the suffering (e.g., 'Year 3', 'Since 2018').")
    threat_to_personhood: str = Field(description="Classify the core loss into a psychological threat (e.g., 'The Loss of Unplanned Time', 'Ambiguous Grief', 'The Erosion of Epistemic Trust').")
    description: str = Field(description="A concise summary of the specific context of this loss.")
    narrative_fragment: str = Field(description="The raw, visceral quoted text describing the friction. Paraphrase slightly to remove exact names/locations but maintain the brutal honesty and texture.")
    compensatory_rituals: str = Field(description="The specific hacks, routines, or workarounds the patient uses to survive, highlighting exactly where current tools or society fails them.")
    original_source_url: str = Field(description="The public URL where the story was found.")

class WitnessIndex(BaseModel):
    cases: list[WitnessCase]

def ingest_from_internet(topic: str, output_path: str):
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        print("Error: GEMINI_API_KEY environment variable not set. Please export it.")
        return

    client = genai.Client(api_key=api_key)
    
    prompt = f"""
You are an expert medical anthropologist and product researcher indexing 'The Witness Archive'.
Your task is to search the web (particularly Reddit, patient blogs, or NORD/rare disease communities) for deeply visceral 'Case Stories of Suffering' related to: {topic}.

Using your Google Search tool, find 3-4 highly detailed, raw, first-person narratives where patients describe the agonizing daily realities of their condition.
Then, extract these stories strictly into the required JSON schema, mapping their experiences to our specific ontological framework.

CRITICAL INSTRUCTIONS / ETHICS:
- Do NOT sanitize the raw human experience. We want the "truth inspiration" to remain intact.
- However, you MUST paraphrase slightly to ensure full anonymization (remove names, specific hospitals, unique identifiers).
- Map the core suffering to a `threat_to_personhood` (e.g., "The Spontaneity Death", "Medical Gaslighting").
- Emphasize hyper-specific frustrations in the `narrative_fragment`.
- Detail the exact `compensatory_rituals` they rely on.
- Always include the actual `original_source_url`.
    """

    print(f"Executing Agentic Web Indexing using Gemini 3 Flash (Search Active) for topic: '{topic}'...")
    
    try:
        response = client.models.generate_content(
            model='gemini-3-flash-preview',
            contents=prompt,
            config=types.GenerateContentConfig(
                tools=[{"google_search": {}}],
                response_mime_type="application/json",
                response_schema=WitnessIndex,
                temperature=0.3, # Low temp for adherence, slight freedom for taxonomy grouping
            )
        )
        
        result_text = response.text
        print("Extraction complete. Validating ontology...")
        
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, 'w') as f:
            parsed_json = json.loads(result_text)
            json.dump(parsed_json, f, indent=2)
            
        print(f"\nSuccessfully indexed {len(parsed_json.get('cases', []))} narratives to: {output_path}")
        print("These cases are now ready to be rendered in The Witness Archive or upserted to the database.")
        
    except Exception as e:
        print(f"Error during Gemini Indexing Protocol: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Use Google GenAI Search + Gemini 3 Flash to actively scour the web and index suffering narratives.")
    parser.add_argument("--topic", default="severe ME/CFS and post exertional malaise", help="The medical condition or specific query to index.")
    parser.add_argument("--output", default="data/seed/gemini_indexed_cases.json", help="Path to save the structured JSON index.")
    args = parser.parse_args()
    
    os.environ["GEMINI_API_KEY"] = os.environ.get("GEMINI_API_KEY", "") 
    ingest_from_internet(args.topic, args.output)
