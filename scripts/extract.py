import json
import os
import sys
import argparse

try:
    # Requires: pip install git+https://github.com/ysz/recursive-llm.git
    from rlm import RLM
except ImportError:
    print("Warning: 'rlm' package not found. Please install it to run the extraction.")
    print("Command: pip install git+https://github.com/ysz/recursive-llm.git")
    RLM = None

def extract_case_stories(raw_data_path: str, output_path: str):
    with open(raw_data_path, 'r') as f:
        raw_data = json.load(f)

    if not RLM:
        sys.exit(1)

    # Initialize RLM
    # You can change this to "claude-3-5-sonnet-20240620" or others via LiteLLM
    # Make sure OPENAI_API_KEY or ANTHROPIC_API_KEY is exported
    print("Initializing Recursive Language Model (gpt-4o)...")
    rlm = RLM(model="gpt-4o")
    
    # THE ONTOLOGY / EXTRACTION PROMPT
    # This prompt is designed to prevent the LLM from sanitizing the raw human experience.
    extraction_prompt = """
You are an expert medical anthropologist and product researcher. 
Your task is to analyze the raw patient stories stored in the `context` variable.

Using your recursive exploration capabilities, thoroughly read through the context and extract deeply visceral 'Case Stories of Suffering'.
For EACH distinct story in the context, construct a JSON object conforming strictly to this ontology:

1. "Condition": The illness or overarching trigger.
2. "Physical_Frictions": Specific, granular painful tasks (e.g., "Turning a doorknob sends shooting pain"). DO NOT use generic medical terms. Quote the raw visceral text.
3. "Emotional_Toll": The mental weight, isolation, grief, or anxiety. Preserve the raw emotional intensity.
4. "Journey_Timeline": Chronological failures, misdiagnoses, and treatment attempts mentioned.
5. "Compensatory_Behaviors": "Hacks" or workarounds the patient uses to survive, highlighting exactly where current tools/products fail.
6. "Original_Source_Ref": A reference identifier to the original text.

Return ONLY a valid JSON list containing these objects. Do not wrap in markdown unless it's a single ```json block.
We want the "truth inspiration" to remain intact. Emphasize actual quotes and hyper-specific frustrations.
    """

    print("Running RLM extraction. The agent will now recursively process the context...")
    
    # RLM avoids context window limits by storing this as a variable the LLM can explore programmatically
    huge_document = json.dumps(raw_data, indent=2)
    
    try:
        result = rlm.complete(
            query=extraction_prompt,
            context=huge_document
        )
    except Exception as e:
        print(f"Error during RLM execution: {e}")
        sys.exit(1)

    print("Extraction complete. Saving to output...")
    
    # Basic cleanup if the LLM wrapped in markdown
    if isinstance(result, str):
        if result.startswith("```json"):
            result = result[7:-3].strip()
        elif result.startswith("```"):
            result = result[3:-3].strip()

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, 'w') as f:
        f.write(result)
    print(f"Saved extracted structured narratives to {output_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract structured suffering ontology from raw text using Recursive LLMs.")
    parser.add_argument("--input", default="../data/raw/sample_stories.json", help="Path to raw json stories")
    parser.add_argument("--output", default="../data/processed/extracted_stories.json", help="Path to save structured output")
    args = parser.parse_args()
    
    # Remind user about API keys
    if not os.environ.get("OPENAI_API_KEY") and not os.environ.get("ANTHROPIC_API_KEY"):
        print("CRITICAL: No API keys found in the environment. RLM will likely fail unless using a local model.")
        print("Please export OPENAI_API_KEY or ANTHROPIC_API_KEY.")
        
    extract_case_stories(args.input, args.output)
