import asyncio
import os
import json
import argparse
import logging
from google import genai
from google.genai import types
from pydantic import BaseModel, Field

# Setup basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Strict output Schema
class CaseStory(BaseModel):
    condition: str = Field(description="The illness or overarching trigger.")
    physical_frictions: str = Field(description="Granular painful tasks and direct physical symptoms. Quote visceral text.")
    emotional_toll: str = Field(description="Mental weight, isolation, or anxiety. Preserve raw emotional intensity.")
    journey_timeline: str = Field(description="Chronological history mentioned.")
    compensatory_behaviors_and_hacks: str = Field(description="Workarounds used, highlighting where products fail.")

def build_prompt(raw_text: str) -> str:
    return f"""
    You are an expert medical anthropologist analyzing a raw patient story.
    Carefully extract the 'Case Story of Suffering' from the following text into the exact JSON schema.
    
    CRITICAL INSTRUCTIONS:
    - Do NOT summarize broadly. We want raw, hyper-specific quotes preserved.
    - Identify specific compensatory behaviors or hacks they use to survive their daily life.
    
    RAW TEXT:
    {raw_text}
    """

async def process_document(client: genai.Client, doc: dict, output_file, semaphore: asyncio.Semaphore) -> None:
    """Process a single document asynchronously, adhering to concurrency limits."""
    async with semaphore:
        prompt = build_prompt(doc.get("text", ""))
        try:
            # We use the sync client with asyncio.to_thread since the official google-genai 
            # documentation for 'aio' is sometimes newer/beta. 
            # However, google-genai does support async natively via client.aio
            response = await client.aio.models.generate_content(
                model='gemini-3-flash-preview',
                contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json",
                    response_schema=CaseStory,
                    temperature=0.1,
                )
            )
            
            # Parse the returned JSON safely
            extracted_data = json.loads(response.text)
            
            # We will merge the ID and URL into the final output so it can map to Postgres later
            final_record = {
                "source_id": doc.get("id"),
                "url": doc.get("url"),
                "platform": doc.get("platform"),
                "extracted_case": extracted_data
            }
            
            # Write to output file in append mode (thread-safe enough for async if careful, 
            # but better to just use a lock or simple `print` to file with flush)
            # For robust mass ingestion, writing synchronously in python's async loop is usually fast enough for NDJSON.
            with open(output_file, 'a') as f:
                f.write(json.dumps(final_record) + "\n")
                
            logging.info(f"Successfully processed and extracted doc ID: {doc.get('id')}")

        except Exception as e:
            logging.error(f"Failed to process doc ID {doc.get('id')}: {e}")

async def run_pipeline(input_path: str, output_path: str, max_concurrent: int):
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        logging.error("GEMINI_API_KEY environment variable not set.")
        return

    # Initialize async client
    client = genai.Client(api_key=api_key)
    
    # Ensure output directory exists and file is clean
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    if os.path.exists(output_path):
        os.remove(output_path)

    # Read the JSONL file
    documents = []
    try:
        with open(input_path, 'r') as f:
            for line in f:
                if line.strip():
                    documents.append(json.loads(line))
    except FileNotFoundError:
        logging.error(f"Input file {input_path} not found.")
        return

    logging.info(f"Loaded {len(documents)} documents. Starting massive parallel extraction with {max_concurrent} workers...")
    
    # We use a Semaphore to strictly control how many concurrent requests hit Gemini
    # This is critical for scaling to millions of cases without hitting 429 Rate Limits from Google.
    semaphore = asyncio.Semaphore(max_concurrent)
    
    tasks = [process_document(client, doc, output_path, semaphore) for doc in documents]
    
    # Run all tasks concurrently
    await asyncio.gather(*tasks)
    
    logging.info("Pipeline complete. Output saved to NDJSON.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Massive parallel extraction using Gemini Async API.")
    parser.add_argument("--input", default="data/raw/bulk_documents.jsonl", help="Path to input NDJSON file.")
    parser.add_argument("--output", default="data/processed/bulk_extracted_stories.jsonl", help="Path to save output NDJSON file.")
    parser.add_argument("--concurrency", type=int, default=15, help="Max concurrent requests to Gemini.")
    args = parser.parse_args()
    
    # Ensure env is set
    os.environ["GEMINI_API_KEY"] = os.environ.get("GEMINI_API_KEY", "")
    
    asyncio.run(run_pipeline(args.input, args.output, args.concurrency))
