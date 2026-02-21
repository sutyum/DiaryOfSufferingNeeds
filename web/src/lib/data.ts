import fs from 'fs';
import path from 'path';

export interface WitnessCase {
  id: string; // generated from hash + index
  condition: string;
  onset: string;
  threat_to_personhood: string;
  description: string;
  narrative_fragment: string;
  compensatory_rituals: string;
  source_hash: string;
}

export function getAllCases (): WitnessCase[] {
  // Go up from web/app/lib to the root public_data/processed directory
  const dataDir = path.join(process.cwd(), '..', 'public_data', 'processed');
  const cases: WitnessCase[] = [];

  try
  {
    const files = fs.readdirSync(dataDir);

    for (const file of files)
    {
      if (file.endsWith('.json'))
      {
        const filePath = path.join(dataDir, file);
        const fileContent = fs.readFileSync(filePath, 'utf8');
        try
        {
          const parsed = JSON.parse(fileContent);
          if (parsed.cases && Array.isArray(parsed.cases))
          {
            // Map the parsed JSON cases into our Typescript interface
            parsed.cases.forEach((c: any, index: number) => {
              cases.push({
                id: `${c.source_hash}_${index}`,
                condition: c.condition || 'Unknown',
                onset: c.onset || 'Unknown',
                threat_to_personhood: c.threat_to_personhood || 'Unknown',
                description: c.description || '',
                narrative_fragment: c.narrative_fragment || '',
                compensatory_rituals: c.compensatory_rituals || '',
                source_hash: c.source_hash || 'Unknown',
              });
            });
          }
        } catch (e)
        {
          console.error(`Error parsing JSON in ${file}:`, e);
        }
      }
    }
  } catch (e)
  {
    console.error("Error reading public_data/processed directory:", e);
  }

  return cases;
}
