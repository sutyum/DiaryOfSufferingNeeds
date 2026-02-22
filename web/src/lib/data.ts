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

type JsonRecord = Record<string, unknown>;

function isJsonRecord (value: unknown): value is JsonRecord {
  return typeof value === 'object' && value !== null;
}

function getStringOrFallback (value: unknown, fallback: string): string {
  return typeof value === 'string' && value.trim() ? value : fallback;
}

function resolveDataDir (): string | null {
  const candidates = [
    path.join(process.cwd(), '..', 'public_data', 'processed'),
    path.join(process.cwd(), 'public_data', 'processed'),
  ];

  for (const candidate of candidates)
  {
    if (fs.existsSync(candidate))
    {
      return candidate;
    }
  }

  return null;
}

export function getAllCases (): WitnessCase[] {
  const dataDir = resolveDataDir();
  const cases: WitnessCase[] = [];

  if (!dataDir)
  {
    console.error('Could not find public_data/processed directory from current working directory.');
    return cases;
  }

  try
  {
    const files = fs.readdirSync(dataDir).filter(file => file.endsWith('.json')).sort();

    for (const file of files)
    {
      const filePath = path.join(dataDir, file);
      const fileContent = fs.readFileSync(filePath, 'utf8');
      try
      {
        const parsed: unknown = JSON.parse(fileContent);
        if (!isJsonRecord(parsed) || !Array.isArray(parsed.cases))
        {
          continue;
        }

        parsed.cases.forEach((entry, index) => {
          if (!isJsonRecord(entry))
          {
            return;
          }

          const sourceHash = getStringOrFallback(entry.source_hash, file.replace(/\.json$/, ''));
          cases.push({
            id: `${sourceHash}_${index}`,
            condition: getStringOrFallback(entry.condition, 'Unknown'),
            onset: getStringOrFallback(entry.onset, 'Unknown'),
            threat_to_personhood: getStringOrFallback(entry.threat_to_personhood, 'Unknown'),
            description: getStringOrFallback(entry.description, ''),
            narrative_fragment: getStringOrFallback(entry.narrative_fragment, ''),
            compensatory_rituals: getStringOrFallback(entry.compensatory_rituals, ''),
            source_hash: sourceHash,
          });
        });
      } catch (e)
      {
        console.error(`Error parsing JSON in ${file}:`, e);
      }
    }
  } catch (e)
  {
    console.error('Error reading public_data/processed directory:', e);
  }

  return cases;
}
