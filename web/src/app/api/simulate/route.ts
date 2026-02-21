import { NextResponse } from 'next/server';
import { genai } from 'google-genai'; // Assuming SDK is configured
// @ts-ignore
import { PrismaClient } from '@prisma/client';

const prisma = new PrismaClient();

// The core Prompt that forces the LLM to strictly embody the patient's exact suffering profile
function build_agent_prompt (pitchText: string, caseStory: any) {
  return `
You are an AI Agent embodying a real human patient.
You MUST respond entirely in the first-person ("I", "my") and base your reaction STRICTLY on the facts of your provided Case File.

YOUR CASE FILE:
- Condition: ${caseStory.condition}
- Your Daily Physical Frictions: ${caseStory.physicalFrictions}
- Your Emotional Toll: ${caseStory.emotionalToll}
- Hacks you currently use: ${caseStory.compensatoryBehaviorsAndHacks}
- Additional Medical Metadata: ${JSON.stringify(caseStory.metadata)}

THE PITCH:
A product builder has proposed the following intervention for you:
"${pitchText}"

YOUR INSTRUCTIONS:
Critique this pitch. Does it actually solve your specific physical frictions? 
Does it introduce a new problem based on your specific metadata/comorbidities? 
Rate your acceptance from 1 to 10, and provide a brutally honest, emotional response.
Return JSON: { "acceptanceScore": number, "reactionText": "your first person response", "rejectionReason": "Specific critique" }
  `;
}

export async function POST (request: Request) {
  try
  {
    const { pitchText } = await request.json();

    if (!pitchText)
    {
      return NextResponse.json({ error: 'Pitch text required' }, { status: 400 });
    }

    // 1. Vector Search for relevant cases (Mocked here since pgvector setup requires raw SQL)
    // In production: await prisma.$queryRaw`SELECT * FROM "CaseStory" ORDER BY embedding <-> ${embed(pitchText)} LIMIT 5`;
    const relevantCases = await prisma.caseStory.findMany({ take: 3 });

    // 2. Spawn Agents in Parallel
    const client = new genai.Client({ apiKey: process.env.GEMINI_API_KEY });

    const agentTasks = relevantCases.map(async (caseStory: any) => {
      const prompt = build_agent_prompt(pitchText, caseStory);

      const response = await client.models.generateContent({
        model: 'gemini-3-flash-preview',
        contents: prompt,
        config: {
          responseMimeType: "application/json",
          temperature: 0.4, // Slight variance for human-like response
        }
      });

      const parsedReaction = JSON.parse(response.text || "{}");

      // Calculate outcome and save log
      return {
        caseStoryId: caseStory.id,
        condition: caseStory.condition,
        ...parsedReaction
      };
    });

    const simulationResults = await Promise.all(agentTasks);

    // 3. Save the pitch and the agent responses to the simulation engine log
    const savedPitch = await prisma.interventionPitch.create({
      data: {
        builderId: 'demo-user',
        title: pitchText.substring(0, 50) + '...',
        description: pitchText,
        simulations: {
          create: simulationResults.map(r => ({
            caseStoryId: r.caseStoryId,
            agentReactionText: r.reactionText,
            rejectionReason: r.rejectionReason,
            acceptanceScore: r.acceptanceScore
          }))
        }
      }
    });

    return NextResponse.json({
      success: true,
      pitchId: savedPitch.id,
      simulationResults
    });

  } catch (error)
  {
    console.error('Simulation Failed:', error);
    return NextResponse.json({ error: 'Simulation Engine Exception' }, { status: 500 });
  }
}
