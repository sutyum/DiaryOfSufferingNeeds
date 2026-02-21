import { NextResponse } from 'next/server';
// @ts-ignore - Prisma client will be generated later
import { PrismaClient } from '@prisma/client';

const prisma = new PrismaClient();

export async function POST (
  request: Request,
  { params }: { params: { id: string; }; }
) {
  try
  {
    const caseId = params.id;
    const body = await request.json();
    const { editSummary, metadataUpdate, editorId = "SystemAgent" } = body;

    if (!metadataUpdate)
    {
      return NextResponse.json({ error: 'Metadata update is required' }, { status: 400 });
    }

    // 1. Fetch current case to snapshot its state for the revision history
    const currentCase = await prisma.caseStory.findUnique({
      where: { id: caseId },
    });

    if (!currentCase)
    {
      return NextResponse.json({ error: 'Case not found' }, { status: 404 });
    }

    // 2. Perform the Wikipedia-style Edit (Transaction ensures atomicity)
    const [updatedCase, revision] = await prisma.$transaction([
      // Update the actual CaseStory with merged metadata
      prisma.caseStory.update({
        where: { id: caseId },
        data: {
          metadata: {
            // Prisma JSON merging or overwrite
            ...(currentCase.metadata as object || {}),
            ...metadataUpdate
          }
        },
      }),

      // Log the revision
      prisma.caseRevision.create({
        data: {
          caseStoryId: caseId,
          editorId: editorId,
          editSummary: editSummary || 'Automated living metadata enrichment',
          previousData: currentCase.metadata || {}
        }
      })
    ]);

    return NextResponse.json({
      success: true,
      case: updatedCase,
      revisionId: revision.id
    });

  } catch (error)
  {
    console.error('Failed to apply Living Metadata edit:', error);
    return NextResponse.json({ error: 'Internal Server Error' }, { status: 500 });
  }
}
