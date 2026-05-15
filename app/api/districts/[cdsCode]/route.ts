import { NextRequest } from "next/server";
import { assertApiKey } from "@/lib/env";
import { getDistrictContext } from "@/lib/db";
import { searchNarratives } from "@/lib/neon-vector";
import { topicConfig } from "@/lib/lcap-domain";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";
export const maxDuration = 60;

export async function GET(request: NextRequest, context: { params: Promise<{ cdsCode: string }> }) {
  const auth = assertApiKey(request);
  if (auth) {
    return auth;
  }

  try {
    const { cdsCode } = await context.params;
    const topic = request.nextUrl.searchParams.get("topic") ?? "chronic_absenteeism";
    const includeNarratives = request.nextUrl.searchParams.get("includeNarratives") !== "false";
    const districtContext = await getDistrictContext(cdsCode, topic);
    const narratives = includeNarratives
      ? await searchNarratives({
          query: topicConfig(topic).defaultNarrativeQuery,
          cdsCode,
          limit: 8,
          candidateLimit: 160
        })
      : [];

    return Response.json({ ...districtContext, narrative_hits: narratives });
  } catch (error) {
    return Response.json({ error: error instanceof Error ? error.message : "Unknown error" }, { status: 400 });
  }
}
