import { NextRequest } from "next/server";
import { assertApiKey } from "@/lib/env";
import { getDistrictContext } from "@/lib/db";
import { searchNarratives } from "@/lib/neon-vector";
import { topicConfig } from "@/lib/lcap-domain";
import { captureUsageEvent, errorTelemetry } from "@/lib/usage-analytics";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";
export const maxDuration = 60;

export async function GET(request: NextRequest, context: { params: Promise<{ cdsCode: string }> }) {
  const startedAt = Date.now();
  const auth = assertApiKey(request);
  if (auth) {
    await captureUsageEvent({
      request,
      event: "lcap_api_unauthorized",
      route: "/api/districts/[cdsCode]",
      transport: "rest",
      statusCode: 401,
      durationMs: Date.now() - startedAt
    });
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

    await captureUsageEvent({
      request,
      event: "lcap_district_context",
      route: "/api/districts/[cdsCode]",
      transport: "rest",
      statusCode: 200,
      durationMs: Date.now() - startedAt,
      properties: {
        cds_code: cdsCode,
        topic,
        include_narratives: includeNarratives,
        narrative_result_count: narratives.length,
        district_found: Boolean(districtContext.district)
      }
    });
    return Response.json({ ...districtContext, narrative_hits: narratives });
  } catch (error) {
    await captureUsageEvent({
      request,
      event: "lcap_api_error",
      route: "/api/districts/[cdsCode]",
      transport: "rest",
      statusCode: 400,
      durationMs: Date.now() - startedAt,
      properties: errorTelemetry(error)
    });
    return Response.json({ error: error instanceof Error ? error.message : "Unknown error" }, { status: 400 });
  }
}
