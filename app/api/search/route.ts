import { z } from "zod";
import { assertApiKey } from "@/lib/env";
import { searchNarratives } from "@/lib/neon-vector";
import { captureUsageEvent, errorTelemetry, queryTelemetry } from "@/lib/usage-analytics";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";
export const maxDuration = 60;

const searchSchema = z.object({
  query: z.string().min(1),
  limit: z.number().int().min(1).max(50).default(10),
  candidateLimit: z.number().int().min(10).max(500).optional(),
  county: z.string().optional(),
  cdsCode: z.string().optional(),
  district: z.string().optional(),
  schoolYear: z.string().optional(),
  sectionTypes: z.array(z.string()).optional(),
  groupByDistrict: z.boolean().default(false),
  perDistrict: z.number().int().min(1).max(5).default(2)
});

export async function POST(request: Request) {
  const startedAt = Date.now();
  const auth = assertApiKey(request);
  if (auth) {
    await captureUsageEvent({
      request,
      event: "lcap_api_unauthorized",
      route: "/api/search",
      transport: "rest",
      statusCode: 401,
      durationMs: Date.now() - startedAt
    });
    return auth;
  }

  try {
    const body = await request.json();
    const input = searchSchema.parse(body);
    const hits = await searchNarratives(input);
    await captureUsageEvent({
      request,
      event: "lcap_narrative_search",
      route: "/api/search",
      transport: "rest",
      statusCode: 200,
      durationMs: Date.now() - startedAt,
      properties: {
        ...queryTelemetry(input.query),
        result_count: hits.length,
        limit: input.limit,
        candidate_limit: input.candidateLimit,
        group_by_district: input.groupByDistrict,
        per_district: input.perDistrict,
        county: input.county,
        district_present: Boolean(input.district),
        cds_code_present: Boolean(input.cdsCode),
        school_year: input.schoolYear,
        section_type_count: input.sectionTypes?.length ?? 0
      }
    });
    return Response.json({ hits, count: hits.length });
  } catch (error) {
    await captureUsageEvent({
      request,
      event: "lcap_api_error",
      route: "/api/search",
      transport: "rest",
      statusCode: 400,
      durationMs: Date.now() - startedAt,
      properties: errorTelemetry(error)
    });
    return Response.json({ error: error instanceof Error ? error.message : "Unknown error" }, { status: 400 });
  }
}
