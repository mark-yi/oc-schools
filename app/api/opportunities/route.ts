import { NextRequest } from "next/server";
import { z } from "zod";
import { assertApiKey } from "@/lib/env";
import { findOpportunities } from "@/lib/db";
import { captureUsageEvent, errorTelemetry } from "@/lib/usage-analytics";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";
export const maxDuration = 60;

const querySchema = z.object({
  topic: z.string().default("chronic_absenteeism"),
  outcomeTrend: z.string().default("worsening"),
  rankBy: z.string().default("strict_action_funds"),
  county: z.string().optional(),
  district: z.string().optional(),
  limit: z.coerce.number().int().min(1).max(100).default(25),
  includeActions: z.coerce.boolean().default(true),
  actionLimit: z.coerce.number().int().min(1).max(10).default(3)
});

export async function GET(request: NextRequest) {
  const startedAt = Date.now();
  const auth = assertApiKey(request);
  if (auth) {
    await captureUsageEvent({
      request,
      event: "lcap_api_unauthorized",
      route: "/api/opportunities",
      transport: "rest",
      statusCode: 401,
      durationMs: Date.now() - startedAt
    });
    return auth;
  }

  try {
    const parsed = querySchema.parse(Object.fromEntries(request.nextUrl.searchParams.entries()));
    const rows = await findOpportunities(parsed);
    await captureUsageEvent({
      request,
      event: "lcap_opportunity_query",
      route: "/api/opportunities",
      transport: "rest",
      statusCode: 200,
      durationMs: Date.now() - startedAt,
      properties: {
        topic: parsed.topic,
        outcome_trend: parsed.outcomeTrend,
        rank_by: parsed.rankBy,
        result_count: rows.length,
        limit: parsed.limit,
        include_actions: parsed.includeActions,
        action_limit: parsed.actionLimit,
        county: parsed.county,
        district_present: Boolean(parsed.district)
      }
    });
    return Response.json({ rows, count: rows.length });
  } catch (error) {
    await captureUsageEvent({
      request,
      event: "lcap_api_error",
      route: "/api/opportunities",
      transport: "rest",
      statusCode: 400,
      durationMs: Date.now() - startedAt,
      properties: errorTelemetry(error)
    });
    return Response.json({ error: error instanceof Error ? error.message : "Unknown error" }, { status: 400 });
  }
}
