import { createMcpHandler } from "mcp-handler";
import { z } from "zod";
import { assertApiKey } from "@/lib/env";
import { findOpportunities, getDistrictContext } from "@/lib/db";
import { searchNarratives } from "@/lib/neon-vector";
import { topicConfig } from "@/lib/lcap-domain";
import { captureUsageEvent, errorTelemetry, queryTelemetry } from "@/lib/usage-analytics";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";
export const maxDuration = 60;

function jsonContent(value: unknown) {
  return {
    content: [
      {
        type: "text" as const,
        text: JSON.stringify(value, null, 2)
      }
    ]
  };
}

type JsonRecord = Record<string, unknown>;

async function readMcpRequest(request: Request): Promise<{
  jsonrpc_method?: string;
  tool_name?: string;
  tool_arguments?: JsonRecord;
}> {
  if (request.method !== "POST") {
    return {};
  }
  try {
    const body = (await request.clone().json()) as unknown;
    const envelope = Array.isArray(body) ? body[0] : body;
    if (!envelope || typeof envelope !== "object") {
      return {};
    }
    const record = envelope as JsonRecord;
    const params = record.params && typeof record.params === "object" ? (record.params as JsonRecord) : {};
    const args =
      params.arguments && typeof params.arguments === "object" && !Array.isArray(params.arguments)
        ? (params.arguments as JsonRecord)
        : undefined;
    return {
      jsonrpc_method: typeof record.method === "string" ? record.method : undefined,
      tool_name: typeof params.name === "string" ? params.name : undefined,
      tool_arguments: args
    };
  } catch {
    return {};
  }
}

function numberArg(args: JsonRecord | undefined, key: string): number | undefined {
  const value = args?.[key];
  return typeof value === "number" ? value : undefined;
}

function stringArg(args: JsonRecord | undefined, key: string): string | undefined {
  const value = args?.[key];
  return typeof value === "string" ? value : undefined;
}

function booleanArg(args: JsonRecord | undefined, key: string): boolean | undefined {
  const value = args?.[key];
  return typeof value === "boolean" ? value : undefined;
}

function mcpArgumentTelemetry(args: JsonRecord | undefined): JsonRecord {
  return {
    ...queryTelemetry(stringArg(args, "query")),
    topic: stringArg(args, "topic"),
    outcome_trend: stringArg(args, "outcomeTrend"),
    rank_by: stringArg(args, "rankBy"),
    limit: numberArg(args, "limit"),
    candidate_limit: numberArg(args, "candidateLimit"),
    action_limit: numberArg(args, "actionLimit"),
    include_actions: booleanArg(args, "includeActions"),
    include_narratives: booleanArg(args, "includeNarratives"),
    group_by_district: booleanArg(args, "groupByDistrict"),
    per_district: numberArg(args, "perDistrict"),
    county: stringArg(args, "county"),
    district_present: Boolean(stringArg(args, "district")),
    cds_code_present: Boolean(stringArg(args, "cdsCode")),
    cds_code: stringArg(args, "cdsCode"),
    school_year: stringArg(args, "schoolYear"),
    section_type_count: Array.isArray(args?.sectionTypes) ? args.sectionTypes.length : undefined
  };
}

async function inferMcpResultCount(response: Response): Promise<number | undefined> {
  try {
    const text = await response.clone().text();
    const dataLine = text
      .split("\n")
      .map((line) => line.trim())
      .find((line) => line.startsWith("data:"));
    if (!dataLine) {
      return undefined;
    }
    const payload = JSON.parse(dataLine.replace(/^data:\s*/, "")) as JsonRecord;
    const result = payload.result && typeof payload.result === "object" ? (payload.result as JsonRecord) : undefined;
    const content = Array.isArray(result?.content) ? result.content : [];
    const firstText = content.find(
      (item): item is JsonRecord =>
        Boolean(item) && typeof item === "object" && (item as JsonRecord).type === "text"
    );
    const rawText = typeof firstText?.text === "string" ? firstText.text : undefined;
    if (!rawText) {
      return undefined;
    }
    const parsed = JSON.parse(rawText) as unknown;
    if (Array.isArray(parsed)) {
      return parsed.length;
    }
    if (parsed && typeof parsed === "object") {
      const record = parsed as JsonRecord;
      if (Array.isArray(record.narrative_hits)) {
        return record.narrative_hits.length;
      }
      if (Array.isArray(record.rows)) {
        return record.rows.length;
      }
      if (Array.isArray(record.hits)) {
        return record.hits.length;
      }
    }
    return undefined;
  } catch {
    return undefined;
  }
}

const mcpHandler = createMcpHandler(
  (server) => {
    server.registerTool(
      "lcap_find_opportunities",
      {
        title: "Find LCAP Opportunities",
        description:
          "Find districts by joining California School Dashboard outcomes to LCAP goals/actions/metrics and ranked spend.",
        inputSchema: {
          topic: z.string().default("chronic_absenteeism"),
          outcomeTrend: z
            .enum(["worsening", "improving", "decreasing_rate", "increasing_rate", "any"])
            .default("worsening"),
          rankBy: z
            .enum([
              "strict_action_funds",
              "broad_action_funds",
              "affected_student_count",
              "current_status",
              "outcome_change",
              "opportunity_score"
            ])
            .default("strict_action_funds"),
          county: z.string().optional(),
          district: z.string().optional(),
          limit: z.number().int().min(1).max(50).default(10),
          includeActions: z.boolean().default(true),
          actionLimit: z.number().int().min(1).max(10).default(3)
        }
      },
      async (input) => jsonContent(await findOpportunities(input))
    );

    server.registerTool(
      "lcap_search_narratives",
      {
        title: "Search LCAP Narratives",
        description:
          "Hybrid Neon pgvector + Postgres full-text search over section-tagged narrative LCAP chunks using RRF.",
        inputSchema: {
          query: z.string().min(1),
          limit: z.number().int().min(1).max(30).default(10),
          candidateLimit: z.number().int().min(10).max(500).optional(),
          cdsCode: z.string().optional(),
          county: z.string().optional(),
          district: z.string().optional(),
          schoolYear: z.string().optional(),
          sectionTypes: z.array(z.string()).optional(),
          groupByDistrict: z.boolean().default(false),
          perDistrict: z.number().int().min(1).max(5).default(2)
        }
      },
      async (input) => jsonContent(await searchNarratives(input))
    );

    server.registerTool(
      "lcap_get_district_context",
      {
        title: "Get District Context",
        description:
          "Fetch one district's Dashboard outcome, matching LCAP goals/metrics/actions, and optional default narrative evidence.",
        inputSchema: {
          cdsCode: z.string().min(1),
          topic: z.string().default("chronic_absenteeism"),
          includeNarratives: z.boolean().default(true)
        }
      },
      async ({ cdsCode, topic, includeNarratives }) => {
        const context = await getDistrictContext(cdsCode, topic);
        const narrative_hits = includeNarratives
          ? await searchNarratives({
              query: topicConfig(topic).defaultNarrativeQuery,
              cdsCode,
              limit: 8,
              candidateLimit: 160
            })
          : [];
        return jsonContent({ ...context, narrative_hits });
      }
    );

    server.registerTool(
      "lcap_explain_account",
      {
        title: "Explain LCAP Account",
        description:
          "Produce a compact account brief for an AE by combining outcome, spend, actionability, and narrative evidence.",
        inputSchema: {
          cdsCode: z.string().min(1),
          topic: z.string().default("chronic_absenteeism")
        }
      },
      async ({ cdsCode, topic }) => {
        const context = await getDistrictContext(cdsCode, topic);
        const narrative_hits = await searchNarratives({
          query: topicConfig(topic).defaultNarrativeQuery,
          cdsCode,
          limit: 5,
          candidateLimit: 160
        });
        return jsonContent({
          district: context.district,
          dashboard_outcome: context.dashboard_outcome,
          strict_topic_actions: context.strict_topic_actions,
          broad_topic_actions: context.broad_topic_actions,
          topic_goals: context.topic_goals,
          topic_metrics: context.topic_metrics,
          narrative_hits
        });
      }
    );
  },
  {},
  {
    basePath: "/api",
    disableSse: true,
    maxDuration: 60,
    verboseLogs: false
  }
);

async function secured(request: Request) {
  const startedAt = Date.now();
  const mcpRequest = await readMcpRequest(request);
  const auth = assertApiKey(request);
  if (auth) {
    await captureUsageEvent({
      request,
      event: "lcap_mcp_unauthorized",
      route: "/api/mcp",
      transport: "mcp",
      statusCode: 401,
      durationMs: Date.now() - startedAt,
      properties: {
        jsonrpc_method: mcpRequest.jsonrpc_method,
        tool_name: mcpRequest.tool_name,
        ...mcpArgumentTelemetry(mcpRequest.tool_arguments)
      }
    });
    return auth;
  }

  try {
    const response = await mcpHandler(request);
    const event = mcpRequest.tool_name ? "lcap_mcp_tool_call" : "lcap_mcp_request";
    const resultCount = mcpRequest.tool_name ? await inferMcpResultCount(response) : undefined;
    await captureUsageEvent({
      request,
      event,
      route: "/api/mcp",
      transport: "mcp",
      statusCode: response.status,
      durationMs: Date.now() - startedAt,
      properties: {
        jsonrpc_method: mcpRequest.jsonrpc_method,
        tool_name: mcpRequest.tool_name,
        result_count: resultCount,
        ...mcpArgumentTelemetry(mcpRequest.tool_arguments)
      }
    });
    return response;
  } catch (error) {
    await captureUsageEvent({
      request,
      event: "lcap_mcp_error",
      route: "/api/mcp",
      transport: "mcp",
      statusCode: 500,
      durationMs: Date.now() - startedAt,
      properties: {
        jsonrpc_method: mcpRequest.jsonrpc_method,
        tool_name: mcpRequest.tool_name,
        ...mcpArgumentTelemetry(mcpRequest.tool_arguments),
        ...errorTelemetry(error)
      }
    });
    throw error;
  }
}

export { secured as DELETE, secured as GET, secured as POST };
