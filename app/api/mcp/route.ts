import { createMcpHandler } from "mcp-handler";
import { z } from "zod";
import { assertApiKey } from "@/lib/env";
import { findOpportunities, getDistrictContext } from "@/lib/db";
import { searchNarratives } from "@/lib/neon-vector";
import { topicConfig } from "@/lib/lcap-domain";

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
  const auth = assertApiKey(request);
  if (auth) {
    return auth;
  }
  return mcpHandler(request);
}

export { secured as DELETE, secured as GET, secured as POST };
