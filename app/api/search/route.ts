import { z } from "zod";
import { assertApiKey } from "@/lib/env";
import { searchNarratives } from "@/lib/neon-vector";

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
  const auth = assertApiKey(request);
  if (auth) {
    return auth;
  }

  try {
    const body = await request.json();
    const input = searchSchema.parse(body);
    const hits = await searchNarratives(input);
    return Response.json({ hits, count: hits.length });
  } catch (error) {
    return Response.json({ error: error instanceof Error ? error.message : "Unknown error" }, { status: 400 });
  }
}
