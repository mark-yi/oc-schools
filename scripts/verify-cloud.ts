import dotenv from "dotenv";
import { collectionStats, searchNarratives } from "../lib/neon-vector";
import { findOpportunities, getSql } from "../lib/db";

dotenv.config({ path: ".env.local", quiet: true });
dotenv.config({ path: ".env", quiet: true });

async function tableCount(table: string): Promise<number> {
  if (!/^[a-z_][a-z0-9_]*$/i.test(table)) {
    throw new Error(`Unsafe table name: ${table}`);
  }
  const rows = (await getSql().query(`select count(*)::int as count from "${table}"`)) as Array<{ count: number }>;
  return Number(rows[0]?.count ?? 0);
}

async function main() {
  const tables = ["districts", "lcap_actions", "dashboard_indicators", "rag_chunks"];
  for (const table of tables) {
    console.log(`${table}: ${(await tableCount(table)).toLocaleString()} rows`);
  }

  const opportunities = await findOpportunities({
    topic: "chronic_absenteeism",
    outcomeTrend: "worsening",
    rankBy: "strict_action_funds",
    limit: 5,
    actionLimit: 1
  });
  console.log("top opportunity districts:");
  for (const row of opportunities) {
    console.log(`- ${row.district} (${row.county}) strict=$${Number(row.strict_action_funds ?? 0).toLocaleString()}`);
  }

  const stats = await collectionStats();
  console.log(
    `${stats.collection}: ${stats.count.toLocaleString()} embeddings / ${stats.chunk_count.toLocaleString()} chunks`
  );

  const hits = await searchNarratives({
    query: "chronic absenteeism attendance barriers family outreach re-engagement",
    limit: 5,
    groupByDistrict: true
  });
  console.log("sample narrative hits:");
  for (const hit of hits) {
    console.log(`- ${hit.metadata?.district ?? "unknown district"} / ${hit.metadata?.section_type ?? "section"}`);
  }
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
});
