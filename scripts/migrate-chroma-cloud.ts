import Database from "better-sqlite3";
import dotenv from "dotenv";
import { existsSync } from "node:fs";
import { basename } from "node:path";
import type { Metadata } from "chromadb";
import { defaultChromaCollection, envOptional, envRequired } from "../lib/env";
import {
  buildLcapNarrativeSchema,
  getChromaClient
} from "../lib/chroma";

dotenv.config({ path: ".env.local", quiet: true });
dotenv.config({ path: ".env", quiet: true });

type ChunkRow = {
  chunk_id: string;
  section_id: string | null;
  cds_code: string | null;
  county: string | null;
  district: string | null;
  school_year: string | null;
  source_path: string | null;
  pdf_url: string | null;
  page_start: number | null;
  page_end: number | null;
  section_type: string | null;
  section_path: string | null;
  prompt_label: string | null;
  goal_number: string | null;
  action_number: string | null;
  chunk_kind: string | null;
  chunk_index: number | null;
  token_count: number | null;
  text_hash: string | null;
  authored_confidence: number | null;
  body_text: string;
  search_text: string | null;
  prev_chunk_id: string | null;
  next_chunk_id: string | null;
};

const DEFAULT_RAG_SQLITE = "outputs/rag/2025/lcap_retrieval.sqlite";
const MAX_CHROMA_DOCUMENT_BYTES = 15_500;

function argValue(name: string): string | undefined {
  const index = process.argv.indexOf(name);
  if (index === -1) {
    return undefined;
  }
  return process.argv[index + 1];
}

function hasArg(name: string): boolean {
  return process.argv.includes(name);
}

function toInt(value: string | undefined, fallback: number): number {
  if (!value) {
    return fallback;
  }
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(`Expected positive integer, got: ${value}`);
  }
  return parsed;
}

function sourceDocumentId(row: ChunkRow): string {
  const cds = row.cds_code ?? "unknown";
  const source = row.source_path ?? row.pdf_url ?? "lcap";
  return `${cds}:${basename(source)}`;
}

function truncateUtf8(input: string, maxBytes: number): string {
  if (Buffer.byteLength(input, "utf8") <= maxBytes) {
    return input;
  }
  let output = input;
  while (Buffer.byteLength(output, "utf8") > maxBytes) {
    output = output.slice(0, Math.floor(output.length * 0.95));
  }
  return `${output.trimEnd()}\n[truncated to fit Chroma document limit]`;
}

function documentForRow(row: ChunkRow): string {
  const context = [
    row.district ? `District: ${row.district}` : null,
    row.county ? `County: ${row.county}` : null,
    row.school_year ? `School year: ${row.school_year}` : null,
    row.section_type ? `Section type: ${row.section_type}` : null,
    row.section_path ? `Section path: ${row.section_path}` : null,
    row.goal_number ? `Goal: ${row.goal_number}` : null,
    row.action_number ? `Action: ${row.action_number}` : null,
    row.page_start ? `Pages: ${row.page_start}${row.page_end && row.page_end !== row.page_start ? `-${row.page_end}` : ""}` : null
  ].filter(Boolean);
  return truncateUtf8(`${context.join("\n")}\n\n${row.body_text}`.trim(), MAX_CHROMA_DOCUMENT_BYTES);
}

function metadataForRow(row: ChunkRow): Metadata {
  return {
    section_id: row.section_id,
    cds_code: row.cds_code,
    county: row.county,
    district: row.district,
    school_year: row.school_year,
    source_path: row.source_path,
    pdf_url: row.pdf_url,
    source_document_id: sourceDocumentId(row),
    district_doc_id: row.cds_code,
    page_start: row.page_start,
    page_end: row.page_end,
    section_type: row.section_type,
    section_path: row.section_path,
    prompt_label: row.prompt_label,
    goal_number: row.goal_number,
    action_number: row.action_number,
    chunk_kind: row.chunk_kind,
    chunk_index: row.chunk_index,
    token_count: row.token_count,
    text_hash: row.text_hash,
    authored_confidence: row.authored_confidence,
    prev_chunk_id: row.prev_chunk_id,
    next_chunk_id: row.next_chunk_id
  };
}

function readBatch(db: Database.Database, offset: number, limit: number, sectionType?: string, maxRows?: number): ChunkRow[] {
  const params: Array<string | number> = [];
  const filters = ["body_text is not null", "trim(body_text) != ''"];
  if (sectionType) {
    filters.push("section_type = ?");
    params.push(sectionType);
  }
  const boundedLimit = maxRows == null ? limit : Math.min(limit, Math.max(maxRows - offset, 0));
  if (boundedLimit <= 0) {
    return [];
  }
  params.push(boundedLimit, offset);
  return db
    .prepare(
      `
        select *
        from chunks
        where ${filters.join(" and ")}
        order by cds_code, section_type, chunk_index
        limit ?
        offset ?
      `
    )
    .all(...params) as ChunkRow[];
}

async function main() {
  envRequired("CHROMA_API_KEY");
  envRequired("CHROMA_TENANT");
  envRequired("CHROMA_DATABASE");
  process.env.CHROMA_HOST = envOptional("CHROMA_HOST") ?? "api.trychroma.com";

  const ragPath = argValue("--rag-sqlite") ?? process.env.LOCAL_RAG_SQLITE ?? DEFAULT_RAG_SQLITE;
  const collectionName = argValue("--collection") ?? defaultChromaCollection();
  const batchSize = toInt(argValue("--batch-size"), 48);
  const limit = argValue("--limit") ? toInt(argValue("--limit"), 0) : undefined;
  const sectionType = argValue("--section-type");
  const reset = hasArg("--reset");

  if (!existsSync(ragPath)) {
    throw new Error(`RAG SQLite not found: ${ragPath}`);
  }

  const client = getChromaClient();
  if (reset) {
    try {
      await client.deleteCollection({ name: collectionName });
      console.log(`deleted existing Chroma collection ${collectionName}`);
    } catch {
      console.log(`no existing Chroma collection named ${collectionName}`);
    }
  }

  const collection = await client.getOrCreateCollection({
    name: collectionName,
    schema: buildLcapNarrativeSchema(client),
    metadata: {
      app: "california-lcap-intelligence",
      content_type: "lcap_narrative_chunks",
      dense_embedding: "chroma-cloud-qwen",
      sparse_embedding: "chroma-cloud-splade"
    }
  });

  const maxBatch = await client.getMaxBatchSize().catch(() => batchSize);
  const effectiveBatchSize = Math.max(1, Math.min(batchSize, maxBatch));
  const db = new Database(ragPath, { readonly: true, fileMustExist: true });
  let uploaded = 0;

  try {
    for (let offset = 0; ; offset += effectiveBatchSize) {
      const rows = readBatch(db, offset, effectiveBatchSize, sectionType, limit);
      if (!rows.length) {
        break;
      }

      await collection.upsert({
        ids: rows.map((row) => row.chunk_id),
        documents: rows.map(documentForRow),
        metadatas: rows.map(metadataForRow)
      });

      uploaded += rows.length;
      console.log(`upserted ${uploaded.toLocaleString()} chunks into ${collectionName}`);
      if (limit != null && uploaded >= limit) {
        break;
      }
    }
  } finally {
    db.close();
  }

  const count = await collection.count();
  console.log(`Chroma migration complete. Collection ${collectionName} now reports ${count.toLocaleString()} records.`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
});
