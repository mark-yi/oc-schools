import dotenv from "dotenv";
import { neon, type NeonQueryFunction } from "@neondatabase/serverless";
import { embedTexts, embeddingDimensions, embeddingModel, vectorLiteral } from "../lib/openai-embeddings";
import { envOptional, envRequired } from "../lib/env";

dotenv.config({ path: ".env.local", quiet: true });
dotenv.config({ path: ".env", quiet: true });

type Sql = NeonQueryFunction<false, false>;

type RagChunkRow = {
  chunk_id: string;
  district: string | null;
  county: string | null;
  school_year: string | null;
  section_type: string | null;
  section_path: string | null;
  goal_number: string | null;
  action_number: string | null;
  page_start: number | null;
  page_end: number | null;
  body_text: string;
};

const MAX_EMBEDDING_TEXT_CHARS = 24_000;

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

async function executeBlock(sql: Sql, block: string) {
  const statements = block
    .split(";")
    .map((statement) => statement.trim())
    .filter(Boolean);
  for (const statement of statements) {
    await sql.query(statement);
  }
}

async function ensureSchema(sql: Sql) {
  await executeBlock(sql, `
    create extension if not exists vector;

    create table if not exists rag_chunk_embeddings (
      chunk_id text primary key references rag_chunks(chunk_id) on delete cascade,
      embedding halfvec(512) not null,
      embedding_model text not null,
      embedding_dimensions integer not null,
      embedded_at timestamptz not null default now()
    );

    create index if not exists idx_neon_rag_chunk_embeddings_model on rag_chunk_embeddings(embedding_model);
  `);
}

async function createVectorIndex(sql: Sql) {
  await sql.query(`
    create index if not exists idx_neon_rag_chunk_embeddings_embedding_hnsw
      on rag_chunk_embeddings
      using hnsw (embedding halfvec_cosine_ops)
      with (m = 16, ef_construction = 64)
  `);
}

function documentForRow(row: RagChunkRow): string {
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
  const document = `${context.join("\n")}\n\n${row.body_text}`.trim();
  return document.length > MAX_EMBEDDING_TEXT_CHARS
    ? `${document.slice(0, MAX_EMBEDDING_TEXT_CHARS).trimEnd()}\n[truncated for embedding]`
    : document;
}

async function readBatch(sql: Sql, batchSize: number, model: string, dimensions: number): Promise<RagChunkRow[]> {
  return (await sql.query(
    `
      select
        c.chunk_id,
        c.district,
        c.county,
        c.school_year,
        c.section_type,
        c.section_path,
        c.goal_number,
        c.action_number,
        c.page_start,
        c.page_end,
        c.body_text
      from rag_chunks c
      where c.body_text is not null
        and trim(c.body_text) != ''
        and not exists (
          select 1
          from rag_chunk_embeddings e
          where e.chunk_id = c.chunk_id
            and e.embedding_model = $1
            and e.embedding_dimensions = $2
        )
      order by c.cds_code, c.section_type, c.chunk_index
      limit $3
    `,
    [model, dimensions, batchSize]
  )) as RagChunkRow[];
}

async function upsertEmbeddings(sql: Sql, rows: RagChunkRow[], embeddings: number[][], model: string, dimensions: number) {
  const params: unknown[] = [];
  const values = rows
    .map((row, index) => {
      params.push(row.chunk_id, vectorLiteral(embeddings[index]), model, dimensions);
      const base = params.length - 3;
      return `($${base}, $${base + 1}::halfvec, $${base + 2}, $${base + 3})`;
    })
    .join(", ");

  await sql.query(
    `
      insert into rag_chunk_embeddings (
        chunk_id,
        embedding,
        embedding_model,
        embedding_dimensions
      )
      values ${values}
      on conflict (chunk_id) do update set
        embedding = excluded.embedding,
        embedding_model = excluded.embedding_model,
        embedding_dimensions = excluded.embedding_dimensions,
        embedded_at = now()
    `,
    params
  );
}

async function counts(sql: Sql, model: string, dimensions: number) {
  const [row] = (await sql.query(
    `
      select
        (select count(*)::int from rag_chunks where body_text is not null and trim(body_text) != '') chunk_count,
        (
          select count(*)::int
          from rag_chunk_embeddings
          where embedding_model = $1 and embedding_dimensions = $2
        ) embedding_count
    `,
    [model, dimensions]
  )) as Array<{ chunk_count: number; embedding_count: number }>;
  return {
    chunkCount: Number(row?.chunk_count ?? 0),
    embeddingCount: Number(row?.embedding_count ?? 0)
  };
}

async function main() {
  envRequired("OPENAI_API_KEY");
  const sql = neon(envOptional("DATABASE_URL_UNPOOLED") ?? envRequired("DATABASE_URL"));
  const model = embeddingModel();
  const dimensions = embeddingDimensions();
  const batchSize = toInt(argValue("--batch-size"), 64);
  const limit = argValue("--limit") ? toInt(argValue("--limit"), 0) : undefined;
  const rebuild = hasArg("--rebuild");
  const skipIndex = hasArg("--skip-index") || limit != null;

  if (dimensions !== 512) {
    throw new Error("This migration currently expects OPENAI_EMBEDDING_DIMENSIONS=512.");
  }

  await ensureSchema(sql);

  if (rebuild) {
    await sql.query("delete from rag_chunk_embeddings where embedding_model = $1 and embedding_dimensions = $2", [
      model,
      dimensions
    ]);
    console.log(`deleted existing ${model}/${dimensions} embeddings`);
  }

  const before = await counts(sql, model, dimensions);
  console.log(
    `starting Neon pgvector embedding migration: ${before.embeddingCount.toLocaleString()} / ${before.chunkCount.toLocaleString()} chunks embedded`
  );

  let uploaded = 0;
  for (;;) {
    if (limit != null && uploaded >= limit) {
      break;
    }
    const remainingLimit = limit == null ? batchSize : Math.min(batchSize, limit - uploaded);
    const rows = await readBatch(sql, remainingLimit, model, dimensions);
    if (!rows.length) {
      break;
    }

    const embeddings = await embedTexts(rows.map(documentForRow));
    await upsertEmbeddings(sql, rows, embeddings, model, dimensions);
    uploaded += rows.length;

    const current = before.embeddingCount + uploaded;
    console.log(`embedded ${current.toLocaleString()} / ${before.chunkCount.toLocaleString()} chunks`);
  }

  if (!skipIndex) {
    console.log("ensuring HNSW vector index");
    await createVectorIndex(sql);
  }

  const after = await counts(sql, model, dimensions);
  console.log(
    `Neon pgvector embedding migration complete: ${after.embeddingCount.toLocaleString()} / ${after.chunkCount.toLocaleString()} chunks embedded`
  );
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
});
