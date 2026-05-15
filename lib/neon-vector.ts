import { embedQuery, vectorLiteral } from "./openai-embeddings";
import { getSql } from "./db";
import { envOptional } from "./env";
import type { NarrativeHit, NarrativeMetadata, SearchNarrativesInput } from "./types";

function pushParam(params: unknown[], value: unknown): string {
  params.push(value);
  return `$${params.length}`;
}

function filterClause(input: SearchNarrativesInput, params: unknown[]): string {
  const filters: string[] = [];
  if (input.cdsCode) {
    filters.push(`c.cds_code = ${pushParam(params, input.cdsCode)}`);
  }
  if (input.county) {
    filters.push(`c.county = ${pushParam(params, input.county)}`);
  }
  if (input.schoolYear) {
    filters.push(`c.school_year = ${pushParam(params, input.schoolYear)}`);
  }
  if (input.district) {
    filters.push(`c.district ilike ${pushParam(params, input.district.replaceAll("*", "%"))}`);
  }
  if (input.sectionTypes?.length) {
    filters.push(`c.section_type = any(${pushParam(params, input.sectionTypes)}::text[])`);
  }
  return filters.length ? filters.join(" and ") : "true";
}

function numberOrNull(value: unknown): number | null {
  if (value == null) {
    return null;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function metadataFromRow(row: Record<string, unknown>): NarrativeMetadata {
  return {
    cds_code: String(row.cds_code ?? ""),
    county: (row.county as string | null) ?? null,
    district: (row.district as string | null) ?? null,
    district_doc_id: (row.district_doc_id as string | null) ?? null,
    source_document_id: (row.source_document_id as string | null) ?? null,
    school_year: (row.school_year as string | null) ?? null,
    section_type: (row.section_type as string | null) ?? null,
    section_path: (row.section_path as string | null) ?? null,
    chunk_kind: (row.chunk_kind as string | null) ?? null,
    chunk_index: numberOrNull(row.chunk_index),
    goal_number: (row.goal_number as string | null) ?? null,
    action_number: (row.action_number as string | null) ?? null,
    page_start: numberOrNull(row.page_start),
    page_end: numberOrNull(row.page_end),
    source_path: (row.source_path as string | null) ?? null
  };
}

function keywordRrfEnabled(): boolean {
  return ["1", "true", "yes"].includes((envOptional("NEON_ENABLE_KEYWORD_RRF") ?? "").toLowerCase());
}

export async function searchNarratives(input: SearchNarrativesInput): Promise<NarrativeHit[]> {
  const query = input.query.trim();
  if (!query) {
    return [];
  }

  const limit = Math.max(1, Math.min(input.limit ?? 10, 50));
  const candidateLimit = Math.max(limit, Math.min(input.candidateLimit ?? 180, 500));
  const vector = vectorLiteral(await embedQuery(query));
  const params: unknown[] = [vector, query];
  const where = filterClause(input, params);
  const candidateRef = pushParam(params, candidateLimit);
  const finalLimitRef = pushParam(params, limit);
  const groupFilter = input.groupByDistrict
    ? `where district_rank <= ${pushParam(params, Math.max(1, Math.min(input.perDistrict ?? 2, 5)))}`
    : "";
  const keywordRrf = keywordRrfEnabled();
  const keywordMatchCte = keywordRrf
    ? `,
      keyword_matches as (
        select
          c.chunk_id,
          row_number() over (
            order by ts_rank_cd(
              to_tsvector('english', coalesce(c.search_text, '') || ' ' || coalesce(c.body_text, '')),
              websearch_to_tsquery('english', $2)
            ) desc
          ) rank
        from rag_chunks c
        where to_tsvector('english', coalesce(c.search_text, '') || ' ' || coalesce(c.body_text, ''))
          @@ websearch_to_tsquery('english', $2)
          and ${where}
        order by ts_rank_cd(
          to_tsvector('english', coalesce(c.search_text, '') || ' ' || coalesce(c.body_text, '')),
          websearch_to_tsquery('english', $2)
        ) desc
        limit ${candidateRef}
      )`
    : "";
  const combinedKeyword = keywordRrf
    ? "union all select chunk_id, 0.42::double precision / (60 + rank) score from keyword_matches"
    : "";
  const vectorWeight = keywordRrf ? "0.58" : "1.0";

  const rows = (await getSql().query(
    `
      with vector_matches as (
        select
          c.chunk_id,
          row_number() over (order by e.embedding <=> $1::halfvec) rank
        from rag_chunk_embeddings e
        join rag_chunks c on c.chunk_id = e.chunk_id
        where ${where}
          and $2::text is not null
        order by e.embedding <=> $1::halfvec
        limit ${candidateRef}
      )
      ${keywordMatchCte},
      combined as (
        select chunk_id, ${vectorWeight}::double precision / (60 + rank) score from vector_matches
        ${combinedKeyword}
      ),
      ranked as (
        select chunk_id, sum(score) score
        from combined
        group by chunk_id
      ),
      joined as (
        select
          c.chunk_id,
          c.body_text,
          c.cds_code,
          c.county,
          c.district,
          c.district_doc_id,
          c.source_document_id,
          c.school_year,
          c.section_type,
          c.section_path,
          c.chunk_kind,
          c.chunk_index,
          c.goal_number,
          c.action_number,
          c.page_start,
          c.page_end,
          c.source_path,
          r.score,
          row_number() over (partition by c.district_doc_id order by r.score desc) district_rank
        from ranked r
        join rag_chunks c on c.chunk_id = r.chunk_id
      )
      select *
      from joined
      ${groupFilter}
      order by score desc
      limit ${finalLimitRef}
    `,
    params
  )) as Record<string, unknown>[];

  return rows.map((row) => ({
    id: String(row.chunk_id ?? ""),
    document: (row.body_text as string | null) ?? null,
    score: numberOrNull(row.score),
    metadata: metadataFromRow(row)
  }));
}

export async function collectionStats() {
  const [row] = (await getSql().query(
    `
      select
        (select count(*)::int from rag_chunks) chunk_count,
        (select count(*)::int from rag_chunk_embeddings) embedding_count
    `
  )) as Array<{ chunk_count: number; embedding_count: number }>;

  return {
    collection: "neon_pgvector",
    count: Number(row?.embedding_count ?? 0),
    chunk_count: Number(row?.chunk_count ?? 0)
  };
}
