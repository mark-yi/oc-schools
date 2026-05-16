import Database from "better-sqlite3";
import dotenv from "dotenv";
import { neon, type NeonQueryFunction } from "@neondatabase/serverless";
import { existsSync } from "node:fs";
import { basename } from "node:path";

dotenv.config({ path: ".env.local", quiet: true });
dotenv.config({ path: ".env", quiet: true });

type Sql = NeonQueryFunction<false, false>;
type SqliteRow = Record<string, string | number | null>;

const ANALYTICS_TABLES = [
  "districts",
  "lcap_documents",
  "lcap_goals",
  "lcap_actions",
  "lcap_metrics",
  "dashboard_indicators",
  "dashboard_student_groups",
  "dashboard_trends"
] as const;

const DEFAULT_ANALYTICS_SQLITE = "outputs/analytics/2025/analytics.sqlite";
const DEFAULT_RAG_SQLITE = "outputs/rag/2025/lcap_retrieval.sqlite";

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

function requiredEnv(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Missing ${name}. Add it to .env.local or export it before running this script.`);
  }
  return value;
}

function sqlId(identifier: string): string {
  if (!/^[a-z_][a-z0-9_]*$/i.test(identifier)) {
    throw new Error(`Unsafe SQL identifier: ${identifier}`);
  }
  return `"${identifier}"`;
}

async function createSchema(sql: Sql) {
  await executeBlock(sql, `
    create extension if not exists vector;

    create table if not exists districts (
      cds_code text,
      cd_code text,
      county text,
      district text,
      doc text,
      doc_type text,
      status_type text,
      street text,
      city text,
      zip text,
      state text,
      phone text,
      admin_first_name text,
      admin_last_name text,
      latitude text,
      longitude text,
      has_lcap integer,
      has_dashboard integer
    );

    create table if not exists lcap_documents (
      cds_code text,
      cd_code text,
      county text,
      district text,
      parsed_district_name text,
      district_name_match integer,
      school_year text,
      source_file text,
      source_path text,
      pdf_url text,
      goal_count integer,
      metric_count integer,
      action_count integer,
      extraction_warning_count integer,
      extraction_error_count integer,
      extraction_warnings text,
      extraction_errors text
    );

    create table if not exists lcap_goals (
      goal_id text,
      cds_code text,
      county text,
      district text,
      school_year text,
      goal_number text,
      goal_type text,
      description text,
      source_pages text
    );

    create table if not exists lcap_actions (
      action_id text,
      goal_id text,
      cds_code text,
      county text,
      district text,
      school_year text,
      goal_number text,
      action_number text,
      title text,
      description text,
      total_funds double precision,
      total_funds_raw text,
      contributing integer,
      contributing_raw text,
      source_pages text
    );

    create table if not exists lcap_metrics (
      metric_id text,
      goal_id text,
      cds_code text,
      county text,
      district text,
      school_year text,
      goal_number text,
      metric_number text,
      metric_name text,
      baseline_raw text,
      year_1_outcome_raw text,
      year_2_outcome_raw text,
      year_3_target_raw text,
      current_difference_from_baseline_raw text,
      source_pages text
    );

    create table if not exists dashboard_indicators (
      cds_code text,
      county text,
      district text,
      school_year_id integer,
      indicator_id text,
      indicator_name text,
      student_group text,
      status double precision,
      change double precision,
      status_id integer,
      change_id integer,
      performance integer,
      count integer,
      chronic_count integer,
      red integer,
      orange integer,
      yellow integer,
      green integer,
      blue integer,
      is_private_data integer
    );

    create table if not exists dashboard_student_groups (
      cds_code text,
      county text,
      district text,
      school_year_id integer,
      indicator_id text,
      indicator_name text,
      student_group text,
      status double precision,
      change double precision,
      status_id integer,
      change_id integer,
      performance integer,
      count integer,
      chronic_count integer,
      red integer,
      orange integer,
      yellow integer,
      green integer,
      blue integer,
      is_private_data integer
    );

    create table if not exists dashboard_trends (
      cds_code text,
      county text,
      district text,
      school_year_id integer,
      indicator_id text,
      indicator_name text,
      grade text,
      current_year double precision,
      one_year_ago double precision,
      two_years_ago double precision,
      three_years_ago double precision,
      four_years_ago double precision
    );

    create table if not exists district_directory_profiles (
      cds_code text primary key,
      county text,
      district text,
      district_address text,
      mailing_address text,
      phone text,
      fax text,
      email text,
      website text,
      status text,
      district_type text,
      low_grade text,
      high_grade text,
      nces_district_id text,
      cde_detail_url text,
      cde_last_updated text,
      fetched_at timestamptz not null default now(),
      parse_status text,
      parse_error text,
      source text
    );

    create table if not exists district_directory_contacts (
      cds_code text not null references district_directory_profiles(cds_code) on delete cascade,
      role text not null,
      name text,
      title text,
      phone text,
      email text,
      source text,
      fetched_at timestamptz not null default now(),
      primary key (cds_code, role)
    );

    create table if not exists waitlist_signups (
      email text primary key,
      source text,
      created_at timestamptz not null default now(),
      updated_at timestamptz not null default now()
    );

    create table if not exists rag_chunks (
      chunk_id text primary key,
      section_id text,
      cds_code text,
      county text,
      district text,
      school_year text,
      source_path text,
      pdf_url text,
      source_document_id text,
      district_doc_id text,
      page_start integer,
      page_end integer,
      section_type text,
      section_path text,
      prompt_label text,
      goal_number text,
      action_number text,
      chunk_kind text,
      chunk_index integer,
      token_count integer,
      text_hash text,
      authored_confidence double precision,
      body_text text,
      search_text text,
      prev_chunk_id text,
      next_chunk_id text
    );

    create table if not exists rag_chunk_embeddings (
      chunk_id text primary key references rag_chunks(chunk_id) on delete cascade,
      embedding halfvec(512) not null,
      embedding_model text not null,
      embedding_dimensions integer not null,
      embedded_at timestamptz not null default now()
    );
  `);

  await executeBlock(sql, `
    create index if not exists idx_neon_districts_cds on districts(cds_code);
    create index if not exists idx_neon_lcap_documents_cds on lcap_documents(cds_code);
    create index if not exists idx_neon_lcap_actions_cds on lcap_actions(cds_code);
    create index if not exists idx_neon_lcap_actions_funds on lcap_actions(total_funds);
    create index if not exists idx_neon_lcap_goals_cds on lcap_goals(cds_code);
    create index if not exists idx_neon_lcap_metrics_cds on lcap_metrics(cds_code);
    create index if not exists idx_neon_dashboard_indicators_lookup
      on dashboard_indicators(indicator_name, student_group, change);
    create index if not exists idx_neon_dashboard_student_groups_lookup
      on dashboard_student_groups(indicator_name, student_group, performance);
    create index if not exists idx_neon_dashboard_trends_lookup on dashboard_trends(indicator_name, cds_code);
    create index if not exists idx_neon_directory_profiles_district on district_directory_profiles(district);
    create index if not exists idx_neon_directory_profiles_county on district_directory_profiles(county);
    create index if not exists idx_neon_directory_contacts_role on district_directory_contacts(role);
    create index if not exists idx_waitlist_signups_created_at on waitlist_signups(created_at);
    create index if not exists idx_neon_rag_chunks_cds on rag_chunks(cds_code);
    create index if not exists idx_neon_rag_chunks_section on rag_chunks(section_type);
    create index if not exists idx_neon_rag_chunks_district_doc on rag_chunks(district_doc_id);
    create index if not exists idx_neon_rag_chunk_embeddings_model on rag_chunk_embeddings(embedding_model);
  `);
}

async function truncateManagedTables(sql: Sql) {
  await sql.query(`
    truncate table
      rag_chunk_embeddings,
      rag_chunks,
      dashboard_trends,
      dashboard_student_groups,
      dashboard_indicators,
      lcap_metrics,
      lcap_actions,
      lcap_goals,
      lcap_documents,
      districts;
  `);
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

async function insertRows(sql: Sql, table: string, columns: string[], rows: SqliteRow[], batchSize: number) {
  if (!rows.length) {
    return;
  }

  for (let offset = 0; offset < rows.length; offset += batchSize) {
    const batch = rows.slice(offset, offset + batchSize);
    const params: unknown[] = [];
    const values = batch
      .map((row) => {
        const placeholders = columns.map((column) => {
          params.push(row[column] ?? null);
          return `$${params.length}`;
        });
        return `(${placeholders.join(", ")})`;
      })
      .join(", ");
    await sql.query(
      `insert into ${sqlId(table)} (${columns.map(sqlId).join(", ")}) values ${values}`,
      params
    );
  }
}

function sourceDocumentId(row: SqliteRow): string {
  const cds = String(row.cds_code ?? "unknown");
  const source = String(row.source_path ?? row.pdf_url ?? "lcap");
  return `${cds}:${basename(source)}`;
}

function loadRows(db: Database.Database, table: string): SqliteRow[] {
  return db.prepare(`select * from ${sqlId(table)}`).all() as SqliteRow[];
}

async function copyAnalytics(sql: Sql, analyticsPath: string, batchSize: number) {
  const db = new Database(analyticsPath, { readonly: true, fileMustExist: true });
  try {
    for (const table of ANALYTICS_TABLES) {
      const rows = loadRows(db, table);
      const columns = rows.length
        ? Object.keys(rows[0])
        : ((db.prepare(`pragma table_info(${sqlId(table)})`).all() as Array<{ name: string }>).map((row) => row.name));
      await insertRows(sql, table, columns, rows, batchSize);
      console.log(`copied ${rows.length.toLocaleString()} rows into ${table}`);
    }
  } finally {
    db.close();
  }
}

async function copyChunks(sql: Sql, ragPath: string, batchSize: number) {
  const db = new Database(ragPath, { readonly: true, fileMustExist: true });
  try {
    const rows = (db
      .prepare(
        `
          select
            chunk_id,
            section_id,
            cds_code,
            county,
            district,
            school_year,
            source_path,
            pdf_url,
            page_start,
            page_end,
            section_type,
            section_path,
            prompt_label,
            goal_number,
            action_number,
            chunk_kind,
            chunk_index,
            token_count,
            text_hash,
            authored_confidence,
            body_text,
            search_text,
            prev_chunk_id,
            next_chunk_id
          from chunks
          where body_text is not null and trim(body_text) != ''
          order by cds_code, section_type, chunk_index
        `
      )
      .all() as SqliteRow[]).map((row) => ({
      ...row,
      source_document_id: sourceDocumentId(row),
      district_doc_id: row.cds_code
    }));

    const columns = [
      "chunk_id",
      "section_id",
      "cds_code",
      "county",
      "district",
      "school_year",
      "source_path",
      "pdf_url",
      "source_document_id",
      "district_doc_id",
      "page_start",
      "page_end",
      "section_type",
      "section_path",
      "prompt_label",
      "goal_number",
      "action_number",
      "chunk_kind",
      "chunk_index",
      "token_count",
      "text_hash",
      "authored_confidence",
      "body_text",
      "search_text",
      "prev_chunk_id",
      "next_chunk_id"
    ];
    await insertRows(sql, "rag_chunks", columns, rows, batchSize);
    console.log(`copied ${rows.length.toLocaleString()} rows into rag_chunks`);
  } finally {
    db.close();
  }
}

async function main() {
  const analyticsPath =
    argValue("--analytics-sqlite") ?? process.env.LOCAL_ANALYTICS_SQLITE ?? DEFAULT_ANALYTICS_SQLITE;
  const ragPath = argValue("--rag-sqlite") ?? process.env.LOCAL_RAG_SQLITE ?? DEFAULT_RAG_SQLITE;
  const batchSize = Number(argValue("--batch-size") ?? 400);
  const append = hasArg("--append");
  const skipChunks = hasArg("--skip-chunks");

  if (!existsSync(analyticsPath)) {
    throw new Error(`Analytics SQLite not found: ${analyticsPath}`);
  }
  if (!skipChunks && !existsSync(ragPath)) {
    throw new Error(`RAG SQLite not found: ${ragPath}`);
  }

  const sql = neon(requiredEnv("DATABASE_URL"));
  await createSchema(sql);
  if (!append) {
    await truncateManagedTables(sql);
    console.log("reset managed Neon tables");
  }
  await copyAnalytics(sql, analyticsPath, batchSize);
  if (!skipChunks) {
    await copyChunks(sql, ragPath, Math.min(batchSize, 250));
  }
  console.log("Neon migration complete");
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
});
