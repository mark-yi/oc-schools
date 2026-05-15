# Vercel + Neon Pgvector Deployment

This repo can now run as a small Next.js app, REST API, and MCP server on Vercel.

The deployed shape is:

```text
Next.js on Vercel
  /                         AE demo UI
  /api/opportunities        deterministic Dashboard + LCAP spend query
  /api/search               Neon pgvector narrative search
  /api/districts/[cdsCode]  account context endpoint
  /api/mcp                  MCP endpoint for Codex, Claude, Cursor, etc.

Neon Postgres
  flattened district, LCAP, Dashboard, and chunk metadata tables
  section-tagged LCAP narrative chunks
  OpenAI embeddings stored as pgvector halfvec(512)
  optional Postgres full-text RRF when NEON_ENABLE_KEYWORD_RRF=true
```

## 1. Rotate The Pasted Secrets

The Chroma and Neon credentials were pasted into a chat. Before putting this on
a public repo or public Vercel project, rotate them in Chroma Cloud and Neon.
Then use the new values below. The deployed app no longer requires Chroma for
search; Chroma variables are only needed for the legacy migration script.

Do not commit `.env.local`.

## 2. Local Environment

Create `.env.local` from `.env.example`:

```sh
cp .env.example .env.local
```

Fill these values locally:

```text
DATABASE_URL=...
DATABASE_URL_UNPOOLED=...
OPENAI_API_KEY=...
OPENAI_EMBEDDING_MODEL=text-embedding-3-small
OPENAI_EMBEDDING_DIMENSIONS=512
CHROMA_HOST=api.trychroma.com
CHROMA_API_KEY=...
CHROMA_TENANT=...
CHROMA_DATABASE=...
CHROMA_COLLECTION=lcap_narrative_chunks
DEMO_API_KEY=...
LOCAL_ANALYTICS_SQLITE=outputs/analytics/2025/analytics.sqlite
LOCAL_RAG_SQLITE=outputs/rag/2025/lcap_retrieval.sqlite
```

`DEMO_API_KEY` is optional. Leave it blank for a public browser demo, because
the client-side UI does not attach a secret. If set, REST and MCP requests must
send either:

```text
Authorization: Bearer <DEMO_API_KEY>
```

or:

```text
x-api-key: <DEMO_API_KEY>
```

## 3. Install

```sh
npm install
```

## 4. Migrate Neon

This copies the generated local SQLite outputs into Neon. By default it resets
the managed tables before loading the snapshot.

```sh
npm run db:migrate
```

Useful options:

```sh
npm run db:migrate -- --skip-chunks
npm run db:migrate -- --append
npm run db:migrate -- --batch-size 250
```

Neon stores the deterministic layer: districts, LCAP goals/actions/metrics,
Dashboard outcomes, `rag_chunks` metadata/text, and narrative embeddings. The app
uses Neon for numeric claims, account ranking, and semantic narrative search.

## 5. Embed Narratives In Neon Pgvector

Smoke test with a small embed first:

```sh
npm run neon:embed -- --limit 100
```

If that works, embed all missing chunks:

```sh
npm run neon:embed -- --batch-size 128 --skip-index
```

Useful options:

```sh
npm run neon:embed -- --batch-size 64
npm run neon:embed -- --limit 1000
npm run neon:embed -- --rebuild
```

The embedding table uses `halfvec(512)` to fit the current Vercel-managed Neon
project cap while preserving semantic retrieval quality for this corpus. The
script is resumable; it only embeds chunks missing from `rag_chunk_embeddings`.

The script can create an HNSW index if run without `--skip-index`, but on the
512 MB Neon tier the sequential `halfvec` scan is currently fast enough and
leaves more storage headroom.

## 6. Verify Cloud Data

```sh
npm run verify:cloud
```

This checks Neon row counts, runs a chronic absenteeism opportunity query, checks
the pgvector embedding count, and runs a sample narrative search.

## 7. Run Locally

```sh
npm run dev
```

Open:

```text
http://localhost:3000
```

## 8. Add Vercel Environment Variables

In the Vercel project for this repo, add these variables for Production,
Preview, and Development as needed:

```text
DATABASE_URL
DATABASE_URL_UNPOOLED
OPENAI_API_KEY
OPENAI_EMBEDDING_MODEL
OPENAI_EMBEDDING_DIMENSIONS
NEON_ENABLE_KEYWORD_RRF
DEMO_API_KEY
```

You do not need `LOCAL_ANALYTICS_SQLITE` or `LOCAL_RAG_SQLITE` on Vercel. Those
are only for local migration scripts. You also do not need Chroma variables on
Vercel unless you switch the API back to the legacy Chroma implementation.

For a public demo UI, leave `DEMO_API_KEY` unset and rely on Vercel project
visibility, domain obscurity, or Vercel Authentication/Password Protection if
you need a gate. For an MCP-only demo, set `DEMO_API_KEY` and configure the MCP
client to send it.

## 9. Deploy

Import this repo into Vercel as a Next.js project. Build settings can stay at
the defaults:

```text
Install Command: npm install
Build Command: npm run build
Output Directory: .next
```

After deploy:

```text
https://<your-vercel-domain>/
https://<your-vercel-domain>/api/mcp
```

For your existing `markyi.com` portfolio, keep this as a separate Vercel project
and add a subdomain such as:

```text
lcap.markyi.com
```

That avoids merging this data/API code into the portfolio repo. Vercel supports
multiple projects under the same account and separate custom domains per project.

## 10. MCP Client Config

For clients that support remote MCP over Streamable HTTP:

```json
{
  "lcap-intelligence": {
    "url": "https://<your-vercel-domain>/api/mcp"
  }
}
```

For stdio-only clients, use `mcp-remote`:

```json
{
  "lcap-intelligence": {
    "command": "npx",
    "args": ["-y", "mcp-remote", "https://<your-vercel-domain>/api/mcp"]
  }
}
```

If `DEMO_API_KEY` is set, configure the client to send an auth header or use a
small proxy wrapper that adds `Authorization: Bearer <DEMO_API_KEY>`.
