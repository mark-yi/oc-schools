import {
  CloudClient,
  FtsIndexConfig,
  GroupBy,
  K,
  Knn,
  Metadata,
  MinK,
  Rrf,
  Schema,
  Search,
  SparseVectorIndexConfig,
  StringInvertedIndexConfig,
  VectorIndexConfig,
  type Collection,
  type WhereExpression
} from "chromadb";
import {
  ChromaCloudQwenEmbeddingFunction,
  ChromaCloudQwenEmbeddingModel,
  ChromaCloudQwenEmbeddingTarget
} from "@chroma-core/chroma-cloud-qwen";
import {
  ChromaCloudSpladeEmbeddingFunction,
  ChromaCloudSpladeEmbeddingModel
} from "@chroma-core/chroma-cloud-splade";
import { defaultChromaCollection, envOptional, envRequired } from "./env";
import type { NarrativeHit, NarrativeMetadata, SearchNarrativesInput } from "./types";

export const SPARSE_EMBEDDING_KEY = "sparse_embedding";
const QWEN_TASK = "lcap-narrative-retrieval";

let cachedClient: CloudClient | null = null;
let cachedCollection: Promise<Collection> | null = null;

export function getChromaClient(): CloudClient {
  if (!cachedClient) {
    cachedClient = new CloudClient({
      host: envOptional("CHROMA_HOST") ?? "api.trychroma.com",
      apiKey: envRequired("CHROMA_API_KEY"),
      tenant: envRequired("CHROMA_TENANT"),
      database: envRequired("CHROMA_DATABASE")
    });
  }
  return cachedClient;
}

export function getDenseEmbeddingFunction(client = getChromaClient()) {
  return new ChromaCloudQwenEmbeddingFunction({
    model: ChromaCloudQwenEmbeddingModel.QWEN3_EMBEDDING_0p6B,
    task: QWEN_TASK,
    apiKeyEnvVar: "CHROMA_API_KEY",
    client,
    instructions: {
      [QWEN_TASK]: {
        [ChromaCloudQwenEmbeddingTarget.DOCUMENTS]:
          "Represent this California LCAP narrative chunk for education go-to-market retrieval. Preserve goals, actions, barriers, evidence, budgets, student outcomes, and district context.",
        [ChromaCloudQwenEmbeddingTarget.QUERY]:
          "Represent this account-executive research query for retrieving relevant California LCAP narrative evidence."
      }
    }
  });
}

export function getSparseEmbeddingFunction(client = getChromaClient()) {
  return new ChromaCloudSpladeEmbeddingFunction({
    model: ChromaCloudSpladeEmbeddingModel.SPLADE_PP_EN_V1,
    apiKeyEnvVar: "CHROMA_API_KEY",
    client
  });
}

export function buildLcapNarrativeSchema(client = getChromaClient()): Schema {
  const dense = getDenseEmbeddingFunction(client);
  const sparse = getSparseEmbeddingFunction(client);
  const schema = new Schema();

  schema.createIndex(
    new VectorIndexConfig({
      space: "cosine",
      sourceKey: K.DOCUMENT,
      embeddingFunction: dense
    })
  );
  schema.createIndex(new FtsIndexConfig(), "#document");
  schema.createIndex(
    new SparseVectorIndexConfig({
      sourceKey: K.DOCUMENT,
      embeddingFunction: sparse
    }),
    SPARSE_EMBEDDING_KEY
  );

  for (const key of [
    "cds_code",
    "district_doc_id",
    "source_document_id",
    "county",
    "district",
    "school_year",
    "section_type",
    "chunk_kind",
    "goal_number",
    "action_number"
  ]) {
    schema.createIndex(new StringInvertedIndexConfig(), key);
  }

  return schema;
}

export async function getNarrativeCollection(): Promise<Collection> {
  if (!cachedCollection) {
    const client = getChromaClient();
    const collectionName = defaultChromaCollection();
    cachedCollection = client.getCollection({ name: collectionName }).catch((error) => {
      const detail = error instanceof Error ? error.message : String(error);
      throw new Error(
        `Chroma collection "${collectionName}" is not ready. Run "npm run chroma:migrate -- --reset" after setting Chroma env vars. ${detail}`
      );
    });
  }
  return cachedCollection;
}

function combineWhere(filters: Array<WhereExpression | undefined>): WhereExpression | undefined {
  return filters.filter(Boolean).reduce<WhereExpression | undefined>((combined, filter) => {
    if (!filter) {
      return combined;
    }
    return combined ? combined.and(filter) : filter;
  }, undefined);
}

function whereFromInput(input: SearchNarrativesInput): WhereExpression | undefined {
  return combineWhere([
    input.cdsCode ? K("cds_code").eq(input.cdsCode) : undefined,
    input.county ? K("county").eq(input.county) : undefined,
    input.schoolYear ? K("school_year").eq(input.schoolYear) : undefined,
    input.district ? K("district").eq(input.district) : undefined,
    input.sectionTypes?.length ? K("section_type").isIn(input.sectionTypes) : undefined
  ]);
}

function scalarMetadata(metadata: Metadata | null | undefined): NarrativeMetadata | null {
  if (!metadata) {
    return null;
  }
  const output: NarrativeMetadata = {};
  for (const [key, value] of Object.entries(metadata)) {
    if (typeof value === "string" || typeof value === "number" || typeof value === "boolean" || value === null) {
      output[key] = value;
    }
  }
  return output;
}

export async function searchNarratives(input: SearchNarrativesInput): Promise<NarrativeHit[]> {
  const query = input.query.trim();
  if (!query) {
    return [];
  }

  const limit = Math.max(1, Math.min(input.limit ?? 10, 50));
  const candidateLimit = Math.max(limit, Math.min(input.candidateLimit ?? 180, 500));
  const collection = await getNarrativeCollection();
  const where = whereFromInput(input);
  const denseRank = Knn({ query, limit: candidateLimit, returnRank: true });
  const sparseRank = Knn({
    query,
    key: SPARSE_EMBEDDING_KEY,
    limit: candidateLimit,
    returnRank: true
  });

  let search = new Search()
    .rank(
      Rrf({
        ranks: [denseRank, sparseRank],
        weights: [0.58, 0.42],
        k: 60
      })
    )
    .limit(limit)
    .select(
      K.DOCUMENT,
      K.SCORE,
      "cds_code",
      "county",
      "district",
      "district_doc_id",
      "source_document_id",
      "school_year",
      "section_type",
      "section_path",
      "chunk_kind",
      "chunk_index",
      "goal_number",
      "action_number",
      "page_start",
      "page_end",
      "source_path"
    );

  if (where) {
    search = search.where(where);
  }

  if (input.groupByDistrict) {
    search = search.groupBy(
      new GroupBy([K("district_doc_id")], new MinK([K.SCORE], Math.max(1, Math.min(input.perDistrict ?? 2, 5))))
    );
  }

  const results = await collection.search(search);
  return results.rows()[0].map((row) => ({
    id: row.id,
    document: row.document ?? null,
    score: row.score ?? null,
    metadata: scalarMetadata(row.metadata)
  }));
}

export async function collectionStats() {
  const collection = await getNarrativeCollection();
  return {
    collection: collection.name,
    count: await collection.count()
  };
}

export function metadataString(metadata: Metadata | null | undefined, key: string): string | null {
  const value = metadata?.[key];
  if (typeof value === "string") {
    return value;
  }
  if (typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }
  return null;
}
