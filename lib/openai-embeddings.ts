import OpenAI from "openai";
import { envOptional, envRequired, numberFromEnv } from "./env";

export const DEFAULT_EMBEDDING_MODEL = "text-embedding-3-small";
export const DEFAULT_EMBEDDING_DIMENSIONS = 512;

let cachedOpenAI: OpenAI | null = null;

export function embeddingModel(): string {
  return envOptional("OPENAI_EMBEDDING_MODEL") ?? DEFAULT_EMBEDDING_MODEL;
}

export function embeddingDimensions(): number {
  return numberFromEnv("OPENAI_EMBEDDING_DIMENSIONS", DEFAULT_EMBEDDING_DIMENSIONS);
}

export function getOpenAIClient(): OpenAI {
  if (!cachedOpenAI) {
    cachedOpenAI = new OpenAI({ apiKey: envRequired("OPENAI_API_KEY") });
  }
  return cachedOpenAI;
}

export async function embedTexts(texts: string[]): Promise<number[][]> {
  const cleaned = texts.map((text) => text.trim());
  if (cleaned.some((text) => !text)) {
    throw new Error("Cannot embed empty text.");
  }

  const response = await getOpenAIClient().embeddings.create({
    model: embeddingModel(),
    input: cleaned,
    dimensions: embeddingDimensions(),
    encoding_format: "float"
  });

  return response.data
    .sort((a, b) => a.index - b.index)
    .map((item) => item.embedding);
}

export async function embedQuery(query: string): Promise<number[]> {
  const [embedding] = await embedTexts([query]);
  return embedding;
}

export function vectorLiteral(embedding: number[]): string {
  return `[${embedding.map((value) => {
    if (!Number.isFinite(value)) {
      throw new Error("Embedding contains a non-finite value.");
    }
    return value.toFixed(8);
  }).join(",")}]`;
}
