import { createHash } from "node:crypto";
import { getPostHogClient } from "./posthog-server";

type Transport = "rest" | "mcp";
type UsageProperties = Record<string, unknown>;

const FLUSH_TIMEOUT_MS = Number(process.env.POSTHOG_FLUSH_TIMEOUT_MS ?? 750);

function hash(value: string): string {
  return createHash("sha256").update(value).digest("hex").slice(0, 16);
}

function firstHeader(request: Request, names: string[]): string | null {
  for (const name of names) {
    const value = request.headers.get(name);
    if (value) {
      return value;
    }
  }
  return null;
}

function presentedApiKey(request: Request): string | null {
  const bearer = request.headers.get("authorization")?.match(/^Bearer\s+(.+)$/i)?.[1]?.trim();
  return bearer || request.headers.get("x-api-key")?.trim() || null;
}

function distinctIdForRequest(request: Request): string {
  const apiKey = presentedApiKey(request);
  if (apiKey) {
    return `api_key:${hash(apiKey)}`;
  }
  return process.env.DEMO_API_KEY ? "missing_api_key" : "public_demo";
}

function authModeForRequest(request: Request): string {
  if (!process.env.DEMO_API_KEY) {
    return "public";
  }
  return presentedApiKey(request) ? "api_key" : "missing_api_key";
}

function requestBaseProperties(request: Request, route: string, transport: Transport): UsageProperties {
  const url = new URL(request.url);
  const ip = firstHeader(request, ["x-forwarded-for", "x-real-ip", "cf-connecting-ip"]);
  const apiKey = presentedApiKey(request);

  return {
    route,
    transport,
    http_method: request.method,
    path: url.pathname,
    environment: process.env.VERCEL_ENV ?? process.env.NODE_ENV ?? "development",
    vercel_region: process.env.VERCEL_REGION,
    deployment_url: process.env.VERCEL_URL,
    user_agent: request.headers.get("user-agent"),
    referrer: request.headers.get("referer"),
    ip_hash: ip ? hash(ip.split(",")[0]?.trim() ?? ip) : undefined,
    auth_mode: authModeForRequest(request),
    api_key_hash: apiKey ? hash(apiKey) : undefined
  };
}

function sanitizeValue(value: unknown): unknown {
  if (value == null || ["string", "number", "boolean"].includes(typeof value)) {
    return value;
  }
  if (Array.isArray(value)) {
    return value
      .filter((item) => item == null || ["string", "number", "boolean"].includes(typeof item))
      .slice(0, 25);
  }
  return String(value).slice(0, 500);
}

function sanitizeProperties(properties: UsageProperties): UsageProperties {
  return Object.fromEntries(
    Object.entries(properties)
      .filter(([, value]) => value !== undefined)
      .map(([key, value]) => [key, sanitizeValue(value)])
  );
}

function withTimeout<T>(promise: Promise<T>, timeoutMs: number): Promise<T | null> {
  return Promise.race([
    promise,
    new Promise<null>((resolve) => {
      setTimeout(() => resolve(null), timeoutMs);
    })
  ]);
}

export function queryTelemetry(query?: string | null): UsageProperties {
  const trimmed = query?.trim();
  if (!trimmed) {
    return {
      query_present: false
    };
  }
  return {
    query_present: true,
    query_hash: hash(trimmed),
    query_length: trimmed.length,
    query_word_count: trimmed.split(/\s+/).filter(Boolean).length
  };
}

export function errorTelemetry(error: unknown): UsageProperties {
  return {
    error_type: error instanceof Error ? error.name : typeof error,
    error_message: error instanceof Error ? error.message.slice(0, 300) : "Unknown error"
  };
}

export async function captureUsageEvent({
  request,
  event,
  route,
  transport,
  statusCode,
  durationMs,
  properties = {}
}: {
  request: Request;
  event: string;
  route: string;
  transport: Transport;
  statusCode: number;
  durationMs: number;
  properties?: UsageProperties;
}): Promise<void> {
  const client = getPostHogClient();
  if (!client) {
    return;
  }

  try {
    client.capture({
      distinctId: distinctIdForRequest(request),
      event,
      properties: sanitizeProperties({
        ...requestBaseProperties(request, route, transport),
        status_code: statusCode,
        duration_ms: durationMs,
        success: statusCode >= 200 && statusCode < 400,
        ...properties
      })
    });
    await withTimeout(client.flush(), FLUSH_TIMEOUT_MS);
  } catch {
    // Analytics must never break the public API or MCP server.
  }
}

export function hashPublicValue(value?: string | null): string | undefined {
  return value ? hash(value) : undefined;
}
