import { PostHog } from "posthog-node";

let posthogClient: PostHog | null = null;

export function getPostHogClient(): PostHog | null {
  const token = process.env.POSTHOG_PROJECT_API_KEY ?? process.env.NEXT_PUBLIC_POSTHOG_PROJECT_TOKEN;
  if (!token) {
    return null;
  }

  if (!posthogClient) {
    posthogClient = new PostHog(token, {
      host: process.env.POSTHOG_HOST ?? process.env.NEXT_PUBLIC_POSTHOG_HOST ?? "https://us.i.posthog.com",
      flushAt: 1,
      flushInterval: 0
    });
  }
  return posthogClient;
}
