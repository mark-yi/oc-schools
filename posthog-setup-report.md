<wizard-report>
# PostHog post-wizard report

The wizard has completed a deep integration of PostHog analytics into the California LCAP Intelligence app (Next.js App Router). Here is a summary of all changes made:

- **`instrumentation-client.ts`** (new): Initializes PostHog client-side using the `posthog-js` SDK via the Next.js 15.3+ instrumentation pattern. Configured with a reverse proxy (`/ingest`) for reliable event delivery and exception capture enabled.
- **`lib/posthog-server.ts`** (new): Singleton server-side PostHog client using `posthog-node` for tracking events from API routes.
- **`next.config.ts`** (edited): Added PostHog reverse proxy rewrites (`/ingest/static/*`, `/ingest/array/*`, `/ingest/*`) and `skipTrailingSlashRedirect: true`.
- **`app/page.tsx`** (edited): Added client-side event tracking for user interactions (search, filters, MCP copy, refresh, preset queries) and error capture on failures.
- **`app/api/opportunities/route.ts`** (edited): Added server-side tracking of opportunity API calls with result count and filter parameters.
- **`app/api/search/route.ts`** (edited): Added server-side tracking of narrative search API calls with query text and result count.
- **`.env.local`** (created): PostHog public token and host set as environment variables.

| Event | Description | File |
|-------|-------------|------|
| `opportunity_filter_changed` | User changes the outcome trend or rank-by filter | `app/page.tsx` |
| `narrative_search_submitted` | User submits a narrative semantic search | `app/page.tsx` |
| `preset_query_selected` | User clicks a preset query button | `app/page.tsx` |
| `mcp_endpoint_copied` | User copies the MCP URL to clipboard | `app/page.tsx` |
| `opportunities_refreshed` | User explicitly clicks Refresh | `app/page.tsx` |
| `opportunities_api_called` | Server: opportunities API route called | `app/api/opportunities/route.ts` |
| `narrative_search_api_called` | Server: narrative search API route called | `app/api/search/route.ts` |

## Next steps

We've built some insights and a dashboard for you to keep an eye on user behavior, based on the events we just instrumented:

- [Analytics basics dashboard](https://us.posthog.com/project/223680/dashboard/1591592)
- [Narrative searches over time](https://us.posthog.com/project/223680/insights/l9OSPCv2)
- [Opportunity filter changes](https://us.posthog.com/project/223680/insights/SuPItOet)
- [MCP endpoint copies](https://us.posthog.com/project/223680/insights/JOZJFiZo)
- [Opportunity refresh vs. search activity](https://us.posthog.com/project/223680/insights/KpPoM0Ex)
- [Preset query selections](https://us.posthog.com/project/223680/insights/iCZCjYDl)

### Agent skill

We've left an agent skill folder in your project. You can use this context for further agent development when using Claude Code. This will help ensure the model provides the most up-to-date approaches for integrating PostHog.

</wizard-report>
