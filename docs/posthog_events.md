# PostHog Events

This app uses PostHog for product analytics over the public demo UI, REST API,
and MCP endpoint. Server-side event capture is best-effort: analytics failures
are swallowed so they never break LCAP API responses.

## Environment

Server routes use `POSTHOG_PROJECT_API_KEY` when present and fall back to
`NEXT_PUBLIC_POSTHOG_PROJECT_TOKEN`.

```text
POSTHOG_PROJECT_API_KEY=
POSTHOG_HOST=https://us.i.posthog.com
POSTHOG_FLUSH_TIMEOUT_MS=750
NEXT_PUBLIC_POSTHOG_PROJECT_TOKEN=
NEXT_PUBLIC_POSTHOG_HOST=https://us.i.posthog.com
```

## Server Events

- `lcap_opportunity_query`: `/api/opportunities` success.
- `lcap_narrative_search`: `/api/search` success.
- `lcap_district_context`: `/api/districts/[cdsCode]` success.
- `lcap_mcp_request`: MCP protocol calls such as `initialize` or `tools/list`.
- `lcap_mcp_tool_call`: MCP `tools/call` requests.
- `lcap_api_unauthorized`: unauthorized REST calls.
- `lcap_mcp_unauthorized`: unauthorized MCP calls.
- `lcap_api_error`: REST route validation or handler failures.
- `lcap_mcp_error`: MCP handler failures.

Common properties:

```text
route
transport
http_method
status_code
duration_ms
success
environment
user_agent
ip_hash
auth_mode
api_key_hash
```

Tool/query properties:

```text
tool_name
jsonrpc_method
topic
outcome_trend
rank_by
limit
result_count
query_hash
query_length
query_word_count
county
district_present
cds_code_present
```

Current MCP tool names include:

```text
lcap_find_opportunities
lcap_search_narratives
lcap_get_district_context
lcap_get_lcap_document
lcap_get_district_contacts
lcap_explain_account
```

Raw search query text and API key values are intentionally not sent.

## Browser Events

- `narrative_search_submitted`
- `opportunities_refreshed`
- `opportunity_filter_changed`
- `preset_query_selected`
- `mcp_endpoint_copied`
