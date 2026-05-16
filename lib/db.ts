import { neon, type NeonQueryFunction } from "@neondatabase/serverless";
import { envRequired } from "./env";
import {
  classifyActionability,
  normalizeText,
  normalizeTopic,
  opportunityScore,
  outcomeRead,
  outcomeTrendClause,
  topicConfig
} from "./lcap-domain";
import type { DistrictDirectoryContact, DistrictDirectoryProfile, LcapDocumentSource, OpportunityRow, TopicAction } from "./types";

type SqlClient = NeonQueryFunction<false, false>;

let cachedSql: SqlClient | null = null;

export function getSql(): SqlClient {
  if (!cachedSql) {
    cachedSql = neon(envRequired("DATABASE_URL"));
  }
  return cachedSql;
}

function likeClauses(expression: string, terms: string[], params: unknown[]): string {
  const pieces = terms.map((term) => {
    params.push(`%${term.toLowerCase()}%`);
    return `lower(${expression}) like $${params.length}`;
  });
  return `(${pieces.join(" or ")})`;
}

function pushParam(params: unknown[], value: unknown): string {
  params.push(value);
  return `$${params.length}`;
}

function rowToAction(row: Record<string, unknown>): TopicAction {
  const descriptionSnippet = normalizeText(row.description).slice(0, 420);
  return {
    action_id: String(row.action_id ?? ""),
    goal_id: (row.goal_id as string | null) ?? null,
    goal_number: (row.goal_number as string | null) ?? null,
    action_number: (row.action_number as string | null) ?? null,
    title: (row.title as string | null) ?? null,
    description_snippet: descriptionSnippet,
    total_funds: row.total_funds == null ? null : Number(row.total_funds),
    total_funds_raw: (row.total_funds_raw as string | null) ?? null,
    contributing: row.contributing == null ? null : Number(row.contributing),
    source_pages: (row.source_pages as string | null) ?? null,
    ...classifyActionability(row.title, descriptionSnippet, row.total_funds)
  };
}

function numberOrNull(value: unknown): number | null {
  if (value == null) {
    return null;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function rowToLcapDocument(row: Record<string, unknown>): LcapDocumentSource {
  return {
    cds_code: String(row.cds_code ?? ""),
    county: (row.county as string | null) ?? null,
    district: (row.district as string | null) ?? null,
    parsed_district_name: (row.parsed_district_name as string | null) ?? null,
    district_name_match: numberOrNull(row.district_name_match),
    school_year: (row.school_year as string | null) ?? null,
    source_file: (row.source_file as string | null) ?? null,
    source_path: (row.source_path as string | null) ?? null,
    pdf_url: (row.pdf_url as string | null) ?? null,
    goal_count: numberOrNull(row.goal_count),
    metric_count: numberOrNull(row.metric_count),
    action_count: numberOrNull(row.action_count),
    extraction_warning_count: numberOrNull(row.extraction_warning_count),
    extraction_error_count: numberOrNull(row.extraction_error_count)
  };
}

function rowToDirectoryContact(row: Record<string, unknown>): DistrictDirectoryContact {
  return {
    role: String(row.role ?? ""),
    name: (row.name as string | null) ?? null,
    title: (row.title as string | null) ?? null,
    phone: (row.phone as string | null) ?? null,
    email: (row.email as string | null) ?? null,
    source: (row.source as string | null) ?? null,
    fetched_at: row.fetched_at == null ? null : String(row.fetched_at)
  };
}

function rowToDirectoryProfile(
  row: Record<string, unknown>,
  contacts: DistrictDirectoryContact[] = []
): DistrictDirectoryProfile {
  return {
    cds_code: String(row.cds_code ?? ""),
    county: (row.county as string | null) ?? null,
    district: (row.district as string | null) ?? null,
    district_address: (row.district_address as string | null) ?? null,
    mailing_address: (row.mailing_address as string | null) ?? null,
    phone: (row.phone as string | null) ?? null,
    fax: (row.fax as string | null) ?? null,
    email: (row.email as string | null) ?? null,
    website: (row.website as string | null) ?? null,
    status: (row.status as string | null) ?? null,
    district_type: (row.district_type as string | null) ?? null,
    low_grade: (row.low_grade as string | null) ?? null,
    high_grade: (row.high_grade as string | null) ?? null,
    nces_district_id: (row.nces_district_id as string | null) ?? null,
    cde_detail_url: (row.cde_detail_url as string | null) ?? null,
    cde_last_updated: (row.cde_last_updated as string | null) ?? null,
    fetched_at: row.fetched_at == null ? null : String(row.fetched_at),
    parse_status: (row.parse_status as string | null) ?? null,
    parse_error: (row.parse_error as string | null) ?? null,
    source: (row.source as string | null) ?? null,
    contacts
  };
}

function rowToFallbackDirectoryProfile(row: Record<string, unknown>): DistrictDirectoryProfile {
  const adminName = [row.admin_first_name, row.admin_last_name]
    .map((part) => (part == null ? "" : String(part).trim()))
    .filter(Boolean)
    .join(" ");
  const contacts: DistrictDirectoryContact[] = adminName
    ? [
        {
          role: "superintendent",
          name: adminName,
          title: "Superintendent",
          phone: (row.phone as string | null) ?? null,
          email: null,
          source: "districts_fallback",
          fetched_at: null
        }
      ]
    : [];

  return {
    cds_code: String(row.cds_code ?? ""),
    county: (row.county as string | null) ?? null,
    district: (row.district as string | null) ?? null,
    district_address: [row.street, row.city, row.state, row.zip]
      .map((part) => (part == null ? "" : String(part).trim()))
      .filter(Boolean)
      .join(", ") || null,
    mailing_address: null,
    phone: (row.phone as string | null) ?? null,
    fax: null,
    email: null,
    website: null,
    status: (row.status_type as string | null) ?? null,
    district_type: (row.doc_type as string | null) ?? null,
    low_grade: null,
    high_grade: null,
    nces_district_id: null,
    cde_detail_url: `https://www.cde.ca.gov/schooldirectory/details?cdscode=${String(row.cds_code ?? "")}`,
    cde_last_updated: null,
    fetched_at: null,
    parse_status: "fallback",
    parse_error: "CDE directory contact table has no stored row for this district.",
    source: "districts_fallback",
    contacts
  };
}

export async function getDistrictDirectoryContacts({
  cdsCode,
  district,
  county,
  limit = 10
}: {
  cdsCode?: string;
  district?: string;
  county?: string;
  limit?: number;
}): Promise<DistrictDirectoryProfile[]> {
  if (!cdsCode && !district) {
    throw new Error("Provide cdsCode or district.");
  }

  const params: unknown[] = [];
  const filters: string[] = [];
  if (cdsCode) {
    filters.push(`p.cds_code = ${pushParam(params, cdsCode)}`);
  }
  if (district) {
    filters.push(`p.district ilike ${pushParam(params, district.replaceAll("*", "%"))}`);
  }
  if (county) {
    filters.push(`p.county = ${pushParam(params, county)}`);
  }
  const limitRef = pushParam(params, Math.max(1, Math.min(limit, 25)));

  let profiles: Record<string, unknown>[] = [];
  try {
    profiles = (await getSql().query(
      `
        select
          p.cds_code,
          p.county,
          p.district,
          p.district_address,
          p.mailing_address,
          p.phone,
          p.fax,
          p.email,
          p.website,
          p.status,
          p.district_type,
          p.low_grade,
          p.high_grade,
          p.nces_district_id,
          p.cde_detail_url,
          p.cde_last_updated,
          p.fetched_at,
          p.parse_status,
          p.parse_error,
          p.source
        from district_directory_profiles p
        where ${filters.join(" and ")}
        order by p.district
        limit ${limitRef}
      `,
      params
    )) as Record<string, unknown>[];
  } catch (error) {
    if (!(error instanceof Error) || !/district_directory_profiles/i.test(error.message)) {
      throw error;
    }
  }

  profiles = profiles.filter((row) => {
    if (row.parse_status === "ok") {
      return true;
    }
    return Boolean(row.district || row.county || row.phone || row.website);
  });

  if (!profiles.length) {
    const fallbackParams: unknown[] = [];
    const fallbackFilters: string[] = [];
    if (cdsCode) {
      fallbackFilters.push(`d.cds_code = ${pushParam(fallbackParams, cdsCode)}`);
    }
    if (district) {
      fallbackFilters.push(`d.district ilike ${pushParam(fallbackParams, district.replaceAll("*", "%"))}`);
    }
    if (county) {
      fallbackFilters.push(`d.county = ${pushParam(fallbackParams, county)}`);
    }
    const fallbackLimitRef = pushParam(fallbackParams, Math.max(1, Math.min(limit, 25)));
    const fallbackRows = (await getSql().query(
      `
        select *
        from districts d
        where ${fallbackFilters.join(" and ")}
        order by d.district
        limit ${fallbackLimitRef}
      `,
      fallbackParams
    )) as Record<string, unknown>[];
    return fallbackRows.map(rowToFallbackDirectoryProfile);
  }

  const profileIds = profiles.map((row) => String(row.cds_code ?? ""));
  const contacts = (await getSql().query(
    `
      select cds_code, role, name, title, phone, email, source, fetched_at
      from district_directory_contacts
      where cds_code = any($1::text[])
      order by
        array_position(array['superintendent', 'chief_business_official', 'cds_coordinator']::text[], role),
        role
    `,
    [profileIds]
  )) as Record<string, unknown>[];
  const contactsByCds = new Map<string, DistrictDirectoryContact[]>();
  for (const contact of contacts) {
    const key = String(contact.cds_code ?? "");
    contactsByCds.set(key, [...(contactsByCds.get(key) ?? []), rowToDirectoryContact(contact)]);
  }

  return profiles.map((row) => rowToDirectoryProfile(row, contactsByCds.get(String(row.cds_code ?? "")) ?? []));
}

export async function getLcapDocuments({
  cdsCode,
  district,
  county,
  schoolYear,
  limit = 10
}: {
  cdsCode?: string;
  district?: string;
  county?: string;
  schoolYear?: string;
  limit?: number;
}): Promise<LcapDocumentSource[]> {
  if (!cdsCode && !district) {
    throw new Error("Provide cdsCode or district.");
  }

  const params: unknown[] = [];
  const filters = ["coalesce(ld.district_name_match, 1) != 0"];
  if (cdsCode) {
    filters.push(`ld.cds_code = ${pushParam(params, cdsCode)}`);
  }
  if (district) {
    filters.push(`ld.district ilike ${pushParam(params, district.replaceAll("*", "%"))}`);
  }
  if (county) {
    filters.push(`ld.county = ${pushParam(params, county)}`);
  }
  if (schoolYear) {
    filters.push(`ld.school_year = ${pushParam(params, schoolYear)}`);
  }
  const limitRef = pushParam(params, Math.max(1, Math.min(limit, 25)));

  const rows = (await getSql().query(
    `
      select
        ld.cds_code,
        ld.county,
        ld.district,
        ld.parsed_district_name,
        ld.district_name_match,
        ld.school_year,
        ld.source_file,
        ld.source_path,
        ld.pdf_url,
        ld.goal_count,
        ld.metric_count,
        ld.action_count,
        ld.extraction_warning_count,
        ld.extraction_error_count
      from lcap_documents ld
      where ${filters.join(" and ")}
      order by
        case when ld.pdf_url is not null and ld.pdf_url != '' then 0 else 1 end,
        ld.school_year desc nulls last,
        ld.district
      limit ${limitRef}
    `,
    params
  )) as Record<string, unknown>[];

  return rows.map(rowToLcapDocument);
}

export async function fetchTopicActions({
  cdsCode,
  topic = "chronic_absenteeism",
  scope = "broad",
  limit = 5
}: {
  cdsCode: string;
  topic?: string;
  scope?: "broad" | "strict";
  limit?: number;
}): Promise<TopicAction[]> {
  const config = topicConfig(topic);
  const params: unknown[] = [cdsCode];
  const match =
    scope === "strict"
      ? likeClauses("coalesce(a.title, '')", config.strictTitleTerms, params)
      : likeClauses("coalesce(a.title, '') || ' ' || coalesce(a.description, '')", config.terms, params);
  const limitRef = pushParam(params, Math.max(1, Math.min(limit, 20)));
  const rows = (await getSql().query(
    `
      select
        a.action_id,
        a.goal_id,
        a.goal_number,
        a.action_number,
        a.title,
        a.description,
        a.total_funds,
        a.total_funds_raw,
        a.contributing,
        a.source_pages
      from lcap_actions a
      where a.cds_code = $1
        and ${match}
        and exists (
          select 1
          from lcap_documents ld
          where ld.cds_code = a.cds_code
            and coalesce(ld.district_name_match, 1) != 0
        )
      order by coalesce(a.total_funds, 0) desc
      limit ${limitRef}
    `,
    params
  )) as Record<string, unknown>[];
  return rows.map(rowToAction);
}

export async function findOpportunities({
  topic = "chronic_absenteeism",
  outcomeTrend = "worsening",
  rankBy = "strict_action_funds",
  county,
  district,
  limit = 25,
  includeActions = true,
  actionLimit = 3
}: {
  topic?: string;
  outcomeTrend?: string;
  rankBy?: string;
  county?: string;
  district?: string;
  limit?: number;
  includeActions?: boolean;
  actionLimit?: number;
}): Promise<OpportunityRow[]> {
  const normalizedTopic = normalizeTopic(topic);
  const config = topicConfig(normalizedTopic);
  const sortKey = rankBy.trim().toLowerCase().replaceAll("-", "_");
  const allowedSorts = new Set([
    "broad_action_funds",
    "strict_action_funds",
    "affected_student_count",
    "current_status",
    "outcome_change",
    "opportunity_score"
  ]);
  if (!allowedSorts.has(sortKey)) {
    throw new Error(
      "Unsupported rank_by. Use strict_action_funds, broad_action_funds, affected_student_count, current_status, outcome_change, or opportunity_score."
    );
  }

  const params: unknown[] = [config.indicatorName, config.studentGroup];
  const actionMatch = likeClauses("coalesce(a.title, '') || ' ' || coalesce(a.description, '')", config.terms, params);
  const titleMatch = likeClauses("coalesce(a.title, '')", config.strictTitleTerms, params);
  const goalMatch = likeClauses("coalesce(g.description, '')", config.terms, params);
  const metricMatch = likeClauses(
    "coalesce(m.metric_name, '') || ' ' || coalesce(m.baseline_raw, '') || ' ' || coalesce(m.year_1_outcome_raw, '') || ' ' || coalesce(m.year_2_outcome_raw, '') || ' ' || coalesce(m.year_3_target_raw, '') || ' ' || coalesce(m.current_difference_from_baseline_raw, '')",
    config.terms,
    params
  );

  const filters: string[] = [];
  if (county) {
    filters.push(`d.county = ${pushParam(params, county)}`);
  }
  if (district) {
    filters.push(`d.district ilike ${pushParam(params, district.replaceAll("*", "%"))}`);
  }
  const filterClause = filters.length ? `and ${filters.join(" and ")}` : "";
  const trendClause = outcomeTrendClause(config, outcomeTrend, "di");
  const limitValue = Math.max(1, Math.min(limit, 100));

  const rows = (await getSql().query(
    `
      with outcomes as (
        select di.*
        from dashboard_indicators di
        where di.indicator_name = $1
          and di.student_group = $2
          ${trendClause}
      ),
      broad_actions as (
        select
          a.cds_code,
          count(distinct a.action_id)::int action_count,
          round(sum(coalesce(a.total_funds, 0))) action_funds
        from lcap_actions a
        where ${actionMatch}
          and exists (
            select 1
            from lcap_documents ld
            where ld.cds_code = a.cds_code
              and coalesce(ld.district_name_match, 1) != 0
          )
        group by a.cds_code
      ),
      strict_actions as (
        select
          a.cds_code,
          count(distinct a.action_id)::int action_count,
          round(sum(coalesce(a.total_funds, 0))) action_funds
        from lcap_actions a
        where ${titleMatch}
          and exists (
            select 1
            from lcap_documents ld
            where ld.cds_code = a.cds_code
              and coalesce(ld.district_name_match, 1) != 0
          )
        group by a.cds_code
      ),
      goal_matches as (
        select g.cds_code, count(distinct g.goal_id)::int goal_count
        from lcap_goals g
        where ${goalMatch}
        group by g.cds_code
      ),
      metric_matches as (
        select m.cds_code, count(distinct m.metric_id)::int metric_count
        from lcap_metrics m
        where ${metricMatch}
        group by m.cds_code
      )
      select
        d.cds_code,
        d.county,
        d.district,
        o.indicator_name,
        o.student_group,
        o.status current_status,
        o.change outcome_change,
        o.count enrollment_count,
        o.chronic_count affected_student_count,
        coalesce(ba.action_count, 0) broad_action_count,
        coalesce(ba.action_funds, 0) broad_action_funds,
        coalesce(sa.action_count, 0) strict_action_count,
        coalesce(sa.action_funds, 0) strict_action_funds,
        coalesce(gm.goal_count, 0) topic_goal_count,
        coalesce(mm.metric_count, 0) topic_metric_count,
        case
          when coalesce(ba.action_funds, 0) > 0
          then 100.0 * coalesce(sa.action_funds, 0) / ba.action_funds
          else 0
        end strict_share_pct
      from outcomes o
      join districts d on d.cds_code = o.cds_code
      left join broad_actions ba on ba.cds_code = o.cds_code
      left join strict_actions sa on sa.cds_code = o.cds_code
      left join goal_matches gm on gm.cds_code = o.cds_code
      left join metric_matches mm on mm.cds_code = o.cds_code
      where (
        coalesce(ba.action_count, 0) > 0
        or coalesce(sa.action_count, 0) > 0
        or coalesce(gm.goal_count, 0) > 0
        or coalesce(mm.metric_count, 0) > 0
      )
      ${filterClause}
    `,
    params
  )) as OpportunityRow[];

  const actionScope: "broad" | "strict" = sortKey === "strict_action_funds" ? "strict" : "broad";
  const enriched = await Promise.all(
    rows.map(async (row) => {
      const base = {
        ...row,
        topic: normalizedTopic,
        outcome_trend: outcomeTrend,
        opportunity_score: opportunityScore(row as unknown as Record<string, unknown>),
        outcome_read: outcomeRead(normalizedTopic, row as unknown as Record<string, unknown>)
      };
      if (!includeActions) {
        return base;
      }
      return {
        ...base,
        top_action_scope: actionScope,
        top_actions: await fetchTopicActions({
          cdsCode: row.cds_code,
          topic: normalizedTopic,
          scope: actionScope,
          limit: actionLimit
        })
      };
    })
  );

  enriched.sort((a, b) => {
    if (sortKey === "opportunity_score") {
      return b.opportunity_score - a.opportunity_score || String(a.district).localeCompare(String(b.district));
    }
    if (sortKey === "outcome_change") {
      return Math.abs(Number(b.outcome_change ?? 0)) - Math.abs(Number(a.outcome_change ?? 0));
    }
    return Number((b as unknown as Record<string, unknown>)[sortKey] ?? 0) - Number((a as unknown as Record<string, unknown>)[sortKey] ?? 0);
  });

  return enriched.slice(0, limitValue);
}

export async function getDistrictContext(cdsCode: string, topic = "chronic_absenteeism") {
  const config = topicConfig(topic);
  const [district] = (await getSql().query("select * from districts where cds_code = $1 limit 1", [cdsCode])) as Record<
    string,
    unknown
  >[];
  const [dashboardOutcome] = (await getSql().query(
    `
      select *
      from dashboard_indicators
      where cds_code = $1
        and indicator_name = $2
        and student_group = $3
      limit 1
    `,
    [cdsCode, config.indicatorName, config.studentGroup]
  )) as Record<string, unknown>[];

  const goalParams: unknown[] = [cdsCode];
  const goalMatch = likeClauses("coalesce(g.description, '')", config.terms, goalParams);
  const metricParams: unknown[] = [cdsCode];
  const metricMatch = likeClauses(
    "coalesce(m.metric_name, '') || ' ' || coalesce(m.baseline_raw, '') || ' ' || coalesce(m.year_1_outcome_raw, '') || ' ' || coalesce(m.year_2_outcome_raw, '') || ' ' || coalesce(m.year_3_target_raw, '') || ' ' || coalesce(m.current_difference_from_baseline_raw, '')",
    config.terms,
    metricParams
  );

  const topicGoals = (await getSql().query(
    `
      select goal_id, goal_number, goal_type, left(description, 650) description_snippet, source_pages
      from lcap_goals g
      where cds_code = $1 and ${goalMatch}
      order by goal_number
      limit 6
    `,
    goalParams
  )) as Record<string, unknown>[];
  const topicMetrics = (await getSql().query(
    `
      select
        metric_id,
        goal_number,
        metric_number,
        metric_name,
        baseline_raw,
        year_1_outcome_raw,
        year_2_outcome_raw,
        year_3_target_raw,
        current_difference_from_baseline_raw,
        source_pages
      from lcap_metrics m
      where cds_code = $1 and ${metricMatch}
      order by goal_number, metric_number
      limit 6
    `,
    metricParams
  )) as Record<string, unknown>[];

  return {
    cds_code: cdsCode,
    topic: normalizeTopic(topic),
    district: district ?? null,
    dashboard_outcome: dashboardOutcome ?? null,
    directory_contacts: await getDistrictDirectoryContacts({ cdsCode, limit: 1 }),
    lcap_documents: await getLcapDocuments({ cdsCode, limit: 3 }),
    topic_goals: topicGoals,
    topic_metrics: topicMetrics,
    broad_topic_actions: await fetchTopicActions({ cdsCode, topic, scope: "broad", limit: 6 }),
    strict_topic_actions: await fetchTopicActions({ cdsCode, topic, scope: "strict", limit: 6 })
  };
}
