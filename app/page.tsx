"use client";

import { FormEvent, useEffect, useState } from "react";
import { ArrowDownUp, Building2, Loader2, Search, Sparkles, Target } from "lucide-react";
import type { NarrativeHit, OpportunityRow } from "@/lib/types";
import { compactMoney, percent } from "@/lib/lcap-domain";

type OpportunityResponse = { rows: OpportunityRow[]; count: number; error?: string };
type SearchResponse = { hits: NarrativeHit[]; count: number; error?: string };

const presetQueries = [
  "attendance barriers family outreach chronic absenteeism re-engagement",
  "districts using data dashboards to monitor attendance interventions",
  "student engagement home visits truancy SARB attendance teams"
];

export default function Page() {
  const [opportunities, setOpportunities] = useState<OpportunityRow[]>([]);
  const [hits, setHits] = useState<NarrativeHit[]>([]);
  const [query, setQuery] = useState(presetQueries[0]);
  const [outcomeTrend, setOutcomeTrend] = useState("worsening");
  const [rankBy, setRankBy] = useState("strict_action_funds");
  const [loadingOpps, setLoadingOpps] = useState(false);
  const [loadingSearch, setLoadingSearch] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function loadOpportunities() {
    setLoadingOpps(true);
    setError(null);
    try {
      const params = new URLSearchParams({
        topic: "chronic_absenteeism",
        outcomeTrend,
        rankBy,
        limit: "12",
        actionLimit: "2"
      });
      const response = await fetch(`/api/opportunities?${params}`);
      const data = (await response.json()) as OpportunityResponse;
      if (!response.ok || data.error) {
        throw new Error(data.error ?? "Opportunity query failed.");
      }
      setOpportunities(data.rows);
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "Opportunity query failed.");
    } finally {
      setLoadingOpps(false);
    }
  }

  async function runSearch(event?: FormEvent) {
    event?.preventDefault();
    setLoadingSearch(true);
    setError(null);
    try {
      const response = await fetch("/api/search", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          query,
          limit: 10,
          candidateLimit: 180,
          groupByDistrict: true,
          perDistrict: 2
        })
      });
      const data = (await response.json()) as SearchResponse;
      if (!response.ok || data.error) {
        throw new Error(data.error ?? "Narrative search failed.");
      }
      setHits(data.hits);
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "Narrative search failed.");
    } finally {
      setLoadingSearch(false);
    }
  }

  useEffect(() => {
    void loadOpportunities();
  }, [outcomeTrend, rankBy]);

  useEffect(() => {
    void runSearch();
  }, []);

  return (
    <main className="app-shell">
      <section className="hero-band">
        <div>
          <p className="eyebrow">California LCAP Intelligence</p>
          <h1>Search public LCAP narratives and rank district opportunities.</h1>
          <p className="hero-copy">
            Built for AE workflows: deterministic Dashboard and LCAP spend joins in Neon, plus pgvector semantic
            retrieval over section-tagged narrative chunks.
          </p>
        </div>
        <div className="hero-actions">
          <button className="primary-button" type="button" onClick={loadOpportunities} disabled={loadingOpps}>
            {loadingOpps ? <Loader2 className="spin" size={18} /> : <Target size={18} />}
            Refresh
          </button>
          <a className="secondary-button" href="/api/mcp">
            <Sparkles size={18} />
            MCP
          </a>
        </div>
      </section>

      {error ? <div className="error-banner">{error}</div> : null}

      <section className="control-band">
        <div className="field">
          <label htmlFor="trend">Outcome trend</label>
          <select id="trend" value={outcomeTrend} onChange={(event) => setOutcomeTrend(event.target.value)}>
            <option value="worsening">Worsening rate</option>
            <option value="decreasing_rate">Declining rate</option>
            <option value="any">Any trend</option>
          </select>
        </div>
        <div className="field">
          <label htmlFor="rank">Rank by</label>
          <select id="rank" value={rankBy} onChange={(event) => setRankBy(event.target.value)}>
            <option value="strict_action_funds">Strict attendance $</option>
            <option value="broad_action_funds">Broad attendance $</option>
            <option value="opportunity_score">Opportunity score</option>
            <option value="affected_student_count">Affected students</option>
            <option value="current_status">Current rate</option>
          </select>
        </div>
        <form className="search-form" onSubmit={runSearch}>
          <label htmlFor="semantic">Narrative search</label>
          <div className="search-row">
            <input id="semantic" value={query} onChange={(event) => setQuery(event.target.value)} />
            <button className="icon-button" type="submit" aria-label="Search narratives" disabled={loadingSearch}>
              {loadingSearch ? <Loader2 className="spin" size={18} /> : <Search size={18} />}
            </button>
          </div>
        </form>
      </section>

      <section className="content-grid">
        <div className="panel">
          <div className="panel-heading">
            <div>
              <p className="eyebrow">Structured opportunity query</p>
              <h2>Chronic absenteeism targets</h2>
            </div>
            <ArrowDownUp size={18} />
          </div>
          <div className="result-list">
            {opportunities.map((row, index) => (
              <article className="result-card" key={row.cds_code}>
                <div className="card-topline">
                  <span className="rank">{index + 1}</span>
                  <div>
                    <h3>{row.district}</h3>
                    <p>{row.county}</p>
                  </div>
                </div>
                <div className="metric-grid">
                  <Metric label="Rate" value={percent(row.current_status)} />
                  <Metric label="Change" value={`${Number(row.outcome_change ?? 0).toFixed(1)} pts`} />
                  <Metric label="Strict $" value={compactMoney(row.strict_action_funds)} />
                  <Metric label="Broad $" value={compactMoney(row.broad_action_funds)} />
                </div>
                {row.top_actions?.[0] ? (
                  <p className="evidence-line">
                    {row.top_actions[0].title || "Untitled action"} · {compactMoney(row.top_actions[0].total_funds)}
                  </p>
                ) : null}
              </article>
            ))}
          </div>
        </div>

        <div className="panel">
          <div className="panel-heading">
            <div>
              <p className="eyebrow">Hybrid narrative retrieval</p>
              <h2>Section-cited signal</h2>
            </div>
            <Building2 size={18} />
          </div>
          <div className="preset-row">
            {presetQueries.map((item) => (
              <button key={item} type="button" onClick={() => setQuery(item)}>
                {item}
              </button>
            ))}
          </div>
          <div className="result-list">
            {hits.map((hit) => (
              <article className="result-card narrative-card" key={hit.id}>
                <div className="card-topline">
                  <span className="score">{hit.score == null ? "n/a" : hit.score.toFixed(4)}</span>
                  <div>
                    <h3>{String(hit.metadata?.district ?? "Unknown district")}</h3>
                    <p>
                      {String(hit.metadata?.section_type ?? "section")} · pages{" "}
                      {String(hit.metadata?.page_start ?? "?")}
                    </p>
                  </div>
                </div>
                <p>{hit.document}</p>
              </article>
            ))}
          </div>
        </div>
      </section>
    </main>
  );
}

function Metric({ label, value }: { label: string; value: string }) {
  return (
    <div className="metric">
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}
