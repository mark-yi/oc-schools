export type NarrativeMetadata = Record<string, string | number | boolean | null>;

export interface TopicAction {
  action_id: string;
  goal_id: string | null;
  goal_number: string | null;
  action_number: string | null;
  title: string | null;
  description_snippet: string;
  total_funds: number | null;
  total_funds_raw: string | null;
  contributing: number | null;
  source_pages: string | null;
  actionability: string;
  actionability_confidence: string;
  sales_read: string;
}

export interface OpportunityRow {
  cds_code: string;
  county: string | null;
  district: string | null;
  indicator_name: string;
  student_group: string;
  current_status: number | null;
  outcome_change: number | null;
  enrollment_count: number | null;
  affected_student_count: number | null;
  broad_action_count: number;
  broad_action_funds: number;
  strict_action_count: number;
  strict_action_funds: number;
  topic_goal_count: number;
  topic_metric_count: number;
  strict_share_pct: number;
  topic: string;
  outcome_trend: string;
  opportunity_score: number;
  outcome_read: string;
  top_action_scope?: "broad" | "strict";
  top_actions?: TopicAction[];
}

export interface NarrativeHit {
  id: string;
  document: string | null;
  score: number | null;
  metadata: NarrativeMetadata | null;
}

export interface SearchNarrativesInput {
  query: string;
  limit?: number;
  candidateLimit?: number;
  county?: string;
  cdsCode?: string;
  district?: string;
  schoolYear?: string;
  sectionTypes?: string[];
  groupByDistrict?: boolean;
  perDistrict?: number;
}
