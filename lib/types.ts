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

export interface LcapDocumentSource {
  cds_code: string;
  county: string | null;
  district: string | null;
  parsed_district_name: string | null;
  district_name_match: number | null;
  school_year: string | null;
  source_file: string | null;
  source_path: string | null;
  pdf_url: string | null;
  goal_count: number | null;
  metric_count: number | null;
  action_count: number | null;
  extraction_warning_count: number | null;
  extraction_error_count: number | null;
}

export interface DistrictDirectoryContact {
  role: "superintendent" | "chief_business_official" | "cds_coordinator" | string;
  name: string | null;
  title: string | null;
  phone: string | null;
  email: string | null;
  source: string | null;
  fetched_at: string | null;
}

export interface DistrictDirectoryProfile {
  cds_code: string;
  county: string | null;
  district: string | null;
  district_address: string | null;
  mailing_address: string | null;
  phone: string | null;
  fax: string | null;
  email: string | null;
  website: string | null;
  status: string | null;
  district_type: string | null;
  low_grade: string | null;
  high_grade: string | null;
  nces_district_id: string | null;
  cde_detail_url: string | null;
  cde_last_updated: string | null;
  fetched_at: string | null;
  parse_status: string | null;
  parse_error: string | null;
  source: string | null;
  contacts: DistrictDirectoryContact[];
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
