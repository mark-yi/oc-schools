# Analytics Schema

`scripts/build_analytics_tables.py` turns nested LCAP and Dashboard JSON into
CSV files plus `analytics.sqlite`. The tables are deliberately flat so they can
be queried with SQLite, DuckDB, Postgres, pandas, or spreadsheet tools.

## Key

`cds_code` is the primary join key. It is California's 14-digit county-district-
school code. For district-level records the school segment is `0000000`.

## Tables

### `districts`

Canonical district metadata from the CDE directory/download manifest.

Important columns:

- `cds_code`
- `cd_code`
- `county`
- `district`
- `doc_type`
- `status_type`
- `admin_first_name`
- `admin_last_name`
- `phone`
- `has_lcap`
- `has_dashboard`

### `lcap_documents`

One row per parsed LCAP PDF.

Important columns:

- `cds_code`
- `district`
- `parsed_district_name`
- `district_name_match`
- `school_year`
- `source_file`
- `source_path`
- `pdf_url`
- `goal_count`
- `metric_count`
- `action_count`
- `extraction_warning_count`
- `extraction_error_count`

`district_name_match = 0` means the parsed PDF district name obviously conflicts
with the manifest district. Exclude these rows from spend reports until repaired.

### `lcap_goals`

One row per extracted goal.

Important columns:

- `goal_id`
- `cds_code`
- `goal_number`
- `goal_type`
- `description`
- `source_pages`

### `lcap_actions`

One row per extracted action.

Important columns:

- `action_id`
- `goal_id`
- `cds_code`
- `goal_number`
- `action_number`
- `title`
- `description`
- `total_funds`
- `total_funds_raw`
- `contributing`
- `source_pages`

This is the main table for spend and program analysis.

### `lcap_metrics`

One row per extracted LCAP metric.

Important columns:

- `metric_id`
- `goal_id`
- `cds_code`
- `goal_number`
- `metric_number`
- `metric_name`
- `baseline_raw`
- `year_1_outcome_raw`
- `year_2_outcome_raw`
- `year_3_target_raw`
- `current_difference_from_baseline_raw`
- `source_pages`

The metric fields intentionally preserve raw text. Many districts report subgroup
values in prose/table fragments that need a later normalization pass before they
should be treated as clean quantitative facts.

### `dashboard_indicators`

One row per district-level California School Dashboard summary indicator.

Important columns:

- `cds_code`
- `indicator_id`
- `indicator_name`
- `student_group`
- `status`
- `change`
- `performance`
- `count`
- `chronic_count`
- `red`
- `orange`
- `yellow`
- `green`
- `blue`

Known indicator names:

- `chronic_absenteeism`
- `suspension_rate`
- `english_learner_progress`
- `graduation_rate`
- `college_career`
- `ela`
- `math`
- `science`

### `dashboard_student_groups`

One row per district, Dashboard indicator, and student group.

Use this table for subgroup pain, such as chronic absenteeism among homeless
students or suspension among students with disabilities.

### `dashboard_trends`

One row per district and indicator trend series, where available.

Important columns:

- `current_year`
- `one_year_ago`
- `two_years_ago`
- `three_years_ago`
- `four_years_ago`

## Example Queries

Districts where chronic absenteeism is declining:

```sql
select county, district, status, change, count, chronic_count
from dashboard_indicators
where indicator_name = 'chronic_absenteeism'
  and student_group = 'ALL'
  and change < 0
order by change asc;
```

Attendance-related LCAP actions by spend:

```sql
select district, goal_number, action_number, title, total_funds, source_pages
from lcap_actions
where lower(title || ' ' || description) like '%attendance%'
   or lower(title || ' ' || description) like '%absen%'
   or lower(title || ' ' || description) like '%truanc%'
order by total_funds desc;
```

