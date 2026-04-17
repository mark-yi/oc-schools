# MTSS, Tutoring & Targeted Intervention Deep Dive

As of March 26, 2026, this lane is still commercially interesting, but the raw score needs to be unpacked before using it for go-to-market decisions.

## Current Snapshot

Using the same broad matcher as the service-opportunity pipeline, the current `mtss_intervention` row contains:

- `29 / 29` districts
- `432` matched actions
- `8` matched metrics
- `$1,042,228,471.84` in direct matched action dollars
- `$790,506,099.46` in estimated externalizable dollars
- `15` parsed movement points, with `9` worsening (`60.0%`)
- `2` pace-scorable metrics, both negative (`100.0%`)
- `4` districts with negative pressure flags

The earlier `50% negative metric share` read is outdated. The live output on March 26, 2026 is now worse numerically, but the denominator is extremely thin, so the category still should not be read as “MTSS is broadly failing.”

## Bottom Line

This is not a clean `MTSS budget` category.

It is a blended intervention ecosystem made up of:

- explicit MTSS and RTI operating actions,
- after-school / summer / ELOP programming,
- tutoring and targeted intervention,
- progress monitoring and data tools,
- subgroup case management,
- and many broad academic or student-support actions whose descriptions mention intervention work.

The strongest commercial signal is:

**districts are funding intervention infrastructure and programming at scale, but only a small number of districts expose clean MTSS-style operational metrics that let you see whether the spend is working.**

## How The Dollars Got So Big

The raw `$1.04B` comes from broad action matching, not just actions explicitly titled `MTSS`.

### Match quality split

- `157` actions and `$222.0M` are **title matches** where the action title itself looks MTSS/intervention-related.
- `275` actions and `$820.2M` are **description-only matches** where the title is broad, but the description mentions intervention, tutoring, tiered supports, expanded learning, or progress monitoring.

That means about `78.7%` of the matched dollars are coming from description-only matches.

This is why the category is useful for identifying the broader intervention economy, but not yet precise enough to claim that districts have a clean `$1.04B` MTSS line item.

### Description-only inflation examples

These are real actions that get pulled into the MTSS bucket even though they are not clean MTSS budget lines:

- **Fullerton Joint Union High School District**: `Basic Staffing` at `$147.6M`
  - matched because the description references `intervention settings`
- **Orange Unified School District**: `Special Education Programs, Supports, Staff and Supplies` at `$87.7M`
  - matched because the description references `multi-tiered system of support`
- **Santa Ana Unified School District**: `Broad Course of Study` at `$47.1M`
  - matched because the description references `expanded learning opportunities`
- **Garden Grove Unified School District**: `STAFFING AND INSTRUCTIONAL SUPPORT: CONTRIBUTING` at `$44.1M`
  - matched because the description references `one-on-one tutoring`
- **Laguna Beach Unified School District**: `Teaching and Learning` at `$38.3M`
  - matched because the description references `after-school and summer programming`, `academic intervention`, and `Tier 3` metrics

These are not bad matches for ecosystem sizing, but they are noisy if the question is “what can a vendor realistically sell into?”

## Cleaner Read Of The Category

If you focus only on visible action labels, the recurring procurement signals are much smaller and much cleaner:

- explicit `MTSS / RTI / Tier 2 / Tier 3` titles:
  - `31` actions
  - `14` districts
  - `$64.8M`
- `expanded learning / ELOP / ASES / after-school / summer school` titles:
  - `28` actions
  - `15` districts
  - `$134.3M`
- `tutoring / intervention` titles:
  - `107` actions
  - `21` districts
  - `$67.1M`
- `progress monitoring / dashboard / screening / assessment` titles:
  - `41` actions
  - `16` districts
  - `$43.6M`

These label groups overlap, but they are closer to what a district buyer would actually recognize as a service lane.

## Why The Negative Metric Share Looks Scary

The metric side is much weaker than the action side.

The current matcher only finds `8` MTSS-related metrics across all `29` districts, and those metrics are not measuring one consistent thing.

The matched metric types are roughly:

- program utilization
- intervention delivery activity
- intervention-load / Tier 3 need
- student perception of tutoring access
- reading-growth outcome
- maintenance/compliance

That means the category is combining very different KPI types:

- **Orange USD** is tracking participation in expanded learning.
- **Laguna Beach USD** is tracking the percentage of students in Tier 3.
- **Huntington Beach Union HS** is tracking survey sentiment about tutoring access.
- **Centralia** is effectively tracking whether a program remained at 100%.

So the metric denominator is not only small, it is heterogeneous.

### Important parser caveat

One of the two pace-negative metrics is probably not a real problem signal:

- **Centralia Elementary School District**
  - baseline: `100%`
  - year 1: `100%`
  - year 3 target: `100%`
  - current parser status: `off_track`

That is clearly a maintenance metric and should be treated as neutral/on-track.

So the live pipeline currently says `2 / 2` pace-negative, but one of those two is likely a scoring artifact. Commercially, the more meaningful negative evidence is coming from movement-based metrics in Orange, Laguna, and Fullerton.

## District Deep Dives

### 1. Orange Unified School District

Why it matters:

- large blended intervention footprint: `$127.1M`
- estimated externalizable share: `$87.9M`
- one of the clearest negative MTSS-adjacent metrics in the corpus

Relevant actions:

- `Expanded Learning and Summer Enrichment` — `$9,965,531`
- `Multi-Tiered System of Support (MTSS)` — `$255,339`
- `Student Achievement Data Analysis, Progress Monitoring Software...` — `$296,838`
- `Student Intervention Resources and Support` — `$209,000`

Key metric:

- `# of students participating in Expanded Learning Sites in OUSD (CARES, ELOP, ASES)`
  - baseline: `7,900 students`
  - year 1: `3,860 students` at the mid-year point
  - year 3 target: `8,137 students`
  - current difference: `-4,040 students`
  - pipeline pace status: `moving_away`

What this likely means:

- the visible problem is not “MTSS philosophy”
- the visible problem is **utilization**
- the district appears to be paying for programming capacity, but participation is lagging the target

Commercial read:

- best wedge is `expanded_learning_activation`, not generic MTSS consulting
- likely solution angles:
  - student recruitment and seat-fill workflows
  - attendance/retention nudges for after-school and summer
  - multilingual family outreach
  - program operations dashboards tying enrollment, attendance, and subgroup participation together

### 2. Laguna Beach Unified School District

Why it matters:

- clearest intervention-load metric in the whole bucket
- visible explicit MTSS actions, plus broader intervention language inside core academic actions

Relevant actions:

- `Multi-Tiered System of Support (MTSS): Academic Interventions` — `$511,917`
- `Multi-Tiered Systems of Support (MTSS): Social-Emotional, Behavior, and Attendance Intervention Support` — `$251,981`
- `Teaching and Learning` — `$38,340,711` (broad action, but intervention-heavy language)

Key metric:

- `Students Recommended for Tier 3 Academic Interventions at End of Year`
  - baseline:
    - ELA all students `1%`
    - ELA EL `6%`
    - Math all students `1%`
  - year 1:
    - ELA all students `7%`
    - ELA EL `36%`
    - Math all students `5%`
    - Math EL `25%`
  - year 3 target:
    - ELA all students `5%`
    - ELA EL `20%`
    - Math all students `4%`
    - Math EL `15%`

What this likely means:

- the district is seeing a much larger share of students falling into Tier 3 than the baseline implied
- English learner and SWD subgroup load look especially heavy
- the problem may be a mix of:
  - late identification,
  - insufficient Tier 1/Tier 2 support quality,
  - weak intervention scheduling/capacity,
  - and poor exit criteria / progress-monitoring loops

Commercial read:

- this is a strong fit for `academic_tier3_load` and `intervention_workflow_execution`
- likely solution angles:
  - universal screener + referral workflow
  - intervention grouping and staffing logic
  - high-dosage tutoring / intervention delivery
  - dashboards showing movement into and out of Tier 3 by subgroup

### 3. Fullerton School District

Why it matters:

- high-dollar targeted intervention stack focused on high-need subgroups
- metric signal points to persistent Tier 3 SEL need

Relevant actions:

- `Academic Support and Progress Monitoring for Foster Youth, Low Income Students, and Students Experiencing Homelessness` — `$27,746,743`
- `Special Education Student Support` — `$21,178,295`

Key metric:

- `Percentage of Foster Youth and Homeless students needing Tier 3 SEL Interventions`
  - year 1:
    - Foster Youth `21%`
    - Homeless students `18.4%`
  - year 3 target:
    - Foster Youth `24.5% (Decrease approximately 1% per year)` in the extracted text
    - Homeless students `16.3% (Decrease approximately 1% per year)`

Interpretation caveat:

- the extracted baseline/target text for this metric is noisy
- but the district is clearly using MTSS language around subgroup matching, dashboards, family check-ins, RTI, mentoring, and expanded learning

What this likely means:

- the district’s intervention problem is not just academic
- it is also about `priority-group case management` and Tier 3 SEL load for foster youth and homeless students

Commercial read:

- strong fit for `subgroup_case_management` and `sel_tier3_load`
- likely solution angles:
  - student-support case management
  - family touchpoint and follow-up workflows
  - intervention history and accountability dashboards
  - support-team coordination across counseling, MTSS, and expanded learning

### 4. Santa Ana Unified School District

Why it matters:

- one of the clearest examples of a district that is philosophically committed to MTSS at scale
- large explicit action footprint, but weak MTSS-specific measurement in the current extract

Relevant actions:

- `MTSS - Student Achievement` — `$29,099,587`
- `MTSS - Wellness` — `$14,145,389`
- `Expanded Learning Opportunities` — `$53,790,996`
- `Integrated Network of Support` — `$17,485,892`

What the action text says:

- monthly data review
- COST teams
- assignment of students to Tier 2 and Tier 3 interventions
- progress monitoring
- broad support for EL, LTEL, SED, and foster youth

What is missing:

- a clean MTSS operational KPI set in the current matched output

Commercial read:

- this is not the strongest proof-of-pain district, but it is a strong proof-of-buying-category district
- best wedge is `intervention_evidence_gap`
- likely solution angles:
  - intervention operations platform
  - evidence/reporting layer
  - fidelity and outcome dashboards for school leaders and the board

### 5. Westminster School District

Why it matters:

- large explicit MTSS action
- another example of high commitment but thin measurement

Relevant actions:

- `MTSS` — `$11,006,655`
- `Differentiated Instruction` — `$15,423,481`

What the action text says:

- districtwide MTSS
- RtI academic supports
- PBIS
- Tier II / III behavior supports
- support for McKinney-Vento/homeless and foster youth
- expanded learning support for SED, EL, and FY students

What is missing:

- no clean MTSS-specific metric came through strongly in the current corpus

Commercial read:

- similar to Santa Ana, but with a smaller explicit footprint
- strongest angle is not “sell another intervention program”
- strongest angle is “help the district prove, manage, and tune the interventions it is already funding”

### 6. Fullerton Joint Union High School District

Why it matters:

- this district is the best example of **taxonomy inflation**

Relevant action:

- `Basic Staffing` — `$147,575,917`

Why it got matched:

- the description references intervention settings and support for underperforming subgroups

Commercial read:

- this is exactly the kind of record that tells you the raw `$1.04B` should not be treated as a clean TAM figure
- useful for ecosystem context
- not useful as direct evidence of district willingness to buy a new MTSS vendor product

## Direct Insights

The most important direct insights from this lane are:

1. **Expanded learning utilization is a real sub-problem**
   - Orange is the clearest example
   - districts are funding programs, but participation and retention can still lag

2. **Tier 3 load is the sharpest academic pain signal**
   - Laguna is the clearest example
   - if Tier 3 percentages are rising, districts may be identifying students late or failing to reduce need fast enough

3. **Subgroup support is a major intervention use case**
   - Fullerton shows this most clearly with foster youth and homeless students
   - case management and cross-team coordination may matter as much as tutoring content

4. **The evidence layer is weak**
   - Santa Ana and Westminster show large commitments, but not a crisp MTSS KPI architecture
   - this is a strong opportunity for tools that prove efficacy, not just tools that deliver services

5. **The current category is too broad to be a product category by itself**
   - `MTSS` is the umbrella
   - the buyable problems sit one level lower

## Two-Taxonomy v0.2

### Problem taxonomy

- `expanded_learning_activation`
  - after-school / summer / ELOP enrollment, attendance, retention, seat fill
- `academic_tier3_load`
  - percentage of students entering or remaining in Tier 3 for literacy/math
- `sel_tier3_load`
  - percentage of students needing Tier 3 SEL/behavior supports
- `intervention_workflow_execution`
  - screening, referral, grouping, assignment, service logging, exit decisions
- `subgroup_case_management`
  - foster youth, homeless, EL/LTEL, and other priority-group support coordination
- `intervention_evidence_gap`
  - inability to show whether funded interventions are working
- `delivery_capacity_gap`
  - insufficient tutoring/intervention supply relative to identified need

### Solution taxonomy

- `program_activation_ops`
  - outreach, enrollment conversion, attendance recovery, participation tracking
- `mtss_workflow_system`
  - screening-to-referral-to-assignment-to-progress-monitoring platform
- `targeted_tutoring_delivery`
  - direct tutoring / intervention delivery with dosage and fidelity tracking
- `case_management_layer`
  - support-team workflows for high-needs students and families
- `roi_and_evidence_analytics`
  - dashboards tying intervention spend to utilization, subgroup service receipt, and outcomes
- `implementation_coaching`
  - MTSS, PBIS, COST, and intervention scheduling/process coaching

## Best Initial Solution Angles

If the goal is to identify what districts might pay for next, the strongest wedges from this lane are:

1. `program_activation_ops`
   - strongest proof point: Orange
   - sell against underused expanded-learning capacity

2. `mtss_workflow_system`
   - strongest proof point: Laguna
   - sell against Tier 3 load and fragmented intervention routing

3. `case_management_layer`
   - strongest proof point: Fullerton
   - sell against subgroup complexity and Tier 3 SEL need

4. `roi_and_evidence_analytics`
   - strongest proof points: Santa Ana and Westminster
   - sell against measurement weakness in districts already committed to MTSS

## What To Fix In The Pipeline Next

1. Split `broad ecosystem dollars` from `clean product-signal dollars`.
2. Separate title matches from description-only matches in the main opportunity report.
3. Classify MTSS metrics by type:
   - utilization
   - need/load
   - delivery activity
   - perception
   - compliance
   - student outcome
4. Score maintenance metrics like `100% maintained` as neutral/on-track.
5. Build district one-pagers using the problem taxonomy rather than the umbrella MTSS bucket.
