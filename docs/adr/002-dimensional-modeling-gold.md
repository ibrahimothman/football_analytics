# ADR-002 — Use Dimensional Modeling for the Gold Analytical Layer

**Status:** Accepted
**Date:** 2026-08-18

## Context

The Silver layer contains canonical event-level football data.

Its grain is approximately:

```text
one row = one football event
```

While this representation is appropriate for detailed event analysis, repeatedly deriving match-level metrics directly from event data would make analytical queries and reporting unnecessarily complex.

Typical downstream questions include:

- How did a team perform in a match?
- How do xG, shots, passing, and xT compare across matches?
- How does performance differ between home and away matches?
- How did attacking threat evolve during different periods of a match?

These questions require different analytical grains from the event-level Silver model.

A serving model was therefore needed that exposes stable analytical entities and measures without requiring every consumer to understand the event schema and aggregation logic.

## Decision

Use a **dimensional analytical model** for the Gold layer.

The primary facts are:

```text
fact_gold_team
fact_gold_intervals
```

with match metadata providing descriptive context.

### `fact_gold_team`

Grain:

```text
one row = one team × one match
```

Contains measures such as:

- goals;
- shots;
- expected goals;
- passing metrics;
- progressive passes;
- positive xT;
- negative xT;
- net xT.

This fact supports cross-match and season-level analysis.

### `fact_gold_intervals`

Grain:

```text
one row =
one team × one match × period × time interval
```

This model supports time-based analysis such as xT momentum.

### Match Dimension

Match metadata provides descriptive attributes such as:

```text
match
date
season
competition
home team
away team
```

The analytical relationship is conceptually:

```text
             dim_match
                 │
                 │ match_id
                 ▼
          fact_gold_team


             dim_match
                 │
                 │ match_id
                 ▼
       fact_gold_intervals
```

## Rationale

### Analytical queries are measure-by-dimension

The primary consumers ask questions of the form:

```text
measure
   by
match / team / venue / time
```

For example:

```text
xG by match

shots by opponent

performance by venue

xT by match interval
```

This is a natural fit for dimensional modeling.

### Explicit grain prevents ambiguity

Each fact has a clearly defined row meaning.

For example:

```text
fact_gold_team
```

does not mix:

```text
event-level rows
match-level rows
interval-level rows
```

in the same relation.

Defining grain first makes keys, uniqueness constraints, aggregation logic, and tests easier to reason about.

### Different analytical questions require different facts

`fact_gold_team` and `fact_gold_intervals` intentionally have different grains.

Attempting to force both use cases into one table would either duplicate match-level measures across interval rows or require consumers to reconstruct interval-level information from a coarser table.

Separate facts allow each model to represent its natural analytical process.

### Centralized metric definitions

Gold models calculate metrics once.

Consumers therefore query:

```text
fact_gold_team.xg
```

rather than independently reimplementing:

```text
Silver events
→ filter shots
→ handle nulls
→ aggregate xG
```

This reduces the risk of reports and queries producing different definitions of the same metric.

## Alternatives Considered

### Query Silver Events Directly

Consumers could calculate every metric directly from:

```text
silver_events
```

**Advantages**

- no additional analytical tables;
- maximum event-level flexibility;
- fewer persisted models.

**Rejected because**

- every consumer must understand the detailed event schema;
- aggregations are repeatedly implemented;
- metric definitions can diverge;
- season-level analytical queries become unnecessarily complex;
- reporting becomes tightly coupled to Silver implementation details.

Silver remains available when event-level analysis is required.

### Single Wide Match Table

Another option was one wide table containing all match-related attributes and measures.

**Advantages**

- simple queries;
- minimal joins;
- convenient for a fixed reporting workload.

**Rejected because**

different analytical grains would be mixed.

For example, representing both:

```text
team × match
```

and:

```text
team × match × interval
```

would require either duplication or nested structures.

A wide table can still be created later as a consumer-specific mart or view if required.

### Normalized 3NF Model

A highly normalized relational model could separate football entities into many related tables.

**Advantages**

- reduced redundancy;
- strong transactional modeling;
- suitable for operational applications.

**Rejected because**

the primary workload is analytical rather than transactional.

The system predominantly asks for aggregated measures by descriptive dimensions, making a dimensional model easier for downstream analysis.

### Data Vault

Data Vault was considered conceptually as an alternative warehouse modeling approach.

**Rejected because**

the project does not require the historical source integration, auditability across many changing enterprise sources, or modeling scale that would justify the additional hubs, links, and satellites.

Its complexity would not solve a current project requirement.

## Consequences

### Positive

- analytical grain is explicit;
- common football metrics are calculated once;
- reports become simpler;
- cross-match queries become straightforward;
- data-quality rules can be applied at known grains;
- uniqueness expectations are clear;
- reconciliation between Silver and Gold becomes practical;
- downstream consumers are insulated from most event-schema complexity.

### Negative

- additional persisted analytical models must be maintained;
- some data is intentionally aggregated and therefore loses event-level detail;
- multiple facts are required for analyses at different grains;
- dimensional models can duplicate descriptive keys and derived measures;
- changes to metric definitions require rebuilding affected Gold rows.

The canonical Silver event table remains available when detailed event-level analysis is required.

## Data Quality Implications

The explicit grains allow strong model tests.

For `fact_gold_team`, uniqueness can be defined over:

```text
match_id + team_id
```

For `fact_gold_intervals`, uniqueness can be defined over:

```text
match_id
+ team_id
+ period
+ interval
```

The Gold layer is also reconciled against Silver to ensure transformations did not lose or invent analytical measures.

Examples include:

```text
Silver shots
≈ Gold shots

Silver xG
≈ Gold xG

positive_xT + negative_xT
≈ net_xT
```

This is intentionally separate from dataset-local validation such as null checks or accepted values.

## Ownership

dbt owns the Gold analytical models.

```text
Silver
   ↓
PostgreSQL source relation
   ↓
dbt
   ├── fact_gold_team
   └── fact_gold_intervals
```

Python does not maintain a second implementation of these Gold calculations.

This establishes a single owner for analytical metric definitions.

## Revisit This Decision When

The dimensional model should be reconsidered if:

- consumers primarily require raw event-level analytics rather than aggregated measures;
- new analytical processes emerge with substantially different grains;
- the platform becomes operational rather than analytical;
- the number or complexity of upstream sources introduces requirements better suited to another warehouse modeling pattern;
- downstream BI requirements justify dedicated denormalized marts.

New use cases should not automatically be forced into an existing fact.

The correct question remains:

```text
What does one row represent?
```

A new analytical grain may justify a new fact table.

## Outcome

The analytical architecture deliberately separates canonical event data from consumer-oriented facts:

```text
Silver
one row = event
        │
        ▼
       dbt
        │
        ├───────────────┐
        ▼               ▼
fact_gold_team   fact_gold_intervals
team × match     team × match ×
                 period × interval
        │               │
        └───────┬───────┘
                ▼
          Reports / SQL
```

Dimensional modeling was selected because it matches the project's principal workload: **analyzing measures across football matches, teams, and time dimensions while preserving a clear grain for every analytical model.**
