# Architecture Decision Records

[**README**](../../README.md) ·
[**Architecture**](../architecture.md) ·
[**Architecture Decisions**](README.md)

---

Architecture Decision Records (ADRs) document significant technical
decisions made during the development of the platform, including the
context, alternatives considered, trade-offs, and conditions under
which a decision should be revisited.

<details>
<summary><strong>ADR-001 — Use One Match as the Atomic Processing Grain</strong></summary>

### Status

Accepted

### Decision

Process each football match as an independent unit of work while
using a separate controller DAG to discover and dispatch matches.

### Why

This provides:

- failure isolation;
- safe retries;
- selective reprocessing;
- parallel execution;
- simple backfill semantics.

Processing grain is intentionally kept separate from physical storage
partitioning.

**[Read the full ADR →](001-match-processing-grain.md)**

</details>

<details>
<summary><strong>ADR-002 — Use Dimensional Modeling for the Gold Analytical Layer</strong></summary>

### Status

Accepted

### Decision

Model analytical outputs using explicit fact grains:

- `fact_gold_team`: one team × match;
- `fact_gold_intervals`: one team × match × period × interval.

### Why

The primary analytical workload consists of measures analyzed across
matches, teams, opponents, venues, and time.

Explicit fact grains also make uniqueness, reconciliation, and
downstream querying easier to reason about.

**[Read the full ADR →](002-dimensional-modeling-gold.md)**

</details>

<details>
<summary><strong>ADR-003 — Use Python for Silver and dbt for Gold</strong></summary>

### Status

Accepted

### Decision

Use Python for source-specific and football-domain transformations
through Silver, and use dbt for analytical Gold transformations.

### Why

Silver contains provider-specific parsing and domain logic, while Gold
is dominated by relational aggregation and analytical SQL.

This also establishes one authoritative owner for each transformation
and avoids maintaining Python and dbt versions of the same metrics.

**[Read the full ADR →](003-python-silver-dbt-gold.md)**

</details>

<details>
<summary><strong>ADR-004 — Use Apache Iceberg for Canonical Silver Storage</strong></summary>

### Status

Accepted

### Decision

Persist canonical Silver events as:

`football.silver_events`

using Apache Iceberg.

### Why

Iceberg provides table-level capabilities that plain Parquet files do
not provide by themselves:

- snapshots;
- time travel;
- schema evolution;
- partition evolution;
- managed file membership;
- reproducible table versions.

Raw and Bronze remain file-based because they do not currently require
these capabilities.

**[Read the full ADR →](004-iceberg-silver-storage.md)**

</details>

<details>
<summary><strong>ADR-005 — Use PostgreSQL as the Serving and dbt Bridge Layer</strong></summary>

### Status

Accepted

### Decision

Load the required Silver data from Iceberg into PostgreSQL before
running `dbt-postgres`.

### Why

This preserves the existing dbt architecture and avoids introducing
Spark, Trino, or another Iceberg query engine solely to remove one
integration boundary.

Iceberg remains the canonical Silver store; PostgreSQL contains the
SQL-accessible projection used by dbt and downstream consumers.

**[Read the full ADR →](005-postgres-serving-bridge.md)**

</details>

<details>
<summary><strong>ADR-006 — Separate Operational Observability, Data Observability, Lineage, and Metadata</strong></summary>

### Status

Accepted

### Decision

Treat operational health, data health, lineage, and metadata/catalog
management as separate architectural concerns.

### Why

They answer different questions:

- **Operational observability:** Is the machinery working?
- **Data observability:** Is the data healthy?
- **Lineage:** Where did the data come from and what depends on it?
- **Metadata/catalog:** What data assets exist and what do they mean?

Tools may overlap, but the responsibilities should remain conceptually
distinct.

**[Read the full ADR →](006-observability-lineage-metadata.md)**

</details>
