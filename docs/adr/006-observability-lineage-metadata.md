# ADR-006 — Separate Operational Observability, Data Observability, Lineage, and Metadata Concerns

**Status:** Accepted
**Date:** 2026-08-18

## Context

As the pipeline matured, several different monitoring and metadata concerns emerged:

- task failures and retries;
- stage duration;
- row-volume changes;
- data-quality failures;
- cross-layer reconciliation;
- lineage between jobs and datasets;
- metadata describing tables and models.

These concerns are related, but they answer different questions.

Without an explicit separation, it is easy to treat all of them as one generic "observability" problem and either duplicate functionality or select tools that solve the wrong concern.

The project therefore needed a clear model for distinguishing:

- operational observability;
- data observability;
- lineage;
- catalog and metadata management.

## Decision

Treat these as **separate architectural concerns with different responsibilities**, while allowing tools to overlap where appropriate.

The conceptual model is:

```text id="zjg5z2"
                    DATA PLATFORM
                         │
                         ▼
       Airflow → Iceberg → Postgres → dbt
          │          │         │        │
          │          │         │        │
          ▼          ▼         ▼        ▼

   Operational     Data      Lineage   Metadata
  observability observability

```

The project does not require one tool per concern.

The architectural distinction is more important than the number of deployed products.

## Operational Observability

Operational observability answers:

> Is the pipeline infrastructure and execution machinery working correctly?

Typical signals include:

- task success/failure;
- retries;
- DAG duration;
- task duration;
- queue or scheduling delays;
- resource pressure;
- scheduler or worker health;
- execution throughput.

In the current project, operational metadata is captured through Airflow-related execution records and exposed through DuckDB views such as:

```text id="svh0ul"
observability.stage_health
observability.recent_stage_health
observability.retry_summary
```

These provide visibility into execution behavior such as:

```text id="qyltx7"
stage failure
retry occurred
stage unusually slow
```

At larger scale, this responsibility would typically move toward a dedicated telemetry architecture.

Conceptually:

```text id="8eu01a"
Airflow
   ↓
metrics / logs / traces
   ↓
Prometheus-compatible backend
   ↓
Grafana
```

The current DuckDB implementation is therefore a lightweight local implementation of the same underlying concern.

## Data Observability

Data observability answers:

> The pipeline ran, but is the data itself healthy?

Examples include:

- unexpected row-count changes;
- freshness problems;
- schema changes;
- null-rate changes;
- value-distribution shifts;
- broken business invariants;
- reconciliation failures.

The project currently addresses data health through multiple mechanisms.

### dbt Tests

Examples:

```text id="of8si7"
not_null
unique
relationships
accepted values
custom business-rule tests
```

### Model Contracts

Used to enforce expected analytical schemas and data types.

### Cross-Layer Reconciliation

Used to verify that transformations have not lost or invented analytical values.

Examples include:

```text id="2yw5gx"
Silver shots ≈ Gold shots
Silver xG ≈ Gold xG
positive_xT + negative_xT ≈ net_xT
```

### Volume Monitoring

DuckDB observability views calculate rolling historical baselines and classify significant changes.

Examples:

```text id="mzpt6l"
HIGH_VOLUME
LOW_VOLUME
NORMAL
INSUFFICIENT_HISTORY
```

This provides a lightweight data-observability capability.

At larger scale, a specialized data-observability platform such as Elementary or a comparable product could replace much of this custom monitoring logic.

## Operational Health and Data Health Are Independent

A successful task does not imply healthy data.

For example:

```text id="8oh2kr"
Airflow
SILVER task = SUCCESS
```

may still result in:

```text id="bktgnq"
Silver event count
= unexpectedly low
```

or:

```text id="mls7a1"
Gold reconciliation
= FAILED
```

Therefore:

```text id="i11g8k"
execution success
≠
data correctness
```

This distinction is deliberate.

Operational observability tells whether the machinery ran.

Data observability tells whether the produced data is trustworthy.

## Lineage

Lineage answers:

> Where did this dataset come from, how was it transformed, and what depends on it?

The project explored lineage using OpenLineage.

The core model consists of:

```text id="o0r5ic"
Job
Run
Input Dataset
Output Dataset
```

For example:

```text id="h74xrk"
Bronze dataset
      ↓
Silver job
      ↓
Iceberg silver_events
      ↓
Load job
      ↓
PostgreSQL src_silver_events
```

Airflow automatically emits job and run information through the OpenLineage provider.

For Python transformations where dataset dependencies cannot be inferred automatically, explicit dataset lineage can be emitted.

The project demonstrated this integration using Marquez as a local visualization backend.

Marquez is not treated as an architectural dependency.

The architecture depends on the **OpenLineage standard**, not on a particular lineage UI.

## Why Airflow Dependencies Are Not Sufficient Lineage

An Airflow dependency:

```text id="dc0gvx"
silver >> load_postgres
```

means:

> execute `load_postgres` after `silver`.

It does not necessarily describe:

```text id="n7q409"
which dataset silver produced

which dataset load_postgres consumed

whether another job also consumes that dataset
```

Data lineage instead models:

```text id="x9pxu6"
Dataset A
   ↓
Job X
   ↓
Dataset B
```

The task graph and data-lineage graph are therefore related but distinct.

## Catalog and Metadata Management

Catalog and metadata management answer a broader question:

> What data assets exist, what do they mean, who owns them, and how are they related?

Typical metadata includes:

- dataset names;
- table and column schemas;
- descriptions;
- owners;
- business domains;
- tags;
- classifications;
- quality status;
- lineage;
- usage information;
- glossary terms.

A mature data catalog can therefore bring together information from:

```text id="8gj4pm"
database metadata
dbt models
lineage
data quality
ownership
governance
```

Tools such as DataHub or OpenMetadata are examples of this broader metadata-management category.

The project does not currently deploy a full enterprise data catalog because its scope and number of data assets do not justify the operational overhead.

## Iceberg Catalog vs Data Catalog

The term `catalog` has two different meanings in this architecture.

### Iceberg Catalog

Used to locate and manage Iceberg tables.

Conceptually:

```text id="m1ng7i"
football.silver_events
        ↓
Iceberg catalog
        ↓
current table metadata
```

It answers:

> Where is this Iceberg table and what metadata represents its current state?

### Enterprise Data Catalog

Used for data discovery and governance.

It answers:

> What data assets exist across the platform, what do they mean, and who owns them?

These are different responsibilities despite sharing the word `catalog`.

## Current Project Implementation

The current project intentionally uses lightweight implementations rather than a full observability platform.

```text id="9x26wb"
Operational signals
→ Airflow execution metadata
→ DuckDB health views

Data quality
→ Python validations
→ dbt tests
→ dbt contracts
→ reconciliation

Data anomalies
→ DuckDB volume monitoring

Lineage
→ Airflow OpenLineage provider
→ explicit dataset lineage where needed

Metadata
→ dbt documentation
→ Iceberg table metadata
→ architecture documentation
```

This is sufficient for the project's scale.

## Alternatives Considered

### Deploy Prometheus and Grafana

**Advantages**

- standard operational monitoring stack;
- dashboards and alerting;
- mature time-series handling;
- applicable to many infrastructure components.

**Not selected for this project because**

the existing Airflow/DuckDB telemetry is sufficient to demonstrate operational health concepts.

Introducing additional containers would increase resource and operational overhead without teaching a materially new concept.

A production deployment would likely replace the custom local implementation with dedicated telemetry infrastructure.

### Deploy Elementary

**Advantages**

- data-quality monitoring;
- anomaly detection;
- dbt integration;
- freshness and volume monitoring;
- less custom SQL.

**Not selected because**

the project has already implemented and demonstrated the core data-observability concepts through dbt tests, reconciliation, and DuckDB volume monitoring.

Installing another product would duplicate already understood functionality.

### Deploy DataHub or OpenMetadata

**Advantages**

- centralized metadata;
- data discovery;
- ownership;
- lineage visualization;
- governance;
- quality integration.

**Not selected because**

the project contains a small number of known datasets.

A full data catalog would add substantial infrastructure for limited practical benefit.

The concepts are documented, but implementation is deferred to a larger multi-domain data platform.

### Maintain a Custom Lineage Registry

A custom artifact registry could explicitly maintain parent-child relationships across processing stages.

**Rejected because**

OpenLineage already defines a standard model for job and dataset lineage.

Maintaining a custom competing lineage model would increase complexity and reduce interoperability.

## Consequences

### Positive

- operational failures and data failures are not conflated;
- quality mechanisms remain close to the datasets they validate;
- lineage can evolve independently from monitoring;
- metadata/catalog concerns are recognized without forcing enterprise tooling into a small project;
- standard tools can replace custom implementations later without changing the conceptual architecture;
- tool selection remains driven by responsibilities rather than product popularity.

### Negative

- metadata is currently distributed across several systems;
- there is no single enterprise UI showing health, lineage, ownership, and quality;
- DuckDB observability logic is custom;
- OpenLineage coverage is incomplete for arbitrary Python transformations unless lineage is explicitly emitted;
- future production deployment would require stronger telemetry retention, alerting, and metadata integration.

These limitations are accepted because the project prioritizes learning the underlying concepts over deploying every available platform.

## Health Grain

Observability itself has a grain.

Examples include:

```text id="231m9b"
one row = task attempt

one row = stage

one row = pipeline run

one row = match

one row = platform health snapshot
```

These grains should not be mixed unintentionally.

For example, a platform-level `current_health` view may describe the most recent state of several components, while a match-level health view would describe the complete processing status of one `match_id`.

The same grain discipline used in analytical modeling therefore also applies to operational metadata.

## Revisit This Decision When

The observability and metadata architecture should be expanded when:

- the number of pipelines or teams grows significantly;
- operational alerting becomes necessary;
- telemetry retention needs exceed local JSONL/DuckDB capabilities;
- data-quality anomalies require automated detection across many tables;
- consumers need self-service data discovery;
- ownership and governance become important;
- lineage must support organization-wide impact analysis;
- multiple data platforms must be represented in one metadata graph.

At that point, dedicated operational monitoring, data observability, and metadata-management tooling may be justified.

## Outcome

The project recognizes four related but distinct questions:

```text id="skv7xz"
OPERATIONAL OBSERVABILITY
Is the machinery working?

DATA OBSERVABILITY
Is the data healthy?

LINEAGE
Where did the data come from
and what depends on it?

CATALOG / METADATA
What data assets exist,
what do they mean,
and who owns them?
```

The final architecture does not require one tool per question.

Instead, it establishes these responsibilities explicitly so that future tooling can be selected based on the problem being solved rather than treating all monitoring, lineage, and metadata capabilities as one undifferentiated concern.
