# ADR-005 — Use PostgreSQL as the Serving and dbt Bridge Layer

**Status:** Accepted
**Date:** 2026-08-18

## Context

The canonical Silver dataset is stored as an Apache Iceberg table:

```text
football.silver_events
```

The analytical Gold layer is implemented with `dbt-postgres`.

This creates an integration boundary:

```text
Iceberg
   ↓
?
   ↓
dbt-postgres
```

PostgreSQL does not natively operate as an Iceberg query engine, while the existing dbt project is already configured around PostgreSQL.

A decision was therefore required on whether to:

- introduce an Iceberg-native SQL engine;
- change the dbt adapter;
- move Gold processing into another engine;
- or introduce a relational bridge between Iceberg Silver and dbt.

The project also requires a relational serving layer for analytical queries and downstream reporting.

## Decision

Use **PostgreSQL as both the relational serving layer and the compatibility bridge between Iceberg Silver and dbt**.

The processing flow is:

```text
Iceberg
football.silver_events
        ↓
read processed match
from pinned snapshot
        ↓
PostgreSQL
serving.src_silver_events
        ↓
dbt-postgres
        ↓
fact_gold_team
fact_gold_intervals
        ↓
reports / analytical queries
```

The Iceberg table remains the canonical Silver representation.

`serving.src_silver_events` is a SQL-accessible copy used as the dbt source boundary.

## Rationale

### Preserve the existing dbt architecture

The project already uses `dbt-postgres` successfully for:

- Gold transformations;
- incremental processing;
- model contracts;
- generic tests;
- singular business-rule tests;
- reconciliation;
- documentation and lineage.

Introducing another query engine solely to eliminate the Silver copy would increase infrastructure complexity without materially improving the learning outcome or current workload.

### Keep the architecture proportional to scale

An Iceberg-native lakehouse architecture could introduce a compute engine such as Spark, Trino, or another SQL engine capable of querying Iceberg directly.

Conceptually:

```text
Iceberg
   ↓
Trino / Spark / other engine
   ↓
dbt adapter
   ↓
Gold
```

This would remove the PostgreSQL bridge.

However, the current football workload is small enough that the additional engine would create more operational overhead than value.

The architecture therefore prefers:

> Introduce infrastructure when it solves an actual requirement, not merely because a theoretically cleaner architecture exists.

### Provide a stable relational serving layer

PostgreSQL also provides useful relational capabilities for the analytical layer:

- primary and foreign keys;
- check constraints;
- transactional writes;
- SQL querying;
- persistent serving tables;
- integration with dbt;
- straightforward connectivity for downstream consumers.

It therefore performs a useful role beyond acting solely as a compatibility workaround.

### Preserve snapshot reproducibility

The PostgreSQL loading task reads Silver rows from the exact Iceberg snapshot produced by the upstream Silver task.

Conceptually:

```text
Silver commit
      ↓
snapshot_id = X
      ↓
load_silver_to_db
      ↓
read snapshot X
WHERE match_id = M
```

The PostgreSQL source table therefore receives the version of Silver associated with the specific pipeline execution rather than whatever version happens to be current later.

## Source-Layer Loading Strategy

`serving.src_silver_events` uses match-level replacement.

For the current `match_id`:

```text
BEGIN

DELETE existing rows
WHERE match_id = X

INSERT regenerated rows

COMMIT
```

The delete and insert occur within one database transaction.

This provides two important properties.

### Idempotency

Rerunning the same match does not append another copy of its events.

The resulting state remains:

```text
one current set of Silver rows
for match X
```

### Atomicity

Consumers do not observe the intermediate state where the existing match has been deleted but the replacement rows have not yet been fully inserted.

Either:

```text
old match state
```

or:

```text
new match state
```

is committed.

A partially loaded match is not accepted as valid database state.

## PostgreSQL Is Not the Canonical Silver Store

This distinction is important.

The architecture contains two representations of Silver:

```text
Iceberg
football.silver_events

PostgreSQL
serving.src_silver_events
```

They do not have equal authority.

```text
Iceberg Silver
→ canonical dataset

PostgreSQL Silver
→ SQL-accessible downstream projection
```

If the PostgreSQL source table must be rebuilt, it should be regenerated from Iceberg rather than treated as the authoritative historical source.

This ownership rule prevents the two systems from becoming competing sources of truth.

## Alternatives Considered

### Query Iceberg Directly from dbt

A cleaner lakehouse architecture would allow dbt to read the canonical Iceberg table directly.

For example:

```text
Iceberg Silver
      ↓
Iceberg-capable query engine
      ↓
dbt
      ↓
Gold
```

**Advantages**

- removes duplicated Silver storage;
- removes the Iceberg → PostgreSQL load step;
- simplifies canonical-data ownership;
- allows Gold to remain closer to the lake.

**Not selected because**

the current `dbt-postgres` environment cannot directly query the local Iceberg table.

Adopting this architecture would require another query engine and likely another dbt adapter.

The additional infrastructure is not justified by the current scale.

### Make PostgreSQL the Canonical Silver Store

Silver could be written directly to PostgreSQL and Iceberg removed.

**Advantages**

- one Silver representation;
- simple dbt integration;
- fewer technologies;
- relational transactions and constraints.

**Rejected because**

this would remove the table-format capabilities that Silver was specifically selected to demonstrate and use:

- snapshots;
- time travel;
- schema evolution;
- partition evolution;
- object/file-oriented storage;
- explicit table-state management.

PostgreSQL and Iceberg solve different storage concerns.

### Move Gold Back to Python

Another option would be:

```text
Iceberg Silver
      ↓
Python Gold
      ↓
PostgreSQL
```

which would remove the need to expose Silver to dbt.

**Rejected because**

dbt is the authoritative implementation of analytical Gold transformations.

Returning Gold logic to Python would reverse the ownership decision documented in ADR-003 and reintroduce analytical transformation responsibilities that dbt now handles more appropriately.

### Add Spark

Spark could read and write Iceberg directly and perform Gold processing.

**Advantages**

- native Iceberg support;
- distributed processing;
- mature maintenance capabilities;
- ability to perform larger-scale rewrites and compaction.

**Rejected for the current project because**

the data volume does not require distributed processing.

Adding Spark solely because it is common in data-engineering stacks would make the architecture tool-driven rather than requirement-driven.

## Consequences

### Positive

- existing dbt models remain unchanged;
- PostgreSQL provides a simple SQL boundary for dbt;
- no additional distributed query engine is required;
- Gold models remain relational and easy to query;
- database constraints and transactions remain available;
- downstream reporting can consume familiar SQL relations;
- the canonical Iceberg table remains independent of analytical serving concerns.

### Negative

- Silver exists in two physical systems;
- data must be copied from Iceberg into PostgreSQL;
- an additional pipeline stage can fail;
- storage is duplicated;
- Silver changes are not instantly visible in PostgreSQL;
- consistency depends on the load task completing successfully;
- the architecture is not a pure lakehouse design.

These are accepted trade-offs for keeping the project architecture proportional to its scale.

## Failure and Recovery Implications

The additional bridge creates another failure boundary.

For example:

```text
Iceberg commit
    ✓

Postgres load
    ✗
```

In this situation, Silver is still valid because its canonical commit succeeded.

The PostgreSQL load can be retried using:

```text
match_id
+
pinned snapshot_id
```

without rebuilding Silver.

This separation improves recovery because the canonical upstream artifact is already committed.

Similarly:

```text
Postgres load
    ✓

dbt build
    ✗
```

does not require re-ingestion or regeneration of Silver.

Only the failed downstream stage needs to be rerun.

## Serving vs Transformation Responsibilities

PostgreSQL performs two related but distinct roles.

### Transformation bridge

It exposes:

```text
serving.src_silver_events
```

to `dbt-postgres`.

### Analytical serving

It stores dbt-owned analytical relations such as:

```text
fact_gold_team
fact_gold_intervals
```

which can be queried by downstream consumers.

These roles may be separated in a larger production architecture, but combining them is appropriate for the current deployment.

## Operational Implications

Because multiple match DAGs may run concurrently, database writes must be protected from unnecessary contention.

Airflow concurrency controls and resource pools can limit simultaneous write-heavy dbt operations.

This follows the principle:

> Concurrency limits should protect a known constrained resource.

The database should not be globally serialized without a reason, but competing writes can be bounded where necessary.

## Future Architecture

If the platform moved toward a larger lakehouse implementation, the architecture could evolve to:

```text
              Iceberg
                 │
       ┌─────────┴─────────┐
       ↓                   ↓
silver_events        Gold Iceberg tables
       ↑                   ↑
       └──── dbt / SQL ────┘
               engine
```

In this model:

```text
serving.src_silver_events
```

could disappear.

PostgreSQL could then remain only as a consumer-facing serving database, or potentially be removed if consumers query the lakehouse directly.

The current bridge therefore represents a deliberate transitional boundary rather than a claim that Iceberg data must always be copied into PostgreSQL.

## Revisit This Decision When

The PostgreSQL bridge should be reconsidered if:

- Silver data volume makes copying expensive;
- data latency requirements make the additional load unacceptable;
- the organization adopts an Iceberg-native SQL engine;
- dbt moves to an adapter capable of directly querying the Iceberg catalog;
- Gold tables are migrated into the lakehouse;
- PostgreSQL serving becomes a performance bottleneck;
- multiple downstream systems require direct access to canonical Silver.

At that point the cost of the bridge may exceed the simplicity it currently provides.

## Outcome

The architecture deliberately accepts one additional copy:

```text
CANONICAL
Iceberg Silver
      ↓
compatibility / serving boundary
      ↓
PostgreSQL Silver Source
      ↓
dbt Gold
```

This is not the theoretically minimal architecture.

It is the simplest architecture that simultaneously preserves:

- Iceberg as the canonical table format;
- dbt as the analytical transformation owner;
- PostgreSQL as the relational serving environment;
- and a local deployment that remains proportionate to the project's scale.

The duplication is therefore an **explicit architectural trade-off**, not an accidental consequence of the implementation.
