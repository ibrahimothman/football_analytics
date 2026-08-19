# ADR-001 — Use One Match as the Atomic Processing Grain

**Status:** Accepted
**Date:** 2026-08-18

## Context

The pipeline must process football event data across many matches while supporting:

- incremental ingestion;
- isolated retries and failures;
- safe reprocessing;
- parallel execution;
- season-scale discovery and backfills.

A decision was required on the unit of work that an individual processing workflow should own.

Possible scopes included processing an entire season, processing batches of matches, or processing each match independently.

A football match provides a natural business boundary: its source events can be independently ingested, transformed, validated, persisted, and rebuilt without requiring other matches to be processed at the same time.

## Decision

Use **one match as the atomic processing grain** of the worker pipeline.

A controller/discovery workflow determines which matches require processing and dispatches one worker DAG execution per match.

Conceptually:

```text
Discovery / Controller DAG
        │
        ├── Match 101 → Worker DAG
        ├── Match 102 → Worker DAG
        └── Match 103 → Worker DAG
```

Each worker execution processes:

```text
INGEST
  ↓
RAW
  ↓
BRONZE
  ↓
SILVER / ICEBERG
  ↓
POSTGRES
  ↓
DBT
  ↓
REPORTS
```

for one `match_id`.

The match identifier therefore acts as the primary work key across orchestration and reprocessing.

## Rationale

### Failure isolation

A failure while processing one match should not invalidate or require restarting an entire season.

With match-level execution:

```text
Match 101 → SUCCESS
Match 102 → FAILED
Match 103 → SUCCESS
```

only Match 102 needs to be retried or investigated.

### Incremental processing

New matches can be discovered and processed independently rather than rebuilding previously processed data.

This aligns naturally with the source, where matches become individual units of available work.

### Idempotent reprocessing

A specific match can be rebuilt from its pinned upstream version without affecting unrelated matches.

Downstream persistence is designed around the same principle, including match-level Silver replacement and Gold regeneration.

### Parallelism

Independent matches can be processed concurrently while Airflow pools and concurrency limits protect shared resources such as external APIs and PostgreSQL/dbt writes.

### Backfills

A historical range or season can be treated as a collection of match-level work items:

```text
Season backfill
      ↓
resolve matches
      ↓
dispatch match 1
dispatch match 2
dispatch match 3
...
```

The batch scope can therefore change without changing the worker implementation.

## Important Distinction: Processing Grain vs Storage Partitioning

Choosing one match as the processing grain does **not** imply that storage should be physically partitioned by `match_id`.

These are separate concerns:

```text
Processing grain
→ how work is scheduled and retried

Analytical grain
→ what one row represents

Physical partitioning
→ how table data is organized for storage/query efficiency
```

The original match-oriented Parquet layout was convenient operationally but would create high-cardinality partitions and small files at larger scale.

The Iceberg Silver table therefore manages physical table layout independently from the match-level orchestration model.

## Alternatives Considered

### Process an Entire Season in One DAG Run

**Advantages**

- simpler initial orchestration;
- fewer DAG runs;
- straightforward season-wide transformation.

**Rejected because**

- a single match failure could interrupt the whole batch;
- retries would repeat already successful work;
- reprocessing individual matches would be awkward;
- parallelism would be harder to control;
- execution time would increase as the season grows.

### Process Fixed Batches of Matches

For example:

```text
10 matches per worker
```

**Advantages**

- fewer orchestration objects;
- potentially more efficient for very large-scale distributed processing.

**Rejected for the current workload because**

- batch boundaries are artificial;
- failure isolation becomes worse;
- individual match reprocessing becomes more complicated;
- the current data volume does not justify batching for compute efficiency.

Batching could still be introduced later behind the same controller abstraction if orchestration overhead became significant.

### One Task per Individual Event

**Rejected because**

the unit is too granular. Football events are not independently useful processing boundaries and would create excessive orchestration overhead.

## Consequences

### Positive

- failures are isolated to individual matches;
- retries are smaller and safer;
- incremental processing is straightforward;
- match-specific reprocessing is natural;
- controller and worker responsibilities remain separate;
- season or competition scope can grow without changing the worker pipeline;
- concurrency can be controlled explicitly;
- lineage and observability can associate processing with a clear business key.

### Negative

- a season produces many Airflow DAG runs;
- orchestration metadata grows with the number of matches;
- very large datasets could eventually make one-workflow-per-match inefficient;
- match-oriented writes can create small files if physical storage layout is allowed to mirror processing grain.

The Iceberg storage layer and bounded Airflow concurrency mitigate the latter two concerns without changing the logical unit of work.

## Revisit This Decision When

This decision should be reconsidered if:

- orchestration overhead becomes material relative to processing time;
- the source begins delivering large multi-match batches that cannot be efficiently separated;
- transformation moves to a distributed compute engine where larger processing batches are significantly more efficient;
- data volume grows sufficiently that match-level commits create unacceptable metadata or write amplification.

Even in those cases, `match_id` should remain an important logical key for lineage, idempotency, reconciliation, and selective reprocessing.

## Outcome

The architecture separates **scope selection** from **work execution**:

```text
Competition / Season / Discovery Scope
                ↓
         Controller DAG
                ↓
        individual matches
                ↓
           Worker DAG
```

This allows the platform to process an entire season while keeping the atomic operational unit small, independently retryable, and reproducible.
