# ADR-004 — Use Apache Iceberg for Canonical Silver Storage

**Status:** Accepted
**Date:** 2026-08-18

## Context

The pipeline originally persisted Silver events as match-oriented Parquet artifacts.

Conceptually:

```text
silver/
├── match_id=101/
│   └── events.parquet
├── match_id=102/
│   └── events.parquet
└── ...
```

This approach was simple and worked well while each match was treated primarily as an independent processing artifact.

As the pipeline evolved, Silver became a shared canonical dataset consumed across many matches and reprocessing scenarios.

This introduced table-level concerns that plain Parquet files do not manage themselves:

- which physical files currently belong to the Silver dataset;
- how to identify an exact version of the entire table;
- how to safely replace data for a reprocessed match;
- how schema changes should be managed;
- how partitioning can evolve independently from application code;
- how previous table states can be inspected or recovered;
- how physical file layout can be maintained as the dataset grows.

Managing these responsibilities through directory conventions and application code would increasingly reproduce functionality provided by an open table format.

## Decision

Use **Apache Iceberg as the canonical storage format for the Silver event table**.

The canonical table is:

```text
football.silver_events
```

The architecture becomes:

```text
Raw JSON
   ↓
Bronze Parquet
   ↓
Python Silver transformation
   ↓
Apache Iceberg
football.silver_events
   ↓
PostgreSQL
serving.src_silver_events
   ↓
dbt Gold
```

Parquet remains the physical data-file format underneath Iceberg.

Iceberg provides the table-level metadata and transaction model used to manage those files.

## Rationale

### Parquet and Iceberg solve different problems

Parquet defines how data is physically encoded inside files.

Iceberg defines how a collection of files behaves as a table.

Conceptually:

```text
Parquet
→ file format

Iceberg
→ table format
```

Iceberg therefore does not replace Parquet.

Instead:

```text
Iceberg table
     ↓
metadata
snapshots
manifests
partition specifications
     ↓
Parquet data files
```

This allows consumers and pipeline code to reason about:

```text
football.silver_events
```

rather than manually determining which filesystem paths currently represent Silver.

### Snapshot-based table state

Each successful Silver write creates a new committed table state.

Conceptually:

```text
Snapshot 1
→ Match 101

Snapshot 2
→ Match 101 + Match 102

Snapshot 3
→ revised Match 101 + Match 102
```

The current table state is represented by the latest committed snapshot, while previous snapshots can remain available for inspection and time travel.

This gives the pipeline an explicit table version rather than relying on an implicit notion of "whatever files currently exist."

### Reproducibility through snapshot pinning

When Silver commits successfully, the task returns the resulting Iceberg `snapshot_id`.

Downstream processing uses:

```text
table
+
snapshot_id
+
match_id
```

to read the exact Silver state produced by that pipeline execution.

Conceptually:

```text
SILVER
   ↓
commit
   ↓
snapshot_id = 123456
   ↓
LOAD TO POSTGRES
   ↓
read snapshot 123456
```

The downstream task does not independently resolve the current snapshot.

This preserves the pipeline's existing artifact-pinning principle:

> A pipeline run may resolve an upstream version once, but downstream stages should not independently resolve "latest" during the same run.

### Match reprocessing becomes a table operation

The worker pipeline processes one match at a time.

When a match is rebuilt, the Silver rows for that `match_id` are replaced through an Iceberg table commit.

Conceptually:

```text
Snapshot A
-------------
Match 101 v1
Match 102

       ↓ reprocess 101

Snapshot B
-------------
Match 101 v2
Match 102
```

The new snapshot represents the current table state while the prior snapshot preserves the previous state until snapshot-retention maintenance removes it.

This is preferable to application code manually deciding which Parquet file version should be treated as current.

### Partitioning is separated from directory conventions

The original match-based physical layout coupled processing grain with storage layout.

Iceberg allows partition definitions to be maintained as table metadata and evolved without requiring consumers to understand directory structures.

This supports the principle:

```text
processing grain
≠
physical partitioning
```

For example, the pipeline can continue processing one match at a time even if the physical table later uses:

```text
bucket(match_id)
```

or a competition/date-oriented partition strategy.

Consumers continue filtering using logical columns rather than physical directory names.

## What Iceberg Replaces

For the Silver layer, Iceberg replaces responsibility for:

- determining which data files belong to the current table;
- managing table snapshots;
- representing previous table states;
- manual Silver file-version selection;
- partition-specification metadata;
- schema evolution at the table level;
- much of the custom reasoning around Silver artifact membership.

The pipeline should therefore not maintain an independent competing mechanism for determining the authoritative Silver table state.

## What Iceberg Does Not Replace

### The ingestion manifest

The ingestion manifest remains because it answers a different set of questions.

The ingestion manifest records the **source ingestion lifecycle**, such as:

```text
Which StatsBomb artifact was received?

What was its hash?

Was it already ingested?

Was the source revised?

When was it ingested?

Where is the raw source artifact?
```

Iceberg answers table-state questions:

```text
Which files constitute silver_events?

What is the current table snapshot?

What did the table look like previously?

What schema and partition specification apply?
```

Therefore:

```text
INGESTION MANIFEST
→ source provenance and ingestion state

ICEBERG
→ canonical Silver table state
```

They are complementary rather than duplicate metadata systems.

### Raw source storage

Raw remains immutable JSON.

Its purpose is source fidelity and traceability rather than analytical table behavior.

Introducing Iceberg at Raw would provide little value because Raw is intended to preserve the source response rather than expose a canonical tabular model.

### Bronze Parquet

Bronze remains Parquet.

Bronze is an intermediate provider-shaped artifact used primarily by the match-level transformation pipeline.

The project does not currently require Bronze-level:

- time travel;
- cross-match analytical querying;
- partition evolution;
- shared table semantics.

Introducing Iceberg there would add infrastructure without solving a material problem.

## Alternatives Considered

### Continue Using Match-Level Parquet Only

**Advantages**

- simplest storage implementation;
- no catalog required;
- easy local inspection;
- minimal dependencies.

**Rejected for canonical Silver because**

as the dataset grows, application code must increasingly manage:

```text
file membership
versioning
replacement semantics
schema evolution
partition layout
table history
```

This recreates table-management capabilities outside a standard table format.

Parquet remains appropriate for layers where these capabilities are not required.

### Build a Custom Artifact Registry

A persistent registry could have tracked:

```text
artifact_id
parent_artifact_id
path
hash
version
current status
```

**Rejected because**

it would duplicate significant parts of the table-state and lineage functionality already addressed by established standards such as Iceberg and OpenLineage.

A custom registry would introduce an additional metadata system requiring its own consistency and lifecycle rules.

### Store Silver Only in PostgreSQL

Silver could be loaded directly into PostgreSQL and treated as the canonical copy.

**Advantages**

- fewer storage technologies;
- direct SQL access;
- simpler integration with dbt-postgres;
- transactional row updates.

**Rejected because**

the project intentionally requires an analytical file/table layer independent of the serving database.

Iceberg also provides:

- snapshots;
- time travel;
- table-format interoperability;
- partition evolution;
- object-storage-oriented architecture.

PostgreSQL remains a downstream serving and dbt compatibility layer.

### Convert All Data Layers to Iceberg

Raw, Bronze, Silver, and Gold could all be represented as Iceberg tables.

**Rejected because**

the table format should be introduced where its capabilities solve an actual requirement.

For this project:

```text
Raw
→ immutable source archive

Bronze
→ intermediate provider-shaped artifact

Silver
→ shared canonical structured dataset
```

Silver has the strongest need for table-level state management.

Using Iceberg everywhere would increase complexity without proportionate benefit.

## Consequences

### Positive

- Silver has one logical table identity;
- table state is explicit through snapshots;
- downstream stages can pin exact table versions;
- match reprocessing creates a new committed state;
- historical table states can be queried;
- schema evolution is managed through table metadata;
- partition design is decoupled from directory conventions;
- storage layout can evolve without redefining the logical table;
- the pipeline relies less on bespoke file-management logic.

### Negative

- an Iceberg catalog must be operated;
- Iceberg metadata and maintenance must be understood;
- additional dependencies such as PyIceberg are required;
- snapshot retention and file maintenance eventually require operational policies;
- frequent match-level commits can create small files;
- compaction may eventually require an engine with appropriate Iceberg maintenance capabilities;
- PostgreSQL still requires a copy of Silver because the current dbt adapter does not query Iceberg directly.

Iceberg therefore improves table management but does not eliminate all storage or serving complexity.

## Small Files and Maintenance

Processing one match per workflow can naturally produce many small writes.

Operationally:

```text
one match
→ one independent commit
```

is desirable for failure isolation.

Analytically:

```text
many tiny files
→ increased metadata and file-open overhead
```

may become undesirable.

This creates a deliberate separation between:

```text
write pattern
→ optimized for processing

physical table layout
→ optimized for reading
```

At larger scale, periodic Iceberg maintenance would include operations such as:

- data-file compaction;
- snapshot expiration;
- manifest maintenance;
- partition-layout optimization.

The current project demonstrates these concepts but does not introduce a distributed compute engine solely to perform maintenance on a small dataset.

## Catalog Responsibility

An Iceberg catalog is used to locate and manage Iceberg table metadata.

Conceptually:

```text
Catalog
   ↓
football.silver_events
   ↓
current Iceberg metadata
   ↓
snapshots / manifests
   ↓
Parquet files
```

This catalog should not be confused with an enterprise **data catalog** such as DataHub or OpenMetadata.

An Iceberg catalog answers:

```text
Where is this Iceberg table
and what metadata represents it?
```

A data catalog answers broader discovery and governance questions such as:

```text
What datasets exist?
What do they mean?
Who owns them?
What is their lineage and quality?
```

## PostgreSQL Integration

The canonical Silver dataset is Iceberg, but the current Gold transformation environment uses `dbt-postgres`.

The pipeline therefore performs:

```text
Iceberg Silver
      ↓
read match from pinned snapshot
      ↓
PostgreSQL src_silver_events
      ↓
dbt
```

The PostgreSQL copy is a compatibility and serving boundary, not the authoritative Silver table.

This trade-off is documented separately because it introduces temporary duplication between the canonical lake table and the relational transformation environment.

## Revisit This Decision When

This decision should be reconsidered if:

- the dataset remains so small that Iceberg operational overhead provides no practical or educational value;
- Silver becomes primarily warehouse-resident;
- the organization standardizes on another table format such as Delta Lake or Hudi;
- the downstream compute platform can query Iceberg directly and PostgreSQL becomes unnecessary;
- maintenance requirements become large enough to justify Spark, Trino, or another Iceberg-capable compute engine;
- the data platform moves to a managed lakehouse where catalog and maintenance responsibilities are provided by the platform.

The decision to use Iceberg should remain tied to **table-management requirements**, not simply to the popularity of the technology.

## Outcome

Silver changes from application-managed files:

```text
Silver
├── match file
├── match file
└── match file
```

to a catalog-managed logical table:

```text
football.silver_events
          │
          ▼
      Iceberg
   ┌──────────────┐
   │ snapshots    │
   │ schema       │
   │ partitions   │
   │ manifests    │
   └──────┬───────┘
          ▼
       Parquet
       files
```

Apache Iceberg is therefore used where the project first requires **table-level versioning, reproducibility, schema/partition evolution, and managed file membership**, while simpler storage approaches remain in layers where those capabilities are unnecessary.
