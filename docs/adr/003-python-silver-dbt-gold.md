# ADR-003 — Use Python for Silver and dbt for Gold

**Status:** Accepted
**Date:** 2026-08-18

## Context

The pipeline contains two fundamentally different types of transformation.

The Silver layer performs source- and domain-specific processing over football event data.

Examples include:

- flattening provider-specific event structures;
- normalizing football event fields;
- coordinate normalization;
- handling nullable and nested source attributes;
- progressive-pass derivation;
- expected-threat (`xT`) calculations;
- event-level schema validation;
- football-specific transformation logic.

The Gold layer performs relational analytical transformations such as:

- aggregating events by team and match;
- aggregating xT by time interval;
- defining analytical measures;
- joining descriptive dimensions;
- enforcing analytical contracts;
- testing and reconciling analytical outputs.

Initially, both Python and SQL-based approaches were used for parts of the Gold layer, creating the risk of duplicate metric implementations and unclear transformation ownership.

A clear boundary was therefore required between Python transformation logic and dbt-managed analytical SQL.

## Decision

Use **Python for Raw → Bronze → Silver transformation** and **dbt for Gold and analytical models**.

The ownership boundary is:

```text
StatsBomb
   ↓
Raw
   ↓
Bronze
   ↓
Silver
   │
   │ Python owns this boundary
   ▼
Iceberg silver_events
   ↓
PostgreSQL source relation
   │
   │ dbt owns from here
   ▼
dbt
   ├── fact_gold_team
   └── fact_gold_intervals
```

Python Gold implementations are not retained alongside equivalent dbt models.

dbt is the single owner of analytical Gold metric definitions.

## Rationale

### Silver contains source-specific transformation logic

Silver must understand the structure and semantics of the incoming football event data.

Examples include:

```text
nested source structures
coordinate arrays
event subtypes
football-specific classifications
derived event metrics
```

This processing is naturally expressed in Python using pandas and domain-oriented functions.

Python also allows these transformations to be unit-tested independently from the database.

### Gold is primarily relational analytics

Gold transformations are dominated by operations such as:

```text
GROUP BY
JOIN
CASE
SUM
COUNT
FILTER
window functions
```

These are naturally expressed in SQL.

dbt provides a structured environment for this type of transformation through:

```text
source()
ref()
incremental models
model contracts
generic tests
singular tests
documentation
lineage
```

### Transformation ownership must be explicit

Maintaining both:

```text
Python build_gold_team()
```

and:

```text
dbt fact_gold_team
```

would create two possible definitions of the same analytical metric.

Even if they initially produced identical results, future changes could cause them to diverge.

The architecture therefore follows:

> One transformation should have one authoritative implementation.

Once the dbt Gold models were validated through reconciliation, the replaced Python Gold transformations were removed from the active pipeline.

### The boundary follows responsibility rather than technology preference

The decision is not:

```text
Python is for early layers
SQL is for later layers
```

as a universal rule.

The boundary exists because the two layers solve different kinds of problems.

```text
Silver
→ source normalization and domain logic

Gold
→ relational analytical modeling
```

If the characteristics of either layer change, the implementation technology can be reconsidered.

## Alternatives Considered

### Implement All Transformations in Python

The entire pipeline could have remained Python-based.

**Advantages**

- one programming language;
- reuse of existing transformation code;
- straightforward local debugging;
- no additional transformation framework.

**Rejected because**

- analytical SQL logic becomes embedded in application-style Python code;
- dependency management between analytical models must be implemented manually;
- model documentation and testing become more bespoke;
- downstream analysts cannot inspect transformations as easily;
- maintaining incremental analytical models becomes more complex;
- metric definitions become harder to govern centrally.

### Implement Silver and Gold Entirely in dbt

All structured transformations could have been moved into SQL.

**Advantages**

- one transformation framework;
- unified lineage;
- consistent testing and documentation;
- thinner Airflow/Python layer.

**Rejected for the current implementation because**

Silver contains substantial provider-specific and football-domain processing that is currently easier to maintain and test in Python.

Moving this logic to SQL would provide limited benefit while increasing implementation complexity.

This may be reconsidered if the canonical source is later landed directly into a warehouse or distributed SQL engine in a form suitable for ELT.

### Maintain Python Gold and dbt Gold in Parallel

Both implementations could have been retained for validation or fallback purposes.

**Rejected because**

this creates two owners for the same business logic.

The likely result would be:

```text
Python metric definition
        ≠
dbt metric definition
```

after future changes.

Temporary parallel execution was useful during migration and reconciliation, but it is not an acceptable steady-state architecture.

## Consequences

### Positive

- transformation ownership is explicit;
- analytical metric definitions have a single source of truth;
- Python remains focused on source/domain processing;
- dbt handles SQL-native transformations;
- Gold dependencies are visible through `ref()`;
- analytical models gain contracts, tests, documentation, and lineage;
- Python Gold code and hand-written analytical upsert logic can be removed;
- Airflow becomes thinner and more focused on orchestration.

### Negative

- two transformation technologies must be maintained;
- engineers must understand both Python and SQL/dbt;
- Silver-to-Gold processing crosses a system boundary;
- Silver must currently be exposed to PostgreSQL before `dbt-postgres` can consume it;
- some end-to-end transformations cannot be understood from only the Python codebase or only the dbt project.

The lineage and architecture documentation must therefore make this ownership boundary explicit.

## Orchestration Implications

Airflow should orchestrate the transformation engines rather than own analytical transformation logic.

The worker DAG becomes conceptually:

```text
INGEST
   ↓
BRONZE
   ↓
BUILD SILVER
   ↓
COMMIT ICEBERG
   ↓
LOAD SILVER TO POSTGRES
   ↓
RUN DBT
   ↓
REPORTS
```

The dbt task invokes the analytical build but does not reproduce Gold transformation logic inside the DAG.

This keeps:

```text
Airflow
→ orchestration

Python
→ source/domain transformation

dbt
→ analytical transformation
```

as separate responsibilities.

## Data Quality Implications

Quality controls are applied close to the layer that owns the relevant semantics.

### Python / Silver

Responsible for checks such as:

```text
required event columns
source normalization validity
coordinate validity
event-level invariants
duplicate handling
```

### dbt / Gold

Responsible for:

```text
not-null tests
uniqueness
relationships
accepted values
model contracts
analytical business rules
Silver ↔ Gold reconciliation
```

Cross-layer reconciliation provides confidence that moving ownership from Python to dbt did not change analytical meaning unexpectedly.

## Revisit This Decision When

The boundary should be reconsidered if:

- the source is landed directly into a SQL-native warehouse;
- Silver transformation becomes primarily relational SQL;
- Python processing becomes a scalability bottleneck;
- a distributed compute engine becomes necessary;
- dbt gains direct access to the canonical Iceberg tables through an Iceberg-capable query engine;
- domain logic can be expressed more clearly and maintainably in another transformation framework.

The decision should be based on transformation characteristics rather than a rule that a specific layer must always use a particular tool.

## Outcome

The final ownership model is:

```text
Python
────────────────────────────
Raw
Bronze
Silver
domain-specific transformations

            │
            ▼

dbt
────────────────────────────
Gold facts
analytical aggregations
contracts
tests
reconciliation
documentation
```

This boundary keeps source-specific processing close to Python while placing relational analytical logic in the tool designed to manage SQL transformation dependencies, testing, and documentation.

Most importantly, each analytical transformation has **one authoritative owner**, avoiding duplicated business logic across Python and dbt.
