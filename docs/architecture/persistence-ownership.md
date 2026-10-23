# Graph Persistence Ownership

Status: adopted for the Core/Extensions split. Reviewed 2026-10-24.

ARGI Core owns the graph persistence contracts and reference implementations.
This repository owns optional durable backends and their operational behavior.
The decision is tracked with Core issue
[#190](https://github.com/agentic-ai-java/argi/issues/190).

## Owned modules

| Module | Owner scope | Core contract consumed |
| --- | --- | --- |
| `argi-graph-persistence-jdbc` | H2, MySQL, PostgreSQL and Oracle checkpoint savers and `DatabaseStore` | `BaseCheckpointSaver`, `Store`, serializers |
| `argi-graph-persistence-mongodb` | MongoDB checkpoint saver | `BaseCheckpointSaver`, serializers |
| `argi-graph-persistence-redis` | Redis checkpoint saver, versioned CAS, leases, Redis Store and RunStore | checkpoint, lease and runtime contracts |

These modules are the only maintenance locations for durable JDBC, MongoDB and
Redis graph persistence. Backend schemas, keys, transaction behavior, driver
versions, leases, retention and recovery tests stay here.

## Compatibility rules

- The modules consume `argi-loop-core`; no module depends on the removed
  `argi-graph-core` coordinate for the persistence path.
- Existing Java package names in the Extensions adapters remain unchanged.
- Existing checkpoint fields, serializer markers, database table names, Redis
  keys, namespace encoding and configuration meanings remain unchanged.
- The old Core backend classes are removed rather than kept as a second
  implementation. Consumers migrate to the Extensions artifacts listed in the
  module table.
- Cross-version compatibility tests may use the last published graph artifact
  as a test-only historical writer; it is never a production dependency.
- The Extensions BOM must publish the persistence modules with the matching
  Core version.

## Required backend evidence

Each module must keep tests for history/release, retention, serializer
compatibility, concurrent access and backend restart behavior. Redis additionally
must cover revision CAS, lease fencing and durable RunStore commit semantics.
Tests that require PostgreSQL, MySQL, Oracle, MongoDB or Redis must clearly
remain integration tests; an in-memory mock is not evidence of backend
durability.

## Deliberate non-goals

This migration does not rename `graph-persistence` artifacts, change wire
formats, change serializer ownership, or move the neutral `Store` and
`CheckpointSaver` contracts into Extensions. It also does not update unrelated
Extensions consumers of the historical graph coordinate; those belong to their
own migration tasks.
