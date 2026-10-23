# Extensions Clear-Room Decisions

This document records the implementation decisions for Extensions clear-room
issues #31-#35. The goal is independent responsibility boundaries, not
mechanical renaming or removal of valid third-party product terminology.

## MCP registration (#31)

The MCP module contains a registry integration service and a registrar that
publishes a server definition. The previous `register` package and
`NacosMcpRegister` names described the action but not the component boundary.

- Registry integration types use `io.github.agentic.ai.mcp.registry`.
- Runtime executors are `NacosMcpRegistrar` and
  `NacosStatelessMcpRegistrar`.
- Configuration is `NacosMcpRegistryProperties`.
- The existing `argi.mcp.nacos.register.*` keys remain unchanged because they
  are deployment configuration, not Java responsibility names.
- The missing `enabled` property is now bound explicitly and drives the
  existing conditional auto-configuration.

The old Java package and class names are replaced in the next breaking
Extensions release. Nacos service names and protocol payloads are unchanged.

## Tool observation (#32)

Tool observation context, convention, and documentation are generic ARGI
observation contracts. The ARMS starter remains the vendor adapter and keeps
the `argi.arms.*` configuration namespace.

- Generic types use `ToolCallingObservationContext`,
  `ToolCallingObservationConvention`, and
  `ToolCallingObservationDocumentation`.
- `gen_ai.framework` emits `argi` as the canonical framework value.
- Existing span and attribute names remain unchanged to avoid accidental
  dashboard and alert cardinality changes.
- A custom tool convention, ChatClient convention, or ChatModel convention can
  replace the default auto-configured bean.

Consumers that query the old framework value must map `agentic ai` to `argi`.
ARMS exporter-specific mapping remains outside the generic observation module.

## Prompt configuration (#33)

`NacosPromptTmplProperties` and `PromptTmplNacosConfigCondition` were renamed
to `NacosPromptTemplateProperties` and `PromptTemplateNacosConfigCondition`.
The property prefix `argi.nacos.prompt.template.*`, enablement default, bean
conditions, and Nacos update behavior are unchanged.

## Chat memory adapters (#34)

Storage adapters use `io.github.agentic.ai.memory.<backend>` while Spring Boot
assembly remains under `io.github.agentic.ai.model.chat.memory.<backend>.autoconfigure`.
Only Elasticsearch diverged from the implementation convention, so it now
uses `io.github.agentic.ai.memory.elasticsearch`.

No TTL, index, tenant, cache-key, serialized message, or storage protocol
behavior changes. The existing `argi.chat.memory.repository.elasticsearch.*`
configuration keys remain unchanged.

## Document and rerank contracts (#35)

The previous `argi-extensions-model` artifact combined document parsing and
rerank contracts. The responsibilities are now independently consumable:

| Responsibility | Artifact | Java packages |
| --- | --- | --- |
| Document parsing | `argi-document-model` | `io.github.agentic.ai.document` |
| Rerank contracts | `argi-rerank-model` | `io.github.agentic.ai.model` and the existing `DocumentWithScore` FQCN |

`argi-extensions-model` remains a compatibility aggregator that depends on
both artifacts. This preserves existing Maven consumers while allowing new
consumers to select one contract set. Public Java packages and behavior remain
unchanged; no provider SDK is added to either module.

The graph network node now consumes `argi-document-model`, and the graph RAG
node now consumes `argi-rerank-model`. Parsing and ranking algorithms are not
changed.

## Validation and rollback

The affected modules are validated through reactor compilation and their
existing unit tests. External Nacos, Redis, Elasticsearch, and ARMS deployment
environments are not required for these naming and dependency-boundary
changes; integration tests requiring those services remain environment-gated.

Rollback is a source-level revert of the corresponding PR. Configuration keys,
wire payloads, storage formats, and observation attribute names are preserved
unless explicitly listed above.
