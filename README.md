# [Agentic Spring AI Extensions](https://agentic-spring-ai.github.io/website/en/)

[English](README.md) | [简体中文](README-zh.md)

[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](LICENSE)
[![Version](https://img.shields.io/badge/version-2.1.0--dev-blue)](https://github.com/agentic-spring-ai/agentic-spring-ai-extensions)

Agentic Spring AI Extensions provides Spring AI integrations for models, MCP, tool calling, vector stores, chat memory, RAG, document processing, prompt management, and observability. Use these modules directly with Spring AI or combine them with the [Agentic Spring AI](https://github.com/agentic-spring-ai/agentic-spring-ai) framework.

## Features

- **Models**: DashScope chat, image, embedding, speech, and transcription implementations.
- **MCP**: registry, router, distributed service, and gateway modules.
- **Tool calling**: integrations for search, translation, maps, storage, collaboration, and other services.
- **Data and memory**: vector stores and chat memory repositories for common databases and cloud services.
- **RAG and documents**: reusable RAG components, document parsers, and document readers.
- **Operations**: Nacos prompt management and ARMS observation integration.
- **Provider examples**: DashScope multimodal, voice agent, and AgentScope integration examples.

## Getting Started

Requirements: JDK 17 or later and Maven 3.9.1 or later. The commands below install the current development version.

```shell
git clone --depth=1 https://github.com/agentic-spring-ai/agentic-spring-ai-extensions.git
cd agentic-spring-ai-extensions
mvn -DskipTests install
```

Import the BOM and add the extensions you need:

```xml
<dependencyManagement>
  <dependencies>
    <dependency>
      <groupId>io.github.agentic-spring-ai</groupId>
      <artifactId>agentic-spring-ai-extensions-bom</artifactId>
      <version>2.1.0-dev</version>
      <type>pom</type>
      <scope>import</scope>
    </dependency>
  </dependencies>
</dependencyManagement>

<dependencies>
  <dependency>
    <groupId>io.github.agentic-spring-ai</groupId>
    <artifactId>agentic-spring-ai-starter-dashscope</artifactId>
  </dependency>
</dependencies>
```

## Modules

| Area | Directory |
| --- | --- |
| Models | [`models`](models) |
| MCP | [`mcp`](mcp) |
| Tool calling | [`tool-calls`](tool-calls) |
| Vector stores | [`vector-stores`](vector-stores) |
| Chat memory | [`memory-repository`](memory-repository) |
| Graph persistence | [`graph-persistence`](graph-persistence) |
| Graph nodes | [`graph-nodes`](graph-nodes) |
| Code executors | [`code-executors`](code-executors) |
| RAG | [`rag`](rag) |
| Document parsers and readers | [`document-parsers`](document-parsers), [`document-readers`](document-readers) |
| Spring Boot integration | [`starters`](starters), [`auto-configurations`](auto-configurations) |
| Prompt management and observation | [`prompt`](prompt), [`observation`](observation) |
| Examples | [`examples`](examples) |

## Documentation

- [Chat model integrations](https://agentic-spring-ai.github.io/website/en/integration/chatmodels/comparison)
- [ChatClient](https://agentic-spring-ai.github.io/website/en/integration/chatclient)
- [Examples](examples)
- [Agentic Spring AI](https://github.com/agentic-spring-ai/agentic-spring-ai)

## Contributing

Issues and pull requests are welcome. Report problems and suggestions through [GitHub Issues](https://github.com/agentic-spring-ai/agentic-spring-ai-extensions/issues).

## License

Agentic Spring AI Extensions is available under the [Apache License 2.0](LICENSE).
