<div align="center">
  <a href="https://agentic-spring-ai.github.io/website/en/">
    <img src="asset/images/logo.svg" alt="Agentic AI Extensions logo" width="180">
  </a>
  <h1>Agentic AI Extensions</h1>
  <p><strong>Spring AI integrations and ecosystem extensions for Java applications.</strong></p>
  <p>MCP · Vector stores · Chat memory · RAG · Observability</p>
  <p>
    <a href="https://agentic-spring-ai.github.io/website/en/">Documentation</a> ·
    <a href="https://agentic-spring-ai.github.io/website/en/integration/chatclient">Quick Start</a> ·
    <a href="https://github.com/agentic-spring-ai/agentic-spring-ai">Agentic AI</a> ·
    <a href="README-zh.md">简体中文</a>
  </p>
  <p>
    <a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache%202-4EB1BA.svg" alt="License"></a>
    <a href="https://github.com/agentic-spring-ai/agentic-spring-ai-extensions"><img src="https://img.shields.io/badge/version-2.1.0--dev-blue" alt="Version"></a>
    <img src="https://img.shields.io/badge/Java-17%2B-f59e0b" alt="Java 17+">
  </p>
</div>

---

Agentic AI Extensions provides Spring AI integrations for MCP, vector stores, chat memory, RAG, prompt management, and observability. Use these modules directly with Spring AI or combine them with the [Agentic AI](https://github.com/agentic-spring-ai/agentic-spring-ai) framework.

## Features

- **MCP**: registry, router, distributed service, and gateway modules.
- **Data and memory**: vector stores and chat memory repositories for common databases and cloud services.
- **RAG**: reusable retrieval-augmented generation components.
- **Operations**: Nacos prompt management and ARMS observation integration.

## Quick Start

Requirements: JDK 17 or later and Maven 3.9.1 or later. Install the local development modules with Maven:

```shell
git clone --depth=1 https://github.com/agentic-spring-ai/agentic-spring-ai-extensions.git
cd agentic-spring-ai-extensions
mvn -DskipTests install
```

Import the Extensions BOM and add the starters you need to your project:

```xml
<dependencyManagement>
  <dependencies>
    <dependency>
      <groupId>io.github.agentic-ai</groupId>
      <artifactId>agentic-ai-extensions-bom</artifactId>
      <version>2.1.0-dev</version>
      <type>pom</type>
      <scope>import</scope>
    </dependency>
  </dependencies>
</dependencyManagement>

<dependencies>
  <dependency>
    <groupId>io.github.agentic-ai</groupId>
    <artifactId>agentic-ai-starter-mcp-registry</artifactId>
  </dependency>
</dependencies>
```

## Modules

| Module | Description |
| --- | --- |
| [MCP](mcp) | Model Context Protocol registry, router, gateway, and discovery integrations |
| [Vector Stores](vector-stores) | AnalyticDB, OceanBase, OpenSearch, TableStore, and Tair vector stores |
| [Memory Repository](memory-repository) | Chat memory implementations for Redis, MongoDB, Elasticsearch, Memcached, TableStore, and Mem0 |
| [RAG](rag) | Reusable retrieval-augmented generation components |
| [Starters](starters) | Spring Boot starters for convenient dependency management |
| [Auto-Configurations](auto-configurations) | Spring Boot auto-configuration modules |
| [Prompt](prompt) | Dynamic prompt management with Nacos integration |
| [Observation](observation) | Application observability and ARMS integration |

## Documentation

- [Overview](https://agentic-spring-ai.github.io/website/en/)
- [ChatClient](https://agentic-spring-ai.github.io/website/en/integration/chatclient)
- [Agentic AI Framework](https://github.com/agentic-spring-ai/agentic-spring-ai)
- [Examples](https://github.com/agentic-spring-ai/examples/tree/main/examples)

## Contributing

Issues and pull requests are welcome. Report problems and suggestions through [GitHub Issues](https://github.com/agentic-spring-ai/agentic-spring-ai-extensions/issues).

<a href="https://github.com/agentic-spring-ai/agentic-spring-ai-extensions/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=agentic-spring-ai/agentic-spring-ai-extensions&max=500&columns=18&anon=1" alt="contributors"/>
</a>

## License

Agentic AI Extensions is available under the [Apache License 2.0](LICENSE).
