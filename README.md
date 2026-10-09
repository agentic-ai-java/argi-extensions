<div align="center">
  <a href="https://agentic-ai-java.github.io/argi-website/en/">
    <img src="asset/images/logo.svg" alt="ARGI Extensions logo" width="180">
  </a>
  <h1>ARGI Extensions</h1>
  <p><strong>Spring AI integrations and ecosystem extensions for Java applications.</strong></p>
  <p>MCP · Vector stores · Chat memory · RAG · Observability</p>
  <p>
    <a href="https://agentic-ai-java.github.io/argi-website/en/">Documentation</a> ·
    <a href="https://agentic-ai-java.github.io/argi-website/en/integration/chatclient">Quick Start</a> ·
    <a href="https://github.com/agentic-ai-java/argi">ARGI</a> ·
    <a href="README-zh.md">简体中文</a>
  </p>
  <p>
    <a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache%202-4EB1BA.svg" alt="License"></a>
    <a href="https://github.com/agentic-ai-java/argi-extensions"><img src="https://img.shields.io/badge/version-2.1.0--RC1-blue" alt="Version"></a>
    <img src="https://img.shields.io/badge/Java-17%2B-f59e0b" alt="Java 17+">
  </p>
</div>

---

ARGI stands for **Agent Runtime and Graph Intelligence** and is pronounced **"AR-jee"** (`/ˈɑːr.dʒiː/`). ARGI Extensions provides Spring AI integrations for MCP, vector stores, chat memory, RAG, prompt management, and observability. Use these modules directly with Spring AI or combine them with the [ARGI](https://github.com/agentic-ai-java/argi) framework.

## Features

- **MCP**: registry, router, distributed service, and gateway modules.
- **Data and memory**: vector stores and chat memory repositories for common databases and cloud services.
- **RAG**: reusable retrieval-augmented generation components.
- **Operations**: Nacos prompt management and ARMS observation integration.

## Quick Start

Requirements: JDK 17 or later and Maven 3.9.1 or later. Install the local development modules with Maven:

```shell
git clone --depth=1 https://github.com/agentic-ai-java/argi-extensions.git
cd argi-extensions
mvn -DskipTests install
```

Import the Extensions BOM and add the starters you need to your project:

```xml
<dependencyManagement>
  <dependencies>
    <dependency>
      <groupId>io.github.agentic-ai-java</groupId>
      <artifactId>argi-extensions-bom</artifactId>
      <version>2.1.0-RC1</version>
      <type>pom</type>
      <scope>import</scope>
    </dependency>
  </dependencies>
</dependencyManagement>

<dependencies>
  <dependency>
    <groupId>io.github.agentic-ai-java</groupId>
    <artifactId>argi-starter-mcp-registry</artifactId>
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

- [Overview](https://agentic-ai-java.github.io/argi-website/en/)
- [ChatClient](https://agentic-ai-java.github.io/argi-website/en/integration/chatclient)
- [ARGI Framework](https://github.com/agentic-ai-java/argi)
- [Examples](https://github.com/agentic-ai-java/argi-examples/tree/main/examples)

## Contributing

Issues and pull requests are welcome. Report problems and suggestions through [GitHub Issues](https://github.com/agentic-ai-java/argi-extensions/issues).

<a href="https://github.com/agentic-ai-java/argi-extensions/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=agentic-ai-java/argi-extensions&max=500&columns=18&anon=1" alt="contributors"/>
</a>

## License

ARGI Extensions is available under the [Apache License 2.0](LICENSE).
