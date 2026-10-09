<div align="center">
  <a href="https://agentic-ai-java.github.io/argi-website/">
    <img src="asset/images/logo.svg" alt="ARGI Extensions logo" width="180">
  </a>
  <h1>ARGI Extensions</h1>
  <p><strong>面向 Java 应用的 Spring AI 集成与生态扩展。</strong></p>
  <p>MCP · 向量存储 · 聊天记忆 · RAG · 可观测性</p>
  <p>
    <a href="https://agentic-ai-java.github.io/argi-website/">文档</a> ·
    <a href="https://agentic-ai-java.github.io/argi-website/integration/chatclient">快速开始</a> ·
    <a href="https://github.com/agentic-ai-java/argi">ARGI</a> ·
    <a href="README.md">English</a>
  </p>
  <p>
    <a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache%202-4EB1BA.svg" alt="License"></a>
    <a href="https://github.com/agentic-ai-java/argi-extensions"><img src="https://img.shields.io/badge/version-2.1.0--RC1-blue" alt="Version"></a>
    <img src="https://img.shields.io/badge/Java-17%2B-f59e0b" alt="Java 17+">
  </p>
</div>

---

ARGI 是 **Agent Runtime and Graph Intelligence** 的缩写，读作 **“AR-jee”**（`/ˈɑːr.dʒiː/`）。ARGI Extensions 为 Spring AI 提供 MCP、向量存储、聊天记忆、检索增强生成（RAG）、提示词管理和可观测性扩展。开发者可以直接在 Spring AI 中使用这些模块，也可以配合 [ARGI](https://github.com/agentic-ai-java/argi) 框架构建智能体应用。

## 核心能力

- **MCP**：提供注册中心、路由、分布式服务和网关模块。
- **数据与记忆**：提供常用数据库和云服务的向量存储与聊天记忆实现。
- **RAG**：提供可复用的检索增强生成组件。
- **运行管理**：提供 Nacos 提示词管理和 ARMS 可观测性集成。

## 快速开始

环境要求：JDK 17 或更高版本、Maven 3.9.1 或更高版本。通过 Maven 安装当前开发版本：

```shell
git clone --depth=1 https://github.com/agentic-ai-java/argi-extensions.git
cd argi-extensions
mvn -DskipTests install
```

导入 Extensions BOM 并引入所需 Starter：

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

## 项目模块

| 模块 | 说明 |
| --- | --- |
| [MCP](mcp) | MCP 注册中心、路由、网关及服务发现集成 |
| [Vector Stores](vector-stores) | AnalyticDB、OceanBase、OpenSearch、TableStore 与 Tair 向量存储 |
| [Memory Repository](memory-repository) | Redis、MongoDB、Elasticsearch、Memcached、TableStore 与 Mem0 聊天记忆实现 |
| [RAG](rag) | 可复用的检索增强生成组件 |
| [Starters](starters) | 统一的 Spring Boot Starter 依赖管理 |
| [Auto-Configurations](auto-configurations) | 自动装配模块 |
| [Prompt](prompt) | 基于 Nacos 的动态提示词管理 |
| [Observation](observation) | 应用可观测性与 ARMS 集成 |

## 文档

- [项目概览](https://agentic-ai-java.github.io/argi-website/docs/overview)
- [快速开始](https://agentic-ai-java.github.io/argi-website/docs/quick-start)
- [ChatClient](https://agentic-ai-java.github.io/argi-website/integration/chatclient)
- [ARGI 核心框架](https://github.com/agentic-ai-java/argi)
- [示例项目](https://github.com/agentic-ai-java/argi-examples/tree/main/examples)

## 参与贡献

欢迎提交 Issue 和 Pull Request。问题和建议可通过 [GitHub Issues](https://github.com/agentic-ai-java/argi-extensions/issues) 反馈。

<a href="https://github.com/agentic-ai-java/argi-extensions/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=agentic-ai-java/argi-extensions&max=500&columns=18&anon=1" alt="contributors"/>
</a>

## 许可证

本项目采用 [Apache License 2.0](LICENSE) 许可证。
