# [Agentic Spring AI Extensions](https://agentic-spring-ai.github.io/website/)

[English](README.md) | [简体中文](README-zh.md)

[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](LICENSE)
[![Version](https://img.shields.io/badge/version-2.1.0--dev-blue)](https://github.com/agentic-spring-ai/agentic-spring-ai-extensions)

Agentic Spring AI Extensions 为 Spring AI 提供模型、MCP、工具调用、向量存储、聊天记忆、检索增强生成（RAG）、文档处理、提示词管理和可观测性扩展。开发者可以直接在 Spring AI 中使用这些模块，也可以配合 [Agentic Spring AI](https://github.com/agentic-spring-ai/agentic-spring-ai) 框架构建智能体应用。

## 核心能力

- **模型**：提供 DashScope 聊天、图像、向量、语音合成和语音识别实现。
- **MCP**：提供注册中心、路由、分布式服务和网关模块。
- **工具调用**：集成搜索、翻译、地图、存储、协作等服务。
- **数据与记忆**：提供常用数据库和云服务的向量存储与聊天记忆实现。
- **RAG 与文档处理**：提供可复用的 RAG 组件、文档解析器和文档读取器。
- **运行管理**：提供 Nacos 提示词管理和 ARMS 可观测性集成。
- **厂商相关示例**：提供 DashScope 多模态、语音智能体和 AgentScope 集成示例。

## 快速开始

环境要求：JDK 17 或更高版本、Maven 3.9.1 或更高版本。以下命令用于安装当前开发版本。

```shell
git clone --depth=1 https://github.com/agentic-spring-ai/agentic-spring-ai-extensions.git
cd agentic-spring-ai-extensions
mvn -DskipTests install
```

导入 BOM，然后添加需要的扩展：

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

## 项目模块

| 领域 | 目录 |
| --- | --- |
| 模型与文档契约 | [`agentic-spring-ai-extensions-model`](agentic-spring-ai-extensions-model) |
| 模型 | [`models`](models) |
| MCP | [`mcp`](mcp) |
| 工具调用 | [`tool-calls`](tool-calls) |
| 向量存储 | [`vector-stores`](vector-stores) |
| 聊天记忆 | [`memory-repository`](memory-repository) |
| 图节点 | [`graph-nodes`](graph-nodes) |
| RAG | [`rag`](rag) |
| 文档解析与读取 | [`document-parsers`](document-parsers)、[`document-readers`](document-readers) |
| Spring Boot 集成 | [`starters`](starters)、[`auto-configurations`](auto-configurations) |
| 提示词管理与可观测性 | [`prompt`](prompt)、[`observation`](observation) |
| 示例 | [`examples`](examples) |

## 文档

- [聊天模型集成](https://agentic-spring-ai.github.io/website/integration/chatmodels/comparison)
- [ChatClient](https://agentic-spring-ai.github.io/website/integration/chatclient)
- [示例项目](examples)
- [Agentic Spring AI](https://github.com/agentic-spring-ai/agentic-spring-ai)

## 参与贡献

欢迎提交 Issue 和 Pull Request。问题和建议可通过 [GitHub Issues](https://github.com/agentic-spring-ai/agentic-spring-ai-extensions/issues) 反馈。

## 许可证

本项目采用 [Apache License 2.0](LICENSE) 许可证。
