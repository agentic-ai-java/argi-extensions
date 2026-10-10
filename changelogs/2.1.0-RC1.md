# ARGI Extensions 2.1.0-RC1

本候选版本以 `io.github.agentic-ai-java` 为 Maven groupId，配套 ARGI Core `2.1.0-RC1`。Java 包名继续使用 `io.github.agentic.ai`，运行时要求 Java 17 或更高版本。

## 发布内容

发布 73 个 Maven 模块：父 POM、Extensions BOM 和 71 个扩展模块。BOM 新增此前遗漏的模型契约、图持久化、Docker 执行器、图节点、AgentScope/Nacos starter 和 sandbox 的版本管理。

CI 删除检出和构建 Core 的跨仓库 action，直接解析 Central 已发布的 Core。发布使用单个 JDK 17 job，生成并校验 POM、jar、源码、Javadoc、GPG 签名和 Central ZIP，然后上传 Central 并公开 GitHub 预发布。

## 使用与迁移

将 ARGI Core 和 Extensions 的 Maven groupId 改为 `io.github.agentic-ai-java`，将本次使用的版本改为 `2.1.0-RC1`。使用 Core 和可选集成的应用分别导入 `argi-bom` 与 `argi-extensions-bom`，Core BOM 不会导入 Extensions BOM。

```xml
<dependencyManagement>
  <dependencies>
    <dependency>
      <groupId>io.github.agentic-ai-java</groupId>
      <artifactId>argi-bom</artifactId>
      <version>2.1.0-RC1</version>
      <type>pom</type>
      <scope>import</scope>
    </dependency>
    <dependency>
      <groupId>io.github.agentic-ai-java</groupId>
      <artifactId>argi-extensions-bom</artifactId>
      <version>2.1.0-RC1</version>
      <type>pom</type>
      <scope>import</scope>
    </dependency>
  </dependencies>
</dependencyManagement>
```

迁移脚本及回滚步骤见 [发布操作说明](RELEASING.md)。本次发布跳过 Java 单元、容器及跨仓库集成测试；制品校验通过不能替代这些运行时测试。
