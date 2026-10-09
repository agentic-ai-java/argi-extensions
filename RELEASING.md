# Extensions 发布操作说明

本流程使用 GitHub Actions 发布 `2.1.0-RC1`，依赖 Central 已公开的 Core `2.1.0-RC1`。按照本次用户授权，不运行本地构建或测试，不运行跨仓库集成测试，不重复运行全量 CI。

## 准备与发布

1. 根 POM 和独立 BOM 的 `revision` 均为 `2.1.0-RC1`，groupId 为 `io.github.agentic-ai-java`；`argi.version` 固定为 Core `2.1.0-RC1`。
2. 准备提交使用 `[skip ci]`。创建 `v2.1.0-RC1` 标签及指向同一提交 SHA 的 GitHub Release 草稿。
3. 在仓库 secrets 配置 `CENTRAL_USERNAME`、`CENTRAL_PASSWORD` 和 `GPG_PRIVATE_KEY`，私钥有口令时配置 `GPG_PASSPHRASE`。Central 用户令牌须有上述 namespace 的发布权限。凭据仅经 stdin 写入 secrets，不提交到仓库。
4. 手动启动 Release workflow，输入标签并设置 `publish=true`。一个 job 先运行快速发布工具回归，再以 JDK 17、Maven wrapper 3.9.16 执行 `./mvnw -B -Prelease -Dmaven.test.skip=true verify`。发布 profile 使用标准编译器和 Spring Boot 配置处理器，避免 Error Prone 对构建 JDK 的额外要求。
5. 制品校验器检查 73 个模块的坐标、父 POM、完整 BOM、Java 17 字节码、jar 和签名，生成并复核不含 Maven metadata 的 Central ZIP。官方 API 上传同一 ZIP 并等待 `PUBLISHED`，随后公开 GitHub 预发布。签名制品、ZIP 和 deployment 状态归档到 Actions artifact。
6. 检查 Central 公开的 POM、jar、sources、Javadoc 及签名。成功后将 Extensions 的 `revision` 更新为 `2.1.0-RC2-SNAPSHOT`，Core 依赖仍保留已公开的 RC1，使用 `[skip ci]` 推送。

割接窗口从手动启动发布到 Central 与 GitHub 预发布完成；期间不移动候选标签。只有发布工具回归、编译、打包及签名/制品校验在本次执行范围内，Java 测试及覆盖率不在本次验证范围内。

## 迁移与回滚

```shell
python3 tools/scripts/migrate-maven-coordinates.py /path/to/application/pom.xml
python3 tools/scripts/migrate-maven-coordinates.py --write /path/to/application/pom.xml
```

脚本仅迁移本次已知的 Core 和 Extensions 父 POM、依赖坐标，并以独占方式创建 `pom.xml.before-argi-rc1` 备份；不会修改应用自身坐标或版本。预览后执行写入，另将依赖版本更新为 RC1，并导入所需的两个 BOM。消费方回滚时恢复备份 POM 和原版本，涉及持久化数据时恢复相应业务备份。

仓库配置修改前保留 tar 备份；发布前可以恢复准备提交及草稿。Central 公开版本不能覆盖或撤回。公开后有问题时保留原标签和制品，发布新 RC 修复；Central 成功但 GitHub 失败时仅补公开 GitHub Release。Central 状态不明确时先检查归档的 deployment ID 与 Portal 状态，避免重复上传。

本次发布配置为直接替换，无额外数据库迁移。既有历史版本和 GitHub Releases 保留。
