#!/usr/bin/env python3
# Copyright 2024-2026 the original author or authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import re
import xml.etree.ElementTree as ET
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description="Migrate ARGI Maven coordinates, preserving POM formatting")
    parser.add_argument("poms", nargs="+", type=Path)
    parser.add_argument("--write", action="store_true", help="Apply changes with exclusive .before-argi-rc1 backups")
    args = parser.parse_args()
    core_artifacts = {
        "argi", "argi-bom", "argi-graph-core", "argi-agent-framework", "argi-studio",
        "argi-starter-builtin-nodes", "argi-starter-graph-observation",
    }
    core_artifacts.update(['argi-analyticdb-store', 'argi-autoconfigure-arms-observation', 'argi-autoconfigure-mcp-distributed', 'argi-autoconfigure-mcp-gateway', 'argi-autoconfigure-mcp-registry', 'argi-autoconfigure-mcp-router', 'argi-autoconfigure-model-chat-memory', 'argi-autoconfigure-model-chat-memory-mem0', 'argi-autoconfigure-model-chat-memory-repository-elasticsearch', 'argi-autoconfigure-model-chat-memory-repository-jdbc', 'argi-autoconfigure-model-chat-memory-repository-memcached', 'argi-autoconfigure-model-chat-memory-repository-mongodb', 'argi-autoconfigure-model-chat-memory-repository-redis', 'argi-autoconfigure-model-chat-memory-repository-tablestore', 'argi-autoconfigure-nacos-prompt', 'argi-autoconfigure-rag-elasticsearch', 'argi-autoconfigure-vector-store-analyticdb', 'argi-autoconfigure-vector-store-oceanbase', 'argi-autoconfigure-vector-store-opensearch', 'argi-autoconfigure-vector-store-tablestore', 'argi-autoconfigure-vector-store-tair', 'argi-code-executor-docker', 'argi-extensions', 'argi-extensions-bom', 'argi-extensions-model', 'argi-graph-node-network', 'argi-graph-node-rag', 'argi-graph-persistence-jdbc', 'argi-graph-persistence-mongodb', 'argi-graph-persistence-redis', 'argi-mcp-common', 'argi-mcp-distributed', 'argi-mcp-gateway', 'argi-mcp-registry', 'argi-mcp-router', 'argi-model-chat-memory-repository-elasticsearch', 'argi-model-chat-memory-repository-jdbc', 'argi-model-chat-memory-repository-mem0', 'argi-model-chat-memory-repository-memcached', 'argi-model-chat-memory-repository-mongodb', 'argi-model-chat-memory-repository-redis', 'argi-model-chat-memory-repository-tablestore', 'argi-observation', 'argi-oceanbase-store', 'argi-opensearch-store', 'argi-prompt-nacos', 'argi-rag', 'argi-sandbox', 'argi-starter-a2a-nacos', 'argi-starter-agentscope', 'argi-starter-arms-observation', 'argi-starter-config-nacos', 'argi-starter-mcp-distributed', 'argi-starter-mcp-gateway', 'argi-starter-mcp-registry', 'argi-starter-mcp-router', 'argi-starter-model-chat-memory', 'argi-starter-model-chat-memory-mem0', 'argi-starter-model-chat-memory-repository-elasticsearch', 'argi-starter-model-chat-memory-repository-jdbc', 'argi-starter-model-chat-memory-repository-memcached', 'argi-starter-model-chat-memory-repository-mongodb', 'argi-starter-model-chat-memory-repository-redis', 'argi-starter-model-chat-memory-repository-tablestore', 'argi-starter-nacos-prompt', 'argi-starter-rag', 'argi-starter-vector-store-analyticdb', 'argi-starter-vector-store-oceanbase', 'argi-starter-vector-store-opensearch', 'argi-starter-vector-store-tablestore', 'argi-starter-vector-store-tair', 'argi-tablestore-store', 'argi-tair-store'])
    # Restrict migration to known ARGI artifacts, preserving application coordinates.
    block = re.compile(r"<(dependency|parent)>.*?</\1>", re.DOTALL)
    coordinate = re.compile(r"(<groupId>\s*)io\.github\.agentic-ai(\s*</groupId>)")
    artifact = re.compile(r"<artifactId>\s*([^<]+?)\s*</artifactId>")
    plans = []
    for path in dict.fromkeys(args.poms):
        ET.parse(path)
        text = path.read_text(encoding="utf-8")
        def migrate(match):
            value = match.group(0)
            name = artifact.search(value)
            return coordinate.sub(r"\g<1>io.github.agentic-ai-java\2", value) if name and name[1] in core_artifacts else value
        updated = block.sub(migrate, text)
        if updated != text:
            plans.append((path, text, updated))
    if args.write:
        for path, text, updated in plans:
            backup = path.with_name(path.name + ".before-argi-rc1")
            with backup.open("x", encoding="utf-8", newline="") as stream:
                stream.write(text)
            path.write_text(updated, encoding="utf-8", newline="")
            print(f"Migrated {path}; backup: {backup}")
    else:
        for path, _, _ in plans:
            print(f"Would migrate {path}")


if __name__ == "__main__":
    main()
