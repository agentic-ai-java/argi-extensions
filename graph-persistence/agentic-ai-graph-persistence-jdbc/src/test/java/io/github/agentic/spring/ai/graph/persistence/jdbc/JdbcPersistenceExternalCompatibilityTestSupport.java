/*
 * Copyright 2024-2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.agentic.spring.ai.graph.persistence.jdbc;

import io.github.agentic.spring.ai.graph.RunnableConfig;
import io.github.agentic.spring.ai.graph.checkpoint.BaseCheckpointSaver;
import io.github.agentic.spring.ai.graph.checkpoint.Checkpoint;
import io.github.agentic.spring.ai.graph.store.Store;
import io.github.agentic.spring.ai.graph.store.StoreItem;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

final class JdbcPersistenceExternalCompatibilityTestSupport {

	private JdbcPersistenceExternalCompatibilityTestSupport() {
	}

	static void assertSaverRoundTrip(BaseCheckpointSaver writer, BaseCheckpointSaver reader, String threadName,
			String dialect, String writerName) throws Exception {
		RunnableConfig config = RunnableConfig.builder().threadId(threadName).build();
		Checkpoint checkpoint = checkpoint(dialect, writerName);

		writer.put(config, checkpoint);

		Optional<Checkpoint> restored = reader.get(config);
		assertThat(restored).isPresent();
		assertCheckpoint(restored.get(), checkpoint);
	}

	static void assertStoreRoundTrip(Store writer, Store reader, String dialect, String writerName) {
		List<String> namespace = List.of("jdbc-compat", dialect, writerName);
		String key = "profile-" + UUID.randomUUID();
		StoreItem item = StoreItem.of(namespace, key,
				Map.of("dialect", dialect, "writer", writerName, "compatible", true));

		writer.putItem(item);

		Optional<StoreItem> restored = reader.getItem(namespace, key);
		assertThat(restored).isPresent();
		assertThat(restored.get().getNamespace()).isEqualTo(item.getNamespace());
		assertThat(restored.get().getKey()).isEqualTo(item.getKey());
		assertThat(restored.get().getValue()).isEqualTo(item.getValue());
	}

	static String tableName(String prefix) {
		return prefix + "_" + UUID.randomUUID().toString().replace("-", "").substring(0, 12);
	}

	private static Checkpoint checkpoint(String dialect, String writerName) {
		return Checkpoint.builder()
			.id(UUID.randomUUID().toString())
			.nodeId("node-" + writerName)
			.nextNodeId("next")
			.state(Map.of("dialect", dialect, "writer", writerName, "sequence", 1))
			.build();
	}

	private static void assertCheckpoint(Checkpoint actual, Checkpoint expected) {
		assertThat(actual.getId()).isEqualTo(expected.getId());
		assertThat(actual.getNodeId()).isEqualTo(expected.getNodeId());
		assertThat(actual.getNextNodeId()).isEqualTo(expected.getNextNodeId());
		assertThat(actual.getState()).isEqualTo(expected.getState());
	}

}
