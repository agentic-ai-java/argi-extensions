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
import io.github.agentic.spring.ai.graph.checkpoint.Checkpoint;
import io.github.agentic.spring.ai.graph.store.StoreItem;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * H2 storage compatibility tests between the Core JDBC implementations and the
 * Extensions JDBC implementations.
 */
class JdbcPersistenceH2CompatibilityTests {

	@Test
	void h2SaverReadsCheckpointWrittenByCoreSaver() throws Exception {
		JdbcDataSource dataSource = h2DataSource("old_write_new_read_saver");
		var oldSaver = io.github.agentic.spring.ai.graph.checkpoint.savers.h2.H2Saver.builder()
			.dataSource(dataSource)
			.createOption(io.github.agentic.spring.ai.graph.checkpoint.savers.h2.CreateOption.CREATE_OR_REPLACE)
			.build();
		var newSaver = io.github.agentic.spring.ai.graph.persistence.jdbc.h2.H2Saver.builder()
			.dataSource(dataSource)
			.createOption(io.github.agentic.spring.ai.graph.persistence.jdbc.h2.CreateOption.CREATE_IF_NOT_EXISTS)
			.build();
		RunnableConfig config = RunnableConfig.builder().threadId("thread-a").build();
		Checkpoint checkpoint = checkpoint("checkpoint-a", Map.of("answer", 42, "tags", List.of("old", "new")));

		oldSaver.put(config, checkpoint);

		Optional<Checkpoint> restored = newSaver.get(config);
		assertThat(restored).isPresent();
		assertCheckpoint(restored.get(), checkpoint);
	}

	@Test
	void coreH2SaverReadsCheckpointWrittenByExtensionSaver() throws Exception {
		JdbcDataSource dataSource = h2DataSource("new_write_old_read_saver");
		var newSaver = io.github.agentic.spring.ai.graph.persistence.jdbc.h2.H2Saver.builder()
			.dataSource(dataSource)
			.createOption(io.github.agentic.spring.ai.graph.persistence.jdbc.h2.CreateOption.CREATE_OR_REPLACE)
			.build();
		var oldSaver = io.github.agentic.spring.ai.graph.checkpoint.savers.h2.H2Saver.builder()
			.dataSource(dataSource)
			.createOption(io.github.agentic.spring.ai.graph.checkpoint.savers.h2.CreateOption.CREATE_IF_NOT_EXISTS)
			.build();
		RunnableConfig config = RunnableConfig.builder().threadId("thread-b").build();
		Checkpoint checkpoint = checkpoint("checkpoint-b", Map.of("message", "rollback-compatible"));

		newSaver.put(config, checkpoint);

		Optional<Checkpoint> restored = oldSaver.get(config);
		assertThat(restored).isPresent();
		assertCheckpoint(restored.get(), checkpoint);
	}

	@Test
	void databaseStoreReadsItemWrittenByCoreDatabaseStore() {
		JdbcDataSource dataSource = h2DataSource("old_write_new_read_store");
		var oldStore = new io.github.agentic.spring.ai.graph.store.stores.DatabaseStore(dataSource);
		var newStore = new DatabaseStore(dataSource);
		StoreItem item = StoreItem.of(List.of("compat", "core"), "profile",
				Map.of("name", "old", "level", 1));

		oldStore.putItem(item);

		Optional<StoreItem> restored = newStore.getItem(item.getNamespace(), item.getKey());
		assertThat(restored).isPresent();
		assertThat(restored.get().getNamespace()).isEqualTo(item.getNamespace());
		assertThat(restored.get().getKey()).isEqualTo(item.getKey());
		assertThat(restored.get().getValue()).isEqualTo(item.getValue());
	}

	@Test
	void coreDatabaseStoreReadsItemWrittenByExtensionDatabaseStore() {
		JdbcDataSource dataSource = h2DataSource("new_write_old_read_store");
		var newStore = new DatabaseStore(dataSource);
		var oldStore = new io.github.agentic.spring.ai.graph.store.stores.DatabaseStore(dataSource);
		StoreItem item = StoreItem.of(List.of("compat", "extension"), "profile",
				Map.of("name", "new", "level", 2));

		newStore.putItem(item);

		Optional<StoreItem> restored = oldStore.getItem(item.getNamespace(), item.getKey());
		assertThat(restored).isPresent();
		assertThat(restored.get().getNamespace()).isEqualTo(item.getNamespace());
		assertThat(restored.get().getKey()).isEqualTo(item.getKey());
		assertThat(restored.get().getValue()).isEqualTo(item.getValue());
	}

	private static JdbcDataSource h2DataSource(String name) {
		JdbcDataSource dataSource = new JdbcDataSource();
		dataSource.setURL("jdbc:h2:mem:" + name + "_" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE");
		dataSource.setUser("sa");
		dataSource.setPassword("");
		return dataSource;
	}

	private static Checkpoint checkpoint(String id, Map<String, Object> state) {
		return Checkpoint.builder().id(id).nodeId("node").nextNodeId("next").state(state).build();
	}

	private static void assertCheckpoint(Checkpoint actual, Checkpoint expected) {
		assertThat(actual.getId()).isEqualTo(expected.getId());
		assertThat(actual.getNodeId()).isEqualTo(expected.getNodeId());
		assertThat(actual.getNextNodeId()).isEqualTo(expected.getNextNodeId());
		assertThat(actual.getState()).isEqualTo(expected.getState());
	}

}
