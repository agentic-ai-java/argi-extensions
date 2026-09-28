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

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Real PostgreSQL storage compatibility tests for the copied JDBC persistence
 * extension.
 */
@Testcontainers(disabledWithoutDocker = true)
class JdbcPersistencePostgresqlCompatibilityTests {

	private static final String DATABASE_NAME = "jdbc_compat";

	@Container
	private static final PostgreSQLContainer<?> POSTGRESQL = new PostgreSQLContainer<>("postgres:17-alpine")
		.withDatabaseName(DATABASE_NAME)
		.withUsername("postgres")
		.withPassword("postgres");

	@Test
	void postgresSaverReadsOldWritesAndCoreSaverReadsNewWrites() throws Exception {
		DataSource dataSource = dataSource();
		var oldWriter = io.github.agentic.spring.ai.graph.checkpoint.savers.postgresql.PostgresSaver.builder()
			.datasource(dataSource)
			.createOption(
					io.github.agentic.spring.ai.graph.checkpoint.savers.postgresql.CreateOption.CREATE_OR_REPLACE)
			.build();
		var newReader = io.github.agentic.spring.ai.graph.persistence.jdbc.postgresql.PostgresSaver.builder()
			.datasource(dataSource)
			.createOption(
					io.github.agentic.spring.ai.graph.persistence.jdbc.postgresql.CreateOption.CREATE_IF_NOT_EXISTS)
			.build();

		JdbcPersistenceExternalCompatibilityTestSupport.assertSaverRoundTrip(oldWriter, newReader,
				"postgres-old-new", "postgresql", "core");

		var newWriter = io.github.agentic.spring.ai.graph.persistence.jdbc.postgresql.PostgresSaver.builder()
			.datasource(dataSource)
			.createOption(
					io.github.agentic.spring.ai.graph.persistence.jdbc.postgresql.CreateOption.CREATE_OR_REPLACE)
			.build();
		var oldReader = io.github.agentic.spring.ai.graph.checkpoint.savers.postgresql.PostgresSaver.builder()
			.datasource(dataSource)
			.createOption(
					io.github.agentic.spring.ai.graph.checkpoint.savers.postgresql.CreateOption.CREATE_IF_NOT_EXISTS)
			.build();

		JdbcPersistenceExternalCompatibilityTestSupport.assertSaverRoundTrip(newWriter, oldReader,
				"postgres-new-old", "postgresql", "extension");
	}

	@Test
	void postgresDatabaseStoreReadsOldWritesAndCoreDatabaseStoreReadsNewWrites() {
		DataSource dataSource = dataSource();
		String oldToNewTable = JdbcPersistenceExternalCompatibilityTestSupport.tableName("pgon");
		var oldWriter = new io.github.agentic.spring.ai.graph.store.stores.DatabaseStore(dataSource, oldToNewTable);
		var newReader = new DatabaseStore(dataSource, oldToNewTable);

		JdbcPersistenceExternalCompatibilityTestSupport.assertStoreRoundTrip(oldWriter, newReader, "postgresql",
				"core");

		String newToOldTable = JdbcPersistenceExternalCompatibilityTestSupport.tableName("pgno");
		var newWriter = new DatabaseStore(dataSource, newToOldTable);
		var oldReader = new io.github.agentic.spring.ai.graph.store.stores.DatabaseStore(dataSource, newToOldTable);

		JdbcPersistenceExternalCompatibilityTestSupport.assertStoreRoundTrip(newWriter, oldReader, "postgresql",
				"extension");
	}

	private static DataSource dataSource() {
		PGSimpleDataSource dataSource = new PGSimpleDataSource();
		dataSource.setServerNames(new String[] { POSTGRESQL.getHost() });
		dataSource.setPortNumbers(new int[] { POSTGRESQL.getFirstMappedPort() });
		dataSource.setDatabaseName(DATABASE_NAME);
		dataSource.setUser(POSTGRESQL.getUsername());
		dataSource.setPassword(POSTGRESQL.getPassword());
		return dataSource;
	}

}
