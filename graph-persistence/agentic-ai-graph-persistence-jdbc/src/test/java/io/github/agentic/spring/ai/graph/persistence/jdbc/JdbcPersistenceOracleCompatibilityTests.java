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

import java.sql.SQLException;
import java.time.Duration;

import javax.sql.DataSource;

import oracle.jdbc.datasource.OracleDataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.oracle.OracleContainer;

/**
 * Real Oracle storage compatibility tests for the copied JDBC persistence extension.
 */
@EnabledIfSystemProperty(named = "jdbc.persistence.oracle.enabled", matches = "true",
		disabledReason = "Oracle compatibility requires -Poracle-integration or -Djdbc.persistence.oracle.enabled=true")
@Testcontainers(disabledWithoutDocker = true)
class JdbcPersistenceOracleCompatibilityTests {

	@Container
	private static final OracleContainer ORACLE = new OracleContainer("gvenzl/oracle-free:23.7-slim-faststart")
		.withStartupTimeout(Duration.ofSeconds(600))
		.withConnectTimeoutSeconds(600)
		.withDatabaseName("pdb1")
		.withUsername("testuser")
		.withPassword("testpwd");

	@Test
	void oracleSaverReadsOldWritesAndCoreSaverReadsNewWrites() throws Exception {
		DataSource dataSource = dataSource();
		var oldWriter = io.github.agentic.spring.ai.graph.checkpoint.savers.oracle.OracleSaver.builder()
			.dataSource(dataSource)
			.createOption(io.github.agentic.spring.ai.graph.checkpoint.savers.oracle.CreateOption.CREATE_OR_REPLACE)
			.build();
		var newReader = io.github.agentic.spring.ai.graph.persistence.jdbc.oracle.OracleSaver.builder()
			.dataSource(dataSource)
			.createOption(io.github.agentic.spring.ai.graph.persistence.jdbc.oracle.CreateOption.CREATE_IF_NOT_EXISTS)
			.build();

		JdbcPersistenceExternalCompatibilityTestSupport.assertSaverRoundTrip(oldWriter, newReader, "oracle-old-new",
				"oracle", "core");

		var newWriter = io.github.agentic.spring.ai.graph.persistence.jdbc.oracle.OracleSaver.builder()
			.dataSource(dataSource)
			.createOption(io.github.agentic.spring.ai.graph.persistence.jdbc.oracle.CreateOption.CREATE_OR_REPLACE)
			.build();
		var oldReader = io.github.agentic.spring.ai.graph.checkpoint.savers.oracle.OracleSaver.builder()
			.dataSource(dataSource)
			.createOption(io.github.agentic.spring.ai.graph.checkpoint.savers.oracle.CreateOption.CREATE_IF_NOT_EXISTS)
			.build();

		JdbcPersistenceExternalCompatibilityTestSupport.assertSaverRoundTrip(newWriter, oldReader, "oracle-new-old",
				"oracle", "extension");
	}

	@Test
	void oracleDatabaseStoreReadsOldWritesAndCoreDatabaseStoreReadsNewWrites() throws SQLException {
		DataSource dataSource = dataSource();
		String oldToNewTable = JdbcPersistenceExternalCompatibilityTestSupport.tableName("oron");
		var oldWriter = new io.github.agentic.spring.ai.graph.store.stores.DatabaseStore(dataSource, oldToNewTable);
		var newReader = new DatabaseStore(dataSource, oldToNewTable);

		JdbcPersistenceExternalCompatibilityTestSupport.assertStoreRoundTrip(oldWriter, newReader, "oracle", "core");

		String newToOldTable = JdbcPersistenceExternalCompatibilityTestSupport.tableName("orno");
		var newWriter = new DatabaseStore(dataSource, newToOldTable);
		var oldReader = new io.github.agentic.spring.ai.graph.store.stores.DatabaseStore(dataSource, newToOldTable);

		JdbcPersistenceExternalCompatibilityTestSupport.assertStoreRoundTrip(newWriter, oldReader, "oracle",
				"extension");
	}

	private static DataSource dataSource() throws SQLException {
		OracleDataSource dataSource = new oracle.jdbc.datasource.impl.OracleDataSource();
		dataSource.setURL(ORACLE.getJdbcUrl() + "?oracle.jdbc.provider.json=jackson-json-provider");
		dataSource.setUser(ORACLE.getUsername());
		dataSource.setPassword(ORACLE.getPassword());
		return dataSource;
	}

}
