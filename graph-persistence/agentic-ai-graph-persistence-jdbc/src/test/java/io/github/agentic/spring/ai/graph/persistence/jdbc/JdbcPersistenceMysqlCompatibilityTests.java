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

import com.mysql.cj.jdbc.MysqlDataSource;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Real MySQL storage compatibility tests for the copied JDBC persistence extension.
 */
@Testcontainers(disabledWithoutDocker = true)
class JdbcPersistenceMysqlCompatibilityTests {

	@Container
	private static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.0")
		.withDatabaseName("jdbc_compat")
		.withUsername("testuser")
		.withPassword("testpwd");

	@Test
	void mysqlSaverReadsOldWritesAndCoreSaverReadsNewWrites() throws Exception {
		DataSource dataSource = dataSource();
		var oldWriter = io.github.agentic.spring.ai.graph.checkpoint.savers.mysql.MysqlSaver.builder()
			.dataSource(dataSource)
			.createOption(io.github.agentic.spring.ai.graph.checkpoint.savers.mysql.CreateOption.CREATE_OR_REPLACE)
			.build();
		var newReader = io.github.agentic.spring.ai.graph.persistence.jdbc.mysql.MysqlSaver.builder()
			.dataSource(dataSource)
			.createOption(io.github.agentic.spring.ai.graph.persistence.jdbc.mysql.CreateOption.CREATE_IF_NOT_EXISTS)
			.build();

		JdbcPersistenceExternalCompatibilityTestSupport.assertSaverRoundTrip(oldWriter, newReader, "mysql-old-new",
				"mysql", "core");

		var newWriter = io.github.agentic.spring.ai.graph.persistence.jdbc.mysql.MysqlSaver.builder()
			.dataSource(dataSource)
			.createOption(io.github.agentic.spring.ai.graph.persistence.jdbc.mysql.CreateOption.CREATE_OR_REPLACE)
			.build();
		var oldReader = io.github.agentic.spring.ai.graph.checkpoint.savers.mysql.MysqlSaver.builder()
			.dataSource(dataSource)
			.createOption(io.github.agentic.spring.ai.graph.checkpoint.savers.mysql.CreateOption.CREATE_IF_NOT_EXISTS)
			.build();

		JdbcPersistenceExternalCompatibilityTestSupport.assertSaverRoundTrip(newWriter, oldReader, "mysql-new-old",
				"mysql", "extension");
	}

	@Test
	void mysqlDatabaseStoreReadsOldWritesAndCoreDatabaseStoreReadsNewWrites() {
		DataSource dataSource = dataSource();
		String oldToNewTable = JdbcPersistenceExternalCompatibilityTestSupport.tableName("myon");
		var oldWriter = new io.github.agentic.spring.ai.graph.store.stores.DatabaseStore(dataSource, oldToNewTable);
		var newReader = new DatabaseStore(dataSource, oldToNewTable);

		JdbcPersistenceExternalCompatibilityTestSupport.assertStoreRoundTrip(oldWriter, newReader, "mysql", "core");

		String newToOldTable = JdbcPersistenceExternalCompatibilityTestSupport.tableName("myno");
		var newWriter = new DatabaseStore(dataSource, newToOldTable);
		var oldReader = new io.github.agentic.spring.ai.graph.store.stores.DatabaseStore(dataSource, newToOldTable);

		JdbcPersistenceExternalCompatibilityTestSupport.assertStoreRoundTrip(newWriter, oldReader, "mysql",
				"extension");
	}

	private static DataSource dataSource() {
		MysqlDataSource dataSource = new MysqlDataSource();
		dataSource.setURL(MYSQL.getJdbcUrl());
		dataSource.setUser(MYSQL.getUsername());
		dataSource.setPassword(MYSQL.getPassword());
		return dataSource;
	}

}
