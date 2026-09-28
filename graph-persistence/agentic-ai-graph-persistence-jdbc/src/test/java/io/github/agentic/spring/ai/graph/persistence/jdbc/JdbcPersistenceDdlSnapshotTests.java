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

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * DDL snapshot tests for the JDBC persistence extension.
 * <p>
 * These tests capture SQL at the JDBC boundary used during initialization and compare
 * it with checked-in snapshots. They intentionally do not reflect private constants or
 * duplicate SQL literals in assertions.
 */
class JdbcPersistenceDdlSnapshotTests {

	@Test
	void h2SaverInitializationMatchesDdlSnapshot() throws Exception {
		RecordingJdbc jdbc = RecordingJdbc.forProduct("H2");

		io.github.agentic.spring.ai.graph.persistence.jdbc.h2.H2Saver.builder()
			.dataSource(jdbc.dataSource())
			.createOption(io.github.agentic.spring.ai.graph.persistence.jdbc.h2.CreateOption.CREATE_OR_REPLACE)
			.build();

		assertThat(toSnapshot(jdbc.ddlStatements())).isEqualTo(resource("ddl/h2-saver.sql"));
	}

	@Test
	void postgresSaverInitializationMatchesDdlSnapshot() throws Exception {
		RecordingJdbc jdbc = RecordingJdbc.forProduct("PostgreSQL");

		io.github.agentic.spring.ai.graph.persistence.jdbc.postgresql.PostgresSaver.builder()
			.datasource(jdbc.dataSource())
			.createOption(
					io.github.agentic.spring.ai.graph.persistence.jdbc.postgresql.CreateOption.CREATE_OR_REPLACE)
			.build();

		assertThat(toSnapshot(jdbc.ddlStatements())).isEqualTo(resource("ddl/postgresql-saver.sql"));
	}

	@Test
	void mysqlSaverInitializationMatchesDdlSnapshot() throws Exception {
		RecordingJdbc jdbc = RecordingJdbc.forProduct("MySQL");

		io.github.agentic.spring.ai.graph.persistence.jdbc.mysql.MysqlSaver.builder()
			.dataSource(jdbc.dataSource())
			.createOption(io.github.agentic.spring.ai.graph.persistence.jdbc.mysql.CreateOption.CREATE_OR_REPLACE)
			.build();

		assertThat(toSnapshot(jdbc.ddlStatements())).isEqualTo(resource("ddl/mysql-saver.sql"));
	}

	@Test
	void oracleSaverInitializationMatchesDdlSnapshot() throws Exception {
		RecordingJdbc jdbc = RecordingJdbc.forProduct("Oracle");

		io.github.agentic.spring.ai.graph.persistence.jdbc.oracle.OracleSaver.builder()
			.dataSource(jdbc.dataSource())
			.createOption(io.github.agentic.spring.ai.graph.persistence.jdbc.oracle.CreateOption.CREATE_OR_REPLACE)
			.build();

		assertThat(toSnapshot(jdbc.ddlStatements())).isEqualTo(resource("ddl/oracle-saver.sql"));
	}

	@Test
	void databaseStoreH2InitializationMatchesDdlSnapshot() throws Exception {
		RecordingJdbc jdbc = RecordingJdbc.forProduct("H2");

		new DatabaseStore(jdbc.dataSource());

		assertThat(toSnapshot(jdbc.ddlStatements())).isEqualTo(resource("ddl/database-store-h2.sql"));
	}

	@Test
	void databaseStoreMysqlInitializationMatchesDdlSnapshot() throws Exception {
		RecordingJdbc jdbc = RecordingJdbc.forProduct("MySQL");

		new DatabaseStore(jdbc.dataSource());

		assertThat(toSnapshot(jdbc.ddlStatements())).isEqualTo(resource("ddl/database-store-mysql.sql"));
	}

	@Test
	void databaseStorePostgresqlInitializationMatchesDdlSnapshot() throws Exception {
		RecordingJdbc jdbc = RecordingJdbc.forProduct("PostgreSQL");

		new DatabaseStore(jdbc.dataSource());

		assertThat(toSnapshot(jdbc.ddlStatements())).isEqualTo(resource("ddl/database-store-postgresql.sql"));
	}

	@Test
	void databaseStoreOracleInitializationMatchesDdlSnapshot() throws Exception {
		RecordingJdbc jdbc = RecordingJdbc.forProduct("Oracle");

		new DatabaseStore(jdbc.dataSource());

		assertThat(toSnapshot(jdbc.ddlStatements())).isEqualTo(resource("ddl/database-store-oracle.sql"));
	}

	private static String toSnapshot(List<String> statements) {
		return String.join("\n\n-- statement-boundary\n\n",
				statements.stream().map(JdbcPersistenceDdlSnapshotTests::normalizeStatement).toList()) + "\n";
	}

	private static String normalizeStatement(String statement) {
		return String.join("\n", statement.lines().map(String::stripTrailing).toList()).strip();
	}

	private static String resource(String path) throws Exception {
		try (var inputStream = JdbcPersistenceDdlSnapshotTests.class.getClassLoader().getResourceAsStream(path)) {
			assertThat(inputStream).as("resource %s", path).isNotNull();
			return new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
		}
	}

	private record RecordingJdbc(DataSource dataSource, List<String> ddlStatements) {

		static RecordingJdbc forProduct(String productName) throws Exception {
			List<String> ddlStatements = new ArrayList<>();
			DataSource dataSource = mock(DataSource.class);
			Connection connection = mock(Connection.class);
			Statement statement = mock(Statement.class);
			DatabaseMetaData metaData = mock(DatabaseMetaData.class);
			ResultSet emptyResultSet = mock(ResultSet.class);

			when(dataSource.getConnection()).thenReturn(connection);
			when(connection.createStatement()).thenReturn(statement);
			when(connection.getMetaData()).thenReturn(metaData);
			when(metaData.getDatabaseProductName()).thenReturn(productName);
			when(metaData.getTables(any(), any(), any(), any(String[].class))).thenReturn(emptyResultSet);
			when(emptyResultSet.next()).thenReturn(false);

			when(statement.execute(anyString())).thenAnswer(invocation -> {
				ddlStatements.add(invocation.getArgument(0));
				return false;
			});
			when(statement.executeUpdate(anyString())).thenAnswer(invocation -> {
				ddlStatements.add(invocation.getArgument(0));
				return 0;
			});
			when(statement.executeQuery(anyString())).thenReturn(emptyResultSet);
			doAnswer(invocation -> {
				ddlStatements.add(invocation.getArgument(0));
				return null;
			}).when(statement).addBatch(anyString());

			return new RecordingJdbc(dataSource, ddlStatements);
		}

	}

}
