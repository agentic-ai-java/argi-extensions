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
package io.github.agentic.ai.memory.mem0.core;

import com.sun.net.httpserver.HttpServer;
import io.github.agentic.ai.memory.mem0.model.Mem0ServerRequest;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.ai.vectorstore.filter.Filter;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import static io.github.agentic.ai.memory.mem0.core.Mem0FilterExpressionConverter.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests the real Java HTTP client against a loopback stub, not a Mem0 server. */
class Mem0FilterHttpRequestTest {

	private static final JsonMapper JSON = JsonMapper.shared();

	private final AtomicReference<String> requestBody = new AtomicReference<>();

	private HttpServer server;

	private Mem0MemoryStore store;

	@BeforeEach
	void setUp() throws Exception {
		server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		server.createContext("/search", exchange -> {
			try {
				assertThat(exchange.getRequestMethod()).isEqualTo("POST");
				assertThat(exchange.getRequestHeaders().getFirst("Content-Type")).startsWith("application/json");
				requestBody.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
				byte[] response = "{\"results\":[],\"relations\":[]}".getBytes(StandardCharsets.UTF_8);
				exchange.getResponseHeaders().set("Content-Type", "application/json");
				exchange.sendResponseHeaders(200, response.length);
				exchange.getResponseBody().write(response);
			}
			finally {
				exchange.close();
			}
		});
		server.start();
		Mem0Client config = Mem0Client.builder()
			.baseUrl("http://127.0.0.1:" + server.getAddress().getPort())
			.timeoutSeconds(5)
			.maxRetryAttempts(0)
			.build();
		config.getAsync().setEnabled(false);
		store = Mem0MemoryStore
			.builder(new Mem0ServiceClient(config, Mem0Server.builder().build(), new DefaultResourceLoader()))
			.build();
	}

	@AfterEach
	void tearDown() {
		if (server != null) {
			server.stop(0);
		}
	}

	@ParameterizedTest
	@MethodSource("scopes")
	void sendsStructuredFiltersAndPreservesEntityScopes(Map<String, String> scope) {
		Filter.Expression expression = and(or(eq("category", "私密\"\\"), gt("score", 3)),
				not(in("tags", List.of("hidden", "归档"))));
		Mem0ServerRequest.SearchRequest request = Mem0ServerRequest.SearchRequest.mem0Builder()
			.query("寻找记忆")
			.userId(scope.get("user_id"))
			.agentId(scope.get("agent_id"))
			.runId(scope.get("run_id"))
			.topK(3)
			.filterExpression(expression)
			.build();

		assertThat(store.similaritySearch(request)).isEmpty();

		JsonNode body = JSON.readTree(requestBody.get());
		assertThat(body.get("query").asString()).isEqualTo("寻找记忆");
		for (String entity : List.of("user_id", "agent_id", "run_id")) {
			if (scope.containsKey(entity)) {
				assertThat(body.get(entity).asString()).isEqualTo(scope.get(entity));
			}
			else {
				assertThat(body.has(entity)).isFalse();
			}
		}
		assertThat(body.get("filters")).isEqualTo(JSON.valueToTree(Map.of("AND",
				List.of(Map.of("OR", List.of(Map.of("category", "私密\"\\"), Map.of("score", Map.of("gt", 3)))),
						Map.of("NOT", List.of(Map.of("tags", Map.of("in", List.of("hidden", "归档")))))))));
		assertThat(request.getUserId()).isEqualTo(scope.get("user_id"));
		assertThat(request.getAgentId()).isEqualTo(scope.get("agent_id"));
		assertThat(request.getRunId()).isEqualTo(scope.get("run_id"));
	}

	static Stream<Arguments> scopes() {
		return Stream.of(Arguments.of(Map.of("user_id", "user-1")), Arguments.of(Map.of("agent_id", "agent-1")),
				Arguments.of(Map.of("run_id", "run-1")),
				Arguments.of(Map.of("user_id", "user-1", "agent_id", "agent-1", "run_id", "run-1")));
	}

	@Test
	void preservesExplicitFiltersWhenThereIsNoExpression() {
		Map<String, Object> filters = Map.of("category", "private");
		store.similaritySearch(
				Mem0ServerRequest.SearchRequest.mem0Builder().query("query").userId("user-1").filters(filters).build());
		assertThat(JSON.readTree(requestBody.get()).get("filters")).isEqualTo(JSON.valueToTree(filters));
	}

	@Test
	void invalidLogicalOperandFailsBeforeSendingARequest() {
		Filter.Expression invalid = new Filter.Expression(Filter.ExpressionType.OR, new Filter.Key("category"),
				eq("score", 1));
		Mem0ServerRequest.SearchRequest request = Mem0ServerRequest.SearchRequest.mem0Builder()
			.query("query")
			.userId("user-1")
			.filterExpression(invalid)
			.build();
		assertThatThrownBy(() -> store.similaritySearch(request)).isInstanceOf(IllegalArgumentException.class);
		assertThat(requestBody.get()).isNull();
	}

}
