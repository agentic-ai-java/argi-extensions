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
package io.github.agentic.ai.memory.redis;

import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import ch.qos.logback.classic.Level;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.ai.chat.messages.UserMessage;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import redis.clients.jedis.Jedis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@ResourceLock("redis-deserialization-loggers")
class RedisDeserializationLoggingIntegrationTest {

	private static GenericContainer<?> redis;

	private static String host;

	private static int port;

	@BeforeAll
	static void startRedis() {
		String externalPort = System.getProperty("redis.test.port");
		if (externalPort != null) {
			host = System.getProperty("redis.test.host", "127.0.0.1");
			port = Integer.parseInt(externalPort);
		}
		else {
			assumeTrue(DockerClientFactory.instance().isDockerAvailable(),
					"Docker or an explicit isolated Redis (redis.test.port) is required");
			redis = new GenericContainer<>("redis:7.0.15").withExposedPorts(6379);
			redis.start();
			host = redis.getHost();
			port = redis.getMappedPort(6379);
		}
	}

	@AfterAll
	static void stopRedis() {
		if (redis != null) {
			redis.stop();
		}
	}

	static Stream<Arguments> clientsAndLogLevels() {
		return Stream.of("jedis", "lettuce", "redisson")
			.flatMap(client -> Stream.of(Level.INFO, Level.DEBUG).map(level -> Arguments.of(client, level)));
	}

	private static BaseRedisChatMemoryRepository create(String client) {
		return switch (client) {
			case "jedis" -> JedisRedisChatMemoryRepository.builder().host(host).port(port).build();
			case "lettuce" -> LettuceRedisChatMemoryRepository.builder().host(host).port(port).build();
			case "redisson" -> RedissonRedisChatMemoryRepository.builder().host(host).port(port).build();
			default -> throw new IllegalArgumentException("Unknown test client");
		};
	}

	@ParameterizedTest
	@MethodSource("clientsAndLogLevels")
	void readingMixedRecordsKeepsExistingBehaviorWithoutLoggingContents(String client, Level level) throws Exception {
		String conversationId = "logging-test-" + UUID.randomUUID();
		String key = "argi_chat_memory:" + conversationId;
		String secret = RedisDeserializationLoggingTest.SECRET;
		UserMessage message = UserMessage.builder().text(secret).metadata(Map.of("private", secret)).build();
		try (BaseRedisChatMemoryRepository repository = create(client); Jedis connection = new Jedis(host, port)) {
			try {
				connection.rpush(key, repository.serializeMessage(message), "{\"content\":" + secret + "}");
				try (var logs = new RedisDeserializationLoggingTest.CapturedLogs(level)) {
					assertThat(repository.findByConversationId(conversationId)).containsExactly(message);
					logs.assertDiagnostic("Deserialization error");
					logs.assertSafe();
				}
				// Reading still leaves both stored records untouched, including the damaged one.
				assertThat(connection.llen(key)).isEqualTo(2);
			}
			finally {
				connection.del(key);
			}
		}
	}

}
