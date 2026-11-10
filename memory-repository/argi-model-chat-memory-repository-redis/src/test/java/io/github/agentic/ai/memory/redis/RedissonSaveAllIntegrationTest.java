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

import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.ai.chat.messages.Message;
import org.springframework.ai.chat.messages.UserMessage;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import redis.clients.jedis.Jedis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class RedissonSaveAllIntegrationTest {

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

	@Test
	void serializationFailureKeepsStoredHistoryAndExpiry() {
		String conversationId = "saveall-failure-" + UUID.randomUUID();
		String key = "argi_chat_memory:" + conversationId;
		try (var repository = RedissonRedisChatMemoryRepository.builder().host(host).port(port).build();
				var connection = new Jedis(host, port)) {
			try {
				repository.saveAll(conversationId, List.of(new UserMessage("old history")));
				connection.pexpire(key, 60000);
				var oldRecords = connection.lrange(key, 0, -1);
				var broken = UserMessage.builder()
					.text("broken")
					.metadata(Map.of("cycle", new SelfReference()))
					.build();

				assertThatThrownBy(() -> repository.saveAll(conversationId,
						List.of(new UserMessage("valid first message"), broken)))
					.isInstanceOf(RuntimeException.class)
					.hasMessage("Error serializing message");

				assertThat(connection.lrange(key, 0, -1)).isEqualTo(oldRecords);
				assertThat(repository.findByConversationId(conversationId)).extracting(Message::getText)
					.containsExactly("old history");
				assertThat(connection.pttl(key)).isBetween(1L, 60000L);
			}
			finally {
				connection.del(key);
			}
		}
	}

	@Test
	void validReplacementAndEmptySnapshotKeepExistingSemantics() {
		String conversationId = "saveall-valid-" + UUID.randomUUID();
		String key = "argi_chat_memory:" + conversationId;
		try (var repository = RedissonRedisChatMemoryRepository.builder().host(host).port(port).build();
				var connection = new Jedis(host, port)) {
			try {
				repository.saveAll(conversationId, List.of(new UserMessage("old history")));
				connection.pexpire(key, 60000);
				repository.saveAll(conversationId, List.of(new UserMessage("first"), new UserMessage("second")));

				assertThat(repository.findByConversationId(conversationId)).extracting(Message::getText)
					.containsExactly("first", "second");
				assertThat(connection.pttl(key)).isEqualTo(-1L);

				repository.saveAll(conversationId, List.of());

				assertThat(repository.findByConversationId(conversationId)).isEmpty();
				assertThat(connection.exists(key)).isFalse();
			}
			finally {
				connection.del(key);
			}
		}
	}

	static class SelfReference {

		public final SelfReference self = this;

	}

}
