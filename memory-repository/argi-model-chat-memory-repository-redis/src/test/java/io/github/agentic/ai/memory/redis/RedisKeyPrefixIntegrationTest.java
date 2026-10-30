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

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.ai.chat.messages.Message;
import org.springframework.ai.chat.messages.UserMessage;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import redis.clients.jedis.Jedis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Real-backend tests. Use an isolated local Redis via redis.test.port, or Docker.
 * Only uniquely named test keys are deleted; no database-wide cleanup is performed.
 */
class RedisKeyPrefixIntegrationTest {

	private static GenericContainer<?> redis;

	private static String host;

	private static int port;

	enum Client {
		JEDIS, LETTUCE, REDISSON
	}

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
		try (Jedis connection = new Jedis(host, port)) {
			assertThat(connection.ping()).isEqualTo("PONG");
		}
	}

	@AfterAll
	static void stopRedis() {
		if (redis != null) {
			redis.stop();
		}
	}

	private static BaseRedisChatMemoryRepository create(Client client, String prefix) {
		return switch (client) {
			case JEDIS -> JedisRedisChatMemoryRepository.builder().host(host).port(port).keyPrefix(prefix).build();
			case LETTUCE -> LettuceRedisChatMemoryRepository.builder().host(host).port(port).keyPrefix(prefix).build();
			case REDISSON -> RedissonRedisChatMemoryRepository.builder().host(host).port(port).keyPrefix(prefix).build();
		};
	}

	static Stream<Arguments> clientPairs() {
		return Stream.of(Client.values())
			.flatMap(first -> Stream.of(Client.values()).map(second -> Arguments.of(first, second)));
	}

	@ParameterizedTest
	@MethodSource("clientPairs")
	void actualReadWriteListAndDeleteUseEachInstancesPrefix(Client first, Client second) throws Exception {
		String id = UUID.randomUUID().toString();
		String prefix = "argi-prefix-test:" + id + ":";
		String keyA = prefix + "a:" + id;
		String keyB = prefix + "b:" + id;
		String defaultKey = "argi_chat_memory:" + id;
		try (Jedis inspector = new Jedis(host, port)) {
			try (BaseRedisChatMemoryRepository a = create(first, prefix + "a:");
					BaseRedisChatMemoryRepository b = create(second, prefix + "b:");
					BaseRedisChatMemoryRepository defaults = create(second, null);
					BaseRedisChatMemoryRepository same = create(second, prefix + "a:")) {
				a.saveAll(id, List.of(new UserMessage("A")));
				b.saveAll(id, List.of(new UserMessage("B")));
				defaults.saveAll(id, List.of(new UserMessage("default")));
				assertThat(inspector.lrange(keyA, 0, -1)).hasSize(1);
				assertThat(inspector.lrange(keyB, 0, -1)).hasSize(1);
				assertThat(inspector.lrange(defaultKey, 0, -1)).hasSize(1);
				assertThat(a.findByConversationId(id)).extracting(Message::getText).containsExactly("A");
				assertThat(b.findByConversationId(id)).extracting(Message::getText).containsExactly("B");
				assertThat(defaults.findByConversationId(id)).extracting(Message::getText).containsExactly("default");
				assertThat(same.findByConversationId(id)).extracting(Message::getText).containsExactly("A");
				assertThat(a.findConversationIds()).containsExactly(id);
				assertThat(b.findConversationIds()).containsExactly(id);
				assertThat(defaults.findConversationIds()).contains(id);
				same.saveAll(id, List.of(new UserMessage("shared")));
				assertThat(a.findByConversationId(id)).extracting(Message::getText).containsExactly("shared");
				a.deleteByConversationId(id);
				assertThat(inspector.exists(keyA)).isFalse();
				assertThat(same.findByConversationId(id)).isEmpty();
				assertThat(inspector.exists(keyB)).isTrue();
				assertThat(inspector.exists(defaultKey)).isTrue();
				b.deleteByConversationId(id);
				assertThat(inspector.exists(keyB)).isFalse();
				assertThat(inspector.exists(defaultKey)).isTrue();
				defaults.deleteByConversationId(id);
				assertThat(inspector.exists(defaultKey)).isFalse();
			}
			finally {
				inspector.del(keyA, keyB, defaultKey);
			}
		}
	}

	@Test
	void parallelClientsUseIndependentKeys() throws Exception {
		String id = UUID.randomUUID().toString();
		String prefix = "argi-prefix-test:" + id + ":";
		var executor = Executors.newFixedThreadPool(3);
		var barrier = new CyclicBarrier(3);
		try {
			List<Future<?>> futures = new ArrayList<>();
			for (Client client : Client.values()) {
				futures.add(executor.submit(() -> {
					String clientPrefix = prefix + client.name() + ":";
					try (BaseRedisChatMemoryRepository repository = create(client, clientPrefix);
							Jedis inspector = new Jedis(host, port)) {
						barrier.await(20, TimeUnit.SECONDS);
						repository.saveAll(id, List.of(new UserMessage(client.name())));
						assertThat(inspector.exists(clientPrefix + id)).isTrue();
						barrier.await(20, TimeUnit.SECONDS);
						assertThat(repository.findByConversationId(id)).extracting(Message::getText)
							.containsExactly(client.name());
						repository.deleteByConversationId(id);
						assertThat(inspector.exists(clientPrefix + id)).isFalse();
					}
					return null;
				}));
			}
			for (Future<?> future : futures) {
				future.get(45, TimeUnit.SECONDS);
			}
		}
		finally {
			executor.shutdownNow();
			executor.awaitTermination(10, TimeUnit.SECONDS);
			try (Jedis inspector = new Jedis(host, port)) {
				for (Client client : Client.values()) {
					inspector.del(prefix + client.name() + ":" + id);
				}
			}
		}
	}

}
