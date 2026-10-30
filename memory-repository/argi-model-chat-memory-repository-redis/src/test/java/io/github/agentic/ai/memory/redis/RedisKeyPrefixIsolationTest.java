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
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

class RedisKeyPrefixIsolationTest {

	static final String DEFAULT_PREFIX = "argi_chat_memory:";

	enum Client {
		JEDIS, LETTUCE, REDISSON;

		BaseRedisChatMemoryRepository create(String prefix) {
			return switch (this) {
				case JEDIS -> JedisRedisChatMemoryRepository.builder().keyPrefix(prefix).build();
				case LETTUCE -> LettuceRedisChatMemoryRepository.builder().keyPrefix(prefix).build();
				case REDISSON -> RedissonRedisChatMemoryRepository.builder().keyPrefix(prefix).build();
			};
		}

		void failConstruction(String prefix) {
			switch (this) {
				case JEDIS -> JedisRedisChatMemoryRepository.builder().keyPrefix(prefix).nodes(null).build();
				case LETTUCE -> LettuceRedisChatMemoryRepository.builder().keyPrefix(prefix).nodes(null).build();
				case REDISSON -> RedissonRedisChatMemoryRepository.builder().keyPrefix(prefix).nodes(null).build();
			}
		}
	}

	static Stream<Arguments> clientPairs() {
		return Stream.of(Client.values())
			.flatMap(first -> Stream.of(Client.values()).map(second -> Arguments.of(first, second)));
	}

	@ParameterizedTest
	@MethodSource("clientPairs")
	void constructingOtherRepositoriesDoesNotChangePrefix(Client first, Client second) throws Exception {
		try (MockClients ignored = new MockClients();
				BaseRedisChatMemoryRepository a = first.create("tenant-a:");
				BaseRedisChatMemoryRepository b = second.create("tenant-b:")) {
			assertThat(a.getKeyPrefix()).isEqualTo("tenant-a:");
			assertThat(b.getKeyPrefix()).isEqualTo("tenant-b:");
			try (BaseRedisChatMemoryRepository defaults = second.create(null);
					BaseRedisChatMemoryRepository same = second.create("tenant-a:");
					BaseRedisChatMemoryRepository empty = second.create("")) {
				assertThat(defaults.getKeyPrefix()).isEqualTo(DEFAULT_PREFIX);
				assertThat(same.getKeyPrefix()).isEqualTo("tenant-a:");
				assertThat(empty.getKeyPrefix()).isEmpty();
				assertThat(a.getKeyPrefix()).isEqualTo("tenant-a:");
				assertThat(b.getKeyPrefix()).isEqualTo("tenant-b:");
			}
		}
	}

	@ParameterizedTest
	@MethodSource("clientPairs")
	void failedConstructionDoesNotChangeExistingPrefix(Client existing, Client failing) throws Exception {
		try (MockClients ignored = new MockClients();
				BaseRedisChatMemoryRepository repository = existing.create("existing:")) {
			assertThatThrownBy(() -> failing.failConstruction("failed:"))
				.isInstanceOf(NullPointerException.class)
				.hasMessageContaining("nodes");
			assertThat(repository.getKeyPrefix()).isEqualTo("existing:");
		}
	}

	@ParameterizedTest
	@EnumSource(Client.class)
	void reusingBuilderDoesNotChangePreviouslyBuiltRepository(Client client) throws Exception {
		try (MockClients ignored = new MockClients()) {
			switch (client) {
				case JEDIS -> {
					var builder = JedisRedisChatMemoryRepository.builder().keyPrefix("first:");
					try (var first = builder.build(); var second = builder.keyPrefix("second:").build()) {
						assertThat(first.getKeyPrefix()).isEqualTo("first:");
						assertThat(second.getKeyPrefix()).isEqualTo("second:");
					}
				}
				case LETTUCE -> {
					var builder = LettuceRedisChatMemoryRepository.builder().keyPrefix("first:");
					try (var first = builder.build(); var second = builder.keyPrefix("second:").build()) {
						assertThat(first.getKeyPrefix()).isEqualTo("first:");
						assertThat(second.getKeyPrefix()).isEqualTo("second:");
					}
				}
				case REDISSON -> {
					var builder = RedissonRedisChatMemoryRepository.builder().keyPrefix("first:");
					try (var first = builder.build(); var second = builder.keyPrefix("second:").build()) {
						assertThat(first.getKeyPrefix()).isEqualTo("first:");
						assertThat(second.getKeyPrefix()).isEqualTo("second:");
					}
				}
			}
		}
	}

	@Test
	void noArgumentBaseConstructorRetainsDefaultPrefix() throws Exception {
		try (MockClients ignored = new MockClients();
				BaseRedisChatMemoryRepository defaults = mock(BaseRedisChatMemoryRepository.class,
						withSettings().useConstructor().defaultAnswer(CALLS_REAL_METHODS));
				BaseRedisChatMemoryRepository custom = Client.REDISSON.create("custom:")) {
			assertThat(defaults.getKeyPrefix()).isEqualTo(DEFAULT_PREFIX);
			assertThat(custom.getKeyPrefix()).isEqualTo("custom:");
		}
	}

	@Test
	void redissonCustomConfigAlsoUsesInstancePrefix() {
		try (MockClients ignored = new MockClients();
				var first = RedissonRedisChatMemoryRepository.builder().redissonConfig(new Config())
					.keyPrefix("custom:").build();
				var second = RedissonRedisChatMemoryRepository.builder().build()) {
			assertThat(first.getKeyPrefix()).isEqualTo("custom:");
			assertThat(second.getKeyPrefix()).isEqualTo(DEFAULT_PREFIX);
		}
	}

	@Test
	void parallelConstructionKeepsEachInstancesPrefix() throws Exception {
		var executor = Executors.newFixedThreadPool(3);
		var barrier = new CyclicBarrier(3);
		try {
			List<Future<?>> futures = new ArrayList<>();
			for (Client client : Client.values()) {
				futures.add(executor.submit(() -> {
					try (MockClients ignored = new MockClients();
							BaseRedisChatMemoryRepository repository = client.create(client.name() + ":")) {
						barrier.await(10, TimeUnit.SECONDS);
						assertThat(repository.getKeyPrefix()).isEqualTo(client.name() + ":");
					}
					return null;
				}));
			}
			for (Future<?> future : futures) {
				future.get(20, TimeUnit.SECONDS);
			}
		}
		finally {
			executor.shutdownNow();
			assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
		}
	}

	// Intercept only client creation: actual repository builders and constructors run.
	private static final class MockClients implements AutoCloseable {

		private final MockedConstruction<JedisConnectionFactory> jedis = mockConstruction(JedisConnectionFactory.class);

		private final MockedConstruction<LettuceConnectionFactory> lettuce = mockConstruction(LettuceConnectionFactory.class);

		private final MockedStatic<Redisson> redisson = mockStatic(Redisson.class);

		MockClients() {
			RedissonClient client = mock(RedissonClient.class);
			when(client.isShutdown()).thenReturn(true);
			redisson.when(() -> Redisson.create(any(Config.class))).thenReturn(client);
		}

		@Override
		public void close() {
			redisson.close();
			lettuce.close();
			jedis.close();
		}
	}

}
