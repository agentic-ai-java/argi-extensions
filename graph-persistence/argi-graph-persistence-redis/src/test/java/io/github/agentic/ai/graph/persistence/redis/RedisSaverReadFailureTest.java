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
package io.github.agentic.ai.graph.persistence.redis;

import io.github.agentic.ai.graph.OverAllState;
import io.github.agentic.ai.graph.RunnableConfig;
import io.github.agentic.ai.graph.serializer.StateSerializer;
import io.github.agentic.ai.graph.serializer.plain_text.jackson.SpringAIJacksonStateSerializer;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.redisson.Redisson;
import org.redisson.api.RLock;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;
import org.redisson.client.RedisException;
import org.redisson.config.Config;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Testcontainers(disabledWithoutDocker = true)
class RedisSaverReadFailureTest {

	private static final String LOCK_PREFIX = "graph:checkpoint:lock:";

	private static final StateSerializer SERIALIZER = new SpringAIJacksonStateSerializer(OverAllState::new);

	@Container
	private static final GenericContainer<?> REDIS = new GenericContainer<>(DockerImageName.parse("valkey/valkey:8.1.2"))
		.withExposedPorts(6379);

	private static RedissonClient holderClient;

	private static RedissonClient readerClient;

	@BeforeAll
	static void setup() {
		holderClient = newClient();
		readerClient = newClient();
	}

	@AfterAll
	static void tearDown() {
		if (holderClient != null) {
			holderClient.shutdown();
		}
		if (readerClient != null) {
			readerClient.shutdown();
		}
	}

	@AfterEach
	void clearInterruptFlag() {
		Thread.interrupted();
	}

	@Test
	void getThrowsWhenReadLockTimesOut() throws Exception {
		RedisSaver saver = saverWithLock(lock(false));
		RunnableConfig config = RunnableConfig.builder().threadId("contention-get").build();

		assertThatThrownBy(() -> saver.get(config))
			.isInstanceOf(IllegalStateException.class)
			.hasMessageContaining("Timed out acquiring Redis checkpoint read lock");
	}

	@Test
	void listThrowsWhenReadLockTimesOut() throws Exception {
		RedisSaver saver = saverWithLock(lock(false));
		RunnableConfig config = RunnableConfig.builder().threadId("contention-list").build();

		assertThatThrownBy(() -> saver.list(config))
			.isInstanceOf(IllegalStateException.class)
			.hasMessageContaining("Timed out acquiring Redis checkpoint read lock");
	}

	@Test
	void interruptedGetRestoresInterruptFlagAndThrows() throws Exception {
		RedisSaver saver = saverWithLock(interruptedLock());
		RunnableConfig config = RunnableConfig.builder().threadId("interrupted-get").build();

		assertThatThrownBy(() -> saver.get(config))
			.isInstanceOf(IllegalStateException.class)
			.hasMessageContaining("Interrupted acquiring Redis checkpoint read lock");

		assertThat(Thread.currentThread().isInterrupted()).isTrue();
	}

	@Test
	void interruptedGetDoesNotMaskReadFailureWithOwnershipLookup() throws Exception {
		RedisSaver saver = saverWithLock(interruptedLockWithFailingOwnershipLookup());
		RunnableConfig config = RunnableConfig.builder().threadId("interrupted-get-masked").build();

		assertThatThrownBy(() -> saver.get(config))
			.isInstanceOf(IllegalStateException.class)
			.hasMessageContaining("Interrupted acquiring Redis checkpoint read lock")
			.hasCauseInstanceOf(InterruptedException.class);

		assertThat(Thread.currentThread().isInterrupted()).isTrue();
	}

	@Test
	void interruptedListRestoresInterruptFlagAndThrows() throws Exception {
		RedisSaver saver = saverWithLock(interruptedLock());
		RunnableConfig config = RunnableConfig.builder().threadId("interrupted-list").build();

		assertThatThrownBy(() -> saver.list(config))
			.isInstanceOf(IllegalStateException.class)
			.hasMessageContaining("Interrupted acquiring Redis checkpoint read lock");

		assertThat(Thread.currentThread().isInterrupted()).isTrue();
	}

	@Test
	void interruptedListDoesNotMaskReadFailureWithOwnershipLookup() throws Exception {
		RedisSaver saver = saverWithLock(interruptedLockWithFailingOwnershipLookup());
		RunnableConfig config = RunnableConfig.builder().threadId("interrupted-list-masked").build();

		assertThatThrownBy(() -> saver.list(config))
			.isInstanceOf(IllegalStateException.class)
			.hasMessageContaining("Interrupted acquiring Redis checkpoint read lock")
			.hasCauseInstanceOf(InterruptedException.class);

		assertThat(Thread.currentThread().isInterrupted()).isTrue();
	}

	@Test
	void missingThreadMetadataStillReturnsEmpty() throws Exception {
		RedissonClient redisson = mock(RedissonClient.class);
		RLock lock = lock(true);
		RMap<String, String> meta = mock(RMap.class);
		when(redisson.getLock("graph:checkpoint:lock:missing-meta")).thenReturn(lock);
		when(redisson.<String, String>getMap("graph:thread:meta:missing-meta")).thenReturn(meta);
		RedisSaver saver = RedisSaver.builder().redisson(redisson).stateSerializer(SERIALIZER).build();
		RunnableConfig config = RunnableConfig.builder().threadId("missing-meta").build();

		assertThat(saver.get(config)).isEqualTo(Optional.empty());
		assertThat(saver.list(config)).isEmpty();
		assertThat(Thread.currentThread().isInterrupted()).isFalse();
	}

	@Test
	void realRedisReadTimeoutDoesNotReleaseAnotherClientsLock() {
		String threadName = "real-lock-" + UUID.randomUUID();
		RLock heldLock = holderClient.getLock(LOCK_PREFIX + threadName);
		RedisSaver reader = RedisSaver.builder().redisson(readerClient).stateSerializer(SERIALIZER).build();
		RunnableConfig config = RunnableConfig.builder().threadId(threadName).build();
		heldLock.lock();
		try {
			assertThatThrownBy(() -> reader.get(config)).isInstanceOf(IllegalStateException.class);
			assertThatThrownBy(() -> reader.list(config)).isInstanceOf(IllegalStateException.class);
			assertThat(heldLock.isHeldByCurrentThread()).isTrue();
			assertThat(heldLock.isLocked()).isTrue();
		}
		finally {
			heldLock.unlock();
		}
	}

	private static RedissonClient newClient() {
		Config config = new Config();
		config.useSingleServer().setAddress("redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379));
		return Redisson.create(config);
	}

	private static RedisSaver saverWithLock(RLock lock) {
		RedissonClient redisson = mock(RedissonClient.class);
		when(redisson.getLock(anyString())).thenReturn(lock);
		return RedisSaver.builder().redisson(redisson).stateSerializer(SERIALIZER).build();
	}

	private static RLock lock(boolean acquired) throws InterruptedException {
		RLock lock = mock(RLock.class);
		when(lock.tryLock(500, TimeUnit.MILLISECONDS)).thenReturn(acquired);
		when(lock.isHeldByCurrentThread()).thenReturn(acquired);
		return lock;
	}

	private static RLock interruptedLock() throws InterruptedException {
		RLock lock = mock(RLock.class);
		when(lock.tryLock(500, TimeUnit.MILLISECONDS)).thenThrow(new InterruptedException("interrupted"));
		when(lock.isHeldByCurrentThread()).thenReturn(false);
		return lock;
	}

	private static RLock interruptedLockWithFailingOwnershipLookup() throws InterruptedException {
		RLock lock = mock(RLock.class);
		when(lock.tryLock(500, TimeUnit.MILLISECONDS)).thenThrow(new InterruptedException("interrupted"));
		when(lock.isHeldByCurrentThread()).thenThrow(new RedisException("masked ownership lookup"));
		return lock;
	}

}
