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

import io.github.agentic.ai.graph.store.NamespaceListRequest;
import io.github.agentic.ai.graph.store.StoreItem;
import io.github.agentic.ai.graph.store.StoreSearchRequest;
import io.github.agentic.ai.graph.store.StoreSearchResult;
import io.github.agentic.ai.graph.store.VersionedStoreItem;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.redisson.Redisson;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.Config;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Testcontainers(disabledWithoutDocker = true)
class RedisStoreTest {

	@Container
	private static final GenericContainer<?> REDIS = new GenericContainer<>(DockerImageName.parse("valkey/valkey:8.1.2"))
		.withExposedPorts(6379);

	private static RedissonClient primaryClient;

	private static RedissonClient secondaryClient;

	@BeforeAll
	static void setup() {
		primaryClient = newClient();
		secondaryClient = newClient();
	}

	@AfterAll
	static void tearDown() {
		if (primaryClient != null) {
			primaryClient.shutdown();
		}
		if (secondaryClient != null) {
			secondaryClient.shutdown();
		}
	}

	@Test
	void versionedWritesAreSharedAcrossClientsAndTombstonesPreventStaleRecreate() {
		String storageKey = uniqueStorageKey();
		RedisStore a = new RedisStore(primaryClient, storageKey);
		RedisStore b = new RedisStore(secondaryClient, storageKey);
		List<String> namespace = List.of("agent", "memory");
		StoreItem item = item(namespace, "profile", Map.of("name", "Ada", "marker", "first"));
		StoreItem otherItem = item(namespace, "profile", Map.of("name", "Grace", "marker", "second"));

		assertThat(a.putItemIfVersion(item, 0)).isTrue();
		assertThat(b.getVersionedItem(namespace, "profile").version()).isEqualTo(1);
		assertThat(b.putItemIfVersion(otherItem, 0)).isFalse();
		assertThat(b.deleteItemIfVersion(namespace, "profile", 1)).isTrue();
		assertThat(a.getVersionedItem(namespace, "profile").version()).isEqualTo(2);
		assertThat(a.getVersionedItem(namespace, "profile").item()).isEmpty();
		assertThat(a.putItemIfVersion(item, 0)).isFalse();
		assertThat(a.putItemIfVersion(item, 2)).isTrue();
		assertThat(b.getVersionedItem(namespace, "profile").version()).isEqualTo(3);
		assertThat(b.getItem(namespace, "profile")).hasValueSatisfying(restored -> assertThat(restored.getValue())
			.containsEntry("name", "Ada")
			.containsEntry("marker", "first"));
	}

	@Test
	void onlyOneConditionalWriterWinsForTheSameExpectedVersion() throws Exception {
		String storageKey = uniqueStorageKey();
		RedisStore a = new RedisStore(primaryClient, storageKey);
		RedisStore b = new RedisStore(secondaryClient, storageKey);
		List<String> namespace = List.of("race", "conditional");
		CountDownLatch ready = new CountDownLatch(2);
		CountDownLatch start = new CountDownLatch(1);
		AtomicInteger wins = new AtomicInteger();
		ExecutorService executor = Executors.newFixedThreadPool(2);
		try {
			executor.submit(() -> writeAfterStart(a, item(namespace, "key", Map.of("writer", "a")), ready, start, wins));
			executor.submit(() -> writeAfterStart(b, item(namespace, "key", Map.of("writer", "b")), ready, start, wins));
			assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();

			start.countDown();
			executor.shutdown();
			assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
		}
		finally {
			executor.shutdownNow();
		}

		VersionedStoreItem stored = a.getVersionedItem(namespace, "key");
		assertThat(wins).hasValue(1);
		assertThat(stored.version()).isEqualTo(1);
		assertThat(stored.item()).hasValueSatisfying(actual -> assertThat(actual.getValue().get("writer"))
			.isIn("a", "b"));
	}

	@Test
	void ordinaryContendedPutsRetryAndIncrementOncePerMutation() throws Exception {
		String storageKey = uniqueStorageKey();
		RedisStore storeA = new RedisStore(primaryClient, storageKey);
		RedisStore storeB = new RedisStore(secondaryClient, storageKey);
		List<String> namespace = List.of("race", "ordinary");
		int writers = 8;
		CountDownLatch ready = new CountDownLatch(writers);
		CountDownLatch start = new CountDownLatch(1);
		ExecutorService executor = Executors.newFixedThreadPool(writers);
		try {
			for (int i = 0; i < writers; i++) {
				int writer = i;
				RedisStore store = writer % 2 == 0 ? storeA : storeB;
				executor.submit(() -> putAfterStart(store,
						item(namespace, "key", Map.of("writer", writer, "marker", "ordinary")), ready, start));
			}
			assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();

			start.countDown();
			executor.shutdown();
			assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
		}
		finally {
			executor.shutdownNow();
		}

		VersionedStoreItem stored = storeA.getVersionedItem(namespace, "key");
		assertThat(stored.version()).isEqualTo(writers);
		assertThat(stored.item()).isPresent();
	}

	@Test
	void searchNamespaceListingAndScopedClearExcludeTombstones() {
		String storageKey = uniqueStorageKey();
		RedisStore store = new RedisStore(primaryClient, storageKey);
		store.putItem(item(List.of("users", "u1", "prefs"), "theme",
				Map.of("type", "preference", "rank", 3, "text", "dark nested marker")));
		store.putItem(item(List.of("users", "u1", "prefs"), "locale",
				Map.of("type", "preference", "rank", 2, "text", "english marker")));
		store.putItem(item(List.of("users", "u2"), "profile", Map.of("type", "profile", "rank", 1)));
		store.putItem(item(List.of("projects", "p1"), "profile", Map.of("type", "profile", "rank", 4)));
		assertThat(store.deleteItem(List.of("users", "u2"), "profile")).isTrue();

		StoreSearchResult result = store.searchItems(StoreSearchRequest.builder()
			.namespace("users")
			.query("marker")
			.filter(Map.of("type", "preference"))
			.sortFields(List.of("rank"))
			.ascending(false)
			.offset(1)
			.limit(1)
			.build());
		assertThat(result.getTotalCount()).isEqualTo(2);
		assertThat(result.getItems()).singleElement().satisfies(actual -> assertThat(actual.getKey()).isEqualTo("locale"));
		assertThat(store.listNamespaces(NamespaceListRequest.builder().namespace("users", "u1").build()))
			.containsExactly("users/u1", "users/u1/prefs");
		assertThat(store.size()).isEqualTo(3);

		VersionedStoreItem beforeClear = store.getVersionedItem(List.of("users", "u1", "prefs"), "theme");
		store.clear();

		assertThat(store.isEmpty()).isTrue();
		assertThat(store.searchItems(StoreSearchRequest.builder().build()).getItems()).isEmpty();
		VersionedStoreItem afterClear = store.getVersionedItem(List.of("users", "u1", "prefs"), "theme");
		assertThat(afterClear.item()).isEmpty();
		assertThat(afterClear.version()).isEqualTo(beforeClear.version() + 1);
		assertThat(store.putItemIfVersion(item(List.of("users", "u1", "prefs"), "theme", Map.of("restored", true)),
				beforeClear.version()))
			.isFalse();
		assertThat(store.putItemIfVersion(item(List.of("users", "u1", "prefs"), "theme", Map.of("restored", true)),
				afterClear.version()))
			.isTrue();
	}

	@Test
	void separateStorageKeysNestedJsonAndClientReconstructionRoundTrip() {
		String sharedKey = uniqueStorageKey();
		String isolatedKey = uniqueStorageKey();
		List<String> namespace = List.of("json", "nested");
		StoreItem nested = item(namespace, "payload",
				Map.of("marker", "business-json", "nested", Map.of("flag", true, "items", List.of("a", "b"))));
		new RedisStore(primaryClient, sharedKey).putItem(nested);
		new RedisStore(primaryClient, isolatedKey).putItem(item(namespace, "payload", Map.of("marker", "isolated")));

		RedissonClient reconstructed = newClient();
		try {
			RedisStore shared = new RedisStore(reconstructed, sharedKey);
			RedisStore isolated = new RedisStore(reconstructed, isolatedKey);

			assertThat(shared.getItem(namespace, "payload")).hasValueSatisfying(actual -> {
				assertThat(actual.getValue()).containsEntry("marker", "business-json");
				assertThat(actual.getValue().get("nested")).isInstanceOf(Map.class);
			});
			assertThat(isolated.getItem(namespace, "payload")).hasValueSatisfying(actual -> assertThat(actual.getValue())
				.containsEntry("marker", "isolated"));
		}
		finally {
			reconstructed.shutdown();
		}
	}

	@Test
	void negativeExpectedVersionIsRejectedAndOverflowFailsBeforeMutation() {
		String storageKey = uniqueStorageKey();
		RedisStore store = new RedisStore(primaryClient, storageKey);
		List<String> namespace = List.of("overflow");
		StoreItem item = item(namespace, "max", Map.of("marker", "overflow"));

		assertThatThrownBy(() -> store.putItemIfVersion(item, -1)).isInstanceOf(IllegalArgumentException.class);
		assertThatThrownBy(() -> store.deleteItemIfVersion(namespace, "max", -1))
			.isInstanceOf(IllegalArgumentException.class);

		RMap<String, String> hash = primaryClient.getMap(storageKey, StringCodec.INSTANCE);
		String field = storeKeyForTests(namespace, "max");
		String maxEnvelope = "{\"version\":9223372036854775807,\"deleted\":false,\"item\":{\"namespace\":[\"overflow\"],"
				+ "\"key\":\"max\",\"value\":{\"marker\":\"overflow\"},\"createdAt\":10,\"updatedAt\":20}}";
		hash.put(field, maxEnvelope);

		assertThatThrownBy(() -> store.putItemIfVersion(item, Long.MAX_VALUE)).isInstanceOf(ArithmeticException.class);
		assertThat(hash.get(field)).isEqualTo(maxEnvelope);
		assertThat(store.getVersionedItem(namespace, "max").version()).isEqualTo(Long.MAX_VALUE);
		assertThat(store.getVersionedItem(namespace, "max").item()).isPresent();
	}

	private static void writeAfterStart(RedisStore store, StoreItem item, CountDownLatch ready, CountDownLatch start,
			AtomicInteger wins) {
		ready.countDown();
		await(start);
		if (store.putItemIfVersion(item, 0)) {
			wins.incrementAndGet();
		}
	}

	private static void putAfterStart(RedisStore store, StoreItem item, CountDownLatch ready, CountDownLatch start) {
		ready.countDown();
		await(start);
		store.putItem(item);
	}

	private static void await(CountDownLatch latch) {
		try {
			latch.await();
		}
		catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException(ex);
		}
	}

	private static StoreItem item(List<String> namespace, String key, Map<String, Object> value) {
		return new StoreItem(namespace, key, value, 10, 20);
	}

	private static String uniqueStorageKey() {
		return "argi:test:store:" + UUID.randomUUID();
	}

	private static RedissonClient newClient() {
		Config config = new Config();
		config.useSingleServer().setAddress("redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379));
		return Redisson.create(config);
	}

	private static String storeKeyForTests(List<String> namespace, String key) {
		String json = "{\"namespace\":[\"" + String.join("\",\"", namespace) + "\"],\"key\":\"" + key + "\"}";
		return java.util.Base64.getEncoder().encodeToString(json.getBytes(java.nio.charset.StandardCharsets.UTF_8));
	}

}
