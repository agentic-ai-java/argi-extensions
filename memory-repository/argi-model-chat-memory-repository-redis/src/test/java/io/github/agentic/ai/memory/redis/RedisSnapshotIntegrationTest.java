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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import io.github.agentic.ai.memory.redis.builder.RedisChatMemoryBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.redisson.Redisson;
import org.redisson.api.NameMapper;
import org.redisson.codec.SerializationCodec;
import org.redisson.config.Config;
import org.springframework.ai.chat.messages.Message;
import org.springframework.ai.chat.messages.UserMessage;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.UnifiedJedis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Runs against an isolated standalone Redis by default. To exercise the identical
 * contract against a real Cluster, set redis.test.cluster to comma-separated reachable
 * master nodes. The endpoint must be test-owned: failure tests create and remove
 * temporary ACL users.
 */
class RedisSnapshotIntegrationTest {

	private static GenericContainer<?> redis;

	private static List<String> nodes;

	private static String host;

	private static int port;

	private static UnifiedJedis admin;

	enum Client {

		JEDIS, LETTUCE, REDISSON

	}

	@BeforeAll
	static void startRedis() {
		String cluster = System.getProperty("redis.test.cluster");
		if (cluster != null) {
			nodes = List.of(cluster.split(","));
			Set<HostAndPort> addresses = new HashSet<>();
			for (String node : nodes) {
				addresses.add(HostAndPort.from(node));
			}
			admin = new JedisCluster(addresses);
			return;
		}
		host = System.getProperty("redis.test.host", "127.0.0.1");
		String externalPort = System.getProperty("redis.test.port");
		if (externalPort == null) {
			assumeTrue(DockerClientFactory.instance().isDockerAvailable(), "Docker or an isolated Redis is required");
			redis = new GenericContainer<>("redis:7.0.15").withExposedPorts(6379);
			redis.start();
			host = redis.getHost();
			port = redis.getMappedPort(6379);
		}
		else {
			port = Integer.parseInt(externalPort);
		}
		admin = new JedisPooled(host, port);
	}

	@AfterAll
	static void stopRedis() {
		if (admin != null) {
			admin.close();
		}
		if (redis != null) {
			redis.stop();
		}
	}

	private static <T extends RedisChatMemoryBuilder<T>> T configure(T builder, String prefix, String user) {
		builder.keyPrefix(prefix).timeout(10000);
		if (nodes != null) {
			builder.nodes(nodes);
		}
		else {
			builder.host(host).port(port);
		}
		if (user != null) {
			builder.username(user).password("snapshot-test-password");
		}
		return builder;
	}

	private static BaseRedisChatMemoryRepository repository(Client client, String prefix, String user) {
		return switch (client) {
			case JEDIS -> configure(JedisRedisChatMemoryRepository.builder(), prefix, user).build();
			case LETTUCE -> configure(LettuceRedisChatMemoryRepository.builder(), prefix, user).build();
			case REDISSON -> configure(RedissonRedisChatMemoryRepository.builder(), prefix, user).build();
		};
	}

	private static String prefix() {
		return "snapshot-test-" + UUID.randomUUID() + ":";
	}

	private static List<Message> snapshot(String name, int count) {
		return IntStream.range(0, count).mapToObj(i -> (Message) new UserMessage(name + ":" + i)).toList();
	}

	private static List<String> texts(List<Message> messages) {
		return messages.stream().map(Message::getText).toList();
	}

	@ParameterizedTest
	@EnumSource(Client.class)
	void replacementPreservesLegacyKeysEncodingOrderAndTtlSemantics(Client writer) throws Exception {
		String prefix = prefix();
		try (var repository = repository(writer, prefix, null)) {
			for (String id : List.of("plain", "{tag}tail", "{}first{later}", "{{nested}", "unclosed{", "中文🙂")) {
				String key = prefix + id;
				try {
					admin.rpush(key, repository.serializeMessage(new UserMessage("legacy")));
					admin.pexpire(key, 60000);
					for (Client reader : Client.values()) {
						try (var other = repository(reader, prefix, null)) {
							assertThat(texts(other.findByConversationId(id))).containsExactly("legacy");
						}
					}
					var messages = snapshot("中文 replacement", 300);
					repository.saveAll(id, messages);
					assertThat(admin.lrange(key, 0, -1))
						.isEqualTo(messages.stream().map(repository::serializeMessage).toList());
					assertThat(admin.pttl(key)).isEqualTo(-1L);
					for (Client reader : Client.values()) {
						try (var other = repository(reader, prefix, null)) {
							assertThat(texts(other.findByConversationId(id))).isEqualTo(texts(messages));
						}
					}
					repository.saveAll(id, List.of());
					assertThat(admin.exists(key)).isFalse();
					admin.psetex(key, 60000, "wrong type");
					repository.saveAll(id, messages);
					assertThat(admin.type(key)).isEqualTo("list");
					assertThat(admin.pttl(key)).isEqualTo(-1L);
				}
				finally {
					admin.del(key);
				}
			}
		}
		assertNoStagingKeys();
	}

	@ParameterizedTest
	@EnumSource(Client.class)
	void localFailuresRetainBytesAndExpiry(Client writer) throws Exception {
		String prefix = prefix();
		String key = prefix + "history";
		try (var repository = repository(writer, prefix, null)) {
			try {
				repository.saveAll("history", snapshot("old", 2));
				admin.pexpire(key, 60000);
				var old = admin.lrange(key, 0, -1);
				assertThatThrownBy(() -> repository.saveAll(" ", snapshot("new", 1)))
					.isInstanceOf(IllegalArgumentException.class);
				assertThatThrownBy(() -> repository.saveAll("history", null))
					.isInstanceOf(IllegalArgumentException.class);
				assertThatThrownBy(() -> repository.saveAll("history", Arrays.asList(new UserMessage("ok"), null)))
					.isInstanceOf(IllegalArgumentException.class);
				var broken = UserMessage.builder().text("bad").metadata(Map.of("cycle", new SelfReference())).build();
				for (var replacement : List.of(List.<Message>of(broken),
						List.<Message>of(new UserMessage("ok"), broken))) {
					assertThatThrownBy(() -> repository.saveAll("history", replacement))
						.isInstanceOf(RuntimeException.class)
						.hasMessage("Error serializing message");
				}
				assertThat(admin.lrange(key, 0, -1)).isEqualTo(old);
				assertThat(admin.pttl(key)).isBetween(1L, 60000L);
			}
			finally {
				admin.del(key);
			}
		}
		assertNoStagingKeys();
	}

	@ParameterizedTest
	@EnumSource(Client.class)
	void rejectedCommandsDoNotPublishAndLeaveNoStagingKeys(Client writer) throws Exception {
		String prefix = prefix();
		String key = prefix + "history";
		try (var seed = repository(Client.JEDIS, prefix, null)) {
			try {
				seed.saveAll("history", snapshot("old", 2));
				admin.pexpire(key, 60000);
				var old = admin.lrange(key, 0, -1);
				for (String denied : List.of("rpush", "rename", "del", "eval")) {
					String user = "snapshot-" + UUID.randomUUID();
					if (denied.equals("eval")) {
						acl(user, "on", ">snapshot-test-password", "~*", "+@all", "-eval", "-evalsha");
					}
					else {
						acl(user, "on", ">snapshot-test-password", "~*", "+@all", "-" + denied);
					}
					try (var repository = repository(writer, prefix, user)) {
						assertThatThrownBy(() -> repository.saveAll("history", snapshot("new", 600)))
							.isInstanceOf(RuntimeException.class);
						assertThat(admin.lrange(key, 0, -1)).isEqualTo(old);
						assertThat(admin.pttl(key)).isBetween(1L, 60000L);
						assertNoStagingKeys();
					}
					finally {
						deleteUser(user);
					}
				}
			}
			finally {
				admin.del(key);
			}
		}
	}

	@Test
	void concurrentReadersAndThreeWritersOnlyObserveWholeSnapshots() throws Exception {
		String prefix = prefix();
		String id = "concurrent{}";
		String key = prefix + id;
		List<BaseRedisChatMemoryRepository> repositories = new ArrayList<>();
		var executor = Executors.newFixedThreadPool(6);
		var running = new AtomicBoolean(true);
		var ready = new CountDownLatch(6);
		var start = new CountDownLatch(1);
		try {
			for (Client client : Client.values()) {
				repositories.add(repository(client, prefix, null));
			}
			var snapshots = List.of(snapshot("old", 300), snapshot("jedis", 300), snapshot("lettuce", 300),
					snapshot("redisson", 300));
			Set<List<String>> allowed = new HashSet<>();
			snapshots.forEach(messages -> allowed.add(texts(messages)));
			repositories.get(0).saveAll(id, snapshots.get(0));
			var readers = new ArrayList<java.util.concurrent.Future<Integer>>();
			var writers = new ArrayList<java.util.concurrent.Future<?>>();
			for (int i = 0; i < 3; i++) {
				final int index = i;
				writers.add(executor.submit(() -> {
					ready.countDown();
					assertThat(start.await(10, TimeUnit.SECONDS)).isTrue();
					for (int n = 0; n < 30; n++) {
						repositories.get(index).saveAll(id, snapshots.get(index + 1));
					}
					return null;
				}));
				readers.add(executor.submit(() -> {
					ready.countDown();
					assertThat(start.await(10, TimeUnit.SECONDS)).isTrue();
					int samples = 0;
					do {
						assertThat(allowed).contains(texts(repositories.get(index).findByConversationId(id)));
						samples++;
					}
					while (running.get() || samples < 30);
					return samples;
				}));
			}
			assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
			start.countDown();
			for (var writer : writers) {
				writer.get(60, TimeUnit.SECONDS);
			}
			running.set(false);
			for (var reader : readers) {
				assertThat(reader.get(60, TimeUnit.SECONDS)).isGreaterThanOrEqualTo(30);
			}
			assertThat(allowed).contains(texts(repositories.get(0).findByConversationId(id)));
		}
		finally {
			running.set(false);
			start.countDown();
			executor.shutdownNow();
			assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
			for (var repository : repositories) {
				repository.close();
			}
			admin.del(key);
		}
		assertNoStagingKeys();
	}

	@Test
	void redissonPreservesConfiguredCodecAndMappedDestination() throws Exception {
		String prefix = prefix();
		String id = "custom-codec";
		String key = "mapped:" + prefix + id;
		NameMapper mapper = new NameMapper() {
			@Override
			public String map(String name) {
				return "mapped:" + name;
			}

			@Override
			public String unmap(String name) {
				return name.substring("mapped:".length());
			}
		};
		Config config = new Config().setCodec(new SerializationCodec());
		if (nodes == null) {
			config.useSingleServer().setAddress("redis://" + host + ":" + port).setNameMapper(mapper);
		}
		else {
			config.useClusterServers()
				.addNodeAddress(nodes.stream().map(node -> "redis://" + node).toArray(String[]::new))
				.setNameMapper(mapper);
		}
		var legacy = Redisson.create(config);
		try (var repository = RedissonRedisChatMemoryRepository.builder()
			.redissonConfig(config)
			.keyPrefix(prefix)
			.build()) {
			var message = new UserMessage("configured codec history");
			legacy.<String>getList(prefix + id).add(repository.serializeMessage(message));
			var before = admin.lrange(key.getBytes(StandardCharsets.UTF_8), 0, -1);
			repository.saveAll(id, List.of(message));
			assertThat(admin.lrange(key.getBytes(StandardCharsets.UTF_8), 0, -1)).usingRecursiveComparison()
				.isEqualTo(before);
			assertThat(texts(repository.findByConversationId(id))).containsExactly(message.getText());
			assertThat(legacy.<String>getList(prefix + id).readAll())
				.containsExactly(repository.serializeMessage(message));
		}
		finally {
			legacy.shutdown();
			admin.del(key);
		}
		assertNoStagingKeys();
	}

	@Test
	void stagingCollisionCannotOverwriteAnotherKey() {
		String key = prefix() + "destination";
		String staging = RedisSnapshot.stagingKey(key);
		try {
			admin.rpush(key, "old");
			admin.set(staging, "other owner's value");
			assertThatThrownBy(() -> admin.eval(RedisSnapshot.SCRIPT, List.of(key, staging), List.of("new")))
				.isInstanceOf(RuntimeException.class)
				.hasMessageContaining("staging key already exists");
			assertThat(admin.get(staging)).isEqualTo("other owner's value");
			assertThat(admin.lrange(key, 0, -1)).containsExactly("old");
		}
		finally {
			admin.del(key);
			admin.del(staging);
		}
	}

	private static List<HostAndPort> endpoints() {
		return nodes == null ? List.of(new HostAndPort(host, port)) : nodes.stream().map(HostAndPort::from).toList();
	}

	private static void acl(String user, String... rules) {
		for (var endpoint : endpoints()) {
			try (var connection = new Jedis(endpoint)) {
				connection.aclSetUser(user, rules);
			}
		}
	}

	private static void deleteUser(String user) {
		for (var endpoint : endpoints()) {
			try (var connection = new Jedis(endpoint)) {
				connection.aclDelUser(user);
			}
		}
	}

	private static void assertNoStagingKeys() {
		for (var endpoint : endpoints()) {
			try (var connection = new Jedis(endpoint)) {
				assertThat(connection.keys("{argi-*}:snapshot:*")).isEmpty();
			}
		}
	}

	static class SelfReference {

		public final SelfReference self = this;

	}

}
