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

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.Config;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Independent adversarial review probes; real Valkey, one JVM, fake effects only. */
@Testcontainers(disabledWithoutDocker = true)
class RedisProgressProtocolReviewTest {

	@Container
	private static final GenericContainer<?> VALKEY = new GenericContainer<>(
			DockerImageName.parse("valkey/valkey:8.1.2"))
		.withExposedPorts(6379)
		.withCommand("valkey-server", "--appendonly", "yes", "--appendfsync", "always");

	private static RedissonClient client;

	@BeforeAll
	static void setup() {
		Config config = new Config();
		config.useSingleServer().setAddress("redis://" + VALKEY.getHost() + ":" + VALKEY.getMappedPort(6379));
		client = Redisson.create(config);
	}

	@AfterAll
	static void close() {
		if (client != null) {
			client.shutdown();
		}
	}

	@Test
	void terminalAndUncommittedRecordsStillExhaustAdmissionWithoutLosingMembership() {
		String prefix = prefix();
		var store = store(prefix, 2);
		seed(store, "done", "DONE");
		var zero = store.claim("zero", "r", UUID.randomUUID(), Duration.ofSeconds(30));
		store.release(zero);
		assertThatThrownBy(() -> store.claim("new", "r", UUID.randomUUID(), Duration.ofSeconds(30)))
			.hasMessageContaining("DIRECTORY_FULL");
		assertThat(store.snapshot("new", "r")).isEmpty();
		assertThat(client.getKeys().countExists(new RedisProgressKeys(prefix).sessionKey("new"))).isZero();
		var directory = new RedisProgressDirectory(client, prefix, 2);
		assertThat(directory.page("", 10).entries()).hasSize(2);
		var reclaimed = store.claim("done", "r", UUID.randomUUID(), Duration.ofSeconds(30));
		assertThat(store.snapshot("done", "r").orElseThrow().state()).isEqualTo("DONE");
		store.release(reclaimed);
		assertThat(directory.page("", 10).entries()).hasSize(2);
	}

	@Test
	void legacyAndMixedWritersRemainInvisibleUntilExplicitlyInventoried() {
		String prefix = prefix();
		var legacy = store(prefix, 0);
		seed(legacy, "old", "READY");
		var directory = new RedisProgressDirectory(client, prefix, 3);
		var hints = new RedisProgressAdvisoryIndex(client, prefix + ":hints", 3);
		var projection = projection(prefix, hints);
		assertThat(directory.repair("", 10, projection, this::ready, hints).published()).isZero();
		directory.registerExisting("old", "r");
		directory.registerExisting("old", "r");
		assertThat(directory.repair("", 10, projection, this::ready, hints).published()).isEqualTo(1);
		seed(legacy, "late-legacy", "READY");
		assertThat(directory.repair("", 10, projection, this::ready, hints).published()).isZero();
		assertThat(legacy.snapshot("late-legacy", "r")).isPresent();
		assertThat(directory.page("", 10).entries()).containsExactly(new RedisProgressDirectory.Identity("old", "r"));
	}

	@Test
	void stateAndRevisionChangedBetweenEligibilityAndDispatchCannotExecute() {
		String prefix = prefix();
		var store = store(prefix, 2);
		seed(store, "s", "READY");
		var hints = new RedisProgressAdvisoryIndex(client, prefix + ":hints", 2);
		var projection = projection(prefix, hints);
		hints.publish(new RedisProgressAdvisoryIndex.Hint(projection.metadata("s", "r").orElseThrow(), 0));
		AtomicInteger effects = new AtomicInteger();
		var consumer = new RedisProgressReferenceConsumer(store, projection, hints, metadata -> {
			// Advance authority through the real fenced store API while this attempt owns
			// it.
			var ns = client.<String, String>getMap(new RedisProgressKeys(prefix).sessionKey("s"), StringCodec.INSTANCE);
			var lease = new LeasedProgressStore.SessionLease("s", "r", UUID.fromString(ns.get("owner")),
					Long.parseLong(ns.get("fence")), Long.parseLong(ns.get("expires")));
			store.commit(lease, commit("DONE", metadata.revision()));
			return true;
		}, Duration.ofSeconds(30), 10000);
		var delivery = hints.poll(1, 10000).get(0);
		assertThatThrownBy(() -> consumer.consume(delivery, UUID.randomUUID(), execution -> {
			effects.incrementAndGet();
			return commit("EXECUTED", 1);
		})).isInstanceOf(ProgressConflictException.class).hasMessageContaining("Authority changed");
		assertThat(effects).hasValue(0);
		assertThat(store.snapshot("s", "r").orElseThrow().runRevision()).isEqualTo(2);
		assertThat(
				client.getMap(new RedisProgressKeys(prefix).sessionKey("s"), StringCodec.INSTANCE).containsKey("owner"))
			.isFalse();
	}

	@Test
	void reservationIsCheckedIndependentlyOfMatchingOwnerAndFenceAndZeroExpiryIsLost() {
		String prefix = prefix();
		var store = store(prefix, 2);
		var lease = store.claim("s", "r", UUID.randomUUID(), Duration.ofSeconds(30));
		var hints = new RedisProgressAdvisoryIndex(client, prefix + ":hints", 2);
		var projection = projection(prefix, hints);
		var ns = client.<String, String>getMap(new RedisProgressKeys(prefix).sessionKey("s"), StringCodec.INSTANCE);
		ns.put("reservation", "other");
		assertThatThrownBy(() -> projection.metadata(lease))
			.isInstanceOf(io.github.agentic.ai.graph.checkpoint.lease.LeaseLostException.class);
		ns.put("reservation", "r");
		ns.put("expires", "0");
		assertThatThrownBy(() -> projection.metadata(lease))
			.isInstanceOf(io.github.agentic.ai.graph.checkpoint.lease.LeaseLostException.class);
		assertThatThrownBy(() -> store.commit(lease, commit("DONE", 0)))
			.isInstanceOf(io.github.agentic.ai.graph.checkpoint.lease.LeaseLostException.class);
		assertThat(ns.get("expires")).isEqualTo("0");
	}

	@Test
	void oldDeliveryCannotAcknowledgeSameRevisionRepublishedAfterPruning() {
		String prefix = prefix();
		var hints = new RedisProgressAdvisoryIndex(client, prefix + ":hints", 1);
		var hint = new RedisProgressAdvisoryIndex.Hint(new RedisProgressIndexRepairer.Metadata("s", "r", 1, "READY"),
				0);
		hints.publish(hint);
		var first = hints.poll(1, 10000).get(0);
		assertThat(hints.ack(first)).isTrue();
		assertThat(hints.publish(hint)).isTrue();
		assertThat(hints.ack(first)).isFalse();
		assertThat(hints.retry(first, 10000)).isFalse();
		var second = hints.poll(1, 10000).get(0);
		assertThat(second.token()).isGreaterThan(first.token());
		assertThat(hints.ack(first)).isFalse();
		assertThat(hints.ack(second)).isTrue();
		assertThat(client.getMap(prefix + ":hints:deliveries:v1", StringCodec.INSTANCE).size()).isEqualTo(1);
	}

	@Test
	void oneEntryPagesAdvancePastBadAuthorityAndInsertionBehindCursorAppearsNextSweep() {
		String prefix = prefix();
		var store = store(prefix, 8);
		for (String id : List.of("m", "n", "o", "p")) {
			seed(store, id, "READY");
		}
		var keys = new RedisProgressKeys(prefix);
		client.getMap((String) keys.keys("m", "r").get(1), StringCodec.INSTANCE).put("revision", "01");
		client.getMap((String) keys.keys("n", "r").get(1), StringCodec.INSTANCE).put("state", "x".repeat(1000000));
		client.getKeys().delete((String) keys.keys("o", "r").get(1));
		var directory = new RedisProgressDirectory(client, prefix, 8);
		var hints = new RedisProgressAdvisoryIndex(client, prefix + ":hints", 8);
		var projection = projection(prefix, hints);
		String after = "";
		var issues = new ArrayList<RedisProgressDirectory.Issue>();
		int published = 0;
		for (int i = 0; i < 5; i++) {
			var result = directory.repair(after, 1, projection, this::ready, hints);
			assertThat(result.page().entries()).hasSizeLessThanOrEqualTo(1);
			issues.addAll(result.issues());
			published += result.published();
			after = result.page().after();
			if (result.page().done()) {
				break;
			}
		}
		assertThat(issues).hasSize(3);
		assertThat(published).isEqualTo(1);
		seed(store, "a", "READY"); // Its encoded member sorts behind the finished cursor.
		assertThat(directory.page(after, 1).entries()).isEmpty();
		assertThat(directory.repair("", 8, projection, this::ready, hints).published()).isEqualTo(1);
		assertThat(client.getMap((String) keys.keys("m", "r").get(1), StringCodec.INSTANCE).get("revision"))
			.isEqualTo("01");
		assertThat(directory.page("", 8).entries()).hasSize(5);
	}

	private Optional<RedisProgressAdvisoryIndex.Hint> ready(RedisProgressIndexRepairer.Metadata metadata) {
		return "READY".equals(metadata.state()) ? Optional.of(new RedisProgressAdvisoryIndex.Hint(metadata, 0))
				: Optional.empty();
	}

	private static RedisProgressIndexRepairer projection(String prefix, RedisProgressAdvisoryIndex hints) {
		return new RedisProgressIndexRepairer(client, prefix, metadata -> Optional.empty(), hints);
	}

	private static RedisLeasedProgressStore store(String prefix, int capacity) {
		var builder = RedisLeasedProgressStore.builder().redisson(client).keyPrefix(prefix);
		return (capacity == 0 ? builder : builder.directoryCapacity(capacity)).build();
	}

	private static LeasedProgressStore.ProgressCommit commit(String state, long revision) {
		return new LeasedProgressStore.ProgressCommit(state, revision, null, List.of(), List.of());
	}

	private static void seed(RedisLeasedProgressStore store, String session, String state) {
		var lease = store.claim(session, "r", UUID.randomUUID(), Duration.ofSeconds(30));
		store.commit(lease, commit(state, 0));
		store.release(lease);
	}

	private static String prefix() {
		return "review:extensions:" + UUID.randomUUID();
	}

}
