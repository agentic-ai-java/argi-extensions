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
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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

/** Real single-primary storage tests; no model, executor or process-kill fixture. */
@Testcontainers(disabledWithoutDocker = true)
class RedisProgressIndexRepairerTest {

	@Container
	private static final GenericContainer<?> VALKEY = new GenericContainer<>(
			DockerImageName.parse("valkey/valkey:8.1.2"))
		.withExposedPorts(6379)
		.withCommand("valkey-server", "--appendonly", "yes", "--appendfsync", "always");

	private static RedissonClient client;

	@BeforeAll
	static void setup() {
		client = newClient();
	}

	@AfterAll
	static void close() {
		if (client != null) {
			client.shutdown();
		}
	}

	@Test
	void committedRunAndOutboxAreDiscoveredWithoutEverCallingPublisher() {
		String prefix = prefix();
		String indexKey = prefix + ":hints";
		RedissonClient producer = newClient();
		try {
			var store = store(producer, prefix);
			var lease = store.claim("original-session", "original-run", UUID.randomUUID(), Duration.ofSeconds(30));
			store.commit(lease,
					new LeasedProgressStore.ProgressCommit("READY", 0, null,
							List.of(new LeasedProgressStore.Entry("journal", "opaque journal")),
							List.of(new LeasedProgressStore.Entry("event", "opaque event"))));
			// Simulate the boundary after commit and before publish: publisher is never
			// called.
			// Shutdown is graceful; this is NOT an OS kill or a Redis-restart test.
		}
		finally {
			producer.shutdown();
		}
		assertThat(client.getKeys().countExists(indexKey)).isZero();
		var sessionKey = new RedisProgressKeys(prefix).sessionKey("original-session");
		var leaseBeforeRepair = client.getMap(sessionKey, StringCodec.INSTANCE).readAllMap();
		RedissonClient recovery = newClient();
		try {
			var sink = new RedisProgressAdvisoryIndex(recovery, indexKey);
			var repairer = readyRepairer(recovery, prefix, sink);
			assertThat(store(recovery, prefix).snapshot("original-session", "original-run").orElseThrow().outbox())
				.containsEntry("event", "opaque event");
			assertThat(sweep(repairer, 2, 10)).isEqualTo(1);
			assertThat(sink.due(10))
				.containsExactly(new RedisProgressAdvisoryIndex.Candidate("original-session", "original-run", 1));
			assertThat(sweep(repairer, 2, 10)).isZero();
			assertThat(client.getMap(sessionKey, StringCodec.INSTANCE).readAllMap()).isEqualTo(leaseBeforeRepair);
			assertThat(new RedisProgressAdvisoryIndex(client, indexKey).due(10)).isEqualTo(sink.due(10));
			assertThat(store(recovery, prefix).snapshot("original-session", "original-run").orElseThrow().runRevision())
				.isEqualTo(1);
		}
		finally {
			recovery.shutdown();
		}
	}

	@Test
	void concurrentRepairersPublishOneRevisionMember() throws Exception {
		String prefix = prefix();
		commit(prefix, "s", "r", "READY");
		var sink = new RedisProgressAdvisoryIndex(client, prefix + ":hints");
		RedissonClient second = newClient();
		var pool = Executors.newFixedThreadPool(2);
		try {
			Callable<Integer> a = () -> sweep(readyRepairer(client, prefix, sink), 10, 100);
			Callable<Integer> b = () -> sweep(
					readyRepairer(second, prefix, new RedisProgressAdvisoryIndex(second, prefix + ":hints")), 10, 100);
			var results = pool.invokeAll(List.of(a, b));
			assertThat(results.get(0).get() + results.get(1).get()).isEqualTo(1);
			assertThat(sink.due(100)).hasSize(1);
		}
		finally {
			pool.shutdownNow();
			assertThat(pool.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
			second.shutdown();
		}
	}

	@Test
	void continuationPreservesOversizedScanPageWhileProcessingOneKeyPerCall() {
		String prefix = prefix();
		for (int i = 0; i < 30; i++) {
			commit(prefix, "s" + i, "r" + i, "READY");
		}
		var sink = new RedisProgressAdvisoryIndex(client, prefix + ":hints");
		var repairer = readyRepairer(client, prefix, sink);
		var first = repairer.repair(repairer.start(), 1, 10000);
		assertThat(first.inspectedKeys()).isEqualTo(1);
		assertThat(first.cursor().pendingKeys()).isGreaterThan(1);
		int added = first.newHints();
		var cursor = first.cursor();
		int calls = 0;
		while (!cursor.done()) {
			var batch = repairer.repair(cursor, 1, 10000);
			assertThat(batch.inspectedKeys()).isLessThanOrEqualTo(1);
			added += batch.newHints();
			cursor = batch.cursor();
			assertThat(++calls).isLessThan(1000);
		}
		assertThat(added).isEqualTo(30);
		assertThat(sink.due(100)).hasSize(30);
		assertThat(repairer.repair(cursor, 1, 1).inspectedKeys()).isZero();
	}

	@Test
	void unknownAndTerminalStatesRequireAnExplicitAdapterAndOldHintsStayAdvisory() {
		String prefix = prefix();
		var store = store(client, prefix);
		var lease = store.claim("s", "r", UUID.randomUUID(), Duration.ofSeconds(30));
		store.commit(lease, new LeasedProgressStore.ProgressCommit("READY", 0, null, List.of(), List.of()));
		var sink = new RedisProgressAdvisoryIndex(client, prefix + ":hints");
		var repairer = readyRepairer(client, prefix, sink);
		assertThat(sweep(repairer, 10, 100)).isEqualTo(1);
		var old = sink.due(10).get(0);
		store.commit(lease, new LeasedProgressStore.ProgressCommit("DONE", 1, null, List.of(), List.of()));
		commit(prefix, "other", "unknown", "SOME_FUTURE_STATE");
		assertThat(sweep(repairer, 10, 100)).isZero();
		assertThat(sink.due(10)).containsExactly(old);
		var current = repairer.metadata(old.sessionId(), old.runId()).orElseThrow();
		assertThat(current.revision()).isNotEqualTo(old.revision());
		assertThat(current.state()).isEqualTo("DONE");
		// No consumer is supplied: this test rejects no execution and proves no terminal
		// non-rerun claim.
	}

	@Test
	void newRevisionDoesNotLetOldPublicationOverwriteNewCandidate() {
		String prefix = prefix();
		var store = store(client, prefix);
		var lease = store.claim("s", "r", UUID.randomUUID(), Duration.ofSeconds(30));
		store.commit(lease, new LeasedProgressStore.ProgressCommit("READY", 0, null, List.of(), List.of()));
		var sink = new RedisProgressAdvisoryIndex(client, prefix + ":hints");
		var repairer = readyRepairer(client, prefix, sink);
		var old = repairer.metadata("s", "r").orElseThrow();
		store.commit(lease, new LeasedProgressStore.ProgressCommit("READY", 1, null, List.of(), List.of()));
		assertThat(sweep(repairer, 10, 100)).isEqualTo(1);
		assertThat(sink.publish(new RedisProgressAdvisoryIndex.Hint(old, 0))).isTrue();
		assertThat(sink.due(10)).extracting(RedisProgressAdvisoryIndex.Candidate::revision)
			.containsExactlyInAnyOrder(1L, 2L);
	}

	@Test
	void projectionDoesNotReadLargeHistoricalPayloadsAndRejectsOversizedState() throws Exception {
		String prefix = prefix();
		commit(prefix, "s", "r", "READY");
		var keys = new RedisProgressKeys(prefix).keys("s", "r");
		// Seed large historical data directly to isolate projection from commit's
		// existing snapshot read.
		for (int i = 2; i < 4; i++) {
			client.getMap((String) keys.get(i), StringCodec.INSTANCE).put("large", "x".repeat(1_000_000));
		}
		client.getMap((String) keys.get(1), StringCodec.INSTANCE).put("checkpoint", "x".repeat(1_000_000));
		var repairer = readyRepairer(client, prefix, new RedisProgressAdvisoryIndex(client, prefix + ":hints"));
		String hgetallBefore = commandStats("hgetall");
		assertThat(sweep(repairer, 10, 100)).isEqualTo(1);
		assertThat(repairer.metadata("s", "r").orElseThrow().state()).isEqualTo("READY");
		assertThat(commandStats("hgetall")).isEqualTo(hgetallBefore);
		client.getMap((String) keys.get(1), StringCodec.INSTANCE).put("state", "x".repeat(4097));
		String hgetBeforeOversize = commandStats("hget");
		assertThatThrownBy(() -> repairer.metadata("s", "r")).hasMessageContaining("projection limit");
		assertThat(commandStats("hget")).isEqualTo(hgetBeforeOversize);
		assertThat(client.getMap((String) keys.get(3), StringCodec.INSTANCE).size()).isEqualTo(1);
	}

	@Test
	void projectionRejectsInvalidRevisionStatePairsBeforeCallingAdapter() {
		String prefix = prefix();
		var store = store(client, prefix);
		var lease = store.claim("s", "r", UUID.randomUUID(), Duration.ofSeconds(30));
		var run = client.<String, String>getMap((String) new RedisProgressKeys(prefix).keys("s", "r").get(1),
				StringCodec.INSTANCE);
		var sink = new RedisProgressAdvisoryIndex(client, prefix + ":hints");
		var repairer = new RedisProgressIndexRepairer(client, prefix, m -> {
			throw new AssertionError("Corrupt metadata must not reach the application adapter");
		}, sink);
		assertThat(repairer.metadata("s", "r").orElseThrow())
			.isEqualTo(new RedisProgressIndexRepairer.Metadata("s", "r", 0, ""));
		// An absent state is valid at revision zero, but even a present empty state is
		// not.
		for (String state : List.of("", "READY")) {
			run.put("state", state);
			assertThatThrownBy(() -> store.snapshot("s", "r")).hasMessageContaining("corrupt");
			assertThatThrownBy(() -> sweep(repairer, 10, 100)).hasMessageContaining("Invalid progress metadata");
			assertThat(run.get("state")).isEqualTo(state);
		}
		run.remove("state");
		store.commit(lease, new LeasedProgressStore.ProgressCommit("READY", 0, null, List.of(), List.of()));
		// A committed revision requires a nonempty state; missing and empty are corrupt.
		for (boolean missing : List.of(true, false)) {
			if (missing) {
				run.remove("state");
			}
			else {
				run.put("state", "");
			}
			assertThatThrownBy(() -> store.snapshot("s", "r")).hasMessageContaining("corrupt");
			assertThatThrownBy(() -> sweep(repairer, 10, 100)).hasMessageContaining("Invalid progress metadata");
			assertThat(run.containsKey("state")).isEqualTo(!missing);
			assertThat(run.get("revision")).isEqualTo("1");
		}
		assertThat(sink.due(10)).isEmpty();
	}

	@Test
	void projectionPreservesOpaqueUtf8LimitsAndExactInt64Revision() {
		String prefix = prefix();
		String session = "界".repeat(341) + "s"; // Exactly 1024 UTF-8 bytes.
		String state = "界".repeat(1365) + "s"; // Exactly 4096 UTF-8 bytes.
		commit(prefix, session, "r", state);
		var run = client.<String, String>getMap((String) new RedisProgressKeys(prefix).keys(session, "r").get(1),
				StringCodec.INSTANCE);
		run.put("revision", Long.toString(Long.MAX_VALUE));
		var sink = new RedisProgressAdvisoryIndex(client, prefix + ":hints");
		var repairer = new RedisProgressIndexRepairer(client, prefix,
				m -> Optional.of(new RedisProgressAdvisoryIndex.Hint(m, 0)), sink);
		assertThat(repairer.metadata(session, "r").orElseThrow())
			.isEqualTo(new RedisProgressIndexRepairer.Metadata(session, "r", Long.MAX_VALUE, state));
		assertThat(sweep(repairer, 10, 100)).isEqualTo(1);
		assertThat(sink.due(10))
			.containsExactly(new RedisProgressAdvisoryIndex.Candidate(session, "r", Long.MAX_VALUE));
		assertThat(store(client, prefix).snapshot(session, "r").orElseThrow().runRevision()).isEqualTo(Long.MAX_VALUE);
		// The established store accepts these IDs; only the opt-in reader imposes a byte
		// cap.
		String oversized = "界".repeat(342);
		commit(prefix, oversized, "r", "READY");
		commit(prefix, "other", oversized, "READY");
		assertThatThrownBy(() -> repairer.metadata(oversized, "r")).hasMessageContaining("projection limit");
		assertThatThrownBy(() -> repairer.metadata("other", oversized)).hasMessageContaining("projection limit");
		assertThat(store(client, prefix).snapshot(oversized, "r").orElseThrow().state()).isEqualTo("READY");
		assertThat(store(client, prefix).snapshot("other", oversized).orElseThrow().state()).isEqualTo("READY");
	}

	@Test
	void redisPublishErrorLeavesAuthorityIntactAndFreshSweepCanRetry() {
		String prefix = prefix();
		commit(prefix, "s", "r", "READY");
		var before = store(client, prefix).snapshot("s", "r").orElseThrow();
		String indexKey = prefix + ":hints";
		client.getBucket(indexKey, StringCodec.INSTANCE).set("wrong-type");
		var repairer = readyRepairer(client, prefix, new RedisProgressAdvisoryIndex(client, indexKey));
		assertThatThrownBy(() -> sweep(repairer, 10, 100)).hasMessageContaining("WRONGTYPE");
		assertThat(store(client, prefix).snapshot("s", "r").orElseThrow()).isEqualTo(before);
		// Fixture cleanup of its own advisory key only.
		client.getBucket(indexKey).delete();
		assertThat(sweep(repairer, 10, 100)).isEqualTo(1);
	}

	@Test
	void prefixGlobCharactersDoNotDiscoverAnotherNamespace() {
		String prefix = prefix() + "[*?]";
		commit(prefix, "s", "r", "READY");
		commit(prefix + "foreign", "foreign", "foreign", "READY");
		var sink = new RedisProgressAdvisoryIndex(client, prefix + ":hints");
		assertThat(sweep(readyRepairer(client, prefix, sink), 10, 100)).isEqualTo(1);
		assertThat(sink.due(10)).containsExactly(new RedisProgressAdvisoryIndex.Candidate("s", "r", 1));
	}

	@Test
	void dueQueriesUseServerTimeAndPublicationIsIdempotent() {
		String prefix = prefix();
		var sink = new RedisProgressAdvisoryIndex(client, prefix + ":hints");
		var m = new RedisProgressIndexRepairer.Metadata("s", "r", 1, "RETRY");
		var future = new RedisProgressAdvisoryIndex.Hint(m, 9_007_199_254_740_991L);
		assertThat(sink.publish(future)).isTrue();
		assertThat(sink.publish(future)).isFalse();
		assertThat(sink.due(10)).isEmpty();
		assertThat(sink.publish(new RedisProgressAdvisoryIndex.Hint(
				new RedisProgressIndexRepairer.Metadata("s", "now", 1, "READY"), 0)))
			.isTrue();
		assertThat(sink.due(1)).containsExactly(new RedisProgressAdvisoryIndex.Candidate("s", "now", 1));
		assertThatThrownBy(() -> sink.due(1001)).isInstanceOf(IllegalArgumentException.class);
		assertThatThrownBy(() -> sink.publish(new RedisProgressAdvisoryIndex.Hint(m, Long.MAX_VALUE)))
			.isInstanceOf(IllegalArgumentException.class);
	}

	private static String commandStats(String command) throws Exception {
		var reply = VALKEY.execInContainer("valkey-cli", "INFO", "commandstats");
		assertThat(reply.getExitCode()).isZero();
		return reply.getStdout()
			.lines()
			.filter(line -> line.startsWith("cmdstat_" + command + ":"))
			.findFirst()
			.orElse("");
	}

	private static RedisProgressIndexRepairer readyRepairer(RedissonClient connection, String prefix,
			RedisProgressAdvisoryIndex sink) {
		return new RedisProgressIndexRepairer(connection, prefix, m -> "READY".equals(m.state())
				? Optional.of(new RedisProgressAdvisoryIndex.Hint(m, 0)) : Optional.empty(), sink);
	}

	private static int sweep(RedisProgressIndexRepairer repairer, int maxKeys, int count) {
		var cursor = repairer.start();
		int hints = 0;
		int calls = 0;
		while (!cursor.done()) {
			var result = repairer.repair(cursor, maxKeys, count);
			assertThat(result.inspectedKeys()).isLessThanOrEqualTo(maxKeys);
			hints += result.newHints();
			cursor = result.cursor();
			assertThat(++calls).isLessThan(10000);
		}
		return hints;
	}

	private static void commit(String prefix, String session, String run, String state) {
		var store = store(client, prefix);
		var lease = store.claim(session, run, UUID.randomUUID(), Duration.ofSeconds(30));
		store.commit(lease, new LeasedProgressStore.ProgressCommit(state, 0, null, List.of(), List.of()));
		store.release(lease);
	}

	private static RedisLeasedProgressStore store(RedissonClient connection, String prefix) {
		return RedisLeasedProgressStore.builder().redisson(connection).keyPrefix(prefix).build();
	}

	private static String prefix() {
		return "argi:test:repair:" + UUID.randomUUID();
	}

	private static RedissonClient newClient() {
		Config config = new Config();
		config.useSingleServer().setAddress("redis://" + VALKEY.getHost() + ":" + VALKEY.getMappedPort(6379));
		return Redisson.create(config);
	}

}
