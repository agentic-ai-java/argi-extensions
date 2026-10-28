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

import io.github.agentic.ai.graph.checkpoint.lease.LeaseBusyException;
import io.github.agentic.ai.graph.checkpoint.lease.LeaseLostException;
import io.github.agentic.ai.graph.persistence.redis.LeasedProgressStore.CheckpointWrite;
import io.github.agentic.ai.graph.persistence.redis.LeasedProgressStore.Entry;
import io.github.agentic.ai.graph.persistence.redis.LeasedProgressStore.ProgressCommit;
import io.github.agentic.ai.graph.persistence.redis.LeasedProgressStore.ProgressSnapshot;
import io.github.agentic.ai.graph.persistence.redis.LeasedProgressStore.SessionLease;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

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

/**
 * Redis-specific acceptance tests of the unified lease: one owner/fence guards
 * run, checkpoint, journal and outbox progress, stale owners lose every write
 * path, failed commits leave no partial progress, and the deployment is verified
 * to be single-slot at startup. Runs against real Valkey via Testcontainers;
 * expiry is simulated by back-dating the stored lease deadline or by real TTLs.
 */
@Testcontainers(disabledWithoutDocker = true)
class RedisLeasedProgressStoreTest {

	private static final Duration TTL = Duration.ofSeconds(30);

	private static final String SESSION = "session-1";

	@Container
	private static final GenericContainer<?> VALKEY = new GenericContainer<>(DockerImageName.parse("valkey/valkey:8.1.2"))
		.withExposedPorts(6379)
		.withCommand("valkey-server", "--appendonly", "yes", "--appendfsync", "always");

	private static RedissonClient clientA;

	private static RedissonClient clientB;

	@BeforeAll
	static void setup() {
		clientA = newClient();
		clientB = newClient();
	}

	@AfterAll
	static void tearDown() {
		if (clientA != null) {
			clientA.shutdown();
		}
		if (clientB != null) {
			clientB.shutdown();
		}
	}

	@Test
	void claimRenewReleaseLifecycle() throws Exception {
		String prefix = uniquePrefix();
		RedisLeasedProgressStore store = store(clientA, prefix);
		assertThat(store.snapshot(SESSION, "run-1")).isEmpty();

		SessionLease lease = store.claim(SESSION, "run-1", UUID.randomUUID(), TTL);
		assertThat(lease.fencingToken()).isEqualTo(1);
		assertThat(lease.expiresAtMillis()).isPositive();
		ProgressSnapshot claimed = store.snapshot(SESSION, "run-1").orElseThrow();
		assertThat(claimed.runRevision()).isZero();
		assertThat(claimed.state()).isNull();
		assertThat(claimed.checkpoint()).isNull();
		assertThat(claimed.journal()).isEmpty();
		assertThat(claimed.outbox()).isEmpty();

		SessionLease renewed = store.renew(lease, Duration.ofSeconds(60));
		assertThat(renewed.fencingToken()).isEqualTo(lease.fencingToken());
		assertThat(renewed.expiresAtMillis()).isGreaterThan(lease.expiresAtMillis());

		assertThat(store.release(renewed)).isTrue();
		assertThat(store.release(renewed)).isFalse();
		// Releasing keeps the retained fence and the run record; the next claim re-fences.
		SessionLease reclaimed = store.claim(SESSION, "run-1", UUID.randomUUID(), TTL);
		assertThat(reclaimed.fencingToken()).isEqualTo(2);
		assertThat(store.snapshot(SESSION, "run-1").orElseThrow().runRevision()).isZero();
	}

	@Test
	void twoRunIdsOfOneSessionShareOneLiveOwner() throws Exception {
		String prefix = uniquePrefix();
		RedisLeasedProgressStore storeA = store(clientA, prefix);
		RedisLeasedProgressStore storeB = store(clientB, prefix);
		SessionLease first = storeA.claim(SESSION, "run-1", UUID.randomUUID(), TTL);

		assertThatThrownBy(() -> storeB.claim(SESSION, "run-1", UUID.randomUUID(), TTL))
			.isInstanceOf(LeaseBusyException.class);
		assertThatThrownBy(() -> storeB.claim(SESSION, "run-2", UUID.randomUUID(), TTL))
			.isInstanceOf(LeaseBusyException.class);

		// Once the first owner expires, the other run can be claimed with a higher fence,
		// and the previous owner can no longer advance either run.
		expire(prefix, SESSION);
		SessionLease second = storeB.claim(SESSION, "run-2", UUID.randomUUID(), TTL);
		assertThat(second.fencingToken()).isEqualTo(first.fencingToken() + 1);
		assertThatThrownBy(() -> storeA.commit(first, commit("RUNNING", 0)))
			.isInstanceOf(ProgressConflictException.class);
		assertThat(storeB.commit(second, commit("RUNNING", 0)).state()).isEqualTo("RUNNING");
		assertThat(storeA.snapshot(SESSION, "run-1").orElseThrow().runRevision()).isZero();
	}

	@Test
	void expiredStaleOwnerLosesHeartbeatReleaseAndEveryWrite() throws Exception {
		String prefix = uniquePrefix();
		RedisLeasedProgressStore storeA = store(clientA, prefix);
		RedisLeasedProgressStore storeB = store(clientB, prefix);
		SessionLease stale = storeA.claim(SESSION, "run-1", UUID.randomUUID(), TTL);
		storeA.commit(stale, new ProgressCommit("RUNNING", 0, new CheckpointWrite(0, "{\"n\":1}"),
				List.of(new Entry("j1", "{}")), List.of(new Entry("o1", "{}"))));
		ProgressSnapshot before = storeA.snapshot(SESSION, "run-1").orElseThrow();

		expire(prefix, SESSION);
		SessionLease current = storeB.claim(SESSION, "run-1", UUID.randomUUID(), TTL);
		assertThat(current.fencingToken()).isEqualTo(stale.fencingToken() + 1);
		Map<String, String> sessionBefore = sessionHash(prefix, SESSION);

		assertThatThrownBy(() -> storeA.renew(stale, TTL)).isInstanceOf(LeaseLostException.class);
		assertThat(storeA.release(stale)).isFalse();
		assertThatThrownBy(() -> storeA.commit(stale, new ProgressCommit("SUCCEEDED", 1,
				new CheckpointWrite(1, "{\"n\":2}"), List.of(new Entry("j2", "{}")),
				List.of(new Entry("o2", "{}")))))
			.isInstanceOf(LeaseLostException.class);
		// No half checkpoint, no half run progress, and no ownerless fallback happened.
		assertThat(storeA.snapshot(SESSION, "run-1").orElseThrow()).isEqualTo(before);
		assertThat(sessionHash(prefix, SESSION)).containsAllEntriesOf(sessionBefore);

		ProgressSnapshot advanced = storeB.commit(current, new ProgressCommit("SUCCEEDED", 1,
				new CheckpointWrite(1, "{\"n\":2}"), List.of(new Entry("j2", "{}")),
				List.of(new Entry("o2", "{}"))));
		assertThat(advanced.runRevision()).isEqualTo(2);
		assertThat(advanced.state()).isEqualTo("SUCCEEDED");
		assertThat(advanced.checkpoint()).isEqualTo("{\"n\":2}");
		assertThat(advanced.checkpointRevision()).isEqualTo(2);
		assertThat(advanced.journal()).containsOnlyKeys("j1", "j2");
		assertThat(advanced.outbox()).containsOnlyKeys("o1", "o2");
	}

	@Test
	void realTtlExpiryAllowsAnotherOwnerToTakeOver() throws Exception {
		String prefix = uniquePrefix();
		RedisLeasedProgressStore storeA = store(clientA, prefix);
		RedisLeasedProgressStore storeB = store(clientB, prefix);
		SessionLease stale = storeA.claim(SESSION, "run-1", UUID.randomUUID(), Duration.ofMillis(200));
		Thread.sleep(500);

		SessionLease current = storeB.claim(SESSION, "run-1", UUID.randomUUID(), TTL);
		assertThat(current.fencingToken()).isEqualTo(stale.fencingToken() + 1);
		assertThatThrownBy(() -> storeA.renew(stale, TTL)).isInstanceOf(LeaseLostException.class);
		assertThat(storeA.release(stale)).isFalse();
	}

	@Test
	void concurrentClaimsHaveExactlyOneWinner() throws Exception {
		String prefix = uniquePrefix();
		RedisLeasedProgressStore storeA = store(clientA, prefix);
		RedisLeasedProgressStore storeB = store(clientB, prefix);
		int contenders = 16;
		ExecutorService executor = Executors.newFixedThreadPool(contenders);
		try {
			List<Callable<SessionLease>> claims = new ArrayList<>();
			for (int i = 0; i < contenders; i++) {
				RedisLeasedProgressStore target = i % 2 == 0 ? storeA : storeB;
				claims.add(() -> target.claim(SESSION, "run-1", UUID.randomUUID(), TTL));
			}
			int won = 0;
			int busy = 0;
			for (Future<SessionLease> future : executor.invokeAll(claims)) {
				try {
					assertThat(future.get().fencingToken()).isEqualTo(1);
					won++;
				}
				catch (Exception ex) {
					assertThat(ex.getCause()).isInstanceOf(LeaseBusyException.class);
					busy++;
				}
			}
			assertThat(won).isEqualTo(1);
			assertThat(busy).isEqualTo(contenders - 1);
		}
		finally {
			executor.shutdownNow();
			executor.awaitTermination(10, TimeUnit.SECONDS);
		}
	}

	@Test
	void commitAdvancesRunCheckpointJournalAndOutboxTogether() throws Exception {
		String prefix = uniquePrefix();
		RedisLeasedProgressStore store = store(clientA, prefix);
		SessionLease lease = store.claim(SESSION, "run-1", UUID.randomUUID(), TTL);

		ProgressSnapshot first = store.commit(lease, new ProgressCommit("RUNNING", 0,
				new CheckpointWrite(0, "{\"node\":\"start\"}"), List.of(new Entry("j1", "{\"a\":1}")),
				List.of(new Entry("o1", "{\"e\":1}"), new Entry("o2", "{\"e\":2}"))));
		assertThat(first.runRevision()).isEqualTo(1);
		assertThat(first.state()).isEqualTo("RUNNING");
		assertThat(first.checkpoint()).isEqualTo("{\"node\":\"start\"}");
		assertThat(first.checkpointRevision()).isEqualTo(1);
		assertThat(first.journal()).containsOnly(Map.entry("j1", "{\"a\":1}"));
		assertThat(first.outbox()).containsOnly(Map.entry("o1", "{\"e\":1}"), Map.entry("o2", "{\"e\":2}"));

		ProgressSnapshot second = store.commit(lease, new ProgressCommit("SUCCEEDED", 1, null,
				List.of(new Entry("j2", "{}")), List.of()));
		assertThat(second.runRevision()).isEqualTo(2);
		assertThat(second.state()).isEqualTo("SUCCEEDED");
		assertThat(second.checkpoint()).isEqualTo(first.checkpoint());
		assertThat(second.checkpointRevision()).isEqualTo(1);
		assertThat(second.journal()).containsOnlyKeys("j1", "j2");
		assertThat(second.outbox()).isEqualTo(first.outbox());
	}

	@Test
	void runRevisionConflictWritesNothing() throws Exception {
		String prefix = uniquePrefix();
		RedisLeasedProgressStore store = store(clientA, prefix);
		SessionLease lease = store.claim(SESSION, "run-1", UUID.randomUUID(), TTL);
		ProgressSnapshot committed = store.commit(lease, commit("RUNNING", 0));

		assertThatThrownBy(() -> store.commit(lease, new ProgressCommit("SUCCEEDED", 0,
				new CheckpointWrite(0, "{}"), List.of(new Entry("j1", "{}")), List.of(new Entry("o1", "{}")))))
			.isInstanceOf(ProgressConflictException.class)
			.hasMessageContaining("Run revision");
		assertThat(store.snapshot(SESSION, "run-1").orElseThrow()).isEqualTo(committed);
	}

	@Test
	void checkpointRevisionConflictWritesNothing() throws Exception {
		String prefix = uniquePrefix();
		RedisLeasedProgressStore store = store(clientA, prefix);
		SessionLease lease = store.claim(SESSION, "run-1", UUID.randomUUID(), TTL);
		ProgressSnapshot committed = store.commit(lease,
				new ProgressCommit("RUNNING", 0, new CheckpointWrite(0, "{\"n\":1}"), List.of(), List.of()));

		assertThatThrownBy(() -> store.commit(lease, new ProgressCommit("RUNNING", 1,
				new CheckpointWrite(0, "{\"n\":2}"), List.of(new Entry("j1", "{}")), List.of())))
			.isInstanceOf(ProgressConflictException.class)
			.hasMessageContaining("Checkpoint revision");
		assertThat(store.snapshot(SESSION, "run-1").orElseThrow()).isEqualTo(committed);
	}

	@Test
	void simulatedCommitFailureLeavesNoPartialCheckpointOrRunProgress() throws Exception {
		String prefix = uniquePrefix();
		RedisLeasedProgressStore store = store(clientA, prefix);
		SessionLease lease = store.claim(SESSION, "run-1", UUID.randomUUID(), TTL);
		ProgressSnapshot committed = store.commit(lease, new ProgressCommit("RUNNING", 0,
				new CheckpointWrite(0, "{\"n\":1}"), List.of(new Entry("j1", "{}")), List.of(new Entry("o1", "{}"))));
		Map<String, String> sessionBefore = sessionHash(prefix, SESSION);

		// The journal entry id already exists: the commit must abort before its first
		// write, leaving no new checkpoint revision, run revision or outbox entry.
		assertThatThrownBy(() -> store.commit(lease, new ProgressCommit("SUCCEEDED", 1,
				new CheckpointWrite(1, "{\"n\":2}"), List.of(new Entry("j1", "{\"changed\":true}")),
				List.of(new Entry("o2", "{}")))))
			.isInstanceOf(ProgressConflictException.class)
			.hasMessageContaining("j1");
		assertThat(store.snapshot(SESSION, "run-1").orElseThrow()).isEqualTo(committed);
		assertThat(sessionHash(prefix, SESSION)).isEqualTo(sessionBefore);
	}

	@Test
	void releasedOwnerCannotCommitAnymore() throws Exception {
		String prefix = uniquePrefix();
		RedisLeasedProgressStore store = store(clientA, prefix);
		SessionLease lease = store.claim(SESSION, "run-1", UUID.randomUUID(), TTL);
		assertThat(store.release(lease)).isTrue();

		assertThatThrownBy(() -> store.commit(lease, commit("RUNNING", 0))).isInstanceOf(LeaseLostException.class);
		assertThatThrownBy(() -> store.renew(lease, TTL)).isInstanceOf(LeaseLostException.class);
		assertThat(store.snapshot(SESSION, "run-1").orElseThrow().runRevision()).isZero();
	}

	@Test
	void crossSlotConfigFailsFast() throws Exception {
		String prefix = uniquePrefix();
		List<Object> layout = new RedisProgressKeys(prefix).keys(SESSION, "run-1");
		List<String> keys = layout.stream().map(String.class::cast).toList();
		RedisLeasedProgressStore.requireSameSlot(keys);

		List<String> otherSession = new RedisProgressKeys(prefix).keys("other-session", "run-1")
			.stream()
			.map(String.class::cast)
			.toList();
		assertThatThrownBy(() -> RedisLeasedProgressStore.requireSameSlot(List.of(keys.get(0), otherSession.get(0))))
			.isInstanceOf(IllegalStateException.class)
			.hasMessageContaining("Cross-slot");

		assertThatThrownBy(() -> RedisLeasedProgressStore.builder()
			.redisson(clientA)
			.keyPrefix("argi:{bad}:v1")
			.build()).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("keyPrefix");
	}

	@Test
	void sessionsAreIsolated() throws Exception {
		String prefix = uniquePrefix();
		RedisLeasedProgressStore store = store(clientA, prefix);
		SessionLease first = store.claim("session-a", "run-1", UUID.randomUUID(), TTL);
		SessionLease second = store.claim("session-b", "run-1", UUID.randomUUID(), TTL);
		assertThat(first.fencingToken()).isEqualTo(second.fencingToken()).isEqualTo(1);

		store.commit(first, commit("RUNNING", 0));
		assertThat(store.snapshot("session-b", "run-1").orElseThrow().runRevision()).isZero();
		assertThat(store.snapshot("session-b", "run-1").orElseThrow().state()).isNull();
	}

	@Test
	void corruptStoredDataIsRejectedNotRepaired() throws Exception {
		String prefix = uniquePrefix();
		RedisLeasedProgressStore store = store(clientA, prefix);
		List<Object> layout = new RedisProgressKeys(prefix).keys(SESSION, "run-1");
		clientA.getBucket((String) layout.get(1), StringCodec.INSTANCE).set("not-a-hash");

		assertThatThrownBy(() -> store.claim(SESSION, "run-1", UUID.randomUUID(), TTL))
			.isInstanceOf(IllegalStateException.class)
			.hasMessageContaining("corrupt");
		assertThat(clientA.getBucket((String) layout.get(1), StringCodec.INSTANCE).get()).isEqualTo("not-a-hash");
	}

	@Test
	void invalidArgumentsAreRejectedBeforeAnyRedisCall() throws Exception {
		String prefix = uniquePrefix();
		RedisLeasedProgressStore store = store(clientA, prefix);
		SessionLease lease = store.claim(SESSION, "run-1", UUID.randomUUID(), TTL);

		assertThatThrownBy(() -> store.claim(SESSION, "run-1", UUID.randomUUID(), Duration.ZERO))
			.isInstanceOf(IllegalArgumentException.class);
		assertThatThrownBy(() -> store.claim(" ", "run-1", UUID.randomUUID(), TTL))
			.isInstanceOf(IllegalArgumentException.class);
		assertThatThrownBy(() -> store.commit(lease, new ProgressCommit("RUNNING", 0, null,
				List.of(new Entry("j1", "{}"), new Entry("j1", "{}")), List.of())))
			.isInstanceOf(IllegalArgumentException.class)
			.hasMessageContaining("Duplicate");
		assertThatThrownBy(() -> store.commit(lease, new ProgressCommit("RUNNING", Long.MAX_VALUE, null, List.of(),
				List.of()))).isInstanceOf(ArithmeticException.class);
		assertThat(store.snapshot(SESSION, "run-1").orElseThrow().runRevision()).isZero();
	}

	/** Back-dates the stored lease deadline so Redis TIME considers it expired. */
	private static void expire(String prefix, String sessionId) {
		RMap<String, String> session = clientA.getMap(new RedisProgressKeys(prefix).sessionKey(sessionId),
				StringCodec.INSTANCE);
		String expires = session.put("expires", "1");
		assertThat(expires).isNotNull();
	}

	private static ProgressCommit commit(String state, long expectedRevision) {
		return new ProgressCommit(state, expectedRevision, null, List.of(), List.of());
	}

	private static Map<String, String> sessionHash(String prefix, String sessionId) {
		RMap<String, String> session = clientA.getMap(new RedisProgressKeys(prefix).sessionKey(sessionId),
				StringCodec.INSTANCE);
		return session.readAllMap();
	}

	private static RedisLeasedProgressStore store(RedissonClient client, String prefix) {
		return RedisLeasedProgressStore.builder().redisson(client).keyPrefix(prefix).build();
	}

	private static String uniquePrefix() {
		return "argi:test:progress:" + UUID.randomUUID() + ":v1";
	}

	private static RedissonClient newClient() {
		Config config = new Config();
		config.useSingleServer().setAddress("redis://" + VALKEY.getHost() + ":" + VALKEY.getMappedPort(6379));
		return Redisson.create(config);
	}

}
