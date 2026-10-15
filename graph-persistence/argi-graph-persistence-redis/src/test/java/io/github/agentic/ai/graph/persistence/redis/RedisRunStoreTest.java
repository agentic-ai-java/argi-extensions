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
import io.github.agentic.ai.graph.checkpoint.Checkpoint;
import io.github.agentic.ai.graph.checkpoint.lease.ExecutionLease;
import io.github.agentic.ai.graph.checkpoint.lease.LeaseOptions;
import io.github.agentic.ai.graph.execution.DelegationScope;
import io.github.agentic.ai.graph.execution.ExecutionBudget;
import io.github.agentic.ai.graph.execution.PrincipalReference;
import io.github.agentic.ai.graph.execution.TenantReference;
import io.github.agentic.ai.graph.execution.TrustedExecutionContext;
import io.github.agentic.ai.graph.execution.run.MemoryRunStore;
import io.github.agentic.ai.graph.execution.run.RunCommit;
import io.github.agentic.ai.graph.execution.run.RunEntry;
import io.github.agentic.ai.graph.execution.run.RunLease;
import io.github.agentic.ai.graph.execution.run.RunNamespace;
import io.github.agentic.ai.graph.execution.run.RunPayload;
import io.github.agentic.ai.graph.execution.run.RunSnapshot;
import io.github.agentic.ai.graph.execution.run.RunSpec;
import io.github.agentic.ai.graph.execution.run.RunState;
import io.github.agentic.ai.graph.execution.run.RunStore;
import io.github.agentic.ai.graph.execution.run.RunStoreConflictException;
import io.github.agentic.ai.graph.execution.run.TaskSpec;
import io.github.agentic.ai.graph.serializer.StateSerializer;
import io.github.agentic.ai.graph.serializer.plain_text.jackson.ArgiJacksonStateSerializer;
import io.github.agentic.ai.graph.store.StoreItem;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.redisson.Redisson;
import org.redisson.api.RMap;
import org.redisson.api.RScript;
import org.redisson.api.RedissonClient;
import org.redisson.api.options.OptionalOptions;
import org.redisson.client.RedisTimeoutException;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.Config;
import org.redisson.connection.CRC16;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Redis-specific RunStore guarantees that the shared contract cannot express:
 * multi-client atomicity, persistence round trips, raw keyspace layout, corruption
 * handling and isolation from the legacy checkpoint/store keyspace.
 */
@Testcontainers(disabledWithoutDocker = true)
class RedisRunStoreTest {

	private static final String MAX_LONG = Long.toString(Long.MAX_VALUE);

	private static final Duration TTL = Duration.ofSeconds(30);

	private static final StateSerializer STATE_SERIALIZER = new ArgiJacksonStateSerializer(OverAllState::new);

	@Container
	private static final GenericContainer<?> VALKEY = new GenericContainer<>(DockerImageName.parse("valkey/valkey:8.1.2"))
		.withExposedPorts(6379)
		.withCommand("valkey-server", "--appendonly", "yes", "--appendfsync", "always", "--enable-debug-command",
				"yes");

	private static RedissonClient clientA;

	private static RedissonClient clientB;

	private final FixedClock clock = new FixedClock(Instant.parse("2026-10-14T00:00:00Z"));

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
	void concurrentSameKeySubmitsFromTwoClientsCreateExactlyOneRun() throws Exception {
		String prefix = uniquePrefix();
		RedisRunStore storeA = store(clientA, prefix, clock);
		RedisRunStore storeB = store(clientB, prefix, clock);
		List<Callable<RunSnapshot>> submissions = new ArrayList<>();
		for (int i = 0; i < 32; i++) {
			RunSpec spec = spec("run-" + i, "key", "{\"a\":1}");
			RedisRunStore target = i % 2 == 0 ? storeA : storeB;
			submissions.add(() -> target.submit(spec, context(spec.runId())));
		}
		List<RunSnapshot> results = race(submissions);
		assertThat(results).allMatch(snapshot -> snapshot.equals(results.get(0)));
		RunSpec winner = results.get(0).spec();
		RedisRunKeys keys = new RedisRunKeys(prefix);
		String nsKey = keys.namespaceKey(winner.namespace());
		assertThat(keySet(prefix)).containsExactlyInAnyOrder(nsKey, (String) keys.runKeys(winner.namespace(),
				winner.runId()).get(1));
		assertThat(hash(nsKey).keySet()).filteredOn(field -> field.startsWith("s:")).containsExactly("s:key");
		assertThat(storeB.activeRun(winner.namespace())).contains(winner.runId());

		Map<String, String> before = dump(prefix);
		RunSpec otherInput = spec("x1", "key", "{\"a\":2}");
		RunSpec otherGraph = new RunSpec(1, winner.namespace(), "x2", "key", winner.inputHash(), "other-graph", "1",
				winner.input(), context("x2").toReference());
		TrustedExecutionContext otherPrincipal = context("x3", "tenant", "other-user");
		RunSpec principal = RunSpec.from(otherPrincipal, "app", "session", "key", "graph", "1", winner.input());
		assertThatThrownBy(() -> storeA.submit(otherInput, context("x1")))
			.isInstanceOf(RunStoreConflictException.class);
		assertThatThrownBy(() -> storeB.submit(otherGraph, context("x2")))
			.isInstanceOf(RunStoreConflictException.class);
		assertThatThrownBy(() -> storeA.submit(principal, otherPrincipal))
			.isInstanceOf(RunStoreConflictException.class);
		assertThat(dump(prefix)).isEqualTo(before);
	}

	@Test
	void terminalRunSurvivesAofReloadAndIsReadByAFreshClient() throws Exception {
		String prefix = uniquePrefix();
		RedisRunStore writer = store(clientA, prefix, clock);
		RunSpec spec = spec("one", "key", "{}");
		writer.submit(spec, context("one"));
		RunLease lease = writer.acquire(spec.namespace(), "one", UUID.randomUUID(), TTL);
		TaskSpec task = task(spec, "child");
		writer.commit(spec.namespace(), "one", lease,
				new RunCommit(0, RunState.RUNNING, payload("{\"node\":\"start\"}"),
						List.of(new RunCommit.TaskUpdate(task, -1, RunState.PENDING)),
						List.of(entry("model", RunEntry.Kind.MODEL_CALL, "{\"tokens\":3}")),
						List.of(entry("event", RunEntry.Kind.EVENT, "{\"n\":1}"))));
		RunSnapshot terminal = writer.commit(spec.namespace(), "one", lease,
				new RunCommit(1, RunState.SUCCEEDED, payload("{\"node\":\"end\"}"),
						List.of(new RunCommit.TaskUpdate(task, 0, RunState.CANCELLED)),
						List.of(entry("task", RunEntry.Kind.TASK_CALL, "{}")),
						List.of(entry("done", RunEntry.Kind.EVENT, "{}"))));
		Map<String, String> before = dump(prefix);

		// Replace the in-memory dataset with the AOF contents to prove the snapshot is durable.
		ExecResult reload = VALKEY.execInContainer("valkey-cli", "DEBUG", "LOADAOF");
		assertThat(reload.getStdout()).contains("OK");
		assertThat(dump(prefix)).isEqualTo(before);

		RedissonClient fresh = newClient();
		try {
			RedisRunStore reader = store(fresh, prefix, Clock.systemUTC());
			assertThat(reader.find(spec.namespace(), "one")).contains(terminal);
			assertThat(reader.activeRun(spec.namespace())).isEmpty();
			assertThat(reader.submit(spec("retry", "key", "{}"), context("retry"))).isEqualTo(terminal);
		}
		finally {
			fresh.shutdown();
		}
	}

	@Test
	void otherTenantCannotSeeTheRunOrUseItsLease() {
		String prefix = uniquePrefix();
		RedisRunStore store = store(clientA, prefix, clock);
		RunSpec own = spec("same-run", "same-key", "{}");
		store.submit(own, context(own.runId()));
		RunLease lease = store.acquire(own.namespace(), own.runId(), UUID.randomUUID(), TTL);
		TrustedExecutionContext otherContext = context("same-run", "other", "user");
		RunNamespace other = RunNamespace.from(otherContext, "app", "session");
		Map<String, String> before = dump(prefix);

		assertThat(store.find(other, own.runId())).isEmpty();
		assertThat(store.activeRun(other)).isEmpty();
		assertThatThrownBy(() -> store.acquire(other, own.runId(), lease.ownerId(), TTL))
			.isInstanceOf(RunStoreConflictException.class);
		RunLease transplanted = new RunLease(other, lease.ownerId(), lease.fencingToken(), lease.expiresAt());
		assertThatThrownBy(() -> store.renew(transplanted, TTL)).isInstanceOf(RunStoreConflictException.class);
		assertThat(store.release(transplanted)).isFalse();
		assertThatThrownBy(() -> store.commit(other, own.runId(), lease, idle(0)))
			.isInstanceOf(RunStoreConflictException.class);
		assertThatThrownBy(() -> store.commit(other, own.runId(), transplanted, idle(0)))
			.isInstanceOf(RunStoreConflictException.class);
		assertThat(dump(prefix)).isEqualTo(before);

		// The other tenant gets its own run, fence sequence and lease; neither lease works
		// across the tenant boundary even though both use fence 1.
		RunSpec theirs = RunSpec.from(otherContext, "app", "session", "same-key", "graph", "1", own.input());
		store.submit(theirs, otherContext);
		RunLease otherLease = store.acquire(other, own.runId(), UUID.randomUUID(), TTL);
		assertThat(otherLease.fencingToken()).isEqualTo(lease.fencingToken());
		assertThatThrownBy(() -> store.commit(other, own.runId(), transplanted, idle(0)))
			.isInstanceOf(RunStoreConflictException.class);
		RunLease reverse = new RunLease(own.namespace(), otherLease.ownerId(), otherLease.fencingToken(),
				otherLease.expiresAt());
		assertThatThrownBy(() -> store.commit(own.namespace(), own.runId(), reverse, idle(0)))
			.isInstanceOf(RunStoreConflictException.class);
		assertThat(store.find(own.namespace(), own.runId()).orElseThrow().revision()).isZero();
		assertThat(store.find(other, own.runId()).orElseThrow().revision()).isZero();

		// A run record copied under another tenant's key is detected rather than served.
		TrustedExecutionContext thirdContext = context("same-run", "third", "user");
		RunNamespace third = RunNamespace.from(thirdContext, "app", "session");
		RedisRunKeys keys = new RedisRunKeys(prefix);
		String source = (String) keys.runKeys(own.namespace(), own.runId()).get(1);
		String target = (String) keys.runKeys(third, own.runId()).get(1);
		clientA.getBucket(target).restore(clientA.getBucket(source).dump());
		assertThatThrownBy(() -> store.find(third, own.runId())).isInstanceOf(IllegalStateException.class)
			.isNotInstanceOf(RunStoreConflictException.class);
		String thirdNs = keys.namespaceKey(third);
		hash(thirdNs).putAll(Map.of("v", "1", "ns", RedisRunKeys.tuple(third)));
		assertThatThrownBy(() -> store.find(third, own.runId())).isInstanceOf(IllegalStateException.class)
			.isNotInstanceOf(RunStoreConflictException.class)
			.hasMessageContaining(target);
	}

	@Test
	void corruptNamespaceHashFailsEveryOperationWithoutMutation() {
		Set<Op> all = Set.of(Op.values());
		assertCorrupt("v=2", all, fixture -> hash(fixture.nsKey()).put("v", "2"));
		assertCorrupt("ns mismatch", all, fixture -> hash(fixture.nsKey()).put("ns", "3:iam5:other3:app7:session"));
		assertCorrupt("non-numeric fence", all, fixture -> hash(fixture.nsKey()).put("fence", "1x"));
		assertCorrupt("leading zero fence", all, fixture -> hash(fixture.nsKey()).put("fence", "01"));
		assertCorrupt("fence above max", all,
				fixture -> hash(fixture.nsKey()).put("fence", "9223372036854775808"));
		assertCorrupt("partial lease", all, fixture -> hash(fixture.nsKey()).remove("lease_owner"));
		assertCorrupt("lease fence above fence", all, fixture -> hash(fixture.nsKey()).put("lease_fence", "2"));
		assertCorrupt("non-numeric lease expiry", all,
				fixture -> hash(fixture.nsKey()).put("lease_expires", "soon"));
	}

	@Test
	void corruptRunRecordsFailReadsCommitsAndDedupWithoutMutation() {
		Set<Op> readers = Set.of(Op.FIND, Op.SUBMIT, Op.COMMIT);
		assertCorrupt("run v=2", readers, fixture -> hash(fixture.runKey(1)).put("v", "2"));
		assertCorrupt("non-numeric revision", readers, fixture -> hash(fixture.runKey(1)).put("revision", "abc"));
		assertCorrupt("negative revision", readers, fixture -> hash(fixture.runKey(1)).put("revision", "-1"));
		assertCorrupt("unknown state", readers, fixture -> hash(fixture.runKey(1)).put("state", "BOGUS"));
		assertCorrupt("fingerprint mismatch", readers,
				fixture -> hash(fixture.runKey(1)).put("fp", "0".repeat(64)));
		assertCorrupt("unknown run field", readers,
				fixture -> hash(fixture.runKey(1)).put("terminal_at", "1"));
		assertCorrupt("tampered input hash", readers, fixture -> {
			RMap<String, String> run = hash(fixture.runKey(1));
			String spec = run.get("spec");
			String inputHash = fixture.spec().inputHash();
			assertThat(spec).contains(inputHash);
			run.put("spec", spec.replaceFirst(inputHash, "0".repeat(inputHash.length())));
		});
		assertCorrupt("unknown checkpoint JSON field", readers, fixture -> {
			RMap<String, String> run = hash(fixture.runKey(1));
			run.put("checkpoint", run.get("checkpoint").replaceFirst("\\{", "{\"extra\":1,"));
		});
		assertCorrupt("unknown journal JSON field", readers, fixture -> {
			RMap<String, String> journal = hash(fixture.runKey(3));
			journal.put("model", journal.get("model").replaceFirst("\\{", "{\"extra\":1,"));
		});
		assertCorrupt("task key differs from task id", readers, fixture -> {
			RMap<String, String> tasks = hash(fixture.runKey(2));
			tasks.put("other", tasks.remove("child"));
		});
		assertCorrupt("non-event outbox entry", readers, fixture -> {
			RMap<String, String> outbox = hash(fixture.runKey(4));
			outbox.put("event", outbox.get("event").replace("\"EVENT\"", "\"MODEL_CALL\""));
		});
		assertCorrupt("orphan journal without run", Set.of(Op.FIND, Op.COMMIT), fixture -> {
			clientA.getBucket(fixture.runKey(1)).delete();
		});
	}

	@Test
	void corruptDedupEntriesFailSubmitWithoutMutation() {
		assertCorrupt("malformed dedup value", Set.of(Op.SUBMIT),
				fixture -> hash(fixture.nsKey()).put("s:key", "not-a-fingerprint"));
		assertCorrupt("upper-case dedup fingerprint", Set.of(Op.SUBMIT), fixture -> {
			RMap<String, String> ns = hash(fixture.nsKey());
			ns.put("s:key", ns.get("s:key").toUpperCase(Locale.ROOT));
		});
		RunSpec ghost = spec("unused", "ghost-key", "{}");
		assertCorrupt("dedup pointing to a missing run", Set.of(Op.SUBMIT), fixture -> {
			hash(fixture.nsKey()).put("s:ghost-key", RedisRunKeys.fingerprint(ghost) + ":ghost-run");
		}, ghost);
		// Same input, graph and principal, so the fingerprint matches, but run "one" was
		// submitted under another key and must not be served for this one.
		RunSpec borrowed = spec("unused", "borrowed-key", "{}");
		assertCorrupt("dedup pointing to a run of another submission key", Set.of(Op.SUBMIT), fixture -> {
			hash(fixture.nsKey()).put("s:borrowed-key", RedisRunKeys.fingerprint(borrowed) + ":one");
		}, borrowed);
	}

	@Test
	void corruptNumbersAndEnumsInStoredJsonAreNotCoerced() {
		Set<Op> readers = Set.of(Op.FIND, Op.SUBMIT, Op.COMMIT);
		assertCorrupt("float task revision", readers, fixture -> {
			RMap<String, String> tasks = hash(fixture.runKey(2));
			String task = tasks.get("child");
			assertThat(task).contains("\"revision\":0");
			tasks.put("child", task.replace("\"revision\":0", "\"revision\":0.9"));
		});
		assertCorrupt("numeric task state", readers, fixture -> {
			RMap<String, String> tasks = hash(fixture.runKey(2));
			String task = tasks.get("child");
			assertThat(task).contains("\"state\":\"PENDING\"");
			tasks.put("child", task.replace("\"state\":\"PENDING\"", "\"state\":0"));
		});
		assertCorrupt("numeric journal kind", readers, fixture -> {
			RMap<String, String> journal = hash(fixture.runKey(3));
			journal.put("model", journal.get("model").replace("\"MODEL_CALL\"", "0"));
		});
		assertCorrupt("float journal schema version", readers, fixture -> {
			RMap<String, String> journal = hash(fixture.runKey(3));
			String entry = journal.get("model");
			assertThat(entry).startsWith("{\"id\"");
			journal.put("model", entry.replaceFirst("\"schemaVersion\":1", "\"schemaVersion\":1.7"));
		});
	}

	@Test
	void keysOfTheWrongRedisTypeFailClosedWithoutMutation() {
		assertCorrupt("string at namespace key", Set.of(Op.values()), fixture -> {
			clientA.getBucket(fixture.nsKey()).delete();
			clientA.getBucket(fixture.nsKey(), StringCodec.INSTANCE).set("not-a-hash");
		});
		assertCorrupt("string at tasks key", Set.of(Op.FIND, Op.SUBMIT, Op.COMMIT), fixture -> {
			clientA.getBucket(fixture.runKey(2)).delete();
			clientA.getBucket(fixture.runKey(2), StringCodec.INSTANCE).set("not-a-hash");
		});
		assertCorrupt("list at outbox key", Set.of(Op.FIND, Op.SUBMIT, Op.COMMIT), fixture -> {
			clientA.getBucket(fixture.runKey(4)).delete();
			clientA.getList(fixture.runKey(4), StringCodec.INSTANCE).add("x");
		});
	}

	@Test
	void subMillisecondLeaseExpiryMatchesMemoryStore() {
		Instant start = Instant.parse("2026-10-14T00:00:00.000500Z");
		FixedClock redisClock = new FixedClock(start);
		FixedClock memoryClock = new FixedClock(start);
		RunStore redis = store(clientA, uniquePrefix(), redisClock);
		RunStore memory = new MemoryRunStore(memoryClock);
		Duration ttl = Duration.ofMillis(10);
		UUID owner = UUID.randomUUID();
		RunSpec spec = spec("one", "key", "{}");
		for (RunStore store : List.of(redis, memory)) {
			store.submit(spec, context("one"));
		}
		RunLease redisLease = redis.acquire(spec.namespace(), "one", owner, ttl);
		RunLease memoryLease = memory.acquire(spec.namespace(), "one", owner, ttl);
		assertThat(redisLease).isEqualTo(memoryLease);
		assertThat(redisLease.expiresAt()).isEqualTo(Instant.parse("2026-10-14T00:00:00.010500Z"));

		// 0.3ms before the exact expiry the lease is still live in both stores.
		redisClock.advance(Duration.ofNanos(9_700_000));
		memoryClock.advance(Duration.ofNanos(9_700_000));
		assertThat(redis.commit(spec.namespace(), "one", redisLease, idle(0)))
			.isEqualTo(memory.commit(spec.namespace(), "one", memoryLease, idle(0)));
		assertThatThrownBy(() -> redis.acquire(spec.namespace(), "one", UUID.randomUUID(), ttl))
			.isInstanceOf(RunStoreConflictException.class);

		redisClock.advance(Duration.ofNanos(300_000));
		memoryClock.advance(Duration.ofNanos(300_000));
		assertThatThrownBy(() -> redis.commit(spec.namespace(), "one", redisLease, idle(1)))
			.isInstanceOf(RunStoreConflictException.class);
		assertThatThrownBy(() -> memory.commit(spec.namespace(), "one", memoryLease, idle(1)))
			.isInstanceOf(RunStoreConflictException.class);
		UUID next = UUID.randomUUID();
		assertThat(redis.acquire(spec.namespace(), "one", next, ttl))
			.isEqualTo(memory.acquire(spec.namespace(), "one", next, ttl));
	}

	@Test
	void clockBeforeEpochIsRejectedBeforeAnyWrite() {
		String prefix = uniquePrefix();
		FixedClock early = new FixedClock(Instant.parse("1969-12-31T23:59:59Z"));
		RedisRunStore store = store(clientA, prefix, early);
		RunSpec spec = spec("one", "key", "{}");
		store.submit(spec, context("one"));
		Map<String, String> before = dump(prefix);
		assertThatThrownBy(() -> store.acquire(spec.namespace(), "one", UUID.randomUUID(), Duration.ofMillis(100)))
			.isInstanceOf(IllegalStateException.class)
			.hasMessageContaining("before 1970");
		assertThat(dump(prefix)).isEqualTo(before);
		assertThat(store.find(spec.namespace(), "one")).isPresent();

		early.advance(Duration.ofSeconds(2));
		RunLease lease = store.acquire(spec.namespace(), "one", UUID.randomUUID(), TTL);
		RedisRunStore rewound = store(clientA, prefix, new FixedClock(Instant.parse("1969-12-31T23:59:59Z")));
		Map<String, String> leased = dump(prefix);
		assertThatThrownBy(() -> rewound.renew(lease, TTL)).isInstanceOf(IllegalStateException.class);
		assertThatThrownBy(() -> rewound.commit(spec.namespace(), "one", lease, idle(0)))
			.isInstanceOf(IllegalStateException.class);
		assertThat(dump(prefix)).isEqualTo(leased);
		assertThat(store.commit(spec.namespace(), "one", lease, idle(0)).revision()).isEqualTo(1);
	}

	@Test
	void keyPrefixWithHashTagBracesIsRejected() {
		for (String prefix : List.of("app{}", "app{x}", "app}")) {
			assertThatThrownBy(() -> store(clientA, prefix, clock)).isInstanceOf(IllegalArgumentException.class)
				.hasMessage("keyPrefix must not contain '{' or '}'");
		}
	}

	@Test
	void capabilitiesAdvertisePersistenceOnlyForDurableDeployment() {
		RedisRunStore plain = store(clientA, uniquePrefix(), clock);
		assertThat(plain.capabilities().persistent()).isFalse();
		assertThat(plain.capabilities().supportsDurableMode()).isFalse();
		assertThatThrownBy(() -> plain.capabilities().requireDurableMode()).isInstanceOf(IllegalStateException.class);

		RedisRunStore durable = RedisRunStore.builder()
			.redisson(clientA)
			.clock(clock)
			.keyPrefix(uniquePrefix())
			.durableDeployment(true)
			.build();
		assertThat(durable.capabilities().supportsDurableMode()).isTrue();
		durable.capabilities().requireDurableMode();
	}

	@Test
	void ttlValidationMatchesCore() {
		Fixture fixture = fixture();
		RunStore memory = new MemoryRunStore(clock);
		memory.submit(fixture.spec(), context("one"));
		RunLease memoryLease = memory.acquire(fixture.namespace(), "one", fixture.lease().ownerId(), TTL);
		Map<String, String> before = dump(fixture.prefix());
		List<Duration> invalid = new ArrayList<>(List.of(Duration.ZERO, Duration.ofMillis(-1), Duration.ofNanos(1),
				Duration.ofMillis(1).plusNanos(1), Duration.ofDays(1).plusMillis(1), Duration.ofDays(2)));
		invalid.add(null);
		for (Duration ttl : invalid) {
			assertSameFailure("acquire " + ttl, () -> fixture.store().acquire(fixture.namespace(), "one", UUID.randomUUID(), ttl),
					() -> memory.acquire(fixture.namespace(), "one", UUID.randomUUID(), ttl));
			assertSameFailure("renew " + ttl, () -> fixture.store().renew(fixture.lease(), ttl),
					() -> memory.renew(memoryLease, ttl));
		}
		assertThat(dump(fixture.prefix())).isEqualTo(before);

		RedisRunStore fresh = store(clientA, uniquePrefix(), clock);
		RunSpec spec = spec("one", "key", "{}");
		fresh.submit(spec, context("one"));
		RunLease shortest = fresh.acquire(spec.namespace(), "one", UUID.randomUUID(), Duration.ofMillis(1));
		assertThat(shortest.fencingToken()).isEqualTo(1);
		assertThat(shortest.expiresAt()).isEqualTo(clock.instant().plusMillis(1));
		assertThat(fresh.renew(shortest, Duration.ofDays(1)).expiresAt()).isEqualTo(clock.instant().plus(Duration.ofDays(1)));
	}

	@Test
	void invalidCommitsFailWithTheSameExceptionAndMessageAsTheMemoryStore() {
		RedisRunStore redis = store(clientA, uniquePrefix(), clock);
		RunStore memory = new MemoryRunStore(clock);
		RunSpec spec = spec("one", "key", "{}");
		UUID owner = UUID.randomUUID();
		TaskSpec child = task(spec, "child");
		RunSpec otherRun = spec("other", "other-key", "{}");
		RunCommit.TaskUpdate newChild = new RunCommit.TaskUpdate(child, -1, RunState.PENDING);
		RunEntry model = entry("model", RunEntry.Kind.MODEL_CALL, "{}");
		RunEntry event = entry("event", RunEntry.Kind.EVENT, "{}");
		Map<String, RunLease> leases = new LinkedHashMap<>();
		for (RunStore store : List.of(redis, memory)) {
			store.submit(spec, context("one"));
			leases.put(store == redis ? "redis" : "memory", store.acquire(spec.namespace(), "one", owner, TTL));
		}
		RunLease lease = leases.get("redis");
		assertThat(lease).isEqualTo(leases.get("memory"));
		RunLease stranger = new RunLease(spec.namespace(), UUID.randomUUID(), lease.fencingToken(), lease.expiresAt());

		Map<String, RunCommit> beforeProgress = new LinkedHashMap<>();
		beforeProgress.put("stale revision", idle(3));
		beforeProgress.put("illegal transition", new RunCommit(0, RunState.WAITING, null, List.of(), List.of(), List.of()));
		beforeProgress.put("new task not pending", new RunCommit(0, RunState.RUNNING, null,
				List.of(new RunCommit.TaskUpdate(child, -1, RunState.RUNNING)), List.of(), List.of()));
		beforeProgress.put("new task with revision", new RunCommit(0, RunState.RUNNING, null,
				List.of(new RunCommit.TaskUpdate(child, 0, RunState.PENDING)), List.of(), List.of()));
		beforeProgress.put("duplicate task update", new RunCommit(0, RunState.RUNNING, null,
				List.of(newChild, newChild), List.of(), List.of()));
		beforeProgress.put("task of another run", new RunCommit(0, RunState.RUNNING, null,
				List.of(new RunCommit.TaskUpdate(task(otherRun, "child"), -1, RunState.PENDING)), List.of(),
				List.of()));
		beforeProgress.put("non-event outbox", new RunCommit(0, RunState.RUNNING, null, List.of(), List.of(),
				List.of(model)));
		for (Map.Entry<String, RunCommit> invalid : beforeProgress.entrySet()) {
			assertSameFailure(invalid.getKey(), () -> redis.commit(spec.namespace(), "one", lease, invalid.getValue()),
					() -> memory.commit(spec.namespace(), "one", lease, invalid.getValue()));
		}
		assertSameFailure("foreign owner", () -> redis.commit(spec.namespace(), "one", stranger, idle(0)),
				() -> memory.commit(spec.namespace(), "one", stranger, idle(0)));
		assertSameFailure("unreserved run", () -> redis.commit(spec.namespace(), "missing", lease, idle(0)),
				() -> memory.commit(spec.namespace(), "missing", lease, idle(0)));

		RunCommit progress = new RunCommit(0, RunState.RUNNING, payload("{}"), List.of(newChild), List.of(model),
				List.of(event));
		assertThat(redis.commit(spec.namespace(), "one", lease, progress))
			.isEqualTo(memory.commit(spec.namespace(), "one", lease, progress));

		Map<String, RunCommit> afterProgress = new LinkedHashMap<>();
		afterProgress.put("task revision mismatch", new RunCommit(1, RunState.RUNNING, null,
				List.of(new RunCommit.TaskUpdate(child, 4, RunState.RUNNING)), List.of(), List.of()));
		afterProgress.put("illegal task transition", new RunCommit(1, RunState.RUNNING, null,
				List.of(new RunCommit.TaskUpdate(child, 0, RunState.WAITING)), List.of(), List.of()));
		afterProgress.put("terminal run with active task", new RunCommit(1, RunState.SUCCEEDED, null, List.of(),
				List.of(), List.of()));
		afterProgress.put("append-only id with different data", new RunCommit(1, RunState.RUNNING, null, List.of(),
				List.of(entry("model", RunEntry.Kind.MODEL_CALL, "{\"other\":1}")), List.of()));
		for (Map.Entry<String, RunCommit> invalid : afterProgress.entrySet()) {
			assertSameFailure(invalid.getKey(), () -> redis.commit(spec.namespace(), "one", lease, invalid.getValue()),
					() -> memory.commit(spec.namespace(), "one", lease, invalid.getValue()));
		}
		assertThat(redis.find(spec.namespace(), "one")).isEqualTo(memory.find(spec.namespace(), "one"));
	}

	@Test
	@SuppressWarnings({ "unchecked", "rawtypes" })
	void scriptTransportFailureIsRethrownUnchanged() {
		RedissonClient redisson = mock(RedissonClient.class);
		RScript script = mock(RScript.class);
		RuntimeException transportFailure = new RuntimeException("script transport failed");
		when(redisson.getScript(any(OptionalOptions.class))).thenReturn(script);
		when(script.eval(any(RScript.Mode.class), anyString(), any(RScript.ReturnType.class), any(List.class),
				any(Object[].class))).thenThrow(transportFailure);
		RedisRunStore store = RedisRunStore.builder().redisson(redisson).clock(clock).build();
		RunSpec spec = spec("one", "key", "{}");
		RunLease lease = new RunLease(spec.namespace(), UUID.randomUUID(), 1, clock.instant().plus(TTL));

		assertThatThrownBy(() -> store.submit(spec, context("one"))).isSameAs(transportFailure);
		assertThatThrownBy(() -> store.find(spec.namespace(), "one")).isSameAs(transportFailure);
		assertThatThrownBy(() -> store.activeRun(spec.namespace())).isSameAs(transportFailure);
		assertThatThrownBy(() -> store.acquire(spec.namespace(), "one", UUID.randomUUID(), TTL))
			.isSameAs(transportFailure);
		assertThatThrownBy(() -> store.renew(lease, TTL)).isSameAs(transportFailure);
		assertThatThrownBy(() -> store.release(lease)).isSameAs(transportFailure);
		assertThatThrownBy(() -> store.commit(spec.namespace(), "one", lease, idle(0))).isSameAs(transportFailure);
	}

	@Test
	void lateScriptReplyIsNotResentAndRunsExactlyOnce() throws Exception {
		Config config = new Config();
		config.useSingleServer()
			.setAddress("redis://" + VALKEY.getHost() + ":" + VALKEY.getMappedPort(6379))
			.setTimeout(300)
			.setRetryAttempts(4)
			.setRetryInterval(50);
		RedissonClient impatient = Redisson.create(config);
		try {
			String prefix = uniquePrefix();
			RedisRunStore store = store(impatient, prefix, clock);
			RunSpec spec = spec("one", "key", "{}");
			store.submit(spec, context("one"));
			UUID owner = UUID.randomUUID();
			String nsKey = new RedisRunKeys(prefix).namespaceKey(spec.namespace());
			// Load ACQUIRE into the server script cache so the delayed EVALSHA executes it.
			RunSpec warmup = RunSpec.from(context("warmup"), "app", "warmup", "key", "graph", "1", spec.input());
			store.submit(warmup, context("warmup"));
			store.acquire(warmup.namespace(), "warmup", UUID.randomUUID(), TTL);

			// Hold every script on the server so the reply arrives after the client timeout.
			// A resent ACQUIRE would see its own first run as a live owner and report a
			// conflict. The retry setting is per store, so COMMIT is covered the same way.
			assertThat(VALKEY.execInContainer("valkey-cli", "CONFIG", "RESETSTAT").getStdout()).contains("OK");
			// The pause outlasts the client's whole retry window (5 x (300ms + 50ms)).
			ExecResult pause = VALKEY.execInContainer("valkey-cli", "CLIENT", "PAUSE", "2500", "WRITE");
			assertThat(pause.getStdout()).contains("OK");
			assertThatThrownBy(() -> store.acquire(spec.namespace(), "one", owner, TTL))
				.isInstanceOf(RedisTimeoutException.class)
				.isNotInstanceOf(RunStoreConflictException.class);
			Thread.sleep(3000);
			String stats = VALKEY.execInContainer("valkey-cli", "INFO", "commandstats").getStdout();
			assertThat(scriptCalls(stats)).as(stats).isEqualTo(1);
			assertThat(hash(nsKey).get("fence")).isEqualTo("1");
			assertThat(hash(nsKey).get("lease_owner")).isEqualTo(owner.toString());
			RunLease lease = new RunLease(spec.namespace(), owner, 1, clock.instant().plus(TTL));
			assertThat(store.commit(spec.namespace(), "one", lease, idle(0)).revision()).isEqualTo(1);
		}
		finally {
			impatient.shutdown();
		}
	}

	@Test
	void keysNeverExpireAndShareOneClusterSlotPerNamespace() {
		String prefix = uniquePrefix();
		RedisRunStore store = store(clientA, prefix, clock);
		RunSpec spec = spec("one", "key", "{}");
		store.submit(spec, context("one"));
		assertNoTtl(prefix);
		RunLease lease = store.acquire(spec.namespace(), "one", UUID.randomUUID(), TTL);
		assertNoTtl(prefix);
		lease = store.renew(lease, TTL);
		TaskSpec task = task(spec, "child");
		store.commit(spec.namespace(), "one", lease, new RunCommit(0, RunState.RUNNING, payload("{}"),
				List.of(new RunCommit.TaskUpdate(task, -1, RunState.PENDING)),
				List.of(entry("model", RunEntry.Kind.MODEL_CALL, "{}")),
				List.of(entry("event", RunEntry.Kind.EVENT, "{}"))));
		assertNoTtl(prefix);
		assertThat(store.release(lease)).isTrue();
		clock.advance(Duration.ofMinutes(5));
		RunLease next = store.acquire(spec.namespace(), "one", UUID.randomUUID(), TTL);
		store.commit(spec.namespace(), "one", next, new RunCommit(1, RunState.SUCCEEDED, null,
				List.of(new RunCommit.TaskUpdate(task, 0, RunState.CANCELLED)), List.of(), List.of()));
		clock.advance(Duration.ofDays(30));
		store.find(spec.namespace(), "one");
		store.activeRun(spec.namespace());
		assertNoTtl(prefix);

		List<Object> runKeys = new RedisRunKeys(prefix).runKeys(spec.namespace(), "one");
		assertThat(keySet(prefix)).containsExactlyInAnyOrderElementsOf(runKeys.stream().map(String.class::cast).toList());
		Set<Integer> slots = new HashSet<>();
		for (Object key : runKeys) {
			slots.add(slot((String) key));
		}
		assertThat(slots).hasSize(1);

		RunSpec otherTenant = RunSpec.from(context("one", "other", "user"), "app", "session", "key", "graph", "1",
				spec.input());
		RunSpec appWithDelimiter = RunSpec.from(context("one"), "app:session", "x", "key", "graph", "1", spec.input());
		RunSpec sessionWithDelimiter = RunSpec.from(context("one"), "app", "session:x", "key", "graph", "1",
				spec.input());
		Set<String> tags = new HashSet<>();
		for (RunSpec candidate : List.of(spec, otherTenant, appWithDelimiter, sessionWithDelimiter)) {
			tags.add(hashTag(new RedisRunKeys(prefix).namespaceKey(candidate.namespace())));
		}
		assertThat(tags).hasSize(4).allMatch(tag -> !tag.contains("{") && !tag.contains("}"));
	}

	@Test
	void runStoreWorkloadLeavesLegacyKeyspaceUntouched() throws Exception {
		String suffix = UUID.randomUUID().toString();
		RunnableConfig legacyConfig = RunnableConfig.builder().threadId("legacy-" + suffix).build();
		RedisSaver.builder()
			.redisson(clientA)
			.stateSerializer(STATE_SERIALIZER)
			.build()
			.put(legacyConfig, checkpoint("legacy"));
		RedisVersionedSaver.builder()
			.redisson(clientA)
			.stateSerializer(STATE_SERIALIZER)
			.storageKey("argi:test:checkpoint:versioned:" + suffix)
			.build()
			.putIfVersion(RunnableConfig.builder().threadId("versioned-" + suffix).build(), checkpoint("versioned"), 0);
		RedisLeasedCheckpointSaver leased = RedisLeasedCheckpointSaver.builder()
			.redisson(clientA)
			.stateSerializer(STATE_SERIALIZER)
			.leaseOptions(new LeaseOptions(Duration.ofSeconds(30), Duration.ofSeconds(5)))
			.storageKeyPrefix("argi:test:checkpoint:leased:" + suffix)
			.build();
		RunnableConfig leasedConfig = RunnableConfig.builder().threadId("leased-" + suffix).build();
		ExecutionLease legacyLease = leased.acquireLease(leasedConfig, UUID.randomUUID());
		leased.putIfLeasedVersion(leasedConfig, checkpoint("leased"), 0, legacyLease);
		new RedisStore(clientA, "argi:test:store:" + suffix)
			.putItem(new StoreItem(List.of("users", "u1"), "profile", Map.of("name", "Ada")));
		Predicate<String> nonRun = key -> !key.startsWith("argi:run:");
		Map<String, String> before = dump("*", nonRun);
		assertThat(before).isNotEmpty();

		// Default key prefix on purpose: the workload must stay inside argi:run:v1.
		RedisRunStore store = RedisRunStore.builder().redisson(clientA).clock(clock).build();
		String session = "session-" + suffix;
		RunSpec spec = RunSpec.from(context("one"), "app", session, "key", "graph", "1", payload("{}"));
		store.submit(spec, context("one"));
		RunLease lease = store.acquire(spec.namespace(), "one", UUID.randomUUID(), TTL);
		lease = store.renew(lease, TTL);
		store.commit(spec.namespace(), "one", lease, new RunCommit(0, RunState.RUNNING, payload("{}"),
				List.of(new RunCommit.TaskUpdate(task(spec, "child"), -1, RunState.PENDING)),
				List.of(entry("model", RunEntry.Kind.MODEL_CALL, "{}")),
				List.of(entry("event", RunEntry.Kind.EVENT, "{}"))));
		store.commit(spec.namespace(), "one", lease, new RunCommit(1, RunState.CANCELLED, null,
				List.of(new RunCommit.TaskUpdate(task(spec, "child"), 0, RunState.CANCELLED)), List.of(), List.of()));
		store.release(lease);
		assertThat(keySet(RedisRunStore.DEFAULT_KEY_PREFIX)).isNotEmpty();

		assertThat(dump("*", nonRun)).isEqualTo(before);
	}

	@Test
	void fenceAndRevisionOverflowThrowArithmeticExceptionWithoutMutation() {
		Fixture fixture = fixture();
		hash(fixture.nsKey()).put("fence", MAX_LONG);
		hash(fixture.nsKey()).put("lease_fence", MAX_LONG);
		// Let the stored lease expire so acquire reaches the fence increment.
		clock.advance(Duration.ofMinutes(1));
		Map<String, String> before = dump(fixture.prefix());
		assertThatThrownBy(() -> fixture.store().acquire(fixture.namespace(), "one", UUID.randomUUID(), TTL))
			.isInstanceOf(ArithmeticException.class);
		assertThat(dump(fixture.prefix())).isEqualTo(before);

		Fixture revision = fixture();
		hash(revision.runKey(1)).put("revision", MAX_LONG);
		Map<String, String> beforeCommit = dump(revision.prefix());
		assertThatThrownBy(() -> revision.store()
			.commit(revision.namespace(), "one", revision.lease(), idle(Long.MAX_VALUE)))
			.isInstanceOf(ArithmeticException.class);
		assertThat(dump(revision.prefix())).isEqualTo(beforeCommit);
	}

	@Test
	void leaseExpiryComparesDecimalValuesNotStrings() {
		// Stored expiry and clock are epoch nanoseconds.
		for (String[] values : List.of(new String[] { "9", "1792195200000000000" },
				new String[] { "999999999999999999", "1000000000000000000" })) {
			String prefix = uniquePrefix();
			FixedClock local = new FixedClock(Instant.ofEpochSecond(0, Long.parseLong(values[1])));
			RedisRunStore store = store(clientA, prefix, local);
			RunSpec spec = spec("one", "key", "{}");
			store.submit(spec, context("one"));
			String nsKey = new RedisRunKeys(prefix).namespaceKey(spec.namespace());
			// A stale lease whose expiry is lexicographically larger but numerically smaller than now.
			hash(nsKey).putAll(Map.of("fence", "1", "lease_owner", UUID.randomUUID().toString(), "lease_fence", "1",
					"lease_expires", values[0]));
			RunLease lease = store.acquire(spec.namespace(), "one", UUID.randomUUID(), TTL);
			assertThat(lease.fencingToken()).isEqualTo(2);
			assertThat(hash(nsKey).get("lease_expires"))
				.isEqualTo(Long.toString(RedisRunStore.epochNanos(lease.expiresAt())));
			local.advance(Duration.ofSeconds(29));
			RunLease renewed = store.renew(lease, TTL);
			assertThat(store.commit(spec.namespace(), "one", renewed, idle(0)).state()).isEqualTo(RunState.RUNNING);
			local.advance(TTL);
			assertThatThrownBy(() -> store.commit(spec.namespace(), "one", renewed, idle(1)))
				.isInstanceOf(RunStoreConflictException.class);
		}
	}

	/** Total EVAL and EVALSHA calls in an INFO commandstats reply. */
	private static long scriptCalls(String stats) {
		long calls = 0;
		Matcher matcher = Pattern.compile("cmdstat_eval(?:sha)?:calls=(\\d+),").matcher(stats);
		while (matcher.find()) {
			calls += Long.parseLong(matcher.group(1));
		}
		return calls;
	}

	private static void assertSameFailure(String name, Runnable redis, Runnable memory) {
		Throwable expected = catchFailure(memory);
		assertThat(expected).as(name + " must fail in the memory store").isNotNull();
		Throwable actual = catchFailure(redis);
		assertThat(actual).as(name).isNotNull();
		assertThat(actual.getClass()).as(name).isEqualTo(expected.getClass());
		assertThat(actual.getMessage()).as(name).isEqualTo(expected.getMessage());
	}

	private static Throwable catchFailure(Runnable action) {
		try {
			action.run();
			return null;
		}
		catch (RuntimeException ex) {
			return ex;
		}
	}

	private void assertCorrupt(String name, Set<Op> ops, Consumer<Fixture> corruption) {
		assertCorrupt(name, ops, corruption, null);
	}

	private void assertCorrupt(String name, Set<Op> ops, Consumer<Fixture> corruption, RunSpec submit) {
		Fixture fixture = fixture();
		corruption.accept(fixture);
		Map<String, String> before = dump(fixture.prefix());
		for (Op op : ops) {
			assertThatThrownBy(() -> run(op, fixture, submit)).as(name + " / " + op)
				.isInstanceOf(IllegalStateException.class)
				.isNotInstanceOf(RunStoreConflictException.class)
				.hasMessageContaining("corrupt");
			assertThat(dump(fixture.prefix())).as(name + " / " + op + " must not mutate").isEqualTo(before);
		}
	}

	private static void run(Op op, Fixture fixture, RunSpec submit) {
		RedisRunStore store = fixture.store();
		RunNamespace namespace = fixture.namespace();
		switch (op) {
			case FIND -> store.find(namespace, "one");
			case ACTIVE -> store.activeRun(namespace);
			case SUBMIT -> {
				RunSpec spec = submit != null ? submit : fixture.spec();
				store.submit(spec, context(spec.runId()));
			}
			case ACQUIRE -> store.acquire(namespace, "one", UUID.randomUUID(), TTL);
			case RENEW -> store.renew(fixture.lease(), TTL);
			case RELEASE -> store.release(fixture.lease());
			case COMMIT -> store.commit(namespace, "one", fixture.lease(), idle(1));
			default -> throw new IllegalArgumentException(op.name());
		}
	}

	/** A RUNNING run at revision 1 with a live lease, checkpoint, task, journal and outbox. */
	private Fixture fixture() {
		String prefix = uniquePrefix();
		RedisRunStore store = store(clientA, prefix, clock);
		RunSpec spec = spec("one", "key", "{}");
		store.submit(spec, context("one"));
		RunLease lease = store.acquire(spec.namespace(), "one", UUID.randomUUID(), TTL);
		store.commit(spec.namespace(), "one", lease,
				new RunCommit(0, RunState.RUNNING, payload("{\"node\":\"start\"}"),
						List.of(new RunCommit.TaskUpdate(task(spec, "child"), -1, RunState.PENDING)),
						List.of(entry("model", RunEntry.Kind.MODEL_CALL, "{}")),
						List.of(entry("event", RunEntry.Kind.EVENT, "{}"))));
		return new Fixture(prefix, store, spec, lease, new RedisRunKeys(prefix).runKeys(spec.namespace(), "one"));
	}

	private static RunCommit idle(long expectedRevision) {
		return new RunCommit(expectedRevision, RunState.RUNNING, null, List.of(), List.of(), List.of());
	}

	private static void assertNoTtl(String prefix) {
		Set<String> keys = keySet(prefix);
		assertThat(keys).isNotEmpty();
		for (String key : keys) {
			assertThat(clientA.getKeys().remainTimeToLive(key)).as("PTTL of " + key).isEqualTo(-1L);
		}
	}

	private static int slot(String key) {
		return CRC16.crc16(hashTag(key).getBytes(StandardCharsets.UTF_8)) % 16384;
	}

	/** Redis cluster hash-tag rule: the first non-empty {...} section, otherwise the whole key. */
	private static String hashTag(String key) {
		int start = key.indexOf('{');
		if (start >= 0) {
			int end = key.indexOf('}', start + 1);
			if (end > start + 1) {
				return key.substring(start + 1, end);
			}
		}
		return key;
	}

	private static RMap<String, String> hash(String key) {
		return clientA.getMap(key, StringCodec.INSTANCE);
	}

	private static Set<String> keySet(String prefix) {
		Set<String> keys = new HashSet<>();
		clientA.getKeys().getKeysByPattern(prefix + "*").forEach(keys::add);
		return keys;
	}

	private static Map<String, String> dump(String prefix) {
		return dump(prefix + "*", key -> true);
	}

	private static Map<String, String> dump(String pattern, Predicate<String> filter) {
		Map<String, String> result = new TreeMap<>();
		for (String key : clientA.getKeys().getKeysByPattern(pattern)) {
			if (filter.test(key)) {
				result.put(key, Base64.getEncoder().encodeToString(clientA.getBucket(key).dump()));
			}
		}
		return result;
	}

	private static <T> List<T> race(List<Callable<T>> actions) throws Exception {
		ExecutorService executor = Executors.newFixedThreadPool(actions.size());
		try {
			CountDownLatch start = new CountDownLatch(1);
			List<Future<T>> futures = new ArrayList<>();
			for (Callable<T> action : actions) {
				futures.add(executor.submit(() -> {
					start.await();
					return action.call();
				}));
			}
			start.countDown();
			List<T> values = new ArrayList<>();
			for (Future<T> future : futures) {
				values.add(future.get(30, TimeUnit.SECONDS));
			}
			return values;
		}
		finally {
			executor.shutdownNow();
		}
	}

	private static RedisRunStore store(RedissonClient client, String prefix, Clock clock) {
		return RedisRunStore.builder().redisson(client).clock(clock).keyPrefix(prefix).build();
	}

	private static String uniquePrefix() {
		return "argi:test:run:" + UUID.randomUUID() + ":v1";
	}

	private static RedissonClient newClient() {
		Config config = new Config();
		config.useSingleServer().setAddress("redis://" + VALKEY.getHost() + ":" + VALKEY.getMappedPort(6379));
		return Redisson.create(config);
	}

	private static Checkpoint checkpoint(String result) {
		return Checkpoint.builder()
			.id("checkpoint-" + result + "-" + UUID.randomUUID())
			.nodeId("node-" + result)
			.nextNodeId("next")
			.state(new LinkedHashMap<>(Map.of("result", result)))
			.build();
	}

	private static RunPayload payload(String json) {
		return new RunPayload(1, "test.input", json);
	}

	private static RunEntry entry(String id, RunEntry.Kind kind, String json) {
		return new RunEntry(1, id, kind, payload(json));
	}

	private static TrustedExecutionContext context(String runId) {
		return context(runId, "tenant", "user");
	}

	private static TrustedExecutionContext context(String runId, String tenant, String user) {
		return TrustedExecutionContext.builder()
			.tenant(new TenantReference("iam", tenant))
			.principal(new PrincipalReference("iam", user))
			.runId(runId)
			.correlationId("trace")
			.budget(new ExecutionBudget(Map.of("tokens", 100L)))
			.delegationScope(new DelegationScope(Set.of("read")))
			.build();
	}

	private static RunSpec spec(String runId, String key, String json) {
		return RunSpec.from(context(runId), "app", "session", key, "graph", "1", payload(json));
	}

	private static TaskSpec task(RunSpec run, String taskId) {
		return new TaskSpec(1, taskId, run.runId(), "task", "1", payload("{}"),
				context(run.runId()).delegate(taskId, Set.of("read"), null, null).toReference());
	}

	private enum Op {

		FIND, ACTIVE, SUBMIT, ACQUIRE, RENEW, RELEASE, COMMIT

	}

	private record Fixture(String prefix, RedisRunStore store, RunSpec spec, RunLease lease, List<Object> keys) {

		RunNamespace namespace() {
			return spec.namespace();
		}

		String nsKey() {
			return (String) keys.get(0);
		}

		String runKey(int index) {
			return (String) keys.get(index);
		}

	}

	private static final class FixedClock extends Clock {

		private Instant now;

		FixedClock(Instant now) {
			this.now = now;
		}

		void advance(Duration duration) {
			now = now.plus(duration);
		}

		@Override
		public ZoneId getZone() {
			return ZoneOffset.UTC;
		}

		@Override
		public Clock withZone(ZoneId zone) {
			return this;
		}

		@Override
		public Instant instant() {
			return now;
		}

	}

}
