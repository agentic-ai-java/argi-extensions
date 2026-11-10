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

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.redisson.Redisson;
import org.redisson.api.RScript;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.Config;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real Valkey, one JVM, explicit application adapter, fake executor. */
@Testcontainers(disabledWithoutDocker = true)
class RedisProgressDirectoryTest {

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
		client.shutdown();
	}

	@Test
	void claimRegistersAtomicallyAndCommitBeforePublisherCrashIsRecoverable() {
		String prefix = prefix();
		RedissonClient producer = newClient();
		try {
			var store = store(producer, prefix, 4);
			var lease = store.claim("s", "r", UUID.randomUUID(), Duration.ofSeconds(30));
			assertThat(new RedisProgressDirectory(client, prefix, 4).page("", 1).entries()).hasSize(1);
			store.commit(lease, commit("READY", 0));
			store.release(lease);
			// Graceful connection boundary, skipped publisher, NOT JVM/Redis kill.
		}
		finally {
			producer.shutdown();
		}
		var hints = new RedisProgressAdvisoryIndex(client, prefix + ":hints", 4);
		assertThat(hints.due(4)).isEmpty();
		var directory = new RedisProgressDirectory(client, prefix, 4);
		var result = repair(directory, "", 4, projection(prefix, hints), hints);
		assertThat(result.published()).isEqualTo(1);
		assertThat(result.issues()).isEmpty();
		assertThat(hints.due(4)).containsExactly(new RedisProgressAdvisoryIndex.Candidate("s", "r", 1));
		assertThat(repair(directory, "", 4, projection(prefix, hints), hints).published()).isZero();
	}

	@Test
	void capacityAndWrongTypeRejectBeforeAuthorityCreationAndExistingMemberCanReclaim() {
		String prefix = prefix();
		var store = store(client, prefix, 1);
		var first = store.claim("s1", "r", UUID.randomUUID(), Duration.ofSeconds(30));
		store.release(first);
		assertThatThrownBy(() -> store.claim("s2", "r", UUID.randomUUID(), Duration.ofSeconds(30)))
			.hasMessageContaining("DIRECTORY_FULL");
		assertThat(store.snapshot("s2", "r")).isEmpty();
		assertThat(client.getKeys().countExists(new RedisProgressKeys(prefix).sessionKey("s2"))).isZero();
		var second = store.claim("s1", "r", UUID.randomUUID(), Duration.ofSeconds(30));
		assertThat(second.fencingToken()).isGreaterThan(first.fencingToken());
		store.release(second);
		assertThatThrownBy(() -> new RedisProgressDirectory(client, prefix, 2)).hasMessageContaining("CONFIG");
		String broken = prefix();
		var brokenStore = store(client, broken, 1);
		client.getBucket(broken + ":directory:v1:members", StringCodec.INSTANCE).set("wrong type");
		assertThatThrownBy(() -> brokenStore.claim("s", "r", UUID.randomUUID(), Duration.ofSeconds(30)))
			.hasMessageContaining("DIRECTORY_TYPE");
		assertThat(brokenStore.snapshot("s", "r")).isEmpty();
	}

	@Test
	void concurrentCapacityAdmissionHasOneWinnerAndNoOrphanAuthority() throws Exception {
		String prefix = prefix();
		var store = store(client, prefix, 1);
		var pool = Executors.newFixedThreadPool(2);
		CountDownLatch start = new CountDownLatch(1);
		try {
			List<Callable<Boolean>> attempts = new ArrayList<>();
			for (String session : List.of("a", "b")) {
				attempts.add(() -> {
					start.await();
					try {
						store.claim(session, "r", UUID.randomUUID(), Duration.ofSeconds(30));
						return true;
					}
					catch (IllegalStateException ex) {
						assertThat(ex).hasMessageContaining("DIRECTORY_FULL");
						assertThat(store.snapshot(session, "r")).isEmpty();
						return false;
					}
				});
			}
			var a = pool.submit(attempts.get(0));
			var b = pool.submit(attempts.get(1));
			start.countDown();
			assertThat(List.of(a.get(10, TimeUnit.SECONDS), b.get(10, TimeUnit.SECONDS)))
				.containsExactlyInAnyOrder(true, false);
			assertThat(new RedisProgressDirectory(client, prefix, 1).page("", 10).entries()).hasSize(1);
		}
		finally {
			pool.shutdownNow();
		}
	}

	@Test
	void lexCursorHasNoOverflowAndConcurrentReadersCoverRetainedMembers() throws Exception {
		String prefix = prefix();
		var store = store(client, prefix, 40);
		for (int i = 0; i < 31; i++) {
			var lease = store.claim("s" + i, "r", UUID.randomUUID(), Duration.ofSeconds(30));
			store.release(lease);
		}
		var directory = new RedisProgressDirectory(client, prefix, 40);
		var pool = Executors.newFixedThreadPool(2);
		try {
			Callable<HashSet<RedisProgressDirectory.Identity>> sweep = () -> {
				var seen = new HashSet<RedisProgressDirectory.Identity>();
				String cursor = "";
				for (int i = 0; i < 40; i++) {
					var page = directory.page(cursor, 1);
					assertThat(page.entries()).hasSizeLessThanOrEqualTo(1);
					for (var id : page.entries()) {
						assertThat(seen.add(id)).isTrue();
					}
					cursor = page.after();
					if (page.done()) {
						return seen;
					}
				}
				throw new AssertionError("Unbounded cursor");
			};
			var results = pool.invokeAll(List.of(sweep, sweep));
			assertThat(results.get(0).get()).hasSize(31).isEqualTo(results.get(1).get());
		}
		finally {
			pool.shutdownNow();
		}
	}

	@Test
	void utf8IdentityBoundaryAndActualRespPageBytesAreBounded() throws Exception {
		String prefix = prefix();
		var store = store(client, prefix, 2);
		String id = "界".repeat(341) + "a";
		var lease = store.claim(id, id, UUID.randomUUID(), Duration.ofSeconds(30));
		store.release(lease);
		String encoded = RedisProgressDirectory.member(id, id);
		assertThat(encoded).hasSize(2733);
		try (Socket socket = new Socket(VALKEY.getHost(), VALKEY.getMappedPort(6379))) {
			socket.setSoTimeout(10000);
			String[] args = { "ZRANGEBYLEX", prefix + ":directory:v1:members", "-", "+", "LIMIT", "0", "1" };
			ByteArrayOutputStream request = new ByteArrayOutputStream();
			request.write(("*" + args.length + "\r\n").getBytes(StandardCharsets.US_ASCII));
			for (String arg : args) {
				byte[] bytes = arg.getBytes(StandardCharsets.UTF_8);
				request.write(("$" + bytes.length + "\r\n").getBytes(StandardCharsets.US_ASCII));
				request.write(bytes);
				request.write("\r\n".getBytes(StandardCharsets.US_ASCII));
			}
			socket.getOutputStream().write(request.toByteArray());
			InputStream in = socket.getInputStream();
			byte[] response = in.readNBytes(2746);
			assertThat(response).hasSize(2746);
			assertThat(new String(response, StandardCharsets.UTF_8)).isEqualTo("*1\r\n$2733\r\n" + encoded + "\r\n");
		}
		assertThatThrownBy(() -> store.claim(id + "a", "x", UUID.randomUUID(), Duration.ofSeconds(30)))
			.hasMessageContaining("1024");
		assertThat(store.snapshot(id + "a", "x")).isEmpty();
		assertThatThrownBy(() -> new RedisProgressDirectory(client, prefix, 2).page("x".repeat(2734), 1))
			.isInstanceOf(IllegalArgumentException.class);
	}

	@Test
	void explicitBackfillIsRequiredAndBadLargeMissingRecordsDoNotBlockOtherAuthority() {
		String prefix = prefix();
		var legacy = RedisLeasedProgressStore.builder().redisson(client).keyPrefix(prefix).build();
		for (String session : List.of("bad", "big", "missing", "good")) {
			var lease = legacy.claim(session, "r", UUID.randomUUID(), Duration.ofSeconds(30));
			legacy.commit(lease, commit("READY", 0));
			legacy.release(lease);
		}
		var directory = new RedisProgressDirectory(client, prefix, 4);
		assertThat(directory.page("", 10).entries()).isEmpty();
		for (String session : List.of("bad", "big", "missing", "good")) {
			directory.registerExisting(session, "r");
			directory.registerExisting(session, "r");
		}
		runMap(prefix, "bad").put("state", "");
		runMap(prefix, "big").put("state", "x".repeat(1000000));
		client.getKeys().delete((String) new RedisProgressKeys(prefix).keys("missing", "r").get(1));
		var hints = new RedisProgressAdvisoryIndex(client, prefix + ":hints", 4);
		var result = repair(directory, "", 4, projection(prefix, hints), hints);
		assertThat(result.issues()).hasSize(3);
		assertThat(result.page().entries()).hasSize(4);
		assertThat(result.published()).isEqualTo(1);
		assertThat(hints.due(10)).containsExactly(new RedisProgressAdvisoryIndex.Candidate("good", "r", 1));
		assertThat(runMap(prefix, "big").get("state")).hasSize(1000000);
		runMap(prefix, "big").put("state", "READY");
		runMap(prefix, "bad").put("state", "READY");
		assertThat(repair(directory, "", 4, projection(prefix, hints), hints).published()).isEqualTo(2);
	}

	@Test
	void visibilityAndRetryAdvancePastStaleBusyAndAbandonedFirstPage() throws Exception {
		String prefix = prefix();
		var hints = new RedisProgressAdvisoryIndex(client, prefix + ":hints", 3);
		for (int i = 1; i <= 3; i++) {
			publish(hints, "s" + i, 1);
		}
		var first = hints.poll(1, 10000).get(0);
		assertThat(hints.retry(first, 10000)).isTrue();
		assertThat(hints.retry(first, 10000)).isFalse();
		var second = hints.poll(1, 10000).get(0);
		var third = hints.poll(1, 10000).get(0);
		assertThat(List.of(first.candidate(), second.candidate(), third.candidate())).doesNotHaveDuplicates();
		assertThat(hints.poll(1, 10000)).isEmpty();
		assertThat(hints.ack(first)).isFalse();
		assertThat(hints.ack(second)).isTrue();
		assertThat(hints.ack(second)).isFalse();
		// Simulate expiry against Redis TIME, without depending on wall-clock sleeps.
		makeDue(prefix, first);
		assertThat(hints.poll(1, 10000).get(0).candidate()).isEqualTo(first.candidate());
	}

	@Test
	void staleAckCannotDeleteNewRevisionOrNewDeliveryAndCapacityRecovers() {
		String prefix = prefix();
		var hints = new RedisProgressAdvisoryIndex(client, prefix + ":hints", 2);
		publish(hints, "s", 1);
		var old = hints.poll(1, 10000).get(0);
		publish(hints, "s", 2);
		assertThatThrownBy(() -> publish(hints, "other", 1)).hasMessageContaining("capacity exhausted");
		assertThat(hints.ack(old)).isTrue();
		var newerRevision = hints.poll(1, 10000).get(0);
		assertThat(newerRevision.candidate().revision()).isEqualTo(2);
		makeDue(prefix, newerRevision);
		var redelivery = hints.poll(1, 10000).get(0);
		assertThat(redelivery.token()).isGreaterThan(newerRevision.token());
		assertThat(hints.ack(newerRevision)).isFalse();
		assertThat(hints.retry(newerRevision, 1)).isFalse();
		assertThat(hints.ack(redelivery)).isTrue();
		publish(hints, "other", 1);
		assertThat(hints.due(10)).hasSize(1);
	}

	@Test
	void concurrentPollsAndAckPublicationRacePreserveNewRevision() throws Exception {
		String prefix = prefix();
		var hints = new RedisProgressAdvisoryIndex(client, prefix + ":hints", 10);
		publish(hints, "s", 1);
		var pool = Executors.newFixedThreadPool(2);
		try {
			Callable<List<RedisProgressAdvisoryIndex.Delivery>> poll = () -> hints.poll(1, 10000);
			var deliveries = pool.invokeAll(List.of(poll, poll));
			List<RedisProgressAdvisoryIndex.Delivery> all = new ArrayList<>(deliveries.get(0).get());
			all.addAll(deliveries.get(1).get());
			assertThat(all).hasSize(1);
			var old = all.get(0);
			Callable<Boolean> ack = () -> hints.ack(old);
			Callable<Boolean> publish = () -> hints.publish(new RedisProgressAdvisoryIndex.Hint(
					new RedisProgressIndexRepairer.Metadata("s", "r", 2, "READY"), 0));
			var results = pool.invokeAll(List.of(ack, publish));
			assertThat(results.get(0).get()).isTrue();
			assertThat(results.get(1).get()).isTrue();
			assertThat(hints.due(10)).containsExactly(new RedisProgressAdvisoryIndex.Candidate("s", "r", 2));
		}
		finally {
			pool.shutdownNow();
		}
	}

	@Test
	void terminalAndUnknownStateAreNotExecutedAndMissingHintCreatesNoAuthority() throws Exception {
		String prefix = prefix();
		var store = store(client, prefix, 10);
		var hints = new RedisProgressAdvisoryIndex(client, prefix + ":hints", 10);
		var projection = projection(prefix, hints);
		var consumer = consumer(store, projection, hints, m -> "READY".equals(m.state()));
		for (String state : List.of("DONE", "FUTURE_UNKNOWN")) {
			seed(store, state, state);
			publish(hints, state, 1);
		}
		publish(hints, "never-created", 1);
		for (var delivery : hints.poll(10, 10000)) {
			assertThat(consumer.consume(delivery, UUID.randomUUID(), execution -> {
				throw new AssertionError("Ineligible authority executed");
			}).outcome()).isIn("STALE_OR_INELIGIBLE", "MISSING_AUTHORITY");
		}
		assertThat(store.snapshot("never-created", "r")).isEmpty();
		assertThat(hints.poll(10, 10000)).isEmpty();
	}

	@Test
	void claimThenRevisionRecheckRejectsOldHintAndSuccessfulAttemptCommitsOnce() throws Exception {
		String prefix = prefix();
		var store = store(client, prefix, 10);
		seed(store, "s", "READY");
		var hints = new RedisProgressAdvisoryIndex(client, prefix + ":hints", 10);
		publish(hints, "s", 0);
		publish(hints, "s", 1);
		var consumer = consumer(store, projection(prefix, hints), hints, m -> "READY".equals(m.state()));
		AtomicInteger calls = new AtomicInteger();
		var result = new ArrayList<String>();
		for (var delivery : hints.poll(10, 10000)) {
			result.add(consumer.consume(delivery, UUID.randomUUID(), execution -> {
				execution.guard();
				calls.incrementAndGet();
				return commit("DONE", execution.metadata().revision());
			}).outcome());
		}
		assertThat(result).containsExactly("STALE_OR_INELIGIBLE", "COMMITTED");
		assertThat(calls).hasValue(1);
		assertThat(store.snapshot("s", "r").orElseThrow().state()).isEqualTo("DONE");
		publish(hints, "s", 1);
		assertThat(consumer.consume(hints.poll(1, 10000).get(0), UUID.randomUUID(), e -> {
			throw new AssertionError("Old revision reran");
		}).outcome()).isEqualTo("STALE_OR_INELIGIBLE");
	}

	@Test
	void twoConsumersCompeteForOneLeaseAndOnlyOneExecutorRuns() throws Exception {
		String prefix = prefix();
		var store = store(client, prefix, 10);
		seed(store, "s", "READY");
		var hints = new RedisProgressAdvisoryIndex(client, prefix + ":hints", 10);
		publish(hints, "s", 1);
		var delivery = hints.poll(1, 10000).get(0);
		var consumer = consumer(store, projection(prefix, hints), hints, m -> "READY".equals(m.state()));
		var pool = Executors.newFixedThreadPool(2);
		CountDownLatch executing = new CountDownLatch(1);
		CountDownLatch finish = new CountDownLatch(1);
		AtomicInteger calls = new AtomicInteger();
		try {
			var first = pool.submit(() -> consumer.consume(delivery, UUID.randomUUID(), e -> {
				executing.countDown();
				assertThat(finish.await(10, TimeUnit.SECONDS)).isTrue();
				e.guard();
				calls.incrementAndGet();
				return commit("DONE", 1);
			}));
			assertThat(executing.await(10, TimeUnit.SECONDS)).isTrue();
			assertThat(consumer.consume(delivery, UUID.randomUUID(), e -> {
				throw new AssertionError("Second owner executed");
			}).outcome()).isEqualTo("BUSY");
			finish.countDown();
			assertThat(first.get(10, TimeUnit.SECONDS).outcome()).isEqualTo("COMMITTED");
			assertThat(calls).hasValue(1);
			// A competing retry may invalidate the old ack token; stale hint is harmless.
			makeDue(prefix, delivery);
			for (var retry : hints.poll(10, 10000)) {
				assertThat(consumer.consume(retry, UUID.randomUUID(), e -> {
					throw new AssertionError("Committed run reran");
				}).outcome()).isEqualTo("STALE_OR_INELIGIBLE");
			}
		}
		finally {
			finish.countDown();
			pool.shutdownNow();
		}
	}

	@Test
	void leaseLostBeforeDispatchAndDuringExecutorBlocksEffectsAndCommit() throws Exception {
		String prefix = prefix();
		var store = store(client, prefix, 10);
		seed(store, "s", "READY");
		var hints = new RedisProgressAdvisoryIndex(client, prefix + ":hints", 10);
		var projection = projection(prefix, hints);
		publish(hints, "s", 1);
		var delivery = hints.poll(1, 10000).get(0);
		AtomicInteger effects = new AtomicInteger();
		var consumer = consumer(store, projection, hints, m -> {
			expire(prefix, "s");
			return "READY".equals(m.state());
		});
		assertThat(consumer.consume(delivery, UUID.randomUUID(), e -> {
			effects.incrementAndGet();
			return commit("DONE", 1);
		}).outcome()).isEqualTo("LEASE_LOST");
		assertThat(effects).hasValue(0);
		makeDue(prefix, delivery);
		var another = hints.poll(1, 10000).get(0);
		var normal = consumer(store, projection, hints, m -> "READY".equals(m.state()));
		assertThat(normal.consume(another, UUID.randomUUID(), e -> {
			expire(prefix, "s");
			var takeover = store.claim("s", "r", UUID.randomUUID(), Duration.ofSeconds(30));
			try {
				e.guard();
				effects.incrementAndGet();
				return commit("DONE", 1);
			}
			finally {
				store.release(takeover);
			}
		}).outcome()).isEqualTo("LEASE_LOST");
		assertThat(effects).hasValue(0);
		assertThat(store.snapshot("s", "r").orElseThrow().runRevision()).isEqualTo(1);
	}

	@Test
	void executorFailureRetainsAuthorityAndCrashAfterEffectDoesNotPromiseExactlyOnce() throws Exception {
		String prefix = prefix();
		var store = store(client, prefix, 10);
		seed(store, "s", "READY");
		var hints = new RedisProgressAdvisoryIndex(client, prefix + ":hints", 10);
		var consumer = consumer(store, projection(prefix, hints), hints, m -> "READY".equals(m.state()));
		publish(hints, "s", 1);
		var delivery = hints.poll(1, 10000).get(0);
		AtomicInteger effects = new AtomicInteger();
		assertThatThrownBy(() -> consumer.consume(delivery, UUID.randomUUID(), e -> {
			e.guard();
			effects.incrementAndGet();
			throw new IllegalStateException("fake crash after external effect before commit");
		})).hasMessageContaining("fake crash");
		assertThat(store.snapshot("s", "r").orElseThrow().runRevision()).isEqualTo(1);
		makeDue(prefix, delivery);
		assertThat(consumer.consume(hints.poll(1, 10000).get(0), UUID.randomUUID(), e -> {
			e.guard();
			effects.incrementAndGet();
			return commit("DONE", 1);
		}).outcome()).isEqualTo("COMMITTED");
		assertThat(effects).hasValue(2); // Explicit counterexample to external
											// exactly-once.
	}

	@Test
	void discoveryContinuationDoesNotRetainAnUnboundedScanPage() {
		String prefix = prefix();
		var store = store(client, prefix, 40);
		for (int i = 0; i < 31; i++) {
			var lease = store.claim("s" + i, "r", UUID.randomUUID(), Duration.ofSeconds(30));
			store.release(lease);
		}
		int retained;
		if (Boolean.getBoolean("acceptance.legacyScan")) {
			var hints = new RedisProgressAdvisoryIndex(client, prefix + ":hints");
			var repairer = projection(prefix, hints);
			retained = repairer.repair(repairer.start(), 1, 10000).cursor().pendingKeys();
		}
		else {
			retained = new RedisProgressDirectory(client, prefix, 40).page("", 1).entries().size();
		}
		assertThat(retained).as("one-item discovery call must not retain SCAN overflow").isLessThanOrEqualTo(1);
	}

	@Test
	void redisTimeVisibilityExpiresWithoutClientClockInput() throws Exception {
		var hints = new RedisProgressAdvisoryIndex(client, prefix() + ":hints", 1);
		publish(hints, "s", 1);
		var first = hints.poll(1, 40).get(0);
		assertThat(hints.poll(1, 40)).isEmpty();
		Thread.sleep(80);
		var second = hints.poll(1, 10000).get(0);
		assertThat(second.candidate()).isEqualTo(first.candidate());
		assertThat(second.token()).isGreaterThan(first.token());
		assertThat(hints.ack(first)).isFalse();
		assertThat(hints.ack(second)).isTrue();
	}

	@Test
	void leasedMetadataValidationRejectsReleasedExpiredAndSupersededFences() {
		String prefix = prefix();
		var store = store(client, prefix, 2);
		var hints = new RedisProgressAdvisoryIndex(client, prefix + ":hints", 2);
		var projection = projection(prefix, hints);
		var first = store.claim("s", "r", UUID.randomUUID(), Duration.ofSeconds(30));
		store.commit(first, commit("READY", 0));
		assertThat(projection.metadata(first).orElseThrow().revision()).isEqualTo(1);
		store.release(first);
		assertThatThrownBy(() -> projection.metadata(first))
			.isInstanceOf(io.github.agentic.ai.graph.checkpoint.lease.LeaseLostException.class);
		var second = store.claim("s", "r", UUID.randomUUID(), Duration.ofSeconds(30));
		assertThatThrownBy(() -> projection.metadata(first))
			.isInstanceOf(io.github.agentic.ai.graph.checkpoint.lease.LeaseLostException.class);
		expire(prefix, "s");
		assertThatThrownBy(() -> projection.metadata(second))
			.isInstanceOf(io.github.agentic.ai.graph.checkpoint.lease.LeaseLostException.class);
		assertThatThrownBy(() -> store.commit(second, commit("DONE", 1)))
			.isInstanceOf(io.github.agentic.ai.graph.checkpoint.lease.LeaseLostException.class);
		assertThat(store.snapshot("s", "r").orElseThrow().state()).isEqualTo("READY");
	}

	@Test
	void directoryStateLimitIsOptInAndPublicationFailureCanBeRepaired() {
		String prefix = prefix();
		var store = store(client, prefix, 2);
		var lease = store.claim("s", "r", UUID.randomUUID(), Duration.ofSeconds(30));
		assertThatThrownBy(() -> store.commit(lease, commit("x".repeat(4097), 0))).hasMessageContaining("4096");
		assertThat(store.snapshot("s", "r").orElseThrow().runRevision()).isZero();
		store.commit(lease, commit("READY", 0));
		store.release(lease);
		var hints = new RedisProgressAdvisoryIndex(client, prefix + ":hints", 2);
		var directory = new RedisProgressDirectory(client, prefix, 2);
		client.getBucket(prefix + ":hints", StringCodec.INSTANCE).set("wrong type");
		assertThatThrownBy(() -> repair(directory, "", 2, projection(prefix, hints), hints))
			.hasMessageContaining("WRONGTYPE");
		assertThat(store.snapshot("s", "r").orElseThrow().runRevision()).isEqualTo(1);
		assertThat(directory.page("", 2).entries()).hasSize(1);
		client.getKeys().delete(prefix + ":hints");
		assertThat(repair(directory, "", 2, projection(prefix, hints), hints).published()).isEqualTo(1);
		String legacyPrefix = prefix();
		var legacy = RedisLeasedProgressStore.builder().redisson(client).keyPrefix(legacyPrefix).build();
		var oldLease = legacy.claim("s", "r", UUID.randomUUID(), Duration.ofSeconds(30));
		assertThat(legacy.commit(oldLease, commit("x".repeat(4097), 0)).state()).hasSize(4097);
		legacy.release(oldLease);
	}

	@Test
	void deliveryTokensKeepExactInt64AndExhaustionMakesNoPartialDelivery() {
		String prefix = prefix();
		var hints = new RedisProgressAdvisoryIndex(client, prefix + ":hints", 1);
		publish(hints, "s", Long.MAX_VALUE);
		var tokens = client.<String, String>getMap(prefix + ":hints:deliveries:v1", StringCodec.INSTANCE);
		tokens.put("_sequence", "9007199254740992");
		var delivery = hints.poll(1, 10000).get(0);
		assertThat(delivery.token()).isEqualTo(9007199254740993L);
		assertThat(delivery.candidate().revision()).isEqualTo(Long.MAX_VALUE);
		assertThat(hints.ack(delivery)).isTrue();
		publish(hints, "s", Long.MAX_VALUE);
		tokens.put("_sequence", Long.toString(Long.MAX_VALUE));
		assertThatThrownBy(() -> hints.poll(1, 10000)).hasMessageContaining("exhausted");
		assertThat(hints.due(1)).hasSize(1);
		assertThat(tokens.size()).isEqualTo(1);
	}

	private static RedisProgressDirectory.RepairBatch repair(RedisProgressDirectory directory, String after, int limit,
			RedisProgressIndexRepairer projection, RedisProgressAdvisoryIndex hints) {
		return directory.repair(after, limit, projection, m -> "READY".equals(m.state())
				? Optional.of(new RedisProgressAdvisoryIndex.Hint(m, 0)) : Optional.empty(), hints);
	}

	private static RedisProgressReferenceConsumer consumer(RedisLeasedProgressStore store,
			RedisProgressIndexRepairer projection, RedisProgressAdvisoryIndex hints,
			Predicate<RedisProgressIndexRepairer.Metadata> runnable) {
		return new RedisProgressReferenceConsumer(store, projection, hints, runnable, Duration.ofSeconds(30), 10000);
	}

	private static RedisProgressIndexRepairer projection(String prefix, RedisProgressAdvisoryIndex hints) {
		return new RedisProgressIndexRepairer(client, prefix, m -> Optional.empty(), hints);
	}

	private static RedisLeasedProgressStore store(RedissonClient client, String prefix, int capacity) {
		return RedisLeasedProgressStore.builder()
			.redisson(client)
			.keyPrefix(prefix)
			.directoryCapacity(capacity)
			.build();
	}

	private static LeasedProgressStore.ProgressCommit commit(String state, long revision) {
		return new LeasedProgressStore.ProgressCommit(state, revision, null, List.of(), List.of());
	}

	private static void seed(RedisLeasedProgressStore store, String session, String state) {
		var lease = store.claim(session, "r", UUID.randomUUID(), Duration.ofSeconds(30));
		store.commit(lease, commit(state, 0));
		store.release(lease);
	}

	private static void publish(RedisProgressAdvisoryIndex hints, String session, long revision) {
		hints.publish(new RedisProgressAdvisoryIndex.Hint(
				new RedisProgressIndexRepairer.Metadata(session, "r", revision, "READY"), 0));
	}

	private static void makeDue(String prefix, RedisProgressAdvisoryIndex.Delivery delivery) {
		var c = delivery.candidate();
		String member = RedisProgressDirectory.member(c.sessionId(), c.runId()) + "." + c.revision();
		client.getScript(StringCodec.INSTANCE)
			.eval(RScript.Mode.READ_WRITE, "return redis.call('ZADD', KEYS[1], 'XX', 0, ARGV[1])",
					RScript.ReturnType.INTEGER, List.of(prefix + ":hints"), member);
	}

	private static void expire(String prefix, String session) {
		client.getMap(new RedisProgressKeys(prefix).sessionKey(session), StringCodec.INSTANCE).put("expires", "0");
	}

	private static org.redisson.api.RMap<String, String> runMap(String prefix, String session) {
		return client.getMap((String) new RedisProgressKeys(prefix).keys(session, "r").get(1), StringCodec.INSTANCE);
	}

	private static String prefix() {
		return "test:directory:" + UUID.randomUUID();
	}

	private static RedissonClient newClient() {
		Config config = new Config();
		config.useSingleServer().setAddress("redis://" + VALKEY.getHost() + ":" + VALKEY.getMappedPort(6379));
		return Redisson.create(config);
	}

}
