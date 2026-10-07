/*
 * Copyright 2025-2026 the original author or authors.
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

import io.github.agentic.ai.graph.CompileConfig;
import io.github.agentic.ai.graph.CompiledGraph;
import io.github.agentic.ai.graph.NodeOutput;
import io.github.agentic.ai.graph.OverAllState;
import io.github.agentic.ai.graph.RunnableConfig;
import io.github.agentic.ai.graph.StateGraph;
import io.github.agentic.ai.graph.checkpoint.BaseCheckpointSaver;
import io.github.agentic.ai.graph.checkpoint.Checkpoint;
import io.github.agentic.ai.graph.checkpoint.CheckpointSnapshot;
import io.github.agentic.ai.graph.checkpoint.config.SaverConfig;
import io.github.agentic.ai.graph.checkpoint.lease.ExecutionLease;
import io.github.agentic.ai.graph.checkpoint.lease.ExecutionLeaseScope;
import io.github.agentic.ai.graph.checkpoint.lease.LeaseBusyException;
import io.github.agentic.ai.graph.checkpoint.lease.LeaseLostException;
import io.github.agentic.ai.graph.checkpoint.lease.LeaseOptions;
import io.github.agentic.ai.graph.checkpoint.lease.LeaseRequiredException;
import io.github.agentic.ai.graph.serializer.Serializer;
import io.github.agentic.ai.graph.serializer.StateSerializer;
import io.github.agentic.ai.graph.serializer.check_point.CheckPointSerializer;
import io.github.agentic.ai.graph.serializer.plain_text.jackson.SpringAIJacksonStateSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.redisson.Redisson;
import org.redisson.api.RScript;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.Config;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

import static io.github.agentic.ai.graph.StateGraph.END;
import static io.github.agentic.ai.graph.StateGraph.START;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Testcontainers(disabledWithoutDocker = true)
class RedisLeasedCheckpointSaverTest {

	private static final StateSerializer STATE_SERIALIZER = new SpringAIJacksonStateSerializer(OverAllState::new);

	private static final Serializer<Checkpoint> CHECKPOINT_SERIALIZER = new CheckPointSerializer(STATE_SERIALIZER);

	private static final String OWNER = "owner";

	private static final String FENCE = "fence";

	private static final String EXPIRES = "expires";

	private static final String REVISION = "revision";

	private static final String HISTORY = "history";

	private static final String LEGACY_CHECKPOINT_PREFIX = "graph:checkpoint:content:";

	private static final String VERSIONED_STORAGE_KEY_PREFIX = "argi:test:checkpoint:versioned:";

	@Container
	private static final GenericContainer<?> VALKEY = new GenericContainer<>(DockerImageName.parse("valkey/valkey:8.1.2"))
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
	void busyLeaseStopsSecondGraphBeforeAnyNodeRuns() throws Exception {
		String prefix = uniquePrefix();
		RedisLeasedCheckpointSaver saverA = saver(primaryClient, prefix);
		RedisLeasedCheckpointSaver saverB = saver(secondaryClient, prefix);
		RunnableConfig config = config("busy-before-node");
		CountDownLatch firstEntered = new CountDownLatch(1);
		CountDownLatch releaseFirst = new CountDownLatch(1);
		AtomicInteger firstCalls = new AtomicInteger();
		AtomicInteger secondCalls = new AtomicInteger();
		CompiledGraph firstGraph = graph(saverA, "first", firstCalls, firstEntered, releaseFirst, false);
		CompiledGraph secondGraph = graph(saverB, "second", secondCalls);

		CompletableFuture<List<NodeOutput>> firstRun = firstGraph.stream(Map.of("request", "first"), config)
			.collectList()
			.toFuture();
		assertThat(firstEntered.await(5, TimeUnit.SECONDS)).isTrue();

		assertThatThrownBy(() -> secondGraph.invoke(Map.of("request", "second"), config))
			.isInstanceOf(LeaseBusyException.class);
		releaseFirst.countDown();
		assertThat(firstRun.get(5, TimeUnit.SECONDS)).isNotEmpty();

		assertThat(firstCalls).hasValue(1);
		assertThat(secondCalls).hasValue(0);
	}

	@Test
	void expiredOwnerCannotReleaseOrMutateAfterIndependentClientReacquiresAtUnchangedRevision() throws Exception {
		String prefix = uniquePrefix();
		RedisLeasedCheckpointSaver saverA = saver(primaryClient, prefix);
		RedisLeasedCheckpointSaver saverB = saver(secondaryClient, prefix);
		RunnableConfig config = config("expired-stale-owner");
		ExecutionLease oldLease = saverA.acquireLease(config, UUID.randomUUID());
		saverA.putIfLeasedVersion(config, checkpoint("seed"), 0, oldLease);
		expireLease(prefix, config);

		ExecutionLease winner = saverB.acquireLease(config, UUID.randomUUID());

		assertThat(winner.fencingToken()).isGreaterThan(oldLease.fencingToken());
		assertThat(saverB.getVersioned(config).revision()).isEqualTo(1);
		assertThatThrownBy(() -> saverA.releaseIfLeasedVersion(config, 1, oldLease))
			.isInstanceOf(LeaseLostException.class);
		assertThatThrownBy(() -> saverA.putIfLeasedVersion(config, checkpoint("stale"), 1, oldLease))
			.isInstanceOf(LeaseLostException.class);
		assertThat(saverA.releaseLease(config, oldLease)).isFalse();
		assertThat(saverB.getVersioned(config).revision()).isEqualTo(1);
		assertThat(saverB.releaseLease(config, winner)).isTrue();
	}

	@Test
	void exactInt64CountersRoundTripAboveDoublePrecisionAndOverflowFailsBeforeMutation() throws Exception {
		String prefix = uniquePrefix();
		RedisLeasedCheckpointSaver saver = saver(primaryClient, prefix);
		RunnableConfig config = config("large-counters");
		RMap<String, String> hash = leasedHash(prefix, config);
		hash.put(FENCE, "9007199254740992");
		hash.put(REVISION, "9007199254740993");
		hash.put(HISTORY, serializeCheckpoints(List.of(checkpointWithId("large", 1))));

		ExecutionLease lease = saver.acquireLease(config, UUID.randomUUID());
		CheckpointSnapshot snapshot = saver.getVersioned(config);
		BaseCheckpointSaver.Tag tag = saver.releaseIfLeasedVersion(config, 9007199254740993L, lease);

		assertThat(lease.fencingToken()).isEqualTo(9007199254740993L);
		assertThat(snapshot.revision()).isEqualTo(9007199254740993L);
		assertThat(tag.checkpoints()).extracting(Checkpoint::getId).containsExactly("large");
		assertThat(saver.getVersioned(config).revision()).isEqualTo(9007199254740994L);

		RunnableConfig fenceOverflow = config("fence-overflow");
		RMap<String, String> fenceOverflowHash = leasedHash(prefix, fenceOverflow);
		fenceOverflowHash.put(FENCE, Long.toString(Long.MAX_VALUE));
		Map<String, String> beforeFenceOverflow = fenceOverflowHash.readAllMap();
		assertThatThrownBy(() -> saver.acquireLease(fenceOverflow, UUID.randomUUID()))
			.isInstanceOf(ArithmeticException.class);
		assertThat(fenceOverflowHash.readAllMap()).isEqualTo(beforeFenceOverflow);

		RunnableConfig revisionOverflow = config("revision-overflow");
		ExecutionLease overflowLease = saver.acquireLease(revisionOverflow, UUID.randomUUID());
		RMap<String, String> revisionOverflowHash = leasedHash(prefix, revisionOverflow);
		revisionOverflowHash.put(REVISION, Long.toString(Long.MAX_VALUE));
		revisionOverflowHash.put(HISTORY, serializeCheckpoints(List.of(checkpoint("max"))));
		Map<String, String> beforeRevisionOverflow = revisionOverflowHash.readAllMap();
		assertThatThrownBy(() -> saver.putIfLeasedVersion(revisionOverflow, checkpoint("overflow"), Long.MAX_VALUE,
				overflowLease)).isInstanceOf(ArithmeticException.class);
		assertThat(revisionOverflowHash.readAllMap()).isEqualTo(beforeRevisionOverflow);
	}

	@Test
	void malformedCountersAndFieldPairsFailBeforeLeaseMutation() throws Exception {
		String prefix = uniquePrefix();
		RedisLeasedCheckpointSaver saver = saver(primaryClient, prefix);
		RunnableConfig malformedFence = config("malformed-fence");
		RMap<String, String> malformedFenceHash = leasedHash(prefix, malformedFence);
		malformedFenceHash.put(FENCE, "01");
		malformedFenceHash.put(REVISION, "0");
		malformedFenceHash.put(HISTORY, serializeCheckpoints(List.of()));
		Map<String, String> beforeFence = malformedFenceHash.readAllMap();

		assertThatThrownBy(() -> saver.acquireLease(malformedFence, UUID.randomUUID()))
			.isInstanceOf(IllegalStateException.class);
		assertThat(malformedFenceHash.readAllMap()).isEqualTo(beforeFence);

		RunnableConfig missingHistory = config("missing-history");
		RMap<String, String> missingHistoryHash = leasedHash(prefix, missingHistory);
		missingHistoryHash.put(REVISION, "1");
		Map<String, String> beforeMissingHistory = missingHistoryHash.readAllMap();
		assertThatThrownBy(() -> saver.getVersioned(missingHistory)).isInstanceOf(IllegalStateException.class);
		assertThatThrownBy(() -> saver.acquireLease(missingHistory, UUID.randomUUID()))
			.isInstanceOf(IllegalStateException.class);
		assertThat(missingHistoryHash.readAllMap()).isEqualTo(beforeMissingHistory);
	}

	@Test
	void renewAndReleaseOnlyAffectLeaseFieldsAndNeverReviveExpiredOwnership() throws Exception {
		String prefix = uniquePrefix();
		RedisLeasedCheckpointSaver saver = saver(primaryClient, prefix);
		RunnableConfig config = config("renew-isolation");
		ExecutionLease lease = saver.acquireLease(config, UUID.randomUUID());
		saver.putIfLeasedVersion(config, checkpoint("first"), 0, lease);
		Map<String, String> beforeRenew = leasedHash(prefix, config).readAllMap();

		ExecutionLease renewed = saver.renewLease(config, lease);
		Map<String, String> afterRenew = leasedHash(prefix, config).readAllMap();
		expireLease(prefix, config);

		assertThat(renewed.fencingToken()).isEqualTo(lease.fencingToken());
		assertThat(afterRenew).containsEntry(REVISION, beforeRenew.get(REVISION)).containsEntry(HISTORY,
				beforeRenew.get(HISTORY));
		assertThat(afterRenew.get(EXPIRES)).isNotEqualTo(beforeRenew.get(EXPIRES));
		assertThatThrownBy(() -> saver.renewLease(config, renewed)).isInstanceOf(LeaseLostException.class);
		assertThat(saver.getVersioned(config).revision()).isEqualTo(1);
	}

	@Test
	void wrongOwnerOrFenceCannotRenewUnlockMutateOrReleaseCheckpoints() throws Exception {
		String prefix = uniquePrefix();
		RedisLeasedCheckpointSaver saver = saver(primaryClient, prefix);
		RunnableConfig config = config("wrong-credentials");
		ExecutionLease lease = saver.acquireLease(config, UUID.randomUUID());
		saver.putIfLeasedVersion(config, checkpoint("first"), 0, lease);
		ExecutionLease wrongOwner = new ExecutionLease(lease.namespace(), UUID.randomUUID(), lease.fencingToken(),
				lease.expiresAtMillis());
		ExecutionLease wrongFence = new ExecutionLease(lease.namespace(), lease.ownerId(), lease.fencingToken() + 1,
				lease.expiresAtMillis());

		assertThatThrownBy(() -> saver.renewLease(config, wrongOwner)).isInstanceOf(LeaseLostException.class);
		assertThatThrownBy(() -> saver.renewLease(config, wrongFence)).isInstanceOf(LeaseLostException.class);
		assertThat(saver.releaseLease(config, wrongOwner)).isFalse();
		assertThat(saver.releaseLease(config, wrongFence)).isFalse();
		assertThatThrownBy(() -> saver.putIfLeasedVersion(config, checkpoint("wrong-owner"), 1, wrongOwner))
			.isInstanceOf(LeaseLostException.class);
		assertThatThrownBy(() -> saver.putIfLeasedVersion(config, checkpoint("wrong-fence"), 1, wrongFence))
			.isInstanceOf(LeaseLostException.class);
		assertThatThrownBy(() -> saver.releaseIfLeasedVersion(config, 1, wrongFence))
			.isInstanceOf(LeaseLostException.class);
		assertThat(saver.getVersioned(config).revision()).isEqualTo(1);
		assertThat(saver.releaseLease(config, lease)).isTrue();
	}

	@Test
	void retentionSerializationAndOlderRedisKeyspacesStayIsolated() throws Exception {
		String prefix = uniquePrefix();
		String versionedStorageKey = VERSIONED_STORAGE_KEY_PREFIX + UUID.randomUUID();
		RedisLeasedCheckpointSaver saver = saver(primaryClient, prefix);
		RedisSaver legacy = RedisSaver.builder().redisson(primaryClient).stateSerializer(STATE_SERIALIZER).build();
		RedisVersionedSaver versioned = RedisVersionedSaver.builder()
			.redisson(primaryClient)
			.stateSerializer(STATE_SERIALIZER)
			.storageKey(versionedStorageKey)
			.build();
		RunnableConfig config = RunnableConfig.builder()
			.threadId("retention-" + UUID.randomUUID())
			.checkpointsNumRetained(2)
			.build();
		legacy.put(config, checkpoint("legacy"));
		versioned.putIfVersion(config, checkpoint("versioned"), 0);
		String legacyKey = legacyContentKey(config);
		Map<String, String> oldVersionedBefore = primaryClient.<String, String>getMap(versionedStorageKey,
				StringCodec.INSTANCE).readAllMap();

		ExecutionLease lease = saver.acquireLease(config, UUID.randomUUID());
		saver.putIfLeasedVersion(config, checkpointWithId("cp1", 1), 0, lease);
		saver.putIfLeasedVersion(config, checkpointWithId("cp2", 2), 1, lease);
		saver.putIfLeasedVersion(config, checkpointWithId("cp3", 3), 2, lease);
		RedissonClient reconstructed = newClient();
		try {
			RedisLeasedCheckpointSaver restored = saver(reconstructed, prefix);

			assertThat(restored.list(config)).extracting(Checkpoint::getId).containsExactly("cp3", "cp2");
			assertThat(restored.getVersioned(config).revision()).isEqualTo(3);
		}
		finally {
			reconstructed.shutdown();
		}
		assertThat(primaryClient.getBucket(legacyKey).get()).isNotNull();
		assertThat(primaryClient.<String, String>getMap(versionedStorageKey, StringCodec.INSTANCE).readAllMap())
			.isEqualTo(oldVersionedBefore);
	}

	@Test
	void runtimeInvokeStreamManualUpdateReleaseAndOwnerlessSpyGuardUseLeasedMutations() throws Exception {
		String prefix = uniquePrefix();
		RedisLeasedCheckpointSaver saver = spy(saver(primaryClient, prefix));
		doThrow(new AssertionError("runtime must not call ownerless putIfVersion"))
			.when(saver)
			.putIfVersion(any(RunnableConfig.class), any(Checkpoint.class), anyLong());
		doThrow(new AssertionError("runtime must not call ownerless releaseIfVersion"))
			.when(saver)
			.releaseIfVersion(any(RunnableConfig.class), anyLong());
		CompiledGraph graph = simpleGraph(saver);
		RunnableConfig invokeConfig = config("leased-invoke");
		RunnableConfig streamConfig = config("leased-stream");
		RunnableConfig updateConfig = config("leased-update");
		RunnableConfig releaseConfig = config("leased-release");

		assertThat(graph.invoke(Map.of("request", "invoke"), invokeConfig)).isPresent();
		assertThat(saver.getVersioned(invokeConfig).revision()).isEqualTo(2);
		assertThat(graph.stream(Map.of("request", "stream"), streamConfig).last().block(Duration.ofSeconds(5)))
			.isNotNull();
		ExecutionLease updateSeedLease = saver.acquireLease(updateConfig, UUID.randomUUID());
		RunnableConfig seeded = saver.putIfLeasedVersion(updateConfig,
				Checkpoint.builder().state(Map.of("result", "seed")).nodeId(START).nextNodeId("write").build(), 0,
				updateSeedLease);
		assertThat(saver.releaseLease(updateConfig, updateSeedLease)).isTrue();
		assertThat(graph.updateState(seeded, Map.of("request", "manual"), START).checkPointId()).isPresent();
		assertThat(simpleGraph(saver, true).invoke(Map.of("request", "release"), releaseConfig)).isPresent();

		verify(saver, never()).putIfVersion(any(RunnableConfig.class), any(Checkpoint.class), anyLong());
		verify(saver, never()).releaseIfVersion(any(RunnableConfig.class), anyLong());
		RedisLeasedCheckpointSaver ownerlessSaver = saver(secondaryClient, uniquePrefix());
		assertThatThrownBy(() -> ownerlessSaver.putIfVersion(config("ownerless-put"), checkpoint("ownerless"), 0))
			.isInstanceOf(LeaseRequiredException.class);
		assertThatThrownBy(() -> ownerlessSaver.releaseIfVersion(config("ownerless-release"), 0))
			.isInstanceOf(LeaseRequiredException.class);
	}

	@Test
	void sameNamespaceNestedScopeReusesOneRedisLeaseOwnerAcquireAndRelease() {
		String prefix = uniquePrefix();
		RedisLeasedCheckpointSaver saver = spy(saver(primaryClient, prefix));
		RunnableConfig config = config("nested-same");
		AtomicReference<ExecutionLeaseScope> outerScope = new AtomicReference<>();
		AtomicReference<ExecutionLeaseScope> innerScope = new AtomicReference<>();
		AtomicReference<UUID> owner = new AtomicReference<>();

		String value = ExecutionLeaseScope.withLease(saver, config, outer -> {
			outerScope.set(outer);
			owner.set(outer.lease().ownerId());
			return ExecutionLeaseScope.withLease(saver, config, inner -> {
				innerScope.set(inner);
				return Flux.just(inner.lease().ownerId().toString());
			});
		}).single().block(Duration.ofSeconds(5));

		assertThat(value).isEqualTo(owner.get().toString());
		assertThat(innerScope.get()).isSameAs(outerScope.get());
		verify(saver, times(1)).acquireLease(any(RunnableConfig.class), any(UUID.class));
		verify(saver, times(1)).releaseLease(any(RunnableConfig.class), any(ExecutionLease.class));
		assertThat(leasedHash(prefix, config).readAllMap()).doesNotContainKey(OWNER).doesNotContainKey(EXPIRES);
	}

	@Test
	void differentNamespaceNestedScopesKeepIndependentRedisOwnership() {
		String prefix = uniquePrefix();
		RedisLeasedCheckpointSaver saver = spy(saver(primaryClient, prefix));
		RunnableConfig outerConfig = config("nested-outer");
		RunnableConfig innerConfig = config("nested-inner");
		AtomicReference<ExecutionLeaseScope> outerScope = new AtomicReference<>();
		AtomicReference<ExecutionLeaseScope> innerScope = new AtomicReference<>();
		AtomicReference<ExecutionLease> outerLease = new AtomicReference<>();
		AtomicReference<ExecutionLease> innerLease = new AtomicReference<>();

		ExecutionLeaseScope.withLease(saver, outerConfig, outer -> {
			outerScope.set(outer);
			outerLease.set(outer.lease());
			return ExecutionLeaseScope.withLease(saver, innerConfig, inner -> {
				innerScope.set(inner);
				innerLease.set(inner.lease());
				return Flux.just("done");
			});
		}).single().block(Duration.ofSeconds(5));

		assertThat(innerScope.get()).isNotSameAs(outerScope.get());
		assertThat(innerLease.get().namespace()).isNotEqualTo(outerLease.get().namespace());
		assertThat(innerLease.get().ownerId()).isNotEqualTo(outerLease.get().ownerId());
		verify(saver, times(2)).acquireLease(any(RunnableConfig.class), any(UUID.class));
		verify(saver, times(2)).releaseLease(any(RunnableConfig.class), any(ExecutionLease.class));
		assertThat(leasedHash(prefix, outerConfig).readAllMap()).doesNotContainKey(OWNER).doesNotContainKey(EXPIRES);
		assertThat(leasedHash(prefix, innerConfig).readAllMap()).doesNotContainKey(OWNER).doesNotContainKey(EXPIRES);
	}

	@Test
	void coldScopeResubscriptionsUseFreshRedisOwnersAndGuards() {
		String prefix = uniquePrefix();
		RedisLeasedCheckpointSaver saver = spy(saver(primaryClient, prefix));
		RunnableConfig config = config("cold-scope");
		List<ExecutionLease> leases = new ArrayList<>();
		List<Object> guards = new ArrayList<>();
		List<ExecutionLeaseScope> scopes = new ArrayList<>();
		Flux<String> flow = ExecutionLeaseScope.withLease(saver, config, scope -> {
			scopes.add(scope);
			leases.add(scope.lease());
			guards.add(scope.guard());
			return Flux.just(scope.lease().ownerId().toString());
		});

		String first = flow.single().block(Duration.ofSeconds(5));
		String second = flow.single().block(Duration.ofSeconds(5));

		assertThat(first).isNotEqualTo(second);
		assertThat(scopes).hasSize(2);
		assertThat(scopes.get(0)).isNotSameAs(scopes.get(1));
		assertThat(leases.get(0).ownerId()).isNotEqualTo(leases.get(1).ownerId());
		assertThat(guards.get(0)).isNotSameAs(guards.get(1));
		verify(saver, times(2)).acquireLease(any(RunnableConfig.class), any(UUID.class));
		verify(saver, times(2)).releaseLease(any(RunnableConfig.class), any(ExecutionLease.class));
	}

	@Test
	void cancellationRewindsCheckpointBeforeUnlockingRedisLease() throws Exception {
		String prefix = uniquePrefix();
		RedisLeasedCheckpointSaver saver = spy(saver(primaryClient, prefix));
		RedisLeasedCheckpointSaver reader = saver(secondaryClient, prefix);
		RunnableConfig config = config("cancel-rewind");
		List<String> events = new ArrayList<>();
		CountDownLatch subscribed = new CountDownLatch(1);
		CountDownLatch rewound = new CountDownLatch(1);
		CountDownLatch released = new CountDownLatch(1);
		doAnswer(invocation -> {
			events.add("release");
			try {
				return invocation.callRealMethod();
			}
			finally {
				released.countDown();
			}
		}).when(saver).releaseLease(any(RunnableConfig.class), any(ExecutionLease.class));

		Disposable subscription = ExecutionLeaseScope.withLease(saver, config, scope -> {
			subscribed.countDown();
			return Flux.never().doFinally(signal -> {
				try {
					scope.assertActive();
					saver.putIfLeasedVersion(config, checkpoint("rewound"), 0, scope.lease());
					events.add("rewind");
				}
				catch (Exception ex) {
					events.add("failed");
				}
				finally {
					rewound.countDown();
				}
			});
		}).subscribe();

		assertThat(subscribed.await(5, TimeUnit.SECONDS)).isTrue();
		subscription.dispose();

		assertThat(rewound.await(5, TimeUnit.SECONDS)).isTrue();
		assertThat(released.await(5, TimeUnit.SECONDS)).isTrue();
		assertThat(events).containsExactly("rewind", "release");
		assertThat(reader.getVersioned(config).revision()).isEqualTo(1);
		assertThat(reader.getVersioned(config).checkpoint()).hasValueSatisfying(
				actual -> assertThat(actual.getState()).containsEntry("result", "rewound"));
		assertThat(leasedHash(prefix, config).readAllMap()).doesNotContainKey(OWNER).doesNotContainKey(EXPIRES);
	}

	@Test
	void graphLeaseLossDuringNodeRejectsLateResultAtUnchangedRevisionAfterNewOwnerAcquires() throws Exception {
		String prefix = uniquePrefix();
		RedisLeasedCheckpointSaver staleSaver = saver(primaryClient, prefix);
		RedisLeasedCheckpointSaver winnerSaver = saver(secondaryClient, prefix);
		CountDownLatch staleNodeEntered = new CountDownLatch(1);
		CountDownLatch allowStaleToFinish = new CountDownLatch(1);
		AtomicInteger staleCalls = new AtomicInteger();
		CompiledGraph staleGraph = graph(staleSaver, "stale", staleCalls, staleNodeEntered, allowStaleToFinish, false);
		RunnableConfig config = config("unchanged-revision-loss");

		CompletableFuture<List<NodeOutput>> staleRun = staleGraph.stream(Map.of("request", "stale"), config)
			.collectList()
			.toFuture();
		assertThat(staleNodeEntered.await(5, TimeUnit.SECONDS)).isTrue();
		expireLease(prefix, config);
		ExecutionLease winner = winnerSaver.acquireLease(config, UUID.randomUUID());
		assertThat(winnerSaver.getVersioned(config).revision()).isEqualTo(1);
		allowStaleToFinish.countDown();

		assertThatThrownBy(() -> staleRun.get(5, TimeUnit.SECONDS)).satisfies(RedisLeasedCheckpointSaverTest::assertLeaseLoss);
		assertThat(staleCalls).hasValue(1);
		assertThat(winnerSaver.getVersioned(config).revision()).isEqualTo(1);
		assertThat(winnerSaver.getVersioned(config).checkpoint()).hasValueSatisfying(
				actual -> assertThat(actual.getNodeId()).isEqualTo(START));
		assertThat(winnerSaver.releaseLease(config, winner)).isTrue();
	}

	@Test
	@SuppressWarnings({ "unchecked", "rawtypes" })
	void scriptTransportFailurePropagatesWithoutOwnerlessFallback() {
		RedissonClient redisson = mock(RedissonClient.class);
		RScript script = mock(RScript.class);
		RuntimeException transportFailure = new RuntimeException("script transport failed");
		when(redisson.getScript(StringCodec.INSTANCE)).thenReturn(script);
		when(script.eval(any(RScript.Mode.class), anyString(), any(RScript.ReturnType.class), any(List.class),
				any(Object[].class))).thenThrow(transportFailure);
		RedisLeasedCheckpointSaver saver = RedisLeasedCheckpointSaver.builder()
			.redisson(redisson)
			.stateSerializer(STATE_SERIALIZER)
			.build();

		assertThatThrownBy(() -> saver.acquireLease(config("transport-acquire"), UUID.randomUUID()))
			.isSameAs(transportFailure);
	}

	@Test
	void initialSnapshotTransportFailureFailsClosedBeforeNodeDispatchAndReleasesLease() throws Exception {
		String prefix = uniquePrefix();
		FaultingRedisLeasedCheckpointSaver faulting = faultingSaver(primaryClient,
				new LeaseOptions(Duration.ofSeconds(2), Duration.ofMillis(250)), prefix, FaultOperation.SNAPSHOT, 1);
		RedisLeasedCheckpointSaver reader = saver(secondaryClient, prefix);
		AtomicInteger nodeCalls = new AtomicInteger();
		RunnableConfig config = config("snapshot-fault");
		CompiledGraph graph = graph(faulting, "late", nodeCalls);

		assertThatThrownBy(() -> graph.invoke(Map.of("request", "late"), config))
			.isSameAs(faulting.failure());

		assertThat(nodeCalls).hasValue(0);
		assertThat(faulting.ownerlessVersionPutUsed()).isFalse();
		assertThat(faulting.ownerlessVersionReleaseUsed()).isFalse();
		assertThat(reader.getVersioned(config).revision()).isZero();
		assertThat(reader.getVersioned(config).checkpoint()).isEmpty();
		assertThat(leasedHash(prefix, config).readAllMap()).doesNotContainKey(OWNER).doesNotContainKey(EXPIRES);
	}

	@Test
	void renewTransportFailureInvalidatesRuntimeWithoutLateCheckpointMutation() throws Exception {
		String prefix = uniquePrefix();
		FaultingRedisLeasedCheckpointSaver faulting = faultingSaver(primaryClient,
				new LeaseOptions(Duration.ofMillis(500), Duration.ofMillis(50)), prefix, FaultOperation.RENEW, 1);
		RedisLeasedCheckpointSaver reader = saver(secondaryClient, prefix);
		RunnableConfig config = config("renew-fault");
		CountDownLatch nodeEntered = new CountDownLatch(1);
		CountDownLatch neverFinish = new CountDownLatch(1);
		AtomicInteger nodeCalls = new AtomicInteger();
		CompiledGraph graph = graph(faulting, "late", nodeCalls, nodeEntered, neverFinish, false);

		CompletableFuture<List<NodeOutput>> run = graph.stream(Map.of("request", "late"), config)
			.collectList()
			.toFuture();
		assertThat(nodeEntered.await(5, TimeUnit.SECONDS)).isTrue();

		assertThatThrownBy(() -> run.get(5, TimeUnit.SECONDS)).satisfies(RedisLeasedCheckpointSaverTest::assertLeaseLoss);
		assertThat(nodeCalls).hasValue(1);
		assertThat(faulting.ownerlessVersionPutUsed()).isFalse();
		assertThat(faulting.ownerlessVersionReleaseUsed()).isFalse();
		assertThat(reader.getVersioned(config).revision()).isEqualTo(1);
		assertThat(reader.getVersioned(config).checkpoint()).hasValueSatisfying(
				actual -> assertThat(actual.getNodeId()).isEqualTo(START));
	}

	@Test
	void fencedPutTransportFailureInvalidatesRuntimeWithoutOwnerlessFallbackOrHistoryMutation() throws Exception {
		String prefix = uniquePrefix();
		FaultingRedisLeasedCheckpointSaver faulting = faultingSaver(primaryClient,
				new LeaseOptions(Duration.ofSeconds(2), Duration.ofMillis(250)), prefix, FaultOperation.FENCED_PUT, 2);
		RedisLeasedCheckpointSaver reader = saver(secondaryClient, prefix);
		RunnableConfig config = config("put-fault");
		AtomicInteger nodeCalls = new AtomicInteger();
		CompiledGraph graph = graph(faulting, "late", nodeCalls);

		assertThatThrownBy(() -> graph.invoke(Map.of("request", "late"), config))
			.satisfies(RedisLeasedCheckpointSaverTest::assertLeaseLoss);

		assertThat(nodeCalls).hasValue(1);
		assertThat(faulting.ownerlessVersionPutUsed()).isFalse();
		assertThat(faulting.ownerlessVersionReleaseUsed()).isFalse();
		assertThat(reader.getVersioned(config).revision()).isEqualTo(1);
		assertThat(reader.getVersioned(config).checkpoint()).hasValueSatisfying(
				actual -> assertThat(actual.getNodeId()).isEqualTo(START));
	}

	@Test
	void fencedReleaseTransportFailureInvalidatesRuntimeAndRetainsLatestCheckpoint() throws Exception {
		String prefix = uniquePrefix();
		FaultingRedisLeasedCheckpointSaver faulting = faultingSaver(primaryClient,
				new LeaseOptions(Duration.ofSeconds(2), Duration.ofMillis(250)), prefix, FaultOperation.FENCED_RELEASE,
				1);
		RedisLeasedCheckpointSaver reader = saver(secondaryClient, prefix);
		RunnableConfig config = config("release-fault");
		AtomicInteger nodeCalls = new AtomicInteger();
		CompiledGraph graph = graph(faulting, "retained", nodeCalls, null, null, true);

		assertThatThrownBy(() -> graph.invoke(Map.of("request", "retained"), config))
			.satisfies(RedisLeasedCheckpointSaverTest::assertLeaseLoss);

		assertThat(nodeCalls).hasValue(1);
		assertThat(faulting.ownerlessVersionPutUsed()).isFalse();
		assertThat(faulting.ownerlessVersionReleaseUsed()).isFalse();
		assertThat(reader.getVersioned(config).revision()).isEqualTo(2);
		assertThat(reader.getVersioned(config).checkpoint()).hasValueSatisfying(
				actual -> assertThat(actual.getState()).containsEntry("result", "retained"));
	}

	@Test
	void staleGraphLosesLeaseAtUnchangedRevisionAndDoesNotMergeLateResult() throws Exception {
		String prefix = uniquePrefix();
		RedisLeasedCheckpointSaver staleSaver = saver(primaryClient, prefix);
		RedisLeasedCheckpointSaver winnerSaver = saver(secondaryClient, prefix);
		CountDownLatch staleNodeEntered = new CountDownLatch(1);
		CountDownLatch allowStaleToFinish = new CountDownLatch(1);
		AtomicInteger staleCalls = new AtomicInteger();
		AtomicInteger winnerCalls = new AtomicInteger();
		CompiledGraph staleGraph = graph(staleSaver, "stale", staleCalls, staleNodeEntered, allowStaleToFinish, false);
		CompiledGraph winnerGraph = graph(winnerSaver, "winner", winnerCalls);
		RunnableConfig config = config("stale-graph");

		CompletableFuture<List<NodeOutput>> staleRun = staleGraph.stream(Map.of("request", "stale"), config)
			.collectList()
			.toFuture();
		assertThat(staleNodeEntered.await(5, TimeUnit.SECONDS)).isTrue();
		expireLease(prefix, config);

		NodeOutput winner = winnerGraph.stream(Map.of("request", "winner"), config)
			.last()
			.block(Duration.ofSeconds(5));
		allowStaleToFinish.countDown();

		assertThatThrownBy(() -> staleRun.get(5, TimeUnit.SECONDS)).satisfies(RedisLeasedCheckpointSaverTest::assertLeaseLoss);
		assertThat(winner.state().value("result")).contains("winner");
		assertThat(staleCalls).hasValue(1);
		assertThat(winnerCalls).hasValue(1);
		assertThat(winnerSaver.getVersioned(config).checkpoint()).hasValueSatisfying(
				actual -> assertThat(actual.getState()).containsEntry("result", "winner"));
	}

	private static CompiledGraph simpleGraph(RedisLeasedCheckpointSaver saver) throws Exception {
		return simpleGraph(saver, false);
	}

	private static CompiledGraph simpleGraph(RedisLeasedCheckpointSaver saver, boolean releaseThread) throws Exception {
		return graph(saver, null, new AtomicInteger(), null, null, releaseThread);
	}

	private static CompiledGraph graph(RedisLeasedCheckpointSaver saver, String fixedResult, AtomicInteger calls)
			throws Exception {
		return graph(saver, fixedResult, calls, null, null, false);
	}

	private static CompiledGraph graph(RedisLeasedCheckpointSaver saver, String fixedResult, AtomicInteger calls,
			CountDownLatch entered, CountDownLatch proceed, boolean releaseThread) throws Exception {
		return new StateGraph()
			.addNode("write", (state, config) -> {
				calls.incrementAndGet();
				if (entered != null) {
					entered.countDown();
				}
				Object result = fixedResult != null ? fixedResult : state.value("request").orElse("missing");
				if (proceed == null) {
					return CompletableFuture.completedFuture(Map.of("result", result));
				}
				return CompletableFuture.supplyAsync(() -> {
					await(proceed);
					return Map.of("result", result);
				});
			})
			.addEdge(START, "write")
			.addEdge("write", END)
			.compile(CompileConfig.builder()
				.saverConfig(SaverConfig.builder().register(saver).build())
				.releaseThread(releaseThread)
				.build());
	}

	private static RedisLeasedCheckpointSaver saver(RedissonClient redisson, String prefix) {
		return RedisLeasedCheckpointSaver.builder()
			.redisson(redisson)
			.stateSerializer(STATE_SERIALIZER)
			.leaseOptions(new LeaseOptions(Duration.ofSeconds(2), Duration.ofMillis(250)))
			.storageKeyPrefix(prefix)
			.build();
	}

	private static RunnableConfig config(String name) {
		return RunnableConfig.builder().threadId(name + "-" + UUID.randomUUID()).build();
	}

	private static Checkpoint checkpoint(String result) {
		return Checkpoint.builder()
			.id("checkpoint-" + result + "-" + UUID.randomUUID())
			.nodeId("node-" + result)
			.nextNodeId(END)
			.state(Map.of("result", result))
			.build();
	}

	private static Checkpoint checkpointWithId(String id, int version) {
		return Checkpoint.builder()
			.id(id)
			.nodeId("node-" + id)
			.nextNodeId(END)
			.state(Map.of("version", version))
			.build();
	}

	private static void expireLease(String prefix, RunnableConfig config) {
		leasedHash(prefix, config).put(EXPIRES, "1");
	}

	private static RMap<String, String> leasedHash(String prefix, RunnableConfig config) {
		return primaryClient.getMap(leasedKey(prefix, config.threadId().orElseThrow()), StringCodec.INSTANCE);
	}

	private static String leasedKey(String prefix, String namespace) {
		return prefix + ":{"
				+ Base64.getUrlEncoder().withoutPadding().encodeToString(namespace.getBytes(StandardCharsets.UTF_8))
				+ "}";
	}

	private static String legacyContentKey(RunnableConfig config) {
		String threadName = config.threadId().orElseThrow();
		RMap<String, String> meta = primaryClient.getMap("graph:thread:meta:" + threadName);
		return LEGACY_CHECKPOINT_PREFIX + meta.get("thread_id");
	}

	private static String serializeCheckpoints(Collection<Checkpoint> checkpoints) throws Exception {
		try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
				ObjectOutputStream oos = new ObjectOutputStream(baos)) {
			oos.writeInt(checkpoints.size());
			for (Checkpoint checkpoint : checkpoints) {
				CHECKPOINT_SERIALIZER.write(checkpoint, oos);
			}
			oos.flush();
			return Base64.getEncoder().encodeToString(baos.toByteArray());
		}
	}

	private static LinkedList<Checkpoint> deserializeCheckpoints(String content) throws Exception {
		byte[] bytes = Base64.getDecoder().decode(content);
		try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
				ObjectInputStream ois = new ObjectInputStream(bais)) {
			int size = ois.readInt();
			LinkedList<Checkpoint> checkpoints = new LinkedList<>();
			for (int i = 0; i < size; i++) {
				checkpoints.add(CHECKPOINT_SERIALIZER.read(ois));
			}
			return checkpoints;
		}
	}

	private static void assertLeaseLoss(Throwable failure) {
		Throwable current = failure;
		while (current != null) {
			if (current instanceof LeaseLostException) {
				assertThat(current).isInstanceOf(LeaseLostException.class);
				return;
			}
			current = current.getCause();
		}
		assertThat(failure).isInstanceOf(LeaseLostException.class);
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

	private static RedissonClient newClient() {
		Config config = new Config();
		config.useSingleServer().setAddress("redis://" + VALKEY.getHost() + ":" + VALKEY.getMappedPort(6379));
		return Redisson.create(config);
	}

	private static String uniquePrefix() {
		return "argi:test:checkpoint:leased:" + UUID.randomUUID();
	}

	private enum FaultOperation {

		SNAPSHOT,

		RENEW,

		FENCED_PUT,

		FENCED_RELEASE

	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static FaultingRedisLeasedCheckpointSaver faultingSaver(RedissonClient delegate, LeaseOptions leaseOptions,
			String storageKeyPrefix, FaultOperation operation, int failOnMatch) throws Exception {
		ScriptFault fault = new ScriptFault(operation, failOnMatch, serializeCheckpoints(List.of()));
		RedissonClient redisson = spy(delegate);
		RScript script = mock(RScript.class);
		RScript realScript = delegate.getScript(StringCodec.INSTANCE);
		when(redisson.getScript(eq(StringCodec.INSTANCE))).thenReturn(script);
		when(script.eval(any(RScript.Mode.class), anyString(), any(RScript.ReturnType.class), any(List.class),
				any(Object[].class))).thenAnswer(invocation -> {
					RScript.Mode mode = invocation.getArgument(0);
					String scriptText = invocation.getArgument(1);
					RScript.ReturnType returnType = invocation.getArgument(2);
					List<Object> keys = invocation.getArgument(3);
					Object[] args = Arrays.copyOfRange(invocation.getArguments(), 4, invocation.getArguments().length);
					fault.maybeThrow(scriptText, args);
					return realScript.eval(mode, scriptText, returnType, keys, args);
				});
		return new FaultingRedisLeasedCheckpointSaver(redisson, leaseOptions, storageKeyPrefix, fault);
	}

	private static final class ScriptFault {

		private final FaultOperation operation;

		private final int failOnMatch;

		private final RuntimeException failure = new RuntimeException("script transport failed");

		private final String emptyHistory;

		private int matches;

		private ScriptFault(FaultOperation operation, int failOnMatch, String emptyHistory) {
			this.operation = operation;
			this.failOnMatch = failOnMatch;
			this.emptyHistory = emptyHistory;
		}

		private void maybeThrow(String script, Object[] args) {
			if (matches(script, args) && ++matches >= failOnMatch) {
				throw failure;
			}
		}

		private RuntimeException failure() {
			return failure;
		}

		private boolean matches(String script, Object[] args) {
			return switch (operation) {
				case SNAPSHOT -> script.contains("return {'OK', state[5], state[6]}");
				case RENEW -> script.contains("redis.call('HSET', KEYS[1], 'expires', expires)");
				case FENCED_PUT -> script.contains("'revision', ARGV[4], 'history', ARGV[5]")
						&& args.length >= 5 && !emptyHistory.equals(String.valueOf(args[4]));
				case FENCED_RELEASE -> script.contains("'revision', ARGV[4], 'history', ARGV[5]")
						&& args.length >= 5 && emptyHistory.equals(String.valueOf(args[4]));
			};
		}

	}

	private static final class FaultingRedisLeasedCheckpointSaver extends RedisLeasedCheckpointSaver {

		private final ScriptFault fault;

		private boolean ownerlessVersionPutUsed;

		private boolean ownerlessVersionReleaseUsed;

		private FaultingRedisLeasedCheckpointSaver(RedissonClient redisson, LeaseOptions leaseOptions,
				String storageKeyPrefix, ScriptFault fault) {
			super(redisson, STATE_SERIALIZER, leaseOptions, storageKeyPrefix);
			this.fault = fault;
		}

		@Override
		public RunnableConfig putIfVersion(RunnableConfig config, Checkpoint checkpoint, long expectedRevision)
				throws Exception {
			ownerlessVersionPutUsed = true;
			return super.putIfVersion(config, checkpoint, expectedRevision);
		}

		@Override
		public Tag releaseIfVersion(RunnableConfig config, long expectedRevision) throws Exception {
			ownerlessVersionReleaseUsed = true;
			return super.releaseIfVersion(config, expectedRevision);
		}

		private boolean ownerlessVersionPutUsed() {
			return ownerlessVersionPutUsed;
		}

		private boolean ownerlessVersionReleaseUsed() {
			return ownerlessVersionReleaseUsed;
		}

		private RuntimeException failure() {
			return fault.failure();
		}

	}

}
