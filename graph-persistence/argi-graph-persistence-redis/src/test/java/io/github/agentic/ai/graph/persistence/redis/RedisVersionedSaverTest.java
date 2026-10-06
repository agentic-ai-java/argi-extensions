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
import io.github.agentic.ai.graph.checkpoint.CheckpointConflictException;
import io.github.agentic.ai.graph.checkpoint.CheckpointSnapshot;
import io.github.agentic.ai.graph.checkpoint.config.SaverConfig;
import io.github.agentic.ai.graph.serializer.Serializer;
import io.github.agentic.ai.graph.serializer.StateSerializer;
import io.github.agentic.ai.graph.serializer.check_point.CheckPointSerializer;
import io.github.agentic.ai.graph.serializer.plain_text.jackson.SpringAIJacksonStateSerializer;
import io.github.agentic.ai.graph.store.VersionedStoreItem;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.redisson.Redisson;
import org.redisson.api.RBucket;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.Codec;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.Config;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static io.github.agentic.ai.graph.StateGraph.END;
import static io.github.agentic.ai.graph.StateGraph.START;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Testcontainers(disabledWithoutDocker = true)
class RedisVersionedSaverTest {

	private static final StateSerializer STATE_SERIALIZER = new SpringAIJacksonStateSerializer(OverAllState::new);

	private static final Serializer<Checkpoint> CHECKPOINT_SERIALIZER = new CheckPointSerializer(STATE_SERIALIZER);

	private static final List<String> CHECKPOINT_NAMESPACE = List.of("checkpoints");

	private static final String CONTENT_FIELD = "content";

	private static final String LEGACY_CHECKPOINT_PREFIX = "graph:checkpoint:content:";

	private static final String LEGACY_THREAD_META_PREFIX = "graph:thread:meta:";

	private static final String LEGACY_THREAD_REVERSE_PREFIX = "graph:thread:reverse:";

	private static final String FIELD_THREAD_ID = "thread_id";

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
	void conditionalWritesShareRevisionsAcrossIndependentClientsAndConflictsRemainTyped() throws Exception {
		String storageKey = uniqueStorageKey();
		RedisVersionedSaver saverA = saver(primaryClient, storageKey);
		RedisVersionedSaver saverB = saver(secondaryClient, storageKey);
		RunnableConfig config = config("cas-basic");
		Checkpoint first = checkpoint("first");

		RunnableConfig saved = saverA.putIfVersion(config, first, 0);

		assertThat(saved.checkPointId()).contains(first.getId());
		assertThat(saverB.getVersioned(config).revision()).isEqualTo(1);
		assertThat(saverB.getVersioned(config).checkpoint()).hasValueSatisfying(actual -> assertCheckpoint(actual, first));
		assertThatThrownBy(() -> saverB.releaseIfVersion(config, 0))
			.isInstanceOfSatisfying(CheckpointConflictException.class, conflict -> {
				assertThat(conflict.getNamespace()).isEqualTo(config.threadId().orElseThrow());
				assertThat(conflict.getExpectedRevision()).isZero();
				assertThat(conflict.getActualRevision()).isEqualTo(1);
			});
		assertThat(saverA.getVersioned(config).checkpoint()).isPresent();
	}

	@Test
	void appendReplaceRetentionAndSelectedHistoryUseOneVersionedHistoryEnvelope() throws Exception {
		String storageKey = uniqueStorageKey();
		RedisVersionedSaver saver = saver(primaryClient, storageKey);
		RunnableConfig config = RunnableConfig.builder().threadId("history-" + UUID.randomUUID()).checkpointsNumRetained(2).build();
		Checkpoint first = checkpointWithId("cp1", 1);
		Checkpoint second = checkpointWithId("cp2", 2);
		Checkpoint third = checkpointWithId("cp3", 3);
		Checkpoint secondUpdated = checkpointWithId("cp2", 20);

		RunnableConfig firstConfig = saver.putIfVersion(config, first, 0);
		saver.putIfVersion(config, second, 1);
		saver.putIfVersion(config, third, 2);
		saver.putIfVersion(RunnableConfig.builder(config).checkPointId("cp2").build(), secondUpdated, 3);

		assertThat(saver.getVersioned(config).revision()).isEqualTo(4);
		assertThat(saver.list(config)).extracting(Checkpoint::getId).containsExactly("cp3", "cp2");
		assertThat(saver.get(RunnableConfig.builder(config).checkPointId("cp1").build())).isEmpty();
		assertThat(saver.getVersioned(RunnableConfig.builder(config).checkPointId("cp2").build()).checkpoint())
			.hasValueSatisfying(actual -> assertThat(actual.getState()).containsEntry("version", 20));
		assertThat(saver.getVersioned(RunnableConfig.builder(config)
			.checkPointId(firstConfig.checkPointId().orElseThrow())
			.build()).revision()).isEqualTo(4);
		assertStoredHistory(storageKey, config.threadId().orElseThrow(), List.of("cp3", "cp2"), 4);
	}

	@Test
	@SuppressWarnings("unchecked")
	void snapshotsAndListsReturnOwnedCheckpointCopies() throws Exception {
		String storageKey = uniqueStorageKey();
		RedisVersionedSaver saver = saver(primaryClient, storageKey);
		RunnableConfig config = config("snapshot-ownership");
		List<String> seed = new ArrayList<>(List.of("seed"));

		saver.putIfVersion(config, Checkpoint.builder()
			.id("owned")
			.nodeId("node")
			.nextNodeId(END)
			.state(Map.of("items", seed))
			.build(), 0);

		CheckpointSnapshot snapshot = saver.getVersioned(config);
		((List<String>) snapshot.checkpoint().orElseThrow().getState().get("items")).add("mutated");
		((List<String>) saver.list(config).iterator().next().getState().get("items")).add("list-mutated");

		assertThat((List<String>) saver.get(config).orElseThrow().getState().get("items")).containsExactly("seed");
	}

	@Test
	void releaseCreatesEmptyPositiveRevisionAndAllowsReuseOnlyFromObservedRevision() throws Exception {
		String storageKey = uniqueStorageKey();
		RedisVersionedSaver saverA = saver(primaryClient, storageKey);
		RedisVersionedSaver saverB = saver(secondaryClient, storageKey);
		RunnableConfig config = config("release-reuse");

		BaseCheckpointSaver.Tag emptyTag = saverA.releaseIfVersion(config, 0);

		assertThat(emptyTag.threadId()).isEqualTo(config.threadId().orElseThrow());
		assertThat(emptyTag.checkpoints()).isEmpty();
		assertThat(saverB.getVersioned(config).revision()).isEqualTo(1);
		assertThat(saverB.getVersioned(config).checkpoint()).isEmpty();
		assertThatThrownBy(() -> saverB.putIfVersion(config, checkpoint("stale"), 0))
			.isInstanceOfSatisfying(CheckpointConflictException.class,
					conflict -> assertThat(conflict.getActualRevision()).isEqualTo(1));

		saverB.putIfVersion(config, checkpoint("reused"), 1);

		assertThat(saverA.getVersioned(config).revision()).isEqualTo(2);
		assertThat(saverA.getVersioned(config).checkpoint()).hasValueSatisfying(
				actual -> assertThat(actual.getState()).containsEntry("result", "reused"));
	}

	@Test
	void onlyOneContendingConditionalWriterWinsAndLoserDoesNotFallback() throws Exception {
		String storageKey = uniqueStorageKey();
		RedisVersionedSaver saverA = saver(primaryClient, storageKey);
		RedisVersionedSaver saverB = saver(secondaryClient, storageKey);
		RunnableConfig config = config("contention");
		CountDownLatch ready = new CountDownLatch(2);
		CountDownLatch start = new CountDownLatch(1);
		AtomicInteger wins = new AtomicInteger();
		AtomicInteger conflicts = new AtomicInteger();
		ExecutorService executor = Executors.newFixedThreadPool(2);
		try {
			executor.submit(() -> writeAfterStart(saverA, config, "a", ready, start, wins, conflicts));
			executor.submit(() -> writeAfterStart(saverB, config, "b", ready, start, wins, conflicts));
			assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();

			start.countDown();
			executor.shutdown();
			assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
		}
		finally {
			executor.shutdownNow();
		}

		assertThat(wins).hasValue(1);
		assertThat(conflicts).hasValue(1);
		assertThat(saverA.getVersioned(config).revision()).isEqualTo(1);
		assertThat(saverA.list(config)).hasSize(1);
	}

	@Test
	void legacyStorageKeysAreIsolatedFromVersionedStorage() throws Exception {
		String storageKey = uniqueStorageKey();
		RedisVersionedSaver versioned = saver(primaryClient, storageKey);
		RedisSaver legacy = RedisSaver.builder().redisson(primaryClient).stateSerializer(STATE_SERIALIZER).build();
		RunnableConfig config = config("legacy-isolation");
		Checkpoint legacyCheckpoint = checkpoint("legacy");
		Checkpoint versionedCheckpoint = checkpoint("versioned");

		legacy.put(config, legacyCheckpoint);
		String threadName = config.threadId().orElseThrow();
		RMap<String, String> legacyMeta = primaryClient.getMap(LEGACY_THREAD_META_PREFIX + threadName);
		Map<String, String> metaBefore = legacyMeta.readAllMap();
		String physicalThreadId = metaBefore.get(FIELD_THREAD_ID);
		RMap<String, String> legacyReverse = primaryClient.getMap(LEGACY_THREAD_REVERSE_PREFIX + physicalThreadId);
		Map<String, String> reverseBefore = legacyReverse.readAllMap();
		RBucket<String> legacyContent = primaryClient.getBucket(LEGACY_CHECKPOINT_PREFIX + physicalThreadId);
		String contentBefore = legacyContent.get();

		versioned.putIfVersion(config, versionedCheckpoint, 0);

		assertThat(versioned.getVersioned(config).checkpoint())
			.hasValueSatisfying(actual -> assertThat(actual.getState()).containsEntry("result", "versioned"));
		assertThat(legacy.get(config)).hasValueSatisfying(
				actual -> assertThat(actual.getState()).containsEntry("result", "legacy"));
		assertThat(legacyMeta.readAllMap()).isEqualTo(metaBefore);
		assertThat(legacyReverse.readAllMap()).isEqualTo(reverseBefore);
		assertThat(legacyContent.get()).isEqualTo(contentBefore);
		assertThat(new RedisStore(primaryClient, storageKey).getVersionedItem(CHECKPOINT_NAMESPACE,
				config.threadId().orElseThrow()).version()).isEqualTo(1);
	}

	@Test
	void serializerRoundTripSurvivesClientReconstruction() throws Exception {
		String storageKey = uniqueStorageKey();
		RunnableConfig config = config("roundtrip");
		Checkpoint checkpoint = Checkpoint.builder()
			.id("nested")
			.nodeId("node")
			.nextNodeId(END)
			.state(Map.of("nested", Map.of("flag", true, "items", List.of("a", "b")), "number", 7))
			.build();
		saver(primaryClient, storageKey).putIfVersion(config, checkpoint, 0);
		RedissonClient reconstructed = newClient();
		try {
			RedisVersionedSaver restored = saver(reconstructed, storageKey);

			assertThat(restored.getVersioned(config).revision()).isEqualTo(1);
			assertThat(restored.getVersioned(config).checkpoint()).hasValueSatisfying(actual -> {
				assertThat(actual.getState().get("nested")).isInstanceOf(Map.class);
				assertThat(actual.getState()).containsEntry("number", 7);
			});
		}
		finally {
			reconstructed.shutdown();
		}
	}

	@Test
	void negativeExpectedRevisionAndOverflowFailBeforeMutation() throws Exception {
		String storageKey = uniqueStorageKey();
		RedisVersionedSaver saver = saver(primaryClient, storageKey);
		RunnableConfig config = config("validation");
		RedisStore store = new RedisStore(primaryClient, storageKey);

		assertThatThrownBy(() -> saver.putIfVersion(config, checkpoint("negative"), -1))
			.isInstanceOf(IllegalArgumentException.class);
		assertThatThrownBy(() -> saver.releaseIfVersion(config, -1)).isInstanceOf(IllegalArgumentException.class);

		store.putItemIfVersion(newItem(config.threadId().orElseThrow(), List.of(checkpoint("max"))), 0);
		RMap<String, String> hash = primaryClient.getMap(storageKey, StringCodec.INSTANCE);
		String field = hash.readAllKeySet().stream().filter(key -> key.contains("")).findFirst().orElseThrow();
		String maxEnvelope = hash.get(field).replace("\"version\":1", "\"version\":9223372036854775807");
		hash.put(field, maxEnvelope);

		assertThatThrownBy(() -> saver.putIfVersion(config, checkpoint("overflow"), Long.MAX_VALUE))
			.isInstanceOf(ArithmeticException.class);
		assertThatThrownBy(() -> saver.releaseIfVersion(config, Long.MAX_VALUE)).isInstanceOf(ArithmeticException.class);
		assertThat(saver.getVersioned(config).revision()).isEqualTo(Long.MAX_VALUE);
		assertThat(saver.getVersioned(config).checkpoint()).hasValueSatisfying(
				actual -> assertThat(actual.getState()).containsEntry("result", "max"));
	}

	@Test
	void conflictPreservesTypeWhenDiagnosticReadFails() {
		String storageKey = uniqueStorageKey();
		RedissonClient redisson = mock(RedissonClient.class);
		RMap<String, String> hash = mock(RMap.class);
		RuntimeException readFailure = new RuntimeException("diagnostic read failed");
		when(redisson.<String, String>getMap(eq(storageKey), any(Codec.class))).thenReturn(hash);
		when(hash.get(anyString())).thenReturn(null).thenReturn("""
				{"version":1,"deleted":false,"item":{"namespace":["checkpoints"],"key":"diagnostic",\
				"value":{"content":""},"createdAt":10,"updatedAt":20}}\
				""").thenThrow(readFailure);
		RedisVersionedSaver saver = saver(redisson, storageKey);

		assertThatThrownBy(() -> saver.putIfVersion(RunnableConfig.builder().threadId("diagnostic").build(),
				checkpoint("diagnostic"), 0))
			.isInstanceOfSatisfying(CheckpointConflictException.class, conflict -> {
				assertThat(conflict.getActualRevision()).isEqualTo(-1);
				assertThat(conflict.getSuppressed()).containsExactly(readFailure);
			});
	}

	@Test
	void graphInvokeStreamManualUpdateAndCompletionReleaseUseVersionedCas() throws Exception {
		String storageKey = uniqueStorageKey();
		RedisVersionedSaver saverA = saver(primaryClient, storageKey);
		RedisVersionedSaver saverB = saver(secondaryClient, storageKey);
		CompiledGraph graphA = simpleGraph(saverA);
		CompiledGraph graphB = simpleGraph(saverB);
		RunnableConfig invokeConfig = config("graph-invoke");
		RunnableConfig streamConfig = config("graph-stream");
		RunnableConfig updateConfig = config("graph-update");
		RunnableConfig releaseConfig = config("graph-release");

		assertThat(graphA.invoke(Map.of("request", "invoke"), invokeConfig).orElseThrow().value("result"))
			.contains("invoke");
		assertThat(saverB.getVersioned(invokeConfig).revision()).isEqualTo(2);

		NodeOutput streamOutput = graphB.stream(Map.of("request", "stream"), streamConfig)
			.last()
			.block(Duration.ofSeconds(5));
		assertThat(streamOutput.state().value("result")).contains("stream");
		assertThat(saverA.getVersioned(streamConfig).revision()).isEqualTo(2);

		RunnableConfig seeded = saverA.putIfVersion(updateConfig,
				Checkpoint.builder().state(Map.of("result", "seed")).nodeId(START).nextNodeId("write").build(), 0);
		RunnableConfig updated = graphB.updateState(seeded, Map.of("request", "manual"), START);
		assertThat(updated.checkPointId()).isPresent();
		assertThat(saverA.getVersioned(updateConfig).revision()).isEqualTo(2);
		assertThat(saverA.getVersioned(updateConfig).checkpoint()).hasValueSatisfying(
				actual -> assertThat(actual.getState()).containsEntry("request", "manual").containsEntry("result", "seed"));

		CompiledGraph releasingGraph = simpleGraph(saverB, true);
		assertThat(releasingGraph.invoke(Map.of("request", "done"), releaseConfig)).isPresent();
		assertThat(saverA.getVersioned(releaseConfig).revision()).isEqualTo(3);
		assertThat(saverA.getVersioned(releaseConfig).checkpoint()).isEmpty();
	}

	@Test
	void wholeGraphContentionAcrossIndependentClientsFailsStaleWriterWithoutRerun() throws Exception {
		String storageKey = uniqueStorageKey();
		RedisVersionedSaver staleSaver = saver(primaryClient, storageKey);
		RedisVersionedSaver winnerSaver = saver(secondaryClient, storageKey);
		CountDownLatch staleNodeEntered = new CountDownLatch(1);
		CountDownLatch allowStaleNodeToFinish = new CountDownLatch(1);
		AtomicInteger staleNodeCalls = new AtomicInteger();
		AtomicInteger winnerNodeCalls = new AtomicInteger();
		CompiledGraph staleGraph = graph(staleSaver, "stale", staleNodeCalls, staleNodeEntered,
				allowStaleNodeToFinish);
		CompiledGraph winnerGraph = graph(winnerSaver, "winner", winnerNodeCalls);
		RunnableConfig config = config("graph-contention");

		CompletableFuture<List<NodeOutput>> staleRun = staleGraph.stream(Map.of("request", "stale"), config)
			.collectList()
			.toFuture();
		assertThat(staleNodeEntered.await(5, TimeUnit.SECONDS)).isTrue();

		NodeOutput winner = winnerGraph.stream(Map.of("request", "winner"), config)
			.last()
			.block(Duration.ofSeconds(5));
		allowStaleNodeToFinish.countDown();

		assertThatThrownBy(() -> staleRun.get(5, TimeUnit.SECONDS)).satisfies(RedisVersionedSaverTest::assertConflict);
		assertThat(winner.state().value("result")).contains("winner");
		assertThat(winnerSaver.getVersioned(config).revision()).isEqualTo(3);
		assertThat(winnerSaver.getVersioned(config).checkpoint()).hasValueSatisfying(
				actual -> assertThat(actual.getState()).containsEntry("result", "winner"));
		assertThat(staleNodeCalls).hasValue(1);
		assertThat(winnerNodeCalls).hasValue(1);
	}

	@Test
	void staleManualUpdateAndCompletionReleaseConflictAgainstPeerWriteBetweenSnapshotAndCas() throws Exception {
		String updateStorageKey = uniqueStorageKey();
		RedisVersionedSaver updateSeedSaver = saver(primaryClient, updateStorageKey);
		RedisVersionedSaver updatePeer = saver(secondaryClient, updateStorageKey);
		RunnableConfig updateConfig = config("stale-update");
		RunnableConfig seeded = updateSeedSaver.putIfVersion(updateConfig,
				Checkpoint.builder().state(Map.of("result", "seed")).nodeId(START).nextNodeId("write").build(), 0);
		RedisVersionedSaver staleUpdateSaver = new PeerWriteBeforePutSaver(primaryClient, STATE_SERIALIZER,
				updateStorageKey, updatePeer, "winner-update");
		CompiledGraph updateGraph = simpleGraph(staleUpdateSaver);

		assertThatThrownBy(() -> updateGraph.updateState(seeded, Map.of("request", "manual"), START))
			.satisfies(RedisVersionedSaverTest::assertConflict);
		assertThat(updatePeer.getVersioned(updateConfig).revision()).isEqualTo(2);
		assertThat(updatePeer.getVersioned(updateConfig).checkpoint()).hasValueSatisfying(
				actual -> assertThat(actual.getState()).containsEntry("result", "winner-update"));

		String releaseStorageKey = uniqueStorageKey();
		RedisVersionedSaver releasePeer = saver(secondaryClient, releaseStorageKey);
		RedisVersionedSaver staleReleaseSaver = new PeerWriteBeforeReleaseSaver(primaryClient, STATE_SERIALIZER,
				releaseStorageKey, releasePeer, "winner-release");
		CompiledGraph releaseGraph = simpleGraph(staleReleaseSaver, true);
		RunnableConfig releaseConfig = config("stale-release");

		assertThatThrownBy(() -> releaseGraph.invoke(Map.of("request", "release"), releaseConfig))
			.satisfies(RedisVersionedSaverTest::assertConflict);
		assertThat(releasePeer.getVersioned(releaseConfig).revision()).isEqualTo(3);
		assertThat(releasePeer.getVersioned(releaseConfig).checkpoint()).hasValueSatisfying(
				actual -> assertThat(actual.getState()).containsEntry("result", "winner-release"));
	}

	@Test
	void runtimeVersionedPathsDoNotCallCompatibilityPutOrRelease() throws Exception {
		String storageKey = uniqueStorageKey();
		RedisVersionedSaver guardedSaver = spy(saver(primaryClient, storageKey));
		doThrow(new AssertionError("runtime must not call BaseCheckpointSaver.put"))
			.when(guardedSaver)
			.put(any(RunnableConfig.class), any(Checkpoint.class));
		doThrow(new AssertionError("runtime must not call BaseCheckpointSaver.release"))
			.when(guardedSaver)
			.release(any(RunnableConfig.class));
		CompiledGraph graph = simpleGraph(guardedSaver);
		RunnableConfig invokeConfig = config("guard-invoke");
		RunnableConfig streamConfig = config("guard-stream");
		RunnableConfig updateConfig = config("guard-update");
		RunnableConfig releaseConfig = config("guard-release");

		assertThat(graph.invoke(Map.of("request", "invoke"), invokeConfig)).isPresent();
		assertThat(graph.stream(Map.of("request", "stream"), streamConfig).last().block(Duration.ofSeconds(5))).isNotNull();
		RunnableConfig seeded = guardedSaver.putIfVersion(updateConfig,
				Checkpoint.builder().state(Map.of("result", "seed")).nodeId(START).nextNodeId("write").build(), 0);
		assertThat(graph.updateState(seeded, Map.of("request", "manual"), START).checkPointId()).isPresent();
		assertThat(simpleGraph(guardedSaver, true).invoke(Map.of("request", "release"), releaseConfig)).isPresent();

		verify(guardedSaver, never()).put(any(RunnableConfig.class), any(Checkpoint.class));
		verify(guardedSaver, never()).release(any(RunnableConfig.class));
	}

	private static CompiledGraph simpleGraph(RedisVersionedSaver saver) throws Exception {
		return simpleGraph(saver, false);
	}

	private static CompiledGraph simpleGraph(RedisVersionedSaver saver, boolean releaseThread) throws Exception {
		return graph(saver, null, new AtomicInteger(), null, null, releaseThread);
	}

	private static CompiledGraph graph(RedisVersionedSaver saver, String fixedResult, AtomicInteger calls)
			throws Exception {
		return graph(saver, fixedResult, calls, null, null);
	}

	private static CompiledGraph graph(RedisVersionedSaver saver, String fixedResult, AtomicInteger calls,
			CountDownLatch entered, CountDownLatch proceed) throws Exception {
		return graph(saver, fixedResult, calls, entered, proceed, false);
	}

	private static CompiledGraph graph(RedisVersionedSaver saver, String fixedResult, AtomicInteger calls,
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

	private static void writeAfterStart(RedisVersionedSaver saver, RunnableConfig config, String writer,
			CountDownLatch ready, CountDownLatch start, AtomicInteger wins, AtomicInteger conflicts) {
		ready.countDown();
		await(start);
		try {
			saver.putIfVersion(config, checkpoint(writer), 0);
			wins.incrementAndGet();
		}
		catch (CheckpointConflictException ex) {
			conflicts.incrementAndGet();
		}
		catch (Exception ex) {
			throw new IllegalStateException(ex);
		}
	}

	private static void assertConflict(Throwable failure) {
		assertThat(rootCause(failure)).isInstanceOf(CheckpointConflictException.class);
	}

	private static Throwable rootCause(Throwable failure) {
		Throwable current = failure;
		while ((current instanceof ExecutionException || current.getCause() != null) && current.getCause() != null) {
			current = current.getCause();
		}
		return current;
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

	private static RedisVersionedSaver saver(RedissonClient redisson, String storageKey) {
		return RedisVersionedSaver.builder()
			.redisson(redisson)
			.stateSerializer(STATE_SERIALIZER)
			.storageKey(storageKey)
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

	private static void assertCheckpoint(Checkpoint actual, Checkpoint expected) {
		assertThat(actual.getId()).isEqualTo(expected.getId());
		assertThat(actual.getNodeId()).isEqualTo(expected.getNodeId());
		assertThat(actual.getNextNodeId()).isEqualTo(expected.getNextNodeId());
		assertThat(actual.getState()).isEqualTo(expected.getState());
	}

	private static void assertStoredHistory(String storageKey, String threadId, List<String> checkpointIds, long revision)
			throws Exception {
		VersionedStoreItem stored = new RedisStore(primaryClient, storageKey).getVersionedItem(CHECKPOINT_NAMESPACE, threadId);
		assertThat(stored.version()).isEqualTo(revision);
		assertThat(stored.item()).isPresent();
		Object content = stored.item().orElseThrow().getValue().get(CONTENT_FIELD);
		assertThat(content).isInstanceOf(String.class);
		assertThat(deserializeCheckpoints((String) content)).extracting(Checkpoint::getId).containsExactlyElementsOf(checkpointIds);
	}

	private static io.github.agentic.ai.graph.store.StoreItem newItem(String threadId, List<Checkpoint> checkpoints)
			throws Exception {
		return io.github.agentic.ai.graph.store.StoreItem.of(CHECKPOINT_NAMESPACE, threadId,
				Map.of(CONTENT_FIELD, serializeCheckpoints(checkpoints)));
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

	private static RedissonClient newClient() {
		Config config = new Config();
		config.useSingleServer().setAddress("redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379));
		return Redisson.create(config);
	}

	private static String uniqueStorageKey() {
		return "argi:test:checkpoint:versioned:" + UUID.randomUUID();
	}

	private static final class PeerWriteBeforePutSaver extends RedisVersionedSaver {

		private final RedisVersionedSaver peer;

		private final String peerResult;

		private final AtomicInteger writes = new AtomicInteger();

		private PeerWriteBeforePutSaver(RedissonClient redisson, StateSerializer stateSerializer, String storageKey,
				RedisVersionedSaver peer, String peerResult) {
			super(redisson, stateSerializer, storageKey);
			this.peer = peer;
			this.peerResult = peerResult;
		}

		@Override
		public RunnableConfig putIfVersion(RunnableConfig config, Checkpoint checkpoint, long expectedRevision)
				throws Exception {
			if (writes.compareAndSet(0, 1)) {
				peer.putIfVersion(config, checkpoint(peerResult), peer.getVersioned(config).revision());
			}
			return super.putIfVersion(config, checkpoint, expectedRevision);
		}

	}

	private static final class PeerWriteBeforeReleaseSaver extends RedisVersionedSaver {

		private final RedisVersionedSaver peer;

		private final String peerResult;

		private final AtomicInteger writes = new AtomicInteger();

		private PeerWriteBeforeReleaseSaver(RedissonClient redisson, StateSerializer stateSerializer, String storageKey,
				RedisVersionedSaver peer, String peerResult) {
			super(redisson, stateSerializer, storageKey);
			this.peer = peer;
			this.peerResult = peerResult;
		}

		@Override
		public Tag releaseIfVersion(RunnableConfig config, long expectedRevision) throws Exception {
			if (writes.compareAndSet(0, 1)) {
				peer.putIfVersion(config, checkpoint(peerResult), peer.getVersioned(config).revision());
			}
			return super.releaseIfVersion(config, expectedRevision);
		}

	}

}
