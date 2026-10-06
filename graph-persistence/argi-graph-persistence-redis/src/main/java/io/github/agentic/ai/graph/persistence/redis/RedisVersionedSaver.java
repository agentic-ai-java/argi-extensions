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

import io.github.agentic.ai.graph.RunnableConfig;
import io.github.agentic.ai.graph.StateGraph;
import io.github.agentic.ai.graph.checkpoint.Checkpoint;
import io.github.agentic.ai.graph.checkpoint.CheckpointConflictException;
import io.github.agentic.ai.graph.checkpoint.CheckpointSnapshot;
import io.github.agentic.ai.graph.checkpoint.VersionedCheckpointSaver;
import io.github.agentic.ai.graph.serializer.Serializer;
import io.github.agentic.ai.graph.serializer.StateSerializer;
import io.github.agentic.ai.graph.serializer.check_point.CheckPointSerializer;
import io.github.agentic.ai.graph.store.StoreItem;
import io.github.agentic.ai.graph.store.VersionedStoreItem;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Base64;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.stream.IntStream;

import org.redisson.api.RedissonClient;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Redis-backed {@link VersionedCheckpointSaver} using {@link RedisStore}'s versioned
 * envelope as the only compare-and-set primitive.
 * <p>
 * Versioned checkpoint history is stored separately from the legacy {@link RedisSaver}
 * key layout. Each checkpoint namespace is one Store item in the
 * {@value #DEFAULT_STORAGE_KEY} hash by default, with the checkpoint thread id as the
 * Store key and serialized history under the {@code content} value field. No TTL,
 * migration, duplicate Redis lock, or Lua layer is added here.
 */
public class RedisVersionedSaver implements VersionedCheckpointSaver {

	public static final String DEFAULT_STORAGE_KEY = "argi:checkpoint:versioned:v1";

	private static final List<String> CHECKPOINT_NAMESPACE = List.of("checkpoints");

	private static final String CONTENT_FIELD = "content";

	private static final int MAX_COMPATIBILITY_MUTATION_ATTEMPTS = 64;

	private final RedisStore store;

	private final Serializer<Checkpoint> checkpointSerializer;

	protected RedisVersionedSaver(RedissonClient redisson, StateSerializer stateSerializer, String storageKey) {
		requireNonNull(stateSerializer, "stateSerializer cannot be null");
		this.store = new RedisStore(requireNonNull(redisson, "redisson cannot be null"), storageKey);
		this.checkpointSerializer = new CheckPointSerializer(stateSerializer);
	}

	public static Builder builder() {
		return new Builder();
	}

	@Override
	public Collection<Checkpoint> list(RunnableConfig config) {
		return readHistory(config).history();
	}

	@Override
	public Optional<Checkpoint> get(RunnableConfig config) {
		return select(readHistory(config).history(), config);
	}

	@Override
	public CheckpointSnapshot getVersioned(RunnableConfig config) {
		VersionedHistory snapshot = readHistory(config);
		return new CheckpointSnapshot(select(snapshot.history(), config), snapshot.revision());
	}

	@Override
	public RunnableConfig put(RunnableConfig config, Checkpoint checkpoint) throws Exception {
		for (int attempt = 0; attempt < MAX_COMPATIBILITY_MUTATION_ATTEMPTS; attempt++) {
			long expectedRevision = getVersioned(config).revision();
			try {
				return putIfVersion(config, checkpoint, expectedRevision);
			}
			catch (CheckpointConflictException ignored) {
				// Compatibility path only: reload and retry boundedly.
			}
		}
		throw new IllegalStateException("Failed to store Redis checkpoint after "
				+ MAX_COMPATIBILITY_MUTATION_ATTEMPTS + " attempts: " + checkpointThreadId(config));
	}

	@Override
	public RunnableConfig putIfVersion(RunnableConfig config, Checkpoint checkpoint, long expectedRevision)
			throws Exception {
		validateExpectedRevision(expectedRevision);
		requireNonNull(checkpoint, "checkpoint cannot be null");
		String namespace = checkpointThreadId(config);
		VersionedHistory current = readHistory(config);
		requireExpectedRevision(namespace, expectedRevision, current.revision());

		Math.addExact(expectedRevision, 1L);
		LinkedList<Checkpoint> nextHistory = current.history();
		if (config.checkPointId().isPresent()) {
			String checkpointId = config.checkPointId().get();
			int index = IntStream.range(0, nextHistory.size())
				.filter(i -> nextHistory.get(i).getId().equals(checkpointId))
				.findFirst()
				.orElseThrow(() -> new NoSuchElementException(format("Checkpoint with id %s not found!", checkpointId)));
			nextHistory.set(index, cloneCheckpoint(checkpoint));
		}
		else {
			nextHistory.push(cloneCheckpoint(checkpoint));
		}
		retainLatestCheckpoints(nextHistory, config);

		if (!store.putItemIfVersion(historyItem(namespace, nextHistory), expectedRevision)) {
			throw conflictFromDiagnosticRead(namespace, expectedRevision);
		}
		return RunnableConfig.builder(config).checkPointId(checkpoint.getId()).build();
	}

	@Override
	public Tag release(RunnableConfig config) throws Exception {
		for (int attempt = 0; attempt < MAX_COMPATIBILITY_MUTATION_ATTEMPTS; attempt++) {
			long expectedRevision = getVersioned(config).revision();
			try {
				return releaseIfVersion(config, expectedRevision);
			}
			catch (CheckpointConflictException ignored) {
				// Compatibility path only: reload and retry boundedly.
			}
		}
		throw new IllegalStateException("Failed to release Redis checkpoint after "
				+ MAX_COMPATIBILITY_MUTATION_ATTEMPTS + " attempts: " + checkpointThreadId(config));
	}

	@Override
	public Tag releaseIfVersion(RunnableConfig config, long expectedRevision) throws Exception {
		validateExpectedRevision(expectedRevision);
		String namespace = checkpointThreadId(config);
		VersionedHistory current = readHistory(config);
		requireExpectedRevision(namespace, expectedRevision, current.revision());
		Math.addExact(expectedRevision, 1L);

		Tag releaseTag = new Tag(namespace, current.history());
		if (!store.putItemIfVersion(historyItem(namespace, List.of()), expectedRevision)) {
			throw conflictFromDiagnosticRead(namespace, expectedRevision);
		}
		return releaseTag;
	}

	private VersionedHistory readHistory(RunnableConfig config) {
		String namespace = checkpointThreadId(config);
		VersionedStoreItem item = store.getVersionedItem(CHECKPOINT_NAMESPACE, namespace);
		return new VersionedHistory(deserializeHistory(item.item()), item.version());
	}

	private Optional<Checkpoint> select(LinkedList<Checkpoint> checkpoints, RunnableConfig config) {
		if (config.checkPointId().isPresent()) {
			String checkpointId = config.checkPointId().get();
			return checkpoints.stream().filter(checkpoint -> checkpoint.getId().equals(checkpointId)).findFirst();
		}
		return getLast(checkpoints, config);
	}

	private StoreItem historyItem(String namespace, Collection<Checkpoint> checkpoints) throws IOException {
		return StoreItem.of(CHECKPOINT_NAMESPACE, namespace, Map.of(CONTENT_FIELD, serializeCheckpoints(checkpoints)));
	}

	private LinkedList<Checkpoint> deserializeHistory(Optional<StoreItem> item) {
		Object content = item.map(StoreItem::getValue).map(value -> value.get(CONTENT_FIELD)).orElse(null);
		if (content == null) {
			return new LinkedList<>();
		}
		if (!(content instanceof String text)) {
			throw new IllegalStateException("Redis checkpoint history content must be a String");
		}
		try {
			return deserializeCheckpoints(text);
		}
		catch (IOException | ClassNotFoundException ex) {
			throw new RuntimeException("Failed to deserialize Redis versioned checkpoint history", ex);
		}
	}

	private String serializeCheckpoints(Collection<Checkpoint> checkpoints) throws IOException {
		try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
				ObjectOutputStream oos = new ObjectOutputStream(baos)) {
			oos.writeInt(checkpoints.size());
			for (Checkpoint checkpoint : checkpoints) {
				checkpointSerializer.write(checkpoint, oos);
			}
			oos.flush();
			return Base64.getEncoder().encodeToString(baos.toByteArray());
		}
	}

	private LinkedList<Checkpoint> deserializeCheckpoints(String content) throws IOException, ClassNotFoundException {
		if (content == null || content.isEmpty()) {
			return new LinkedList<>();
		}
		byte[] bytes = Base64.getDecoder().decode(content);
		try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
				ObjectInputStream ois = new ObjectInputStream(bais)) {
			int size = ois.readInt();
			LinkedList<Checkpoint> checkpoints = new LinkedList<>();
			for (int i = 0; i < size; i++) {
				checkpoints.add(checkpointSerializer.read(ois));
			}
			return checkpoints;
		}
	}

	private Checkpoint cloneCheckpoint(Checkpoint checkpoint) throws IOException, ClassNotFoundException {
		return deserializeCheckpoints(serializeCheckpoints(List.of(checkpoint))).getFirst();
	}

	private void validateExpectedRevision(long expectedRevision) {
		if (expectedRevision < 0) {
			throw new IllegalArgumentException("expectedRevision cannot be negative");
		}
	}

	private void requireExpectedRevision(String namespace, long expectedRevision, long actualRevision)
			throws CheckpointConflictException {
		if (expectedRevision != actualRevision) {
			throw new CheckpointConflictException(namespace, expectedRevision, actualRevision);
		}
	}

	private CheckpointConflictException conflictFromDiagnosticRead(String namespace, long expectedRevision) {
		try {
			long actualRevision = store.getVersionedItem(CHECKPOINT_NAMESPACE, namespace).version();
			return new CheckpointConflictException(namespace, expectedRevision, actualRevision);
		}
		catch (RuntimeException ex) {
			CheckpointConflictException conflict = new CheckpointConflictException(namespace, expectedRevision, -1);
			conflict.addSuppressed(ex);
			return conflict;
		}
	}

	private record VersionedHistory(LinkedList<Checkpoint> history, long revision) {
	}

	public static class Builder {

		private RedissonClient redisson;

		private StateSerializer stateSerializer;

		private String storageKey = DEFAULT_STORAGE_KEY;

		public Builder redisson(RedissonClient redisson) {
			this.redisson = redisson;
			return this;
		}

		public Builder stateSerializer(StateSerializer stateSerializer) {
			this.stateSerializer = stateSerializer;
			return this;
		}

		public Builder storageKey(String storageKey) {
			this.storageKey = storageKey;
			return this;
		}

		public RedisVersionedSaver build() {
			if (redisson == null) {
				throw new IllegalArgumentException("redisson cannot be null");
			}
			if (stateSerializer == null) {
				stateSerializer = StateGraph.DEFAULT_JACKSON_SERIALIZER;
			}
			return new RedisVersionedSaver(redisson, stateSerializer, storageKey);
		}

	}

}
