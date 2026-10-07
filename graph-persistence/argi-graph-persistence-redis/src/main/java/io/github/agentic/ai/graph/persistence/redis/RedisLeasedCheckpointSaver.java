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
import io.github.agentic.ai.graph.checkpoint.LeasedCheckpointSaver;
import io.github.agentic.ai.graph.checkpoint.lease.ExecutionLease;
import io.github.agentic.ai.graph.checkpoint.lease.LeaseBusyException;
import io.github.agentic.ai.graph.checkpoint.lease.LeaseLostException;
import io.github.agentic.ai.graph.checkpoint.lease.LeaseOptions;
import io.github.agentic.ai.graph.serializer.Serializer;
import io.github.agentic.ai.graph.serializer.StateSerializer;
import io.github.agentic.ai.graph.serializer.check_point.CheckPointSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.IntStream;

import org.redisson.api.RScript;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Redis/Valkey implementation of {@link LeasedCheckpointSaver}.
 * <p>
 * Each checkpoint namespace is stored in one Redis hash under a leased keyspace that
 * is independent from the legacy and versioned Redis savers. Lease ownership,
 * retained fence, checkpoint revision and serialized checkpoint history are mutated
 * only by primary-routed single-key Lua scripts.
 */
public class RedisLeasedCheckpointSaver implements LeasedCheckpointSaver {

	public static final String DEFAULT_STORAGE_KEY_PREFIX = "argi:checkpoint:leased:v1";

	private static final String OWNER_FIELD = "owner";

	private static final String FENCE_FIELD = "fence";

	private static final String EXPIRES_FIELD = "expires";

	private static final String REVISION_FIELD = "revision";

	private static final String HISTORY_FIELD = "history";

	private static final String STATUS_OK = "OK";

	private static final String STATUS_BUSY = "BUSY";

	private static final String STATUS_LOST = "LOST";

	private static final String STATUS_CONFLICT = "CONFLICT";

	private static final String STATUS_CORRUPT = "CORRUPT";

	private static final String STATUS_OVERFLOW = "OVERFLOW";

	private static final String READ_AND_VALIDATE = """
			local MAX_LONG = '9223372036854775807'
			local MAX_INCREMENTABLE = '9223372036854775806'
			local function is_decimal(value)
			  if value == false or value == nil then
			    return false
			  end
			  if value == '0' then
			    return true
			  end
			  if string.sub(value, 1, 1) == '0' then
			    return false
			  end
			  return string.match(value, '^[0-9]+$') ~= nil
			end
			local function leq_decimal(value, max)
			  if string.len(value) < string.len(max) then
			    return true
			  end
			  if string.len(value) > string.len(max) then
			    return false
			  end
			  return value <= max
			end
			local function validate_counter(value, default)
			  if value == false or value == nil then
			    return default
			  end
			  if not is_decimal(value) or not leq_decimal(value, MAX_LONG) then
			    return nil
			  end
			  return value
			end
			local function validate_incrementable(value)
			  if value == false or value == nil then
			    return '0'
			  end
			  if not is_decimal(value) or not leq_decimal(value, MAX_LONG) then
			    return nil
			  end
			  if value == MAX_LONG then
			    return 'OVERFLOW'
			  end
			  if not leq_decimal(value, MAX_INCREMENTABLE) then
			    return nil
			  end
			  return value
			end
			local function now_millis()
			  local time = redis.call('TIME')
			  return tostring((time[1] * 1000) + math.floor(time[2] / 1000))
			end
			local function validate_hash()
			  local values = redis.call('HMGET', KEYS[1], 'owner', 'fence', 'expires', 'revision', 'history')
			  local owner = values[1]
			  local fence = validate_counter(values[2], '0')
			  local expires = validate_counter(values[3], nil)
			  local revision = validate_counter(values[4], '0')
			  local history = values[5]
			  if fence == nil or expires == nil and owner ~= false and owner ~= nil then
			    return {'CORRUPT'}
			  end
			  if owner == false or owner == nil then
			    if expires ~= nil then
			      return {'CORRUPT'}
			    end
			  elseif string.len(owner) == 0 or expires == nil then
			    return {'CORRUPT'}
			  end
			  if revision == nil then
			    return {'CORRUPT'}
			  end
			  if values[4] == false or values[4] == nil then
			    if history ~= false and history ~= nil then
			      return {'CORRUPT'}
			    end
			  elseif history == false or history == nil then
			    return {'CORRUPT'}
			  end
			  return {'OK', owner, fence, expires, revision, history}
			end
			""";

	private static final String ACQUIRE_SCRIPT = READ_AND_VALIDATE + """
			local ttl = ARGV[2]
			if not is_decimal(ttl) or ttl == '0' or not leq_decimal(ttl, '86400000') then
			  return {'CORRUPT'}
			end
			local current_fence = validate_incrementable(redis.call('HGET', KEYS[1], 'fence'))
			if current_fence == nil then
			  return {'CORRUPT'}
			end
			if current_fence == 'OVERFLOW' then
			  return {'OVERFLOW'}
			end
			local state = validate_hash()
			if state[1] ~= 'OK' then
			  return state
			end
			local now = now_millis()
			if state[2] ~= false and state[2] ~= nil and state[4] > now then
			  return {'BUSY'}
			end
			redis.call('HINCRBY', KEYS[1], 'fence', 1)
			local fence = redis.call('HGET', KEYS[1], 'fence')
			local expires = tostring(tonumber(now) + tonumber(ttl))
			redis.call('HSET', KEYS[1], 'owner', ARGV[1], 'expires', expires)
			return {'OK', ARGV[1], fence, expires, state[5]}
			""";

	private static final String RENEW_SCRIPT = READ_AND_VALIDATE + """
			local ttl = ARGV[3]
			if not is_decimal(ttl) or ttl == '0' or not leq_decimal(ttl, '86400000') then
			  return {'CORRUPT'}
			end
			local state = validate_hash()
			if state[1] ~= 'OK' then
			  return state
			end
			local now = now_millis()
			if state[2] ~= ARGV[1] or state[3] ~= ARGV[2] or state[4] == nil or state[4] <= now then
			  return {'LOST'}
			end
			local expires = tostring(tonumber(now) + tonumber(ttl))
			redis.call('HSET', KEYS[1], 'expires', expires)
			return {'OK', ARGV[1], ARGV[2], expires}
			""";

	private static final String RELEASE_LEASE_SCRIPT = READ_AND_VALIDATE + """
			local state = validate_hash()
			if state[1] ~= 'OK' then
			  return state
			end
			local now = now_millis()
			if state[2] ~= ARGV[1] or state[3] ~= ARGV[2] or state[4] == nil or state[4] <= now then
			  return {'LOST'}
			end
			redis.call('HDEL', KEYS[1], 'owner', 'expires')
			return {'OK'}
			""";

	private static final String SNAPSHOT_SCRIPT = READ_AND_VALIDATE + """
			local state = validate_hash()
			if state[1] ~= 'OK' then
			  return state
			end
			return {'OK', state[5], state[6]}
			""";

	private static final String PUT_SCRIPT = READ_AND_VALIDATE + """
			local state = validate_hash()
			if state[1] ~= 'OK' then
			  return state
			end
			local now = now_millis()
			if state[2] ~= ARGV[1] or state[3] ~= ARGV[2] or state[4] == nil or state[4] <= now then
			  return {'LOST'}
			end
			if state[5] ~= ARGV[3] then
			  return {'CONFLICT', state[5]}
			end
			redis.call('HSET', KEYS[1], 'revision', ARGV[4], 'history', ARGV[5])
			return {'OK', ARGV[4]}
			""";

	private static final String RELEASE_CHECKPOINT_SCRIPT = READ_AND_VALIDATE + """
			local state = validate_hash()
			if state[1] ~= 'OK' then
			  return state
			end
			local now = now_millis()
			if state[2] ~= ARGV[1] or state[3] ~= ARGV[2] or state[4] == nil or state[4] <= now then
			  return {'LOST'}
			end
			if state[5] ~= ARGV[3] then
			  return {'CONFLICT', state[5]}
			end
			redis.call('HSET', KEYS[1], 'revision', ARGV[4], 'history', ARGV[5])
			return {'OK', ARGV[4]}
			""";

	private final RedissonClient redisson;

	private final Serializer<Checkpoint> checkpointSerializer;

	private final LeaseOptions leaseOptions;

	private final String storageKeyPrefix;

	protected RedisLeasedCheckpointSaver(RedissonClient redisson, StateSerializer stateSerializer,
			LeaseOptions leaseOptions, String storageKeyPrefix) {
		this.redisson = requireNonNull(redisson, "redisson cannot be null");
		requireNonNull(stateSerializer, "stateSerializer cannot be null");
		this.checkpointSerializer = new CheckPointSerializer(stateSerializer);
		this.leaseOptions = requireNonNull(leaseOptions, "leaseOptions cannot be null");
		this.storageKeyPrefix = requireNonNull(storageKeyPrefix, "storageKeyPrefix cannot be null");
		if (storageKeyPrefix.isBlank()) {
			throw new IllegalArgumentException("storageKeyPrefix cannot be blank");
		}
	}

	public static Builder builder() {
		return new Builder();
	}

	@Override
	public LeaseOptions leaseOptions() {
		return leaseOptions;
	}

	@Override
	public Collection<Checkpoint> list(RunnableConfig config) {
		return readHistory(config).history();
	}

	@Override
	public Optional<Checkpoint> get(RunnableConfig config) {
		VersionedHistory snapshot = readHistory(config);
		return select(snapshot.history(), config);
	}

	@Override
	public CheckpointSnapshot getVersioned(RunnableConfig config) {
		VersionedHistory snapshot = readHistory(config);
		return new CheckpointSnapshot(select(snapshot.history(), config), snapshot.revision());
	}

	@Override
	public ExecutionLease acquireLease(RunnableConfig config, UUID ownerId) {
		requireNonNull(ownerId, "ownerId cannot be null");
		String namespace = checkpointThreadId(config);
		List<Object> result = eval(ACQUIRE_SCRIPT, redisKey(namespace), ownerId.toString(), ttlMillis());
		String status = status(result);
		return switch (status) {
			case STATUS_OK -> new ExecutionLease(namespace, ownerId, parseLong(result.get(2), "fence"),
					parseLong(result.get(3), "expires"));
			case STATUS_BUSY -> throw new LeaseBusyException(namespace);
			case STATUS_OVERFLOW -> throw new ArithmeticException("Redis checkpoint lease fence overflow: " + namespace);
			case STATUS_CORRUPT -> throw corrupt(namespace);
			default -> throw unexpected(status, namespace);
		};
	}

	@Override
	public ExecutionLease renewLease(RunnableConfig config, ExecutionLease lease) {
		requireLeaseForNamespace(config, lease);
		List<Object> result = evalLeased(lease, RENEW_SCRIPT, redisKey(lease.namespace()), lease.ownerId().toString(),
				Long.toString(lease.fencingToken()), ttlMillis());
		String status = status(result);
		return switch (status) {
			case STATUS_OK -> new ExecutionLease(lease.namespace(), lease.ownerId(), parseLong(result.get(2), "fence"),
					parseLong(result.get(3), "expires"));
			case STATUS_LOST -> throw lost(lease, "lease is not active");
			case STATUS_CORRUPT -> throw corrupt(lease.namespace());
			default -> throw unexpected(status, lease.namespace());
		};
	}

	@Override
	public boolean releaseLease(RunnableConfig config, ExecutionLease lease) {
		requireLeaseForNamespace(config, lease);
		List<Object> result = evalLeased(lease, RELEASE_LEASE_SCRIPT, redisKey(lease.namespace()),
				lease.ownerId().toString(), Long.toString(lease.fencingToken()));
		String status = status(result);
		return switch (status) {
			case STATUS_OK -> true;
			case STATUS_LOST -> false;
			case STATUS_CORRUPT -> throw corrupt(lease.namespace());
			default -> throw unexpected(status, lease.namespace());
		};
	}

	@Override
	public RunnableConfig putIfLeasedVersion(RunnableConfig config, Checkpoint checkpoint, long expectedRevision,
			ExecutionLease lease) throws Exception {
		validateExpectedRevision(expectedRevision);
		requireLeaseForNamespace(config, lease);
		requireNonNull(checkpoint, "checkpoint cannot be null");
		long nextRevision = Math.addExact(expectedRevision, 1L);
		LinkedList<Checkpoint> nextHistory = readHistory(config).history();
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
		List<Object> result = evalLeased(lease, PUT_SCRIPT, redisKey(lease.namespace()), lease.ownerId().toString(),
				Long.toString(lease.fencingToken()), Long.toString(expectedRevision), Long.toString(nextRevision),
				serializeCheckpoints(nextHistory));
		handleMutationResult(result, lease, expectedRevision);
		return RunnableConfig.builder(config).checkPointId(checkpoint.getId()).build();
	}

	@Override
	public Tag releaseIfLeasedVersion(RunnableConfig config, long expectedRevision, ExecutionLease lease)
			throws Exception {
		validateExpectedRevision(expectedRevision);
		requireLeaseForNamespace(config, lease);
		long nextRevision = Math.addExact(expectedRevision, 1L);
		VersionedHistory current = readHistory(config);
		Tag releaseTag = new Tag(lease.namespace(), current.history());
		List<Object> result = evalLeased(lease, RELEASE_CHECKPOINT_SCRIPT, redisKey(lease.namespace()),
				lease.ownerId().toString(), Long.toString(lease.fencingToken()), Long.toString(expectedRevision),
				Long.toString(nextRevision), serializeCheckpoints(List.of()));
		handleMutationResult(result, lease, expectedRevision);
		return releaseTag;
	}

	private VersionedHistory readHistory(RunnableConfig config) {
		String namespace = checkpointThreadId(config);
		List<Object> result = eval(SNAPSHOT_SCRIPT, redisKey(namespace));
		String status = status(result);
		if (STATUS_CORRUPT.equals(status)) {
			throw corrupt(namespace);
		}
		if (!STATUS_OK.equals(status)) {
			throw unexpected(status, namespace);
		}
		long revision = parseLong(result.get(1), "revision");
		String content = result.size() > 2 && result.get(2) != null ? result.get(2).toString() : null;
		return new VersionedHistory(deserializeHistory(content), revision);
	}

	private void handleMutationResult(List<Object> result, ExecutionLease lease, long expectedRevision)
			throws CheckpointConflictException {
		String status = status(result);
		switch (status) {
			case STATUS_OK -> {
			}
			case STATUS_LOST -> throw lost(lease, "lease is not active");
			case STATUS_CONFLICT -> throw new CheckpointConflictException(lease.namespace(), expectedRevision,
					parseLong(result.get(1), "revision"));
			case STATUS_CORRUPT -> throw corrupt(lease.namespace());
			default -> throw unexpected(status, lease.namespace());
		}
	}

	private Optional<Checkpoint> select(LinkedList<Checkpoint> checkpoints, RunnableConfig config) {
		if (config.checkPointId().isPresent()) {
			String checkpointId = config.checkPointId().get();
			return checkpoints.stream().filter(checkpoint -> checkpoint.getId().equals(checkpointId)).findFirst();
		}
		return getLast(checkpoints, config);
	}

	private List<Object> eval(String script, String key, Object... args) {
		return redisson.getScript(StringCodec.INSTANCE)
			.eval(RScript.Mode.READ_WRITE, script, RScript.ReturnType.MULTI, List.of(key), args);
	}

	private List<Object> evalLeased(ExecutionLease lease, String script, String key, Object... args) {
		try {
			return eval(script, key, args);
		}
		catch (RuntimeException ex) {
			throw lost(lease, "redis script failed", ex);
		}
	}

	private String redisKey(String namespace) {
		return storageKeyPrefix + ":{"
				+ Base64.getUrlEncoder().withoutPadding().encodeToString(namespace.getBytes(StandardCharsets.UTF_8))
				+ "}";
	}

	private String ttlMillis() {
		return Long.toString(leaseOptions.ttl().toMillis());
	}

	private void requireLeaseForNamespace(RunnableConfig config, ExecutionLease lease) {
		requireNonNull(lease, "lease cannot be null");
		String namespace = checkpointThreadId(config);
		if (!namespace.equals(lease.namespace())) {
			throw lost(lease, "lease namespace does not match checkpoint namespace");
		}
	}

	private void validateExpectedRevision(long expectedRevision) {
		if (expectedRevision < 0) {
			throw new IllegalArgumentException("expectedRevision cannot be negative");
		}
	}

	private String status(List<Object> result) {
		if (result == null || result.isEmpty() || result.get(0) == null) {
			throw new IllegalStateException("Redis checkpoint lease script returned no status");
		}
		return result.get(0).toString();
	}

	private long parseLong(Object value, String name) {
		if (value == null) {
			throw new IllegalStateException("Redis checkpoint lease script omitted " + name);
		}
		return Long.parseLong(value.toString());
	}

	private IllegalStateException corrupt(String namespace) {
		return new IllegalStateException("Redis checkpoint lease hash is corrupt: " + namespace);
	}

	private IllegalStateException unexpected(String status, String namespace) {
		return new IllegalStateException(
				"Unexpected Redis checkpoint lease status '%s' for namespace %s".formatted(status, namespace));
	}

	private LeaseLostException lost(ExecutionLease lease, String reason) {
		return new LeaseLostException(lease.namespace(), lease.ownerId(), lease.fencingToken(), reason);
	}

	private LeaseLostException lost(ExecutionLease lease, String reason, Throwable cause) {
		return new LeaseLostException(lease.namespace(), lease.ownerId(), lease.fencingToken(), reason, cause);
	}

	private LinkedList<Checkpoint> deserializeHistory(String content) {
		if (content == null || content.isEmpty()) {
			return new LinkedList<>();
		}
		try {
			return deserializeCheckpoints(content);
		}
		catch (IOException | ClassNotFoundException ex) {
			throw new RuntimeException("Failed to deserialize Redis leased checkpoint history", ex);
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

	private record VersionedHistory(LinkedList<Checkpoint> history, long revision) {
	}

	public static class Builder {

		private RedissonClient redisson;

		private StateSerializer stateSerializer;

		private LeaseOptions leaseOptions = LeaseOptions.defaults();

		private String storageKeyPrefix = DEFAULT_STORAGE_KEY_PREFIX;

		public Builder redisson(RedissonClient redisson) {
			this.redisson = redisson;
			return this;
		}

		public Builder stateSerializer(StateSerializer stateSerializer) {
			this.stateSerializer = stateSerializer;
			return this;
		}

		public Builder leaseOptions(LeaseOptions leaseOptions) {
			this.leaseOptions = leaseOptions;
			return this;
		}

		public Builder storageKeyPrefix(String storageKeyPrefix) {
			this.storageKeyPrefix = storageKeyPrefix;
			return this;
		}

		public RedisLeasedCheckpointSaver build() {
			if (redisson == null) {
				throw new IllegalArgumentException("redisson cannot be null");
			}
			if (stateSerializer == null) {
				stateSerializer = StateGraph.DEFAULT_JACKSON_SERIALIZER;
			}
			return new RedisLeasedCheckpointSaver(redisson, stateSerializer, leaseOptions, storageKeyPrefix);
		}

	}

}
