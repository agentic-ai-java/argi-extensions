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

import io.github.agentic.ai.graph.execution.TrustedExecutionContext;
import io.github.agentic.ai.graph.execution.run.RunCommit;
import io.github.agentic.ai.graph.execution.run.RunEntry;
import io.github.agentic.ai.graph.execution.run.RunLease;
import io.github.agentic.ai.graph.execution.run.RunNamespace;
import io.github.agentic.ai.graph.execution.run.RunSnapshot;
import io.github.agentic.ai.graph.execution.run.RunSpec;
import io.github.agentic.ai.graph.execution.run.RunState;
import io.github.agentic.ai.graph.execution.run.RunStore;
import io.github.agentic.ai.graph.execution.run.RunStoreCapabilities;
import io.github.agentic.ai.graph.execution.run.TaskRecord;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.redisson.api.RScript;
import org.redisson.api.RedissonClient;
import org.redisson.api.options.OptionalOptions;
import org.redisson.client.codec.StringCodec;

import static io.github.agentic.ai.graph.persistence.redis.RedisRunCommitPlanner.conflict;
import static java.util.Objects.requireNonNull;

/**
 * Redis/Valkey implementation of {@link RunStore}.
 * <p>
 * Each namespace owns one hash (session reservation, fence, lease and submission
 * dedup) plus four hashes per run (run record, tasks, journal, outbox), all sharing one
 * cluster hash tag. Every mutation is a single primary-routed Lua script that re-checks
 * reservation, owner/fence, lease expiry and revision before writing, so a failed
 * operation leaves no partial state. Scripts are sent without client-side retries, so a
 * script runs at most once per call; a transport error or timeout is rethrown unchanged
 * and leaves the outcome unknown, exactly like any other remote store.
 * <p>
 * The injected {@link Clock} is the only time source and must not report instants before
 * the Unix epoch; lease expiry keeps the clock's full nanosecond precision. Keys never
 * expire, and every commit reads the whole run (record, tasks, journal and outbox), so
 * commit cost grows with the run's size; there is no size cap. Persistence and fencing
 * guarantees depend on the Redis deployment, so {@link RunStoreCapabilities#persistent()}
 * is only advertised when {@link Builder#durableDeployment(boolean)} is enabled.
 */
public final class RedisRunStore implements RunStore {

	public static final String DEFAULT_KEY_PREFIX = "argi:run:v1";

	private static final String KEEP = "KEEP";

	private static final String SET = "SET";

	private final RedissonClient redisson;

	private final Clock clock;

	private final RedisRunKeys keys;

	private final RunStoreCapabilities capabilities;

	private final RedisRunRecordCodec codec = new RedisRunRecordCodec();

	private final RScript script;

	private RedisRunStore(Builder builder) {
		this.redisson = requireNonNull(builder.redisson, "redisson cannot be null");
		this.clock = requireNonNull(builder.clock, "clock cannot be null");
		requireNonNull(builder.keyPrefix, "keyPrefix cannot be null");
		if (builder.keyPrefix.isBlank()) {
			throw new IllegalArgumentException("keyPrefix must not be blank");
		}
		if (builder.keyPrefix.indexOf('{') >= 0 || builder.keyPrefix.indexOf('}') >= 0) {
			// The store owns the cluster hash tag; a brace in the prefix would change the slot.
			throw new IllegalArgumentException("keyPrefix must not contain '{' or '}'");
		}
		this.keys = new RedisRunKeys(builder.keyPrefix);
		this.capabilities = new RunStoreCapabilities(builder.durableDeployment, true, true, true, true, true);
		// Redisson resends a command whose reply is late; a resent script would run twice and
		// report its own first run as a conflict, so retries are disabled.
		this.script = redisson.getScript(OptionalOptions.defaults().codec(StringCodec.INSTANCE).retryAttempts(0));
	}

	public static Builder builder() {
		return new Builder();
	}

	@Override
	public RunStoreCapabilities capabilities() {
		return capabilities;
	}

	@Override
	public RunSnapshot submit(RunSpec spec, TrustedExecutionContext context) {
		requireNonNull(spec, "spec cannot be null");
		requireNonNull(context, "trusted context cannot be null");
		if (!spec.context().equals(context.toReference())) {
			throw new IllegalArgumentException("submission must match the authenticated context");
		}
		RunNamespace namespace = spec.namespace();
		List<Object> runKeys = keys.runKeys(namespace, spec.runId());
		Instant now = clock.instant();
		Long deadline = spec.deadlineEpochMillis();
		boolean expired = deadline != null && !now.isBefore(Instant.ofEpochMilli(deadline));
		List<Object> result = eval(RedisRunScripts.SUBMIT, List.of(runKeys.get(0), runKeys.get(1)),
				RedisRunKeys.tuple(namespace), spec.submissionKey(), RedisRunKeys.fingerprint(spec), spec.runId(),
				expired ? "1" : "0", codec.encodeSpec(spec));
		String status = status(result);
		switch (status) {
			case RedisRunScripts.OK:
				return new RunSnapshot(spec, 0, RunState.PENDING, null, Map.of(), Map.of(), Map.of());
			case RedisRunScripts.DEDUP:
				String existingId = string(result, 1);
				// Dedup mappings are never rewritten or removed, so the follow-up read is
				// the latest snapshot of the original run, as with the memory store.
				RunSnapshot existing = read(namespace, existingId).snapshot().orElse(null);
				if (existing == null || !existing.spec().submissionKey().equals(spec.submissionKey())
						|| !RedisRunKeys.fingerprint(existing.spec()).equals(RedisRunKeys.fingerprint(spec))) {
					throw RedisRunRecordCodec.corrupt((String) runKeys.get(0), "s:" + spec.submissionKey(), null);
				}
				return existing;
			case RedisRunScripts.C_DEDUP:
				throw conflict("Submission key already identifies different input, graph or principal");
			case RedisRunScripts.C_RUNID:
				throw conflict("Run id already exists under a different submission key");
			case RedisRunScripts.C_RESERVED:
				throw conflict("Session is reserved by a nonterminal run");
			case RedisRunScripts.E_EXPIRED:
				throw new IllegalArgumentException("Cannot admit an expired run");
			default:
				throw unexpected(result);
		}
	}

	@Override
	public Optional<RunSnapshot> find(RunNamespace namespace, String runId) {
		requireNonNull(namespace, "namespace cannot be null");
		requireText(runId, "runId");
		return read(namespace, runId).snapshot();
	}

	@Override
	public Optional<String> activeRun(RunNamespace namespace) {
		requireNonNull(namespace, "namespace cannot be null");
		List<Object> result = eval(RedisRunScripts.ACTIVE, List.of(keys.namespaceKey(namespace)),
				RedisRunKeys.tuple(namespace));
		String status = status(result);
		if (RedisRunScripts.NONE.equals(status)) {
			return Optional.empty();
		}
		if (RedisRunScripts.OK.equals(status)) {
			return Optional.of(string(result, 1));
		}
		throw unexpected(result);
	}

	@Override
	public RunLease acquire(RunNamespace namespace, String runId, UUID ownerId, Duration ttl) {
		requireNonNull(ownerId, "ownerId cannot be null");
		long ttlMillis = ttlMillis(ttl);
		requireNonNull(namespace, "namespace cannot be null");
		requireText(runId, "runId");
		Instant now = clock.instant();
		Instant expiresAt = now.plusMillis(ttlMillis);
		List<Object> result = eval(RedisRunScripts.ACQUIRE, List.of(keys.namespaceKey(namespace)),
				RedisRunKeys.tuple(namespace), runId, ownerId.toString(), Long.toString(epochNanos(now)),
				Long.toString(epochNanos(expiresAt)));
		switch (status(result)) {
			case RedisRunScripts.OK:
				long fence = RedisRunRecordCodec.parseRevision(keys.namespaceKey(namespace), "fence", string(result, 1));
				return new RunLease(namespace, ownerId, fence, expiresAt);
			case RedisRunScripts.C_NOT_RESERVED:
				throw conflict("Run does not own the session reservation");
			case RedisRunScripts.C_LIVE:
				throw conflict("Namespace already has a live owner");
			case RedisRunScripts.OVERFLOW:
				throw new ArithmeticException("long overflow");
			default:
				throw unexpected(result);
		}
	}

	@Override
	public RunLease renew(RunLease lease, Duration ttl) {
		requireNonNull(lease, "lease cannot be null");
		long ttlMillis = ttlMillis(ttl);
		RunNamespace namespace = lease.namespace();
		Instant now = clock.instant();
		Instant expiresAt = now.plusMillis(ttlMillis);
		List<Object> result = eval(RedisRunScripts.RENEW, List.of(keys.namespaceKey(namespace)),
				RedisRunKeys.tuple(namespace), lease.ownerId().toString(), Long.toString(lease.fencingToken()),
				Long.toString(epochNanos(now)), Long.toString(epochNanos(expiresAt)));
		switch (status(result)) {
			case RedisRunScripts.OK:
				return new RunLease(namespace, lease.ownerId(), lease.fencingToken(), expiresAt);
			case RedisRunScripts.C_LEASE:
				throw conflict("Namespace lease is absent, expired or owned by another fence");
			default:
				throw unexpected(result);
		}
	}

	@Override
	public boolean release(RunLease lease) {
		requireNonNull(lease, "lease cannot be null");
		RunNamespace namespace = lease.namespace();
		List<Object> result = eval(RedisRunScripts.RELEASE, List.of(keys.namespaceKey(namespace)),
				RedisRunKeys.tuple(namespace), lease.ownerId().toString(), Long.toString(lease.fencingToken()));
		if (RedisRunScripts.OK.equals(status(result))) {
			return "1".equals(string(result, 1));
		}
		throw unexpected(result);
	}

	@Override
	public RunSnapshot commit(RunNamespace namespace, String runId, RunLease lease, RunCommit commit) {
		requireNonNull(commit, "commit cannot be null");
		requireNonNull(namespace, "namespace cannot be null");
		requireText(runId, "runId");
		Instant now = clock.instant();
		long nowNanos = epochNanos(now);
		ReadResult current = read(namespace, runId);
		if (current.reservation() != null && current.reservation().equals(runId) && current.snapshot().isEmpty()) {
			throw RedisRunRecordCodec.corrupt((String) current.keys().get(1), RedisRunRecordCodec.SPEC, null);
		}
		RedisRunCommitPlanner.Plan plan = RedisRunCommitPlanner.plan(namespace, runId, current.reservation(),
				current.lease(), lease, current.snapshot().orElse(null), commit, now, nowNanos);
		RunSnapshot result = plan.result();
		List<Object> args = new ArrayList<>();
		args.add(RedisRunKeys.tuple(namespace));
		args.add(runId);
		args.add(lease.ownerId().toString());
		args.add(Long.toString(lease.fencingToken()));
		args.add(Long.toString(nowNanos));
		args.add(Long.toString(commit.expectedRevision()));
		args.add(Long.toString(result.revision()));
		args.add(result.state().name());
		args.add(plan.checkpointChanged() ? SET : KEEP);
		args.add(plan.checkpointChanged() ? codec.encodePayload(result.checkpoint()) : "");
		args.add(result.state().terminal() ? "1" : "0");
		args.add(Integer.toString(plan.tasks().size()));
		for (Map.Entry<String, TaskRecord> task : plan.tasks().entrySet()) {
			args.add(task.getKey());
			args.add(codec.encodeTask(task.getValue()));
		}
		addEntries(args, plan.journal());
		addEntries(args, plan.outbox());
		List<Object> written = eval(RedisRunScripts.COMMIT, current.keys(), args.toArray());
		switch (status(written)) {
			case RedisRunScripts.OK:
				return result;
			case RedisRunScripts.C_NOT_RESERVED:
				throw conflict("Run does not own the session reservation");
			case RedisRunScripts.C_LEASE:
				throw conflict("Namespace lease is absent, expired or owned by another fence");
			case RedisRunScripts.C_REVISION:
				throw conflict("Run revision does not match");
			default:
				throw unexpected(written);
		}
	}

	private void addEntries(List<Object> args, Map<String, RunEntry> entries) {
		args.add(Integer.toString(entries.size()));
		for (Map.Entry<String, RunEntry> entry : entries.entrySet()) {
			args.add(entry.getKey());
			args.add(codec.encodeEntry(entry.getValue()));
		}
	}

	private ReadResult read(RunNamespace namespace, String runId) {
		List<Object> runKeys = keys.runKeys(namespace, runId);
		List<Object> result = eval(RedisRunScripts.READ, runKeys, RedisRunKeys.tuple(namespace));
		if (!RedisRunScripts.OK.equals(status(result))) {
			throw unexpected(result);
		}
		String nsKey = (String) runKeys.get(0);
		boolean namespaceExists = "1".equals(string(result, 1));
		String reservation = optional(string(result, 2));
		RedisRunCommitPlanner.StoredLease lease = null;
		String owner = optional(string(result, 3));
		if (owner != null) {
			lease = new RedisRunCommitPlanner.StoredLease(owner,
					RedisRunRecordCodec.parseRevision(nsKey, "lease_fence", string(result, 4)),
					RedisRunRecordCodec.parseRevision(nsKey, "lease_expires", string(result, 5)));
		}
		List<Map<String, String>> hashes = new ArrayList<>(4);
		int index = 6;
		for (int i = 0; i < 4; i++) {
			int count = Integer.parseInt(string(result, index));
			Map<String, String> hash = new HashMap<>();
			for (int j = 0; j < count; j += 2) {
				hash.put(string(result, index + 1 + j), string(result, index + 2 + j));
			}
			hashes.add(hash);
			index += count + 1;
		}
		Optional<RunSnapshot> snapshot;
		if (hashes.get(0).isEmpty()) {
			for (int i = 1; i < 4; i++) {
				if (!hashes.get(i).isEmpty()) {
					throw RedisRunRecordCodec.corrupt((String) runKeys.get(i + 1), "*", null);
				}
			}
			snapshot = Optional.empty();
		}
		else {
			if (!namespaceExists) {
				throw RedisRunRecordCodec.corrupt(nsKey, "ns", null);
			}
			snapshot = Optional.of(codec.decode(namespace, runId, runKeys, hashes.get(0), hashes.get(1),
					hashes.get(2), hashes.get(3)));
		}
		return new ReadResult(runKeys, reservation, lease, snapshot);
	}

	private List<Object> eval(String source, List<Object> scriptKeys, Object... args) {
		List<Object> result = script.eval(RScript.Mode.READ_WRITE, source, RScript.ReturnType.MULTI, scriptKeys,
				args);
		if (result == null || result.isEmpty()) {
			throw new IllegalStateException("Redis run store script returned no result");
		}
		return result;
	}

	private static String status(List<Object> result) {
		String status = String.valueOf(result.get(0));
		if (RedisRunScripts.CORRUPT.equals(status)) {
			throw new IllegalStateException(
					"Redis run store data is corrupt: " + (result.size() > 1 ? result.get(1) : "unknown"));
		}
		return status;
	}

	private static String string(List<Object> result, int index) {
		if (index >= result.size() || !(result.get(index) instanceof String value)) {
			throw unexpected(result);
		}
		return value;
	}

	private static String optional(String value) {
		return value.isEmpty() ? null : value;
	}

	private static IllegalStateException unexpected(List<Object> result) {
		return new IllegalStateException("Unexpected Redis run store script result: " + result.get(0));
	}

	private static void requireText(String value, String name) {
		if (value == null || value.isBlank()) {
			throw new IllegalArgumentException(name + " must not be blank");
		}
	}

	/**
	 * Exact epoch nanoseconds, compared as decimal strings in Lua. Instants before the
	 * epoch would be stored with a sign the scripts reject, so they are refused up front.
	 */
	static long epochNanos(Instant instant) {
		if (instant.getEpochSecond() < 0) {
			throw new IllegalStateException("Clock instants before 1970-01-01T00:00:00Z are not supported: " + instant);
		}
		try {
			return Math.addExact(Math.multiplyExact(instant.getEpochSecond(), 1_000_000_000L), instant.getNano());
		}
		catch (ArithmeticException ex) {
			throw new IllegalStateException("Clock instants after 2262 are not supported: " + instant, ex);
		}
	}

	// Mirrors the package-private core RunValidation.ttlMillis rule and message.
	private static long ttlMillis(Duration ttl) {
		requireNonNull(ttl, "ttl cannot be null");
		if (ttl.isNegative() || ttl.isZero() || ttl.compareTo(Duration.ofDays(1)) > 0
				|| ttl.getNano() % 1_000_000 != 0) {
			throw new IllegalArgumentException("ttl must be positive whole milliseconds and at most one day");
		}
		return ttl.toMillis();
	}

	private record ReadResult(List<Object> keys, String reservation, RedisRunCommitPlanner.StoredLease lease,
			Optional<RunSnapshot> snapshot) {
	}

	/** Builder for {@link RedisRunStore}. */
	public static final class Builder {

		private RedissonClient redisson;

		private Clock clock = Clock.systemUTC();

		private String keyPrefix = DEFAULT_KEY_PREFIX;

		private boolean durableDeployment;

		private Builder() {
		}

		public Builder redisson(RedissonClient redisson) {
			this.redisson = redisson;
			return this;
		}

		/**
		 * The only time source for deadlines and lease expiry; Redis time is never used. All
		 * JVMs sharing a namespace need synchronized clocks (skew far below the shortest lease
		 * TTL). Instants before 1970 or after 2262 are rejected with
		 * {@link IllegalStateException} by lease operations and commits.
		 */
		public Builder clock(Clock clock) {
			this.clock = clock;
			return this;
		}

		public Builder keyPrefix(String keyPrefix) {
			this.keyPrefix = keyPrefix;
			return this;
		}

		/**
		 * Advertise {@code persistent=true} only when the Redis deployment satisfies the
		 * durability requirements in the module README: {@code maxmemory-policy noeviction},
		 * AOF with {@code appendfsync always}, and no asynchronous failover, backup restore
		 * or other rollback of acknowledged writes. A replica promoted before it received
		 * the latest writes can re-issue a fencing token and lose commits, which this store
		 * cannot detect.
		 */
		public Builder durableDeployment(boolean durableDeployment) {
			this.durableDeployment = durableDeployment;
			return this;
		}

		public RedisRunStore build() {
			return new RedisRunStore(this);
		}

	}

}
