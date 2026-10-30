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

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import org.redisson.api.RScript;
import org.redisson.api.RedissonClient;
import org.redisson.api.options.OptionalOptions;
import org.redisson.client.codec.StringCodec;
import org.redisson.connection.CRC16;

import static java.util.Objects.requireNonNull;

/**
 * Redis/Valkey implementation of {@link LeasedProgressStore}: one session lease
 * (claim/renew/release) whose owner/fence guards run state, checkpoint, journal
 * and outbox writes, so an independent run lock and checkpoint lock cannot drift
 * apart. This is a new opt-in keyspace; {@link RedisRunStore},
 * {@link RedisLeasedCheckpointSaver} and the older savers are untouched.
 * <p>
 * Each session owns one hash (reservation, retained fence, live owner/expiry)
 * plus three hashes per run (run record, journal, outbox), all sharing one cluster
 * hash tag. Every mutation is a single primary-routed Lua script that re-checks
 * reservation, owner, fence, Redis server {@code TIME} and run/checkpoint revision
 * before its first write, so a rejected commit leaves no partial checkpoint or run
 * progress. Scripts are sent without client-side retries, so a script runs at most
 * once per call; after a transport error or timeout the outcome is unknown and the
 * caller should re-read with {@link #snapshot(String, String)} before re-planning.
 * <p>
 * Lease expiry is measured only by the Redis server clock, so callers need no
 * synchronized clocks. Keys never expire; fences, progress and checkpoints are
 * retained. Asynchronous HA failover is not consensus fencing: a promoted replica
 * that missed recent writes can re-issue a fencing token and lose acknowledged
 * progress, and no RPO0 is promised. Same-slot layout and deployment support are
 * verified at startup; a cross-slot or unsupported deployment fails fast.
 */
public final class RedisLeasedProgressStore implements LeasedProgressStore {

	public static final String DEFAULT_KEY_PREFIX = "argi:progress:v1";

	private static final String KEEP = "KEEP";

	private static final String SET = "SET";

	private final RedissonClient redisson;

	private final RedisProgressKeys keys;

	private final RScript script;

	private RedisLeasedProgressStore(Builder builder) {
		this.redisson = requireNonNull(builder.redisson, "redisson cannot be null");
		requireNonNull(builder.keyPrefix, "keyPrefix cannot be null");
		if (builder.keyPrefix.isBlank()) {
			throw new IllegalArgumentException("keyPrefix must not be blank");
		}
		if (builder.keyPrefix.indexOf('{') >= 0 || builder.keyPrefix.indexOf('}') >= 0) {
			// The store owns the cluster hash tag; a brace in the prefix would change the slot.
			throw new IllegalArgumentException("keyPrefix must not contain '{' or '}'");
		}
		this.keys = new RedisProgressKeys(builder.keyPrefix);
		// Redisson resends a command whose reply is late; a resent script would run twice and
		// could double-claim or double-commit, so retries are disabled.
		this.script = redisson.getScript(OptionalOptions.defaults().codec(StringCodec.INSTANCE).retryAttempts(0));
		if (builder.verifySlotsAtStartup) {
			verifyDeployment();
		}
	}

	public static Builder builder() {
		return new Builder();
	}

	@Override
	public SessionLease claim(String sessionId, String runId, UUID ownerId, Duration ttl) throws LeaseBusyException {
		requireText(sessionId, "sessionId");
		requireText(runId, "runId");
		requireNonNull(ownerId, "ownerId cannot be null");
		long ttlMillis = ttlMillis(ttl);
		List<Object> sessionKeys = keys.keys(sessionId, runId);
		List<Object> result = eval(RedisProgressScripts.CLAIM, sessionKeys.subList(0, 2), sessionId, runId,
				ownerId.toString(), Long.toString(ttlMillis));
		switch (status(result)) {
			case RedisProgressScripts.OK:
				return new SessionLease(sessionId, runId, ownerId, parseLong(result, 1), parseLong(result, 2));
			case RedisProgressScripts.BUSY:
				throw new LeaseBusyException(sessionId);
			case RedisProgressScripts.OVERFLOW:
				throw new ArithmeticException("Session lease fence overflow: " + sessionId);
			default:
				throw unexpected(result);
		}
	}

	@Override
	public SessionLease renew(SessionLease lease, Duration ttl) throws LeaseLostException {
		requireNonNull(lease, "lease cannot be null");
		long ttlMillis = ttlMillis(ttl);
		List<Object> result = eval(RedisProgressScripts.RENEW, List.of(keys.sessionKey(lease.sessionId())),
				lease.sessionId(), lease.ownerId().toString(), Long.toString(lease.fencingToken()),
				Long.toString(ttlMillis));
		switch (status(result)) {
			case RedisProgressScripts.OK:
				return new SessionLease(lease.sessionId(), lease.runId(), lease.ownerId(), lease.fencingToken(),
						parseLong(result, 1));
			case RedisProgressScripts.LOST:
				throw lost(lease);
			default:
				throw unexpected(result);
		}
	}

	@Override
	public boolean release(SessionLease lease) {
		requireNonNull(lease, "lease cannot be null");
		List<Object> result = eval(RedisProgressScripts.RELEASE, List.of(keys.sessionKey(lease.sessionId())),
				lease.sessionId(), lease.ownerId().toString(), Long.toString(lease.fencingToken()));
		if (RedisProgressScripts.OK.equals(status(result))) {
			return "1".equals(string(result, 1));
		}
		throw unexpected(result);
	}

	@Override
	public Optional<ProgressSnapshot> snapshot(String sessionId, String runId) {
		requireText(sessionId, "sessionId");
		requireText(runId, "runId");
		List<Object> result = eval(RedisProgressScripts.READ, keys.keys(sessionId, runId), sessionId, runId);
		if (!RedisProgressScripts.OK.equals(status(result))) {
			throw unexpected(result);
		}
		if (!"1".equals(string(result, 1)) || !"1".equals(string(result, 2))) {
			return Optional.empty();
		}
		long revision = parseLong(result, 3);
		String state = optional(string(result, 4));
		String checkpoint = optional(string(result, 5));
		String cpRevision = optional(string(result, 6));
		List<Map<String, String>> sections = readSections(result, 7);
		return Optional.of(new ProgressSnapshot(sessionId, runId, state, revision, checkpoint,
				cpRevision == null ? 0 : Long.parseLong(cpRevision), sections.get(0), sections.get(1)));
	}

	@Override
	public ProgressSnapshot commit(SessionLease lease, ProgressCommit commit) throws LeaseLostException {
		requireNonNull(lease, "lease cannot be null");
		requireNonNull(commit, "commit cannot be null");
		requireNoDuplicateIds(commit.journal(), "journal");
		requireNoDuplicateIds(commit.outbox(), "outbox");
		long nextRevision = Math.addExact(commit.expectedRunRevision(), 1);
		List<Object> args = new ArrayList<>();
		args.add(lease.sessionId());
		args.add(lease.runId());
		args.add(lease.ownerId().toString());
		args.add(Long.toString(lease.fencingToken()));
		args.add(Long.toString(commit.expectedRunRevision()));
		args.add(Long.toString(nextRevision));
		args.add(commit.state());
		CheckpointWrite checkpoint = commit.checkpoint();
		if (checkpoint != null) {
			long nextCheckpointRevision = Math.addExact(checkpoint.expectedRevision(), 1);
			args.add(SET);
			args.add(Long.toString(checkpoint.expectedRevision()));
			args.add(Long.toString(nextCheckpointRevision));
			args.add(checkpoint.payload());
		}
		else {
			args.add(KEEP);
			args.add("0");
			args.add("0");
			args.add("");
		}
		addEntries(args, commit.journal());
		addEntries(args, commit.outbox());
		List<Object> result = eval(RedisProgressScripts.COMMIT, keys.keys(lease.sessionId(), lease.runId()),
				args.toArray());
		switch (status(result)) {
			case RedisProgressScripts.OK:
				// Only the live owner can write, so the follow-up read observes exactly this commit.
				return snapshot(lease.sessionId(), lease.runId())
					.orElseThrow(() -> new IllegalStateException("Committed run progress is missing"));
			case RedisProgressScripts.C_NOT_RESERVED:
				throw new ProgressConflictException("Session is not reserved by run " + lease.runId());
			case RedisProgressScripts.LOST:
				throw lost(lease);
			case RedisProgressScripts.C_REVISION:
				throw new ProgressConflictException("Run revision does not match: expected "
						+ commit.expectedRunRevision() + " but was " + string(result, 1));
			case RedisProgressScripts.C_CP_REVISION:
				throw new ProgressConflictException("Checkpoint revision does not match: stored revision is "
						+ string(result, 1));
			case RedisProgressScripts.C_ENTRY:
				throw new ProgressConflictException("Append-only entry id already exists: " + string(result, 1));
			default:
				throw unexpected(result);
		}
	}

	/**
	 * Verifies the session key layout is single-slot and that the deployment
	 * accepts a multi-key script over it. A cross-slot layout, a cluster that
	 * splits the keys, or a proxy that cannot route the script fails at startup.
	 */
	private void verifyDeployment() {
		List<Object> probeKeys = keys.keys("argi:slot-probe:session", "argi:slot-probe:run");
		requireSameSlot(probeKeys.stream().map(String.class::cast).toList());
		List<Object> result = eval(RedisProgressScripts.SLOT_PROBE, probeKeys);
		if (!RedisProgressScripts.OK.equals(status(result))) {
			throw unexpected(result);
		}
	}

	/** Cluster hash slots of all keys must be identical, or the layout is cross-slot. */
	static void requireSameSlot(List<String> slotKeys) {
		requireNonNull(slotKeys, "keys cannot be null");
		if (slotKeys.isEmpty()) {
			throw new IllegalArgumentException("keys cannot be empty");
		}
		int expected = slot(slotKeys.get(0));
		for (String key : slotKeys) {
			int slot = slot(key);
			if (slot != expected) {
				throw new IllegalStateException(
						"Cross-slot progress key layout: %s and %s are in different Redis cluster slots"
							.formatted(slotKeys.get(0), key));
			}
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

	private static void addEntries(List<Object> args, List<Entry> entries) {
		args.add(Integer.toString(entries.size()));
		for (Entry entry : entries) {
			args.add(entry.id());
			args.add(entry.payload());
		}
	}

	private static void requireNoDuplicateIds(List<Entry> entries, String section) {
		Set<String> ids = new HashSet<>();
		for (Entry entry : entries) {
			if (!ids.add(entry.id())) {
				throw new IllegalArgumentException("Duplicate " + section + " entry id in one commit: " + entry.id());
			}
		}
	}

	private List<Map<String, String>> readSections(List<Object> result, int index) {
		List<Map<String, String>> sections = new ArrayList<>(2);
		for (int i = 0; i < 2; i++) {
			int count = Integer.parseInt(string(result, index));
			Map<String, String> section = new HashMap<>();
			for (int j = 0; j < count; j += 2) {
				section.put(string(result, index + 1 + j), string(result, index + 2 + j));
			}
			sections.add(section);
			index += count + 1;
		}
		return sections;
	}

	private List<Object> eval(String source, List<Object> scriptKeys, Object... args) {
		List<Object> result = script.eval(RScript.Mode.READ_WRITE, source, RScript.ReturnType.MULTI, scriptKeys,
				args);
		if (result == null || result.isEmpty()) {
			throw new IllegalStateException("Redis progress store script returned no result");
		}
		return result;
	}

	private static String status(List<Object> result) {
		String status = String.valueOf(result.get(0));
		if (RedisProgressScripts.CORRUPT.equals(status)) {
			throw new IllegalStateException(
					"Redis progress store data is corrupt: " + (result.size() > 1 ? result.get(1) : "unknown"));
		}
		return status;
	}

	private static String string(List<Object> result, int index) {
		if (index >= result.size() || !(result.get(index) instanceof String value)) {
			throw unexpected(result);
		}
		return value;
	}

	private static long parseLong(List<Object> result, int index) {
		return Long.parseLong(string(result, index));
	}

	private static String optional(String value) {
		return value.isEmpty() ? null : value;
	}

	private static IllegalStateException unexpected(List<Object> result) {
		return new IllegalStateException("Unexpected Redis progress store script result: " + result.get(0));
	}

	private static LeaseLostException lost(SessionLease lease) {
		return new LeaseLostException(lease.sessionId(), lease.ownerId(), lease.fencingToken(),
				"session lease is absent, expired or owned by another fence");
	}

	private static void requireText(String value, String name) {
		if (value == null || value.isBlank()) {
			throw new IllegalArgumentException(name + " must not be blank");
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

	/** Builder for {@link RedisLeasedProgressStore}. */
	public static final class Builder {

		private RedissonClient redisson;

		private String keyPrefix = DEFAULT_KEY_PREFIX;

		private boolean verifySlotsAtStartup = true;

		private Builder() {
		}

		public Builder redisson(RedissonClient redisson) {
			this.redisson = redisson;
			return this;
		}

		/** Key prefix of this opt-in keyspace; must not contain {@code '{' or '}'}. */
		public Builder keyPrefix(String keyPrefix) {
			this.keyPrefix = keyPrefix;
			return this;
		}

		/**
		 * Verify at startup that the key layout is single-slot and the deployment accepts
		 * multi-key scripts over it. Enabled by default; disabling skips the fail-fast
		 * deployment check and is intended for tests only.
		 */
		public Builder verifySlotsAtStartup(boolean verifySlotsAtStartup) {
			this.verifySlotsAtStartup = verifySlotsAtStartup;
			return this;
		}

		public RedisLeasedProgressStore build() {
			return new RedisLeasedProgressStore(this);
		}

	}

}
