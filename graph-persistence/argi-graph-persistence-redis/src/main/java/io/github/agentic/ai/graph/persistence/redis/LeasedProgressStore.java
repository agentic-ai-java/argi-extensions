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

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Opt-in, engine-neutral capability contract for leased run progress: one session
 * lease (claim/renew/release) guards run state, checkpoints, journal and outbox
 * writes with a single owner/fence, replacing the split between an independent run
 * lock and an independent checkpoint lock.
 * <p>
 * Every mutation requires a live {@link SessionLease}. There is deliberately no
 * ownerless write path: an expired or superseded owner cannot heartbeat, release
 * or commit anything, and implementations must never fall back to an unguarded
 * write. A commit advances the run revision, optionally the checkpoint revision,
 * and appends journal and outbox entries atomically, or writes nothing.
 * <p>
 * The contract carries only opaque payloads and state strings; graph/checkpoint
 * serialization and run state machines belong to the caller. Lease expiry is
 * evaluated against the store's authoritative time source, not caller clocks.
 * Durable worker recovery, polling and cross-JVM takeover verification are later
 * milestones that build on this contract.
 */
public interface LeasedProgressStore {

	/**
	 * Claims the session for {@code runId} and returns a new lease with a fencing
	 * token that increases monotonically per claim. A live lease held by any owner
	 * is rejected, whether it belongs to this run or another run of the same
	 * session. Claiming after the previous lease expired or was released succeeds
	 * and fences the previous owner out of every subsequent write.
	 * @param sessionId session whose progress is guarded; must not be blank
	 * @param runId run the reservation binds to; must not be blank
	 * @param ownerId caller identity recorded as the lease owner
	 * @param ttl lease time to live, positive whole milliseconds, at most one day
	 * @return the new lease
	 * @throws LeaseBusyException if another lease is still live
	 */
	SessionLease claim(String sessionId, String runId, UUID ownerId, Duration ttl) throws LeaseBusyException;

	/**
	 * Heartbeat: extends the lease by {@code ttl} measured from the store's current
	 * time. The fencing token is unchanged.
	 * @throws LeaseLostException if the lease is absent, expired or no longer owned
	 * by the supplied owner/fence
	 */
	SessionLease renew(SessionLease lease, Duration ttl) throws LeaseLostException;

	/**
	 * Releases a live lease owned by the supplied owner/fence. Releasing an expired
	 * or superseded lease is rejected and changes nothing; the retained fence and
	 * the run's progress are kept.
	 * @return {@code true} only when the live lease was actually released
	 */
	boolean release(SessionLease lease);

	/**
	 * Reads the run's current progress without requiring a lease.
	 * @return empty when the session or run record does not exist
	 */
	Optional<ProgressSnapshot> snapshot(String sessionId, String runId);

	/**
	 * Atomically advances the run state and revision, optionally replaces the
	 * checkpoint while advancing its revision, and appends journal and outbox
	 * entries. The run revision must match {@code expectedRunRevision} and, when a
	 * checkpoint is supplied, the checkpoint revision must match its expected
	 * revision. Journal and outbox entry ids must not exist yet. Any failed check
	 * aborts the whole commit: no half checkpoint and no half run progress.
	 * @param lease live lease of the session reserved by this run
	 * @param commit validated delta to apply
	 * @return the progress snapshot after the commit
	 * @throws LeaseLostException if the lease is absent, expired or superseded
	 * @throws ProgressConflictException if the reservation, run revision,
	 * checkpoint revision or an append-only entry id conflicts
	 */
	ProgressSnapshot commit(SessionLease lease, ProgressCommit commit) throws LeaseLostException;

	/** Live session lease; the fencing token identifies this exact ownership. */
	record SessionLease(String sessionId, String runId, UUID ownerId, long fencingToken, long expiresAtMillis) {

		public SessionLease {
			requireText(sessionId, "sessionId");
			requireText(runId, "runId");
			if (ownerId == null) {
				throw new IllegalArgumentException("ownerId cannot be null");
			}
			if (fencingToken <= 0) {
				throw new IllegalArgumentException("fencingToken must be positive");
			}
			if (expiresAtMillis <= 0) {
				throw new IllegalArgumentException("expiresAtMillis must be positive");
			}
		}

	}

	/**
	 * Run progress at one point in time. {@code state} and {@code checkpoint} are
	 * {@code null} until the first commit that sets them; revisions start at zero.
	 */
	record ProgressSnapshot(String sessionId, String runId, String state, long runRevision, String checkpoint,
			long checkpointRevision, Map<String, String> journal, Map<String, String> outbox) {

		public ProgressSnapshot {
			requireText(sessionId, "sessionId");
			requireText(runId, "runId");
			if (runRevision < 0 || checkpointRevision < 0) {
				throw new IllegalArgumentException("revisions cannot be negative");
			}
			if (checkpoint != null && checkpointRevision == 0) {
				throw new IllegalArgumentException("checkpoint revision must be positive when a checkpoint exists");
			}
			journal = journal == null ? Map.of() : Map.copyOf(journal);
			outbox = outbox == null ? Map.of() : Map.copyOf(outbox);
		}

	}

	/** One commit delta; {@code checkpoint} is {@code null} to keep the current one. */
	record ProgressCommit(String state, long expectedRunRevision, CheckpointWrite checkpoint, List<Entry> journal,
			List<Entry> outbox) {

		public ProgressCommit {
			requireText(state, "state");
			if (expectedRunRevision < 0) {
				throw new IllegalArgumentException("expectedRunRevision cannot be negative");
			}
			journal = journal == null ? List.of() : List.copyOf(journal);
			outbox = outbox == null ? List.of() : List.copyOf(outbox);
			for (Entry entry : journal) {
				requireNonNullEntry(entry);
			}
			for (Entry entry : outbox) {
				requireNonNullEntry(entry);
			}
		}

		private static void requireNonNullEntry(Entry entry) {
			if (entry == null) {
				throw new IllegalArgumentException("journal and outbox entries cannot be null");
			}
		}

	}

	/** Checkpoint replacement guarded by its expected revision (zero when absent). */
	record CheckpointWrite(long expectedRevision, String payload) {

		public CheckpointWrite {
			if (expectedRevision < 0) {
				throw new IllegalArgumentException("expectedRevision cannot be negative");
			}
			if (payload == null) {
				throw new IllegalArgumentException("checkpoint payload cannot be null");
			}
		}

	}

	/** Append-only journal/outbox entry; ids must be unique per run. */
	record Entry(String id, String payload) {

		public Entry {
			requireText(id, "entry id");
			if (payload == null) {
				throw new IllegalArgumentException("entry payload cannot be null");
			}
		}

	}

	private static void requireText(String value, String name) {
		if (value == null || value.isBlank()) {
			throw new IllegalArgumentException(name + " must not be blank");
		}
	}

}
