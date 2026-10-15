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

import io.github.agentic.ai.graph.execution.run.RunCommit;
import io.github.agentic.ai.graph.execution.run.RunEntry;
import io.github.agentic.ai.graph.execution.run.RunLease;
import io.github.agentic.ai.graph.execution.run.RunNamespace;
import io.github.agentic.ai.graph.execution.run.RunSnapshot;
import io.github.agentic.ai.graph.execution.run.RunState;
import io.github.agentic.ai.graph.execution.run.RunStoreConflictException;
import io.github.agentic.ai.graph.execution.run.TaskRecord;
import io.github.agentic.ai.graph.execution.run.TaskSpec;

import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Pure commit planner that mirrors the check order of {@code MemoryRunStore.commit}
 * line by line. It never touches Redis; the commit script re-checks the reservation,
 * lease and revision guards atomically before applying the planned delta.
 */
final class RedisRunCommitPlanner {

	private RedisRunCommitPlanner() {
	}

	/** Stored namespace lease; the planner receives {@code null} when none is stored. */
	record StoredLease(String owner, long fence, long expiresNanos) {
	}

	/** Planned delta and the resulting snapshot. */
	record Plan(RunSnapshot result, boolean checkpointChanged, Map<String, TaskRecord> tasks,
			Map<String, RunEntry> journal, Map<String, RunEntry> outbox) {
	}

	static Plan plan(RunNamespace namespace, String runId, String reservation, StoredLease stored, RunLease lease,
			RunSnapshot current, RunCommit commit, Instant now, long nowNanos) {
		if (current == null || !runId.equals(reservation)) {
			throw conflict("Run does not own the session reservation");
		}
		requireNonNull(lease, "lease cannot be null");
		if (!namespace.equals(lease.namespace()) || stored == null || !matches(stored, lease)
				|| nowNanos >= stored.expiresNanos()) {
			throw conflict("Namespace lease is absent, expired or owned by another fence");
		}
		if (current.revision() != commit.expectedRevision()) {
			throw conflict("Run revision does not match");
		}
		current.state().requireTransitionTo(commit.state());
		Long deadline = current.spec().deadlineEpochMillis();
		if (deadline != null && !now.isBefore(Instant.ofEpochMilli(deadline)) && commit.state() != RunState.TIMED_OUT
				&& commit.state() != RunState.CANCELLED) {
			throw conflict("Expired run can only time out or be cancelled");
		}
		Map<String, TaskRecord> tasks = new HashMap<>(current.tasks());
		Map<String, TaskRecord> changedTasks = new LinkedHashMap<>();
		Set<String> updatedTasks = new HashSet<>();
		for (RunCommit.TaskUpdate update : commit.tasks()) {
			TaskSpec task = update.spec();
			task.validateFor(current.spec());
			Long taskDeadline = task.deadlineEpochMillis();
			if (taskDeadline != null && !now.isBefore(Instant.ofEpochMilli(taskDeadline))
					&& update.state() != RunState.TIMED_OUT && update.state() != RunState.CANCELLED) {
				throw conflict("Expired task can only time out or be cancelled");
			}
			if (!updatedTasks.add(task.taskId())) {
				throw new IllegalArgumentException("Duplicate task update in one commit");
			}
			TaskRecord previous = tasks.get(task.taskId());
			long nextRevision;
			if (previous == null) {
				if (update.expectedRevision() != -1 || update.state() != RunState.PENDING) {
					throw conflict("New task must start PENDING with expectedRevision=-1");
				}
				nextRevision = 0;
			}
			else {
				if (previous.revision() != update.expectedRevision() || !previous.spec().equals(task)) {
					throw conflict("Task revision or immutable spec does not match");
				}
				previous.state().requireTransitionTo(update.state());
				nextRevision = Math.addExact(previous.revision(), 1);
			}
			TaskRecord next = new TaskRecord(task, nextRevision, update.state());
			tasks.put(task.taskId(), next);
			changedTasks.put(task.taskId(), next);
		}
		if (commit.state().terminal() && tasks.values().stream().anyMatch(task -> !task.state().terminal())) {
			throw conflict("Terminal run cannot retain active tasks");
		}
		Map<String, RunEntry> addedJournal = new LinkedHashMap<>();
		Map<String, RunEntry> journal = append(current.journal(), commit.journal(), false, addedJournal);
		Map<String, RunEntry> addedOutbox = new LinkedHashMap<>();
		Map<String, RunEntry> outbox = append(current.outbox(), commit.outbox(), true, addedOutbox);
		RunSnapshot result = new RunSnapshot(current.spec(), Math.addExact(current.revision(), 1), commit.state(),
				commit.checkpoint() != null ? commit.checkpoint() : current.checkpoint(), tasks, journal, outbox);
		return new Plan(result, commit.checkpoint() != null, changedTasks, addedJournal, addedOutbox);
	}

	private static boolean matches(StoredLease stored, RunLease supplied) {
		return stored.owner().equals(supplied.ownerId().toString()) && stored.fence() == supplied.fencingToken();
	}

	private static Map<String, RunEntry> append(Map<String, RunEntry> previous, List<RunEntry> entries,
			boolean outbox, Map<String, RunEntry> added) {
		Map<String, RunEntry> result = new HashMap<>(previous);
		for (RunEntry entry : entries) {
			if (outbox && entry.kind() != RunEntry.Kind.EVENT) {
				throw new IllegalArgumentException("Outbox only accepts EVENT entries");
			}
			RunEntry existing = result.putIfAbsent(entry.id(), entry);
			if (existing != null && !existing.equals(entry)) {
				throw conflict("Append-only entry id identifies different data");
			}
			if (existing == null) {
				added.put(entry.id(), entry);
			}
		}
		return result;
	}

	static RunStoreConflictException conflict(String message) {
		return new RunStoreConflictException(message);
	}

}
