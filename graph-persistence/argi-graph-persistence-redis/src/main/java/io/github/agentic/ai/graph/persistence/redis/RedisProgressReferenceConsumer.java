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

import java.time.Duration;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Predicate;

import io.github.agentic.ai.graph.checkpoint.lease.LeaseBusyException;
import io.github.agentic.ai.graph.checkpoint.lease.LeaseLostException;

/**
 * Minimal reference consumption contract, not Core/Starter Worker wiring. The application
 * owns the state adapter and executor. Claim, re-read revision/state, then guard lease
 * immediately before invoking execution. An external effect is not atomic with that
 * guard: expiry after the guard still requires external fencing/idempotency and
 * reconciliation.
 */
public final class RedisProgressReferenceConsumer {

	private final RedisLeasedProgressStore store;

	private final RedisProgressIndexRepairer projection;

	private final RedisProgressAdvisoryIndex hints;

	private final Predicate<RedisProgressIndexRepairer.Metadata> runnable;

	private final Duration ttl;

	private final long backoffMillis;

	public RedisProgressReferenceConsumer(RedisLeasedProgressStore store, RedisProgressIndexRepairer projection,
			RedisProgressAdvisoryIndex hints, Predicate<RedisProgressIndexRepairer.Metadata> runnable, Duration ttl,
			long backoffMillis) {
		this.store = Objects.requireNonNull(store);
		this.projection = Objects.requireNonNull(projection);
		this.hints = Objects.requireNonNull(hints);
		if (!store.keyPrefix().equals(projection.prefix()) || store.client() != projection.client()) {
			throw new IllegalArgumentException("Consumer store and projection must share client and prefix");
		}
		this.runnable = Objects.requireNonNull(runnable);
		if (ttl == null || ttl.isZero() || ttl.isNegative() || ttl.compareTo(Duration.ofDays(1)) > 0
				|| ttl.getNano() % 1000000 != 0 || backoffMillis < 1 || backoffMillis > 86400000) {
			throw new IllegalArgumentException("Invalid lease TTL or backoff");
		}
		this.ttl = ttl;
		this.backoffMillis = backoffMillis;
	}

	/**
	 * The owner ID must be unique per attempt. Any unexpected exception propagates after
	 * retry scheduling and lease release. An unknown commit outcome must be reconciled by
	 * re-reading authority on redelivery; a timeout does not roll back external effects.
	 */
	public Result consume(RedisProgressAdvisoryIndex.Delivery delivery, UUID owner, Executor executor)
			throws Exception {
		var candidate = delivery.candidate();
		// Do not bootstrap missing authority merely because an advisory hint exists.
		try {
			if (projection.metadata(candidate.sessionId(), candidate.runId()).isEmpty()) {
				hints.ack(delivery);
				return new Result("MISSING_AUTHORITY", null);
			}
		}
		catch (RedisProgressIndexRepairer.MetadataException ex) {
			hints.retry(delivery, backoffMillis);
			return new Result("METADATA_ERROR", ex.getMessage());
		}
		LeasedProgressStore.SessionLease lease;
		try {
			lease = store.claim(candidate.sessionId(), candidate.runId(), owner, ttl);
		}
		catch (LeaseBusyException ex) {
			hints.retry(delivery, backoffMillis);
			return new Result("BUSY", null);
		}
		boolean settled = false;
		try {
			var metadata = projection.metadata(lease);
			if (metadata.isEmpty() || metadata.get().revision() != candidate.revision()
					|| !runnable.test(metadata.get())) {
				hints.ack(delivery);
				settled = true;
				return new Result("STALE_OR_INELIGIBLE", null);
			}
			Execution execution = new Execution(lease, metadata.get());
			execution.guard();
			LeasedProgressStore.ProgressCommit commit = Objects.requireNonNull(executor.execute(execution));
			if (commit.expectedRunRevision() != candidate.revision()) {
				throw new IllegalArgumentException("Executor commit must match the validated revision");
			}
			store.commit(execution.lease, commit);
			hints.ack(delivery);
			settled = true;
			return new Result("COMMITTED", null);
		}
		catch (LeaseLostException ex) {
			return new Result("LEASE_LOST", null);
		}
		catch (RedisProgressIndexRepairer.MetadataException ex) {
			return new Result("METADATA_ERROR", ex.getMessage());
		}
		finally {
			try {
				if (!settled) {
					hints.retry(delivery, backoffMillis);
				}
			}
			finally {
				store.release(lease);
			}
		}
	}

	/**
	 * Per-attempt context. Call guard before each effect; it cannot fence that effect
	 * itself.
	 */
	public final class Execution {

		private LeasedProgressStore.SessionLease lease;

		private final RedisProgressIndexRepairer.Metadata metadata;

		private Execution(LeasedProgressStore.SessionLease lease, RedisProgressIndexRepairer.Metadata metadata) {
			this.lease = lease;
			this.metadata = metadata;
		}

		public RedisProgressIndexRepairer.Metadata metadata() {
			return metadata;
		}

		public LeasedProgressStore.SessionLease lease() {
			return lease;
		}

		public void guard() {
			lease = store.renew(lease, ttl);
			var current = projection.metadata(lease);
			if (current.isEmpty() || !metadata.equals(current.get())) {
				throw new ProgressConflictException("Authority changed after claim");
			}
		}

	}

	@FunctionalInterface
	public interface Executor {

		LeasedProgressStore.ProgressCommit execute(Execution execution) throws Exception;

	}

	public record Result(String outcome, String error) {
	}

}
