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

import io.github.agentic.ai.graph.checkpoint.lease.LeaseLostException;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Pattern;

import org.redisson.api.RScript;
import org.redisson.api.RedissonClient;
import org.redisson.api.options.OptionalOptions;
import org.redisson.client.codec.StringCodec;

/**
 * Explicitly constructed, single-primary discovery of authoritative progress metadata.
 * Publishes advisory candidates only: it never claims leases or changes progress. A
 * consumer must claim and then re-check authoritative state/revision before execution. No
 * scheduler, terminal-state policy, outbox payload interpretation or exactly-once
 * execution is supplied here.
 */
public final class RedisProgressIndexRepairer {

	private static final String SCAN = "return redis.call('SCAN', ARGV[1], 'MATCH', ARGV[2], 'COUNT', ARGV[3])";

	// HSTRLEN checks run before HGET, so oversized opaque fields never cross the wire.
	// No snapshot, checkpoint, journal entries or outbox payloads are read.
	private static final String PROJECT = """
			for _, key in ipairs(KEYS) do
			  local t = redis.call('TYPE', key).ok
			  if t == 'none' then return {} end
			  if t ~= 'hash' then return {'ERROR', 'Invalid progress metadata type'} end
			end
			local fields = {{1, 'v', 1}, {1, 'session', 1024}, {2, 'v', 1},
			  {2, 'run', 1024}, {2, 'revision', 19}, {2, 'state', 4096}}
			for _, f in ipairs(fields) do
			  if redis.call('HSTRLEN', KEYS[f[1]], f[2]) > f[3] then
			    return {'ERROR', 'Progress metadata exceeds projection limit'}
			  end
			end
			local result = {}
			for _, f in ipairs(fields) do
			  table.insert(result, redis.call('HGET', KEYS[f[1]], f[2]) or '')
			end
			local has_state = redis.call('HEXISTS', KEYS[2], 'state') == 1
			if (result[5] == '0' and has_state) or (result[5] ~= '0' and result[6] == '') then
			  return {'ERROR', 'Invalid progress metadata revision/state'}
			end
			return result
			""";

	private static final String LEASE_PROJECT = "local function project()\n" + PROJECT + "\nend\n" + """
			local result = project()
			if #result == 0 or result[1] == 'ERROR' then return result end
			local fields = {{'owner', 36}, {'fence', 19}, {'expires', 19}, {'reservation', 1024}}
			for _, f in ipairs(fields) do
			  if redis.call('HSTRLEN', KEYS[1], f[1]) > f[2] then
			    return {'ERROR', 'Progress lease exceeds projection limit'}
			  end
			end
			local lease = redis.call('HMGET', KEYS[1], 'owner', 'fence', 'expires', 'reservation')
			if lease[1] ~= ARGV[1] or lease[2] ~= ARGV[2] or lease[4] ~= ARGV[3] then return {'LOST'} end
			local expires = lease[3]
			if not expires or (expires ~= '0' and not string.match(expires, '^[1-9][0-9]*$'))
			  or #expires > 19 or (#expires == 19 and expires > '9223372036854775807') then
			  return {'ERROR', 'Invalid progress lease expiry'}
			end
			local t = redis.call('TIME')
			local now = tostring(tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000))
			if #now > #expires or (#now == #expires and now >= expires) then return {'LOST'} end
			return result
			""";

	private final RScript script;

	private final RedissonClient client;

	private final String prefix;

	private final Pattern runPattern;

	private final RedisProgressKeys keys;

	private final Function<Metadata, Optional<RedisProgressAdvisoryIndex.Hint>> adapter;

	private final RedisProgressAdvisoryIndex sink;

	/**
	 * The adapter owns the opaque state policy and must be deterministic for one
	 * revision. Return empty for states that should not produce a hint. Neither absence
	 * of a known terminal string nor presence of outbox data implies runnable. Prefix
	 * must be the same as the authoritative store; the sink is a separate key.
	 */
	public RedisProgressIndexRepairer(RedissonClient client, String prefix,
			Function<Metadata, Optional<RedisProgressAdvisoryIndex.Hint>> adapter, RedisProgressAdvisoryIndex sink) {
		Objects.requireNonNull(client, "client");
		if (!client.getConfig().isSingleConfig()) {
			throw new IllegalArgumentException("Repair discovery requires an explicit single-server client");
		}
		if (prefix == null || prefix.isBlank() || prefix.contains("{") || prefix.contains("}")
				|| prefix.length() > 256) {
			throw new IllegalArgumentException(
					"Progress prefix must be nonblank, at most 256 chars and contain no braces");
		}
		this.client = client;
		this.prefix = prefix;
		this.runPattern = Pattern.compile(Pattern.quote(prefix) + ":\\{[A-Za-z0-9_-]{43}\\}:run:[A-Za-z0-9_-]{43}");
		this.keys = new RedisProgressKeys(prefix);
		this.adapter = Objects.requireNonNull(adapter, "adapter");
		this.sink = Objects.requireNonNull(sink, "sink");
		this.script = client.getScript(OptionalOptions.defaults().codec(StringCodec.INSTANCE).retryAttempts(0));
	}

	/** Start a new sweep; restart from here if a call fails or its outcome is unknown. */
	public Cursor start() {
		return new Cursor(prefix, "0", List.of(), 0, false);
	}

	/**
	 * Inspects at most maxKeys returned keys and issues at most one SCAN per call.
	 * scanCount is a Redis work hint, NOT a hard response-size/CPU limit. A whole SCAN
	 * reply is retained in the continuation; even COUNT=1 can exceed one key. Thus key
	 * processing and metadata replies are bounded, but SCAN network bytes and cursor
	 * memory are soft bounds. Quiescent full sweeps discover retained records; concurrent
	 * mutations require repeated sweeps (normal SCAN semantics).
	 */
	public Batch repair(Cursor cursor, int maxKeys, int scanCount) {
		Objects.requireNonNull(cursor, "cursor");
		if (!prefix.equals(cursor.prefix) || maxKeys < 1 || maxKeys > 1000 || scanCount < 1 || scanCount > 10000) {
			throw new IllegalArgumentException("Invalid cursor or batch limits");
		}
		if (cursor.done()) {
			return new Batch(cursor, 0, 0);
		}
		List<String> pending = cursor.pending;
		int offset = cursor.offset;
		String next = cursor.redisCursor;
		if (cursor.pendingKeys() == 0) {
			List<?> reply = script.eval(RScript.Mode.READ_WRITE, SCAN, RScript.ReturnType.MULTI, List.of(), next,
					globLiteral(prefix) + ":{*}:run:*", Integer.toString(scanCount));
			next = (String) reply.get(0);
			pending = ((List<?>) reply.get(1)).stream().map(String.class::cast).toList();
			offset = 0;
		}
		int inspected = Math.min(maxKeys, pending.size() - offset);
		int published = 0;
		for (String key : pending.subList(offset, offset + inspected)) {
			if (runPattern.matcher(key).matches()) {
				Optional<Metadata> metadata = project(key);
				if (metadata.isPresent()) {
					Optional<RedisProgressAdvisoryIndex.Hint> hint = Objects
						.requireNonNull(adapter.apply(metadata.get()));
					if (hint.isPresent()) {
						if (!hint.get().metadata().equals(metadata.get())) {
							throw new IllegalArgumentException("Adapter must preserve projected authority metadata");
						}
						if (sink.publish(hint.get())) {
							published++;
						}
					}
				}
			}
		}
		return new Batch(new Cursor(prefix, next, pending, offset + inspected, true), inspected, published);
	}

	/**
	 * Read bounded current metadata for a known ID, for candidate validation. No lease is
	 * acquired.
	 */
	public Optional<Metadata> metadata(String sessionId, String runId) {
		Objects.requireNonNull(sessionId, "sessionId");
		Objects.requireNonNull(runId, "runId");
		return project((String) keys.keys(sessionId, runId).get(1));
	}

	/**
	 * Atomically validates current reservation, owner/fence and Redis TIME with metadata.
	 */
	public Optional<Metadata> metadata(LeasedProgressStore.SessionLease lease) {
		Objects.requireNonNull(lease, "lease");
		String runKey = (String) keys.keys(lease.sessionId(), lease.runId()).get(1);
		List<String> result = script.eval(RScript.Mode.READ_WRITE, LEASE_PROJECT, RScript.ReturnType.MULTI,
				List.of(keys.sessionKey(lease.sessionId()), runKey), lease.ownerId().toString(),
				Long.toString(lease.fencingToken()), lease.runId());
		if (!result.isEmpty() && "LOST".equals(result.get(0))) {
			throw new LeaseLostException(lease.sessionId(), lease.ownerId(), lease.fencingToken(),
					"lease lost during metadata validation");
		}
		return projected(runKey, result);
	}

	RedissonClient client() {
		return client;
	}

	String prefix() {
		return prefix;
	}

	/** A bounded, known metadata fault; transport failures remain distinct. */
	public static final class MetadataException extends IllegalStateException {

		private static final long serialVersionUID = 1L;

		MetadataException(String message) {
			super(message);
		}

	}

	private Optional<Metadata> project(String runKey) {
		String nsKey = runKey.substring(0, runKey.indexOf(":run:", prefix.length())) + ":ns";
		List<String> result = script.eval(RScript.Mode.READ_WRITE, PROJECT, RScript.ReturnType.MULTI,
				List.of(nsKey, runKey));
		return projected(runKey, result);
	}

	private Optional<Metadata> projected(String runKey, List<String> result) {
		if (result.isEmpty()) {
			return Optional.empty();
		}
		if ("ERROR".equals(result.get(0))) {
			throw new MetadataException(result.get(1));
		}
		if (!"1".equals(result.get(0)) || !"1".equals(result.get(2)) || result.get(1).isBlank()
				|| result.get(3).isBlank() || !result.get(4).matches("0|[1-9][0-9]{0,18}")) {
			throw new MetadataException("Invalid progress metadata");
		}
		long revision;
		try {
			revision = Long.parseLong(result.get(4));
		}
		catch (NumberFormatException ex) {
			throw new MetadataException("Invalid progress metadata revision");
		}
		Metadata metadata = new Metadata(result.get(1), result.get(3), revision, result.get(5));
		if (!keys.keys(metadata.sessionId(), metadata.runId()).get(1).equals(runKey)) {
			throw new MetadataException("Progress identity does not match authoritative key");
		}
		return Optional.of(metadata);
	}

	private static String globLiteral(String value) {
		return value.replace("\\", "\\\\").replace("*", "\\*").replace("?", "\\?").replace("[", "\\[");
	}

	/**
	 * State is an opaque string; empty also represents the uncommitted revision-zero
	 * state.
	 */
	public record Metadata(String sessionId, String runId, long revision, String state) {
	}

	/**
	 * In-memory continuation; do not reuse with a different Redis database or keyspace.
	 */
	public static final class Cursor {

		private final String prefix;

		private final String redisCursor;

		private final List<String> pending;

		private final int offset;

		private final boolean started;

		private Cursor(String prefix, String redisCursor, List<String> pending, int offset, boolean started) {
			this.prefix = prefix;
			this.redisCursor = redisCursor;
			this.pending = List.copyOf(pending);
			this.offset = offset;
			this.started = started;
		}

		public boolean done() {
			return started && "0".equals(redisCursor) && pendingKeys() == 0;
		}

		public int pendingKeys() {
			return pending.size() - offset;
		}

	}

	public record Batch(Cursor cursor, int inspectedKeys, int newHints) {
	}

}
