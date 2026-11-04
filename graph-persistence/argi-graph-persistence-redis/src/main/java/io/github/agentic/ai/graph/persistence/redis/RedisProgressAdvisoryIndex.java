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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Objects;

import org.redisson.api.RScript;
import org.redisson.api.RedissonClient;
import org.redisson.api.options.OptionalOptions;
import org.redisson.client.codec.StringCodec;

/**
 * Revision-specific advisory hints, with optional capacity and visibility delivery.
 * Authority lives in progress hashes, never here. Dedicated schema-owned keys only;
 * poll/ack/retry are single-primary and make no external execution guarantee.
 */
public final class RedisProgressAdvisoryIndex {

	private static final long MAX_EXACT_SCORE = 9_007_199_254_740_991L;

	private static final String TIME = """
			local t = redis.call('TIME')
			local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
			""";

	private static final String POLL = TIME + """
			local members = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'LIMIT', 0, ARGV[1])
			local seq = redis.call('HGET', KEYS[2], '_sequence') or '0'
			if not string.match(seq, '^%d+$') or #seq > 19
			  or (#seq == 19 and seq > '9223372036854774807') then
			  return redis.error_reply('Delivery sequence invalid or exhausted')
			end
			local result = {}
			for _, member in ipairs(members) do
			  redis.call('HINCRBY', KEYS[2], '_sequence', 1)
			  local token = redis.call('HGET', KEYS[2], '_sequence')
			  redis.call('HSET', KEYS[2], member, token)
			  redis.call('ZADD', KEYS[1], now + tonumber(ARGV[2]), member)
			  table.insert(result, member)
			  table.insert(result, token)
			end
			return result
			""";

	private final RScript script;

	private final String key;

	private final String deliveries;

	private final String config;

	private final int capacity;

	/** Unbounded legacy reference sink; choose the capacity overload for bounded use. */
	public RedisProgressAdvisoryIndex(RedissonClient client, String key) {
		this(client, key, 0);
	}

	/** Capacity 1..10000; zero preserves the original unbounded publication contract. */
	public RedisProgressAdvisoryIndex(RedissonClient client, String key, int capacity) {
		Objects.requireNonNull(client, "client");
		if (!client.getConfig().isSingleConfig()) {
			throw new IllegalArgumentException("Reference index requires a single-server client");
		}
		if (key == null || key.isBlank() || key.contains("{") || key.contains("}") || key.length() > 256 || capacity < 0
				|| capacity > 10000) {
			throw new IllegalArgumentException("Invalid dedicated hint key or capacity");
		}
		this.key = key;
		this.deliveries = key + ":deliveries:v1";
		this.config = key + ":config:v1";
		this.capacity = capacity;
		this.script = client.getScript(OptionalOptions.defaults().codec(StringCodec.INSTANCE).retryAttempts(0));
		Number initialized = script.eval(RScript.Mode.READ_WRITE, """
				local previous = redis.call('HGET', KEYS[1], 'capacity')
				if previous and previous ~= ARGV[1] then return 0 end
				if tonumber(ARGV[1]) > 0 and redis.call('ZCARD', KEYS[2]) > tonumber(ARGV[1]) then return 0 end
				redis.call('HSET', KEYS[1], 'capacity', ARGV[1])
				return 1
				""", RScript.ReturnType.INTEGER, List.of(config, key), Integer.toString(capacity));
		if (initialized.longValue() != 1) {
			throw new IllegalStateException("Hint capacity differs from existing configuration/data");
		}
	}

	/**
	 * Idempotent NX publication. A full index throws; authority can be repaired later.
	 */
	public boolean publish(Hint hint) {
		Objects.requireNonNull(hint, "hint");
		RedisProgressIndexRepairer.Metadata m = Objects.requireNonNull(hint.metadata(), "metadata");
		if (hint.dueAtMillis() < 0 || hint.dueAtMillis() > MAX_EXACT_SCORE) {
			throw new IllegalArgumentException("Invalid exact millisecond score");
		}
		String member = member(new Candidate(m.sessionId(), m.runId(), m.revision()));
		Number added = script.eval(RScript.Mode.READ_WRITE, """
				if redis.call('HGET', KEYS[2], 'capacity') ~= ARGV[3] then
				  return redis.error_reply('Hint configuration changed')
				end
				if redis.call('ZSCORE', KEYS[1], ARGV[2]) then return 0 end
				if tonumber(ARGV[3]) > 0 and redis.call('ZCARD', KEYS[1]) >= tonumber(ARGV[3]) then
				  return redis.error_reply('Hint capacity exhausted; retry repair after consumption')
				end
				return redis.call('ZADD', KEYS[1], 'NX', ARGV[1], ARGV[2])
				""", RScript.ReturnType.INTEGER, List.of(key, config), Long.toString(hint.dueAtMillis()), member,
				Integer.toString(capacity));
		return added.longValue() == 1;
	}

	/** Diagnostic query; use poll for fair visibility delivery. */
	public List<Candidate> due(int limit) {
		requireLimit(limit);
		List<String> members = script.eval(RScript.Mode.READ_WRITE, TIME + """
				return redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'LIMIT', 0, ARGV[1])
				""", RScript.ReturnType.MULTI, List.of(key), Integer.toString(limit));
		return members.stream().map(RedisProgressAdvisoryIndex::candidate).toList();
	}

	/**
	 * Atomically postpones each delivered member using Redis TIME and issues a unique
	 * exact int64 token. Busy/crashed consumers do not permanently pin the first page. A
	 * lost response redelivers after visibility; it does not delete authority or hints.
	 */
	public List<Delivery> poll(int limit, long visibilityMillis) {
		requireLimit(limit);
		requireDelay(visibilityMillis);
		List<String> raw = script.eval(RScript.Mode.READ_WRITE, POLL, RScript.ReturnType.MULTI,
				List.of(key, deliveries), Integer.toString(limit), Long.toString(visibilityMillis));
		List<Delivery> result = new ArrayList<>();
		for (int i = 0; i < raw.size(); i += 2) {
			result.add(new Delivery(candidate(raw.get(i)), Long.parseLong(raw.get(i + 1))));
		}
		return List.copyOf(result);
	}

	/**
	 * Deletes only this revision and this delivery; newer delivery/revision is untouched.
	 */
	public boolean ack(Delivery delivery) {
		return finish(delivery, 0);
	}

	/** Idempotent exact-delivery retry, scheduled against Redis TIME. */
	public boolean retry(Delivery delivery, long backoffMillis) {
		requireDelay(backoffMillis);
		return finish(delivery, backoffMillis);
	}

	private boolean finish(Delivery delivery, long delay) {
		Objects.requireNonNull(delivery, "delivery");
		if (delivery.token() <= 0) {
			throw new IllegalArgumentException("Delivery token must be positive");
		}
		Number changed = script.eval(RScript.Mode.READ_WRITE, TIME + """
				if redis.call('HGET', KEYS[2], ARGV[1]) ~= ARGV[2] then return 0 end
				if tonumber(ARGV[3]) == 0 then
				  redis.call('ZREM', KEYS[1], ARGV[1])
				elseif redis.call('ZSCORE', KEYS[1], ARGV[1]) then
				  redis.call('ZADD', KEYS[1], now + tonumber(ARGV[3]), ARGV[1])
				end
				redis.call('HDEL', KEYS[2], ARGV[1])
				return 1
				""", RScript.ReturnType.INTEGER, List.of(key, deliveries), member(delivery.candidate()),
				Long.toString(delivery.token()), Long.toString(delay));
		return changed.longValue() == 1;
	}

	private static void requireLimit(int limit) {
		if (limit < 1 || limit > 1000) {
			throw new IllegalArgumentException("limit must be between 1 and 1000");
		}
	}

	private static void requireDelay(long delay) {
		if (delay < 1 || delay > 86_400_000) {
			throw new IllegalArgumentException("Delay must be 1..86400000 milliseconds");
		}
	}

	private static String member(Candidate candidate) {
		if (candidate.revision() < 0) {
			throw new IllegalArgumentException("Revision must be nonnegative");
		}
		return encode(candidate.sessionId()) + "." + encode(candidate.runId()) + "." + candidate.revision();
	}

	private static Candidate candidate(String member) {
		String[] parts = member.split("\\.", -1);
		if (parts.length != 3) {
			throw new IllegalStateException("Invalid advisory member");
		}
		return new Candidate(decode(parts[0]), decode(parts[1]), Long.parseLong(parts[2]));
	}

	private static String encode(String value) {
		Objects.requireNonNull(value, "identity");
		byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
		if (value.isBlank() || bytes.length > 1024) {
			throw new IllegalArgumentException("Identity must be nonblank and at most 1024 UTF-8 bytes");
		}
		return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
	}

	private static String decode(String value) {
		return new String(Base64.getUrlDecoder().decode(value), StandardCharsets.UTF_8);
	}

	public record Hint(RedisProgressIndexRepairer.Metadata metadata, long dueAtMillis) {
	}

	public record Candidate(String sessionId, String runId, long revision) {
	}

	public record Delivery(Candidate candidate, long token) {
	}

}
