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
import java.util.Optional;
import java.util.function.Function;

import org.redisson.api.RScript;
import org.redisson.api.RedissonClient;
import org.redisson.api.options.OptionalOptions;
import org.redisson.client.codec.StringCodec;

/**
 * Opt-in retained authoritative membership directory. All producers must use the store's
 * directoryCapacity option for this prefix. Registration and authority creation share one
 * Lua invocation. No SCAN, deletion, automatic migration or Cluster support is supplied.
 * Membership is authoritative; runnable state and lease ownership remain in progress
 * hashes.
 */
public final class RedisProgressDirectory {

	private static final String GUARD = """
			local dt = redis.call('TYPE', KEYS[4]).ok
			if dt ~= 'none' and dt ~= 'zset' then return {'DIRECTORY_TYPE'} end
			if redis.call('TYPE', KEYS[3]).ok ~= 'hash'
			  or redis.call('HGET', KEYS[3], 'capacity') ~= ARGV[6] then
			  return {'DIRECTORY_CONFIG'}
			end
			local present = redis.call('ZSCORE', KEYS[4], ARGV[5])
			if not present and redis.call('ZCARD', KEYS[4]) >= tonumber(ARGV[6]) then
			  return {'DIRECTORY_FULL'}
			end
			""";

	private static final String CLAIM = GUARD + "local function claim_authority()\n"
			+ "local KEYS = {KEYS[1], KEYS[2]}\n" + RedisProgressScripts.CLAIM + "\nend\n" + """
					local result = claim_authority()
					if result[1] == 'OK' then redis.call('ZADD', KEYS[4], 0, ARGV[5]) end
					return result
					""";

	private static final String BACKFILL = GUARD + """
			for i = 1, 2 do
			  if redis.call('TYPE', KEYS[i]).ok ~= 'hash' then return {'MISSING_OR_TYPE'} end
			end
			if redis.call('HSTRLEN', KEYS[1], 'session') > 1024
			  or redis.call('HSTRLEN', KEYS[2], 'run') > 1024 then return {'IDENTITY_LIMIT'} end
			if redis.call('HGET', KEYS[1], 'session') ~= ARGV[1]
			  or redis.call('HGET', KEYS[2], 'run') ~= ARGV[2] then return {'IDENTITY'} end
			redis.call('ZADD', KEYS[4], 0, ARGV[5])
			return {'OK'}
			""";

	private final RScript script;

	private final RedissonClient client;

	private final RedisProgressKeys keys;

	private final String prefix;

	private final String configKey;

	private final String directoryKey;

	private final int capacity;

	public RedisProgressDirectory(RedissonClient client, String prefix, int capacity) {
		Objects.requireNonNull(client, "client");
		if (!client.getConfig().isSingleConfig() || prefix == null || prefix.isBlank()
				|| prefix.getBytes(StandardCharsets.UTF_8).length > 256 || prefix.contains("{") || prefix.contains("}")
				|| capacity < 1 || capacity > 10000) {
			throw new IllegalArgumentException(
					"Single-server directory requires prefix <=256 UTF-8 bytes and capacity 1..10000");
		}
		this.client = client;
		this.prefix = prefix;
		this.capacity = capacity;
		this.keys = new RedisProgressKeys(prefix);
		this.configKey = prefix + ":directory:v1:config";
		this.directoryKey = prefix + ":directory:v1:members";
		this.script = client.getScript(OptionalOptions.defaults().codec(StringCodec.INSTANCE).retryAttempts(0));
		List<String> result = script.eval(RScript.Mode.READ_WRITE, """
				local t = redis.call('TYPE', KEYS[1]).ok
				if t ~= 'none' and t ~= 'hash' then return {'DIRECTORY_CONFIG'} end
				local old = redis.call('HGET', KEYS[1], 'capacity')
				if old and old ~= ARGV[1] then return {'DIRECTORY_CONFIG'} end
				redis.call('HSET', KEYS[1], 'capacity', ARGV[1])
				return {'OK'}
				""", RScript.ReturnType.MULTI, List.of(configKey), Integer.toString(capacity));
		requireOk(result);
	}

	List<Object> claim(String session, String run, String owner, long ttl) {
		List<Object> result = script.eval(RScript.Mode.READ_WRITE, CLAIM, RScript.ReturnType.MULTI,
				scriptKeys(session, run), session, run, owner, Long.toString(ttl), member(session, run),
				Integer.toString(capacity));
		if (String.valueOf(result.get(0)).startsWith("DIRECTORY_")) {
			throw new IllegalStateException(String.valueOf(result.get(0)));
		}
		return result;
	}

	/**
	 * Explicit one-record backfill from a caller-supplied inventory, with legacy writers
	 * quiesced. This registers identity even if other metadata needs operator repair;
	 * repair() will report that error while continuing past it. No full-keyspace claim.
	 */
	public void registerExisting(String session, String run) {
		List<String> result = script.eval(RScript.Mode.READ_WRITE, BACKFILL, RScript.ReturnType.MULTI,
				scriptKeys(session, run), session, run, "", "", member(session, run), Integer.toString(capacity));
		requireOk(result);
	}

	private List<Object> scriptKeys(String session, String run) {
		return List.of(keys.sessionKey(session), keys.keys(session, run).get(1), configKey, directoryKey);
	}

	/**
	 * Exclusive lexicographic continuation. Inserts behind the cursor appear next sweep.
	 */
	public Page page(String after, int limit) {
		if (after == null || after.length() > 2733
				|| (!after.isEmpty() && !after.matches("[A-Za-z0-9_-]+\\.[A-Za-z0-9_-]+")) || limit < 1
				|| limit > 1000) {
			throw new IllegalArgumentException("Invalid directory cursor or limit");
		}
		List<String> members = script.eval(RScript.Mode.READ_WRITE,
				"return redis.call('ZRANGEBYLEX', KEYS[1], ARGV[1], '+', 'LIMIT', 0, ARGV[2])",
				RScript.ReturnType.MULTI, List.of(directoryKey), after.isEmpty() ? "-" : "(" + after,
				Integer.toString(limit));
		List<Identity> ids = members.stream().map(RedisProgressDirectory::identity).toList();
		return new Page(ids, members.isEmpty() ? after : members.get(members.size() - 1), members.size() < limit);
	}

	/**
	 * Bounded directory page plus one bounded metadata projection per entry. Bad metadata
	 * is returned as a queryable issue and the cursor advances; transport/publication
	 * failures propagate so retrying the input cursor republishes idempotently.
	 */
	public RepairBatch repair(String after, int limit, RedisProgressIndexRepairer projection,
			Function<RedisProgressIndexRepairer.Metadata, Optional<RedisProgressAdvisoryIndex.Hint>> adapter,
			RedisProgressAdvisoryIndex sink) {
		if (!prefix.equals(projection.prefix()) || client != projection.client()) {
			throw new IllegalArgumentException("Projection must share directory client and prefix");
		}
		Page page = page(after, limit);
		List<Issue> issues = new ArrayList<>();
		int published = 0;
		for (Identity id : page.entries()) {
			Optional<RedisProgressIndexRepairer.Metadata> metadata;
			try {
				metadata = projection.metadata(id.sessionId(), id.runId());
			}
			catch (RedisProgressIndexRepairer.MetadataException ex) {
				issues.add(new Issue(id, ex.getMessage()));
				continue;
			}
			if (metadata.isEmpty()) {
				issues.add(new Issue(id, "MISSING_AUTHORITY"));
				continue;
			}
			Optional<RedisProgressAdvisoryIndex.Hint> hint = Objects.requireNonNull(adapter.apply(metadata.get()));
			if (hint.isPresent()) {
				if (!metadata.get().equals(hint.get().metadata())) {
					throw new IllegalArgumentException("Adapter must preserve authority metadata");
				}
				if (sink.publish(hint.get())) {
					published++;
				}
			}
		}
		return new RepairBatch(page, published, List.copyOf(issues));
	}

	static String member(String session, String run) {
		return encode(session) + "." + encode(run);
	}

	private static String encode(String id) {
		if (id == null || id.isBlank() || id.getBytes(StandardCharsets.UTF_8).length > 1024) {
			throw new IllegalArgumentException("Directory identity must be nonblank and <=1024 UTF-8 bytes");
		}
		return Base64.getUrlEncoder().withoutPadding().encodeToString(id.getBytes(StandardCharsets.UTF_8));
	}

	private static Identity identity(String member) {
		String[] parts = member.split("\\.", -1);
		if (parts.length != 2 || member.length() > 2733) {
			throw new IllegalStateException("Invalid directory member; dedicated key has an unsupported writer");
		}
		return new Identity(new String(Base64.getUrlDecoder().decode(parts[0]), StandardCharsets.UTF_8),
				new String(Base64.getUrlDecoder().decode(parts[1]), StandardCharsets.UTF_8));
	}

	private static void requireOk(List<String> result) {
		if (!result.equals(List.of("OK"))) {
			throw new IllegalStateException("Directory operation rejected: " + result.get(0));
		}
	}

	public record Identity(String sessionId, String runId) {
	}

	public record Page(List<Identity> entries, String after, boolean done) {
	}

	public record Issue(Identity identity, String error) {
	}

	public record RepairBatch(Page page, int published, List<Issue> issues) {
	}

}
