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
package io.github.agentic.ai.memory.redis;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.springframework.data.redis.connection.ClusterSlotHashUtil;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

/**
 * Publishes a complete list without deleting the previous snapshot first. The temporary
 * list exists only inside one script invocation: every command failure after creation is
 * caught and cleans it up. DEL permission is checked before creating it, since Redis
 * scripts do not roll back writes on error. No client-side staging or timeout retry is
 * used.
 *
 * <p>
 * RENAME is the publication boundary and removes any previous destination expiration.
 * Concurrent saves are last-publisher-wins; a read-modify-write sequence is not atomic.
 */
final class RedisSnapshot {

	static final String SCRIPT = """
			if #ARGV == 0 then
			    return redis.call('del', KEYS[1])
			end
			if redis.call('exists', KEYS[2]) ~= 0 then
			    return redis.error_reply('Snapshot staging key already exists')
			end
			-- Check cleanup permission before any staging writes. Scripts cannot interleave
			-- with ACL changes, and DEL is permitted even when Redis is out of memory.
			redis.call('del', KEYS[2])
			for first = 1, #ARGV, 256 do
			    local chunk = {}
			    for i = first, math.min(first + 255, #ARGV) do
			        chunk[#chunk + 1] = ARGV[i]
			    end
			    local result = redis.pcall('rpush', KEYS[2], unpack(chunk))
			    if type(result) == 'table' and result.err then
			        redis.call('del', KEYS[2])
			        return redis.error_reply(result.err)
			    end
			end
			local result = redis.pcall('rename', KEYS[2], KEYS[1])
			if type(result) == 'table' and result.err then
			    redis.call('del', KEYS[2])
			    return redis.error_reply(result.err)
			end
			return 1
			""";

	private static final DefaultRedisScript<Long> REDIS_SCRIPT = new DefaultRedisScript<>(SCRIPT, Long.class);

	private static final ConcurrentMap<Integer, String> SLOT_TAGS = new ConcurrentHashMap<>();

	private RedisSnapshot() {
	}

	static void save(RedisTemplate<String, String> template, String key, List<String> messages) {
		if (messages.isEmpty()) {
			template.delete(key);
			return;
		}
		template.execute(REDIS_SCRIPT, List.of(key, stagingKey(key)), messages.toArray());
	}

	static String stagingKey(String key) {
		int slot = ClusterSlotHashUtil.calculateSlot(key);
		String tag = SLOT_TAGS.computeIfAbsent(slot, target -> {
			for (int candidate = 0;; candidate++) {
				String value = "argi-" + candidate;
				if (ClusterSlotHashUtil.calculateSlot(value) == target) {
					return value;
				}
			}
		});
		return "{" + tag + "}:snapshot:" + UUID.randomUUID();
	}

}
