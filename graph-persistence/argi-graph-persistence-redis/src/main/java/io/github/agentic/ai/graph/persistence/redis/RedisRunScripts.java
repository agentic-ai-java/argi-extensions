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

/**
 * Lua scripts of {@link RedisRunStore}. Every script touches only keys declared in KEYS,
 * rejects any declared key that is not a hash before reading or writing, never reads
 * server time and never sets a TTL. Stored int64 counters are never compared numerically
 * in Lua: they are compared as canonical decimal strings, and the fence increment uses
 * {@code HINCRBY} only after an explicit {@code MAX_LONG} guard. Lease expiry and the
 * caller's clock are exact epoch nanoseconds, also passed as canonical decimal strings.
 */
final class RedisRunScripts {

	static final String OK = "OK";

	static final String NONE = "NONE";

	static final String DEDUP = "DEDUP";

	static final String CORRUPT = "CORRUPT";

	static final String OVERFLOW = "OVERFLOW";

	static final String E_EXPIRED = "E_EXPIRED";

	static final String C_DEDUP = "C_DEDUP";

	static final String C_RUNID = "C_RUNID";

	static final String C_RESERVED = "C_RESERVED";

	static final String C_NOT_RESERVED = "C_NOT_RESERVED";

	static final String C_LIVE = "C_LIVE";

	static final String C_LEASE = "C_LEASE";

	static final String C_REVISION = "C_REVISION";

	private static final String PRELUDE = """
			local MAX_LONG = '9223372036854775807'
			local function is_decimal(v)
			  if type(v) ~= 'string' then
			    return false
			  end
			  if v == '0' then
			    return true
			  end
			  if string.sub(v, 1, 1) == '0' then
			    return false
			  end
			  return string.match(v, '^[0-9]+$') ~= nil
			end
			local function cmp_decimal(a, b)
			  if string.len(a) ~= string.len(b) then
			    if string.len(a) < string.len(b) then
			      return -1
			    end
			    return 1
			  end
			  if a == b then
			    return 0
			  end
			  if a < b then
			    return -1
			  end
			  return 1
			end
			local function is_long(v)
			  return is_decimal(v) and cmp_decimal(v, MAX_LONG) <= 0
			end
			local function corrupt(key, field)
			  return {'CORRUPT', key .. '/' .. field}
			end
			-- Returns the first declared key holding a non-hash value, or nil.
			local function wrong_type()
			  for _, k in ipairs(KEYS) do
			    local t = redis.call('TYPE', k)['ok']
			    if t ~= 'hash' and t ~= 'none' then
			      return k
			    end
			  end
			  return nil
			end
			-- Returns nil, nil when absent; hash, nil when valid; nil, field when corrupt.
			local function load_ns(key, tuple)
			  if redis.call('EXISTS', key) == 0 then
			    return nil, nil
			  end
			  local raw = redis.call('HMGET', key, 'v', 'ns', 'fence', 'reservation',
			    'lease_owner', 'lease_fence', 'lease_expires')
			  local h = {v = raw[1], ns = raw[2], fence = raw[3], reservation = raw[4],
			    owner = raw[5], lease_fence = raw[6], expires = raw[7]}
			  if h.v ~= '1' then
			    return nil, 'v'
			  end
			  if h.ns ~= tuple then
			    return nil, 'ns'
			  end
			  if h.fence and not is_long(h.fence) then
			    return nil, 'fence'
			  end
			  if h.reservation == '' then
			    return nil, 'reservation'
			  end
			  local present = 0
			  if h.owner then present = present + 1 end
			  if h.lease_fence then present = present + 1 end
			  if h.expires then present = present + 1 end
			  if present ~= 0 and present ~= 3 then
			    return nil, 'lease'
			  end
			  if present == 3 then
			    if h.owner == '' then
			      return nil, 'lease_owner'
			    end
			    if not is_long(h.lease_fence) or h.lease_fence == '0' or not h.fence
			        or cmp_decimal(h.lease_fence, h.fence) > 0 then
			      return nil, 'lease_fence'
			    end
			    if not is_long(h.expires) then
			      return nil, 'lease_expires'
			    end
			  end
			  return h, nil
			end
			local function lease_live(h, owner, fence, now)
			  return h.owner == owner and h.lease_fence == fence and cmp_decimal(now, h.expires) < 0
			end
			""";

	/** KEYS: ns, run. ARGV: tuple, submissionKey, fp, runId, expired(0|1), specJson. */
	static final String SUBMIT = PRELUDE + """
			local bad = wrong_type()
			if bad then
			  return corrupt(bad, 'type')
			end
			local h, err = load_ns(KEYS[1], ARGV[1])
			if err then
			  return corrupt(KEYS[1], err)
			end
			if h then
			  local existing = redis.call('HGET', KEYS[1], 's:' .. ARGV[2])
			  if existing then
			    local fp = string.sub(existing, 1, 64)
			    if string.len(existing) < 66 or string.sub(existing, 65, 65) ~= ':'
			        or string.match(fp, '^[0-9a-f]+$') == nil then
			      return corrupt(KEYS[1], 's:' .. ARGV[2])
			    end
			    if fp ~= ARGV[3] then
			      return {'C_DEDUP'}
			    end
			    return {'DEDUP', string.sub(existing, 66)}
			  end
			end
			if redis.call('EXISTS', KEYS[2]) == 1 then
			  if not h then
			    return corrupt(KEYS[2], 'ns')
			  end
			  return {'C_RUNID'}
			end
			if h and h.reservation then
			  return {'C_RESERVED'}
			end
			if ARGV[5] == '1' then
			  return {'E_EXPIRED'}
			end
			redis.call('HSET', KEYS[1], 'v', '1', 'ns', ARGV[1], 's:' .. ARGV[2], ARGV[3] .. ':' .. ARGV[4],
			  'reservation', ARGV[4])
			redis.call('HSET', KEYS[2], 'v', '1', 'spec', ARGV[6], 'fp', ARGV[3], 'revision', '0',
			  'state', 'PENDING')
			return {'OK'}
			""";

	/** KEYS: ns. ARGV: tuple, runId, owner, nowNanos, expiresNanos. */
	static final String ACQUIRE = PRELUDE + """
			local bad = wrong_type()
			if bad then
			  return corrupt(bad, 'type')
			end
			local h, err = load_ns(KEYS[1], ARGV[1])
			if err then
			  return corrupt(KEYS[1], err)
			end
			if not h or h.reservation ~= ARGV[2] then
			  return {'C_NOT_RESERVED'}
			end
			if h.owner and cmp_decimal(ARGV[4], h.expires) < 0 then
			  return {'C_LIVE'}
			end
			if h.fence == MAX_LONG then
			  return {'OVERFLOW'}
			end
			redis.call('HINCRBY', KEYS[1], 'fence', 1)
			local fence = redis.call('HGET', KEYS[1], 'fence')
			redis.call('HSET', KEYS[1], 'lease_owner', ARGV[3], 'lease_fence', fence, 'lease_expires', ARGV[5])
			return {'OK', fence}
			""";

	/** KEYS: ns. ARGV: tuple, owner, fence, nowNanos, expiresNanos. */
	static final String RENEW = PRELUDE + """
			local bad = wrong_type()
			if bad then
			  return corrupt(bad, 'type')
			end
			local h, err = load_ns(KEYS[1], ARGV[1])
			if err then
			  return corrupt(KEYS[1], err)
			end
			if not h or not lease_live(h, ARGV[2], ARGV[3], ARGV[4]) then
			  return {'C_LEASE'}
			end
			redis.call('HSET', KEYS[1], 'lease_expires', ARGV[5])
			return {'OK'}
			""";

	/** KEYS: ns. ARGV: tuple, owner, fence. */
	static final String RELEASE = PRELUDE + """
			local bad = wrong_type()
			if bad then
			  return corrupt(bad, 'type')
			end
			local h, err = load_ns(KEYS[1], ARGV[1])
			if err then
			  return corrupt(KEYS[1], err)
			end
			if h and h.owner == ARGV[2] and h.lease_fence == ARGV[3] then
			  redis.call('HDEL', KEYS[1], 'lease_owner', 'lease_fence', 'lease_expires')
			  return {'OK', '1'}
			end
			return {'OK', '0'}
			""";

	/**
	 * KEYS: ns, run, tasks, journal, outbox. ARGV: tuple, runId, owner, fence, nowNanos,
	 * expectedRev, newRev, state, cpMode(KEEP|SET), cpJson, terminal(0|1), then three
	 * sections (tasks, journal, outbox), each a count followed by field/value pairs.
	 */
	static final String COMMIT = PRELUDE + """
			local bad = wrong_type()
			if bad then
			  return corrupt(bad, 'type')
			end
			local h, err = load_ns(KEYS[1], ARGV[1])
			if err then
			  return corrupt(KEYS[1], err)
			end
			if not h or h.reservation ~= ARGV[2] then
			  return {'C_NOT_RESERVED'}
			end
			if not lease_live(h, ARGV[3], ARGV[4], ARGV[5]) then
			  return {'C_LEASE'}
			end
			local revision = redis.call('HGET', KEYS[2], 'revision')
			if not revision then
			  return corrupt(KEYS[2], 'revision')
			end
			if revision ~= ARGV[6] then
			  return {'C_REVISION'}
			end
			local sections = {}
			local index = 12
			for s = 1, 3 do
			  local count = tonumber(ARGV[index])
			  local fields = {}
			  for i = 1, count * 2 do
			    fields[i] = ARGV[index + i]
			  end
			  sections[s] = fields
			  index = index + 1 + count * 2
			end
			for s = 2, 3 do
			  local fields = sections[s]
			  for i = 1, #fields, 2 do
			    if redis.call('HEXISTS', KEYS[s + 2], fields[i]) == 1 then
			      return corrupt(KEYS[s + 2], fields[i])
			    end
			  end
			end
			redis.call('HSET', KEYS[2], 'revision', ARGV[7], 'state', ARGV[8])
			if ARGV[9] == 'SET' then
			  redis.call('HSET', KEYS[2], 'checkpoint', ARGV[10])
			end
			for s = 1, 3 do
			  local fields = sections[s]
			  for i = 1, #fields, 2 do
			    redis.call('HSET', KEYS[s + 2], fields[i], fields[i + 1])
			  end
			end
			if ARGV[11] == '1' then
			  redis.call('HDEL', KEYS[1], 'reservation', 'lease_owner', 'lease_fence', 'lease_expires')
			end
			return {'OK'}
			""";

	/**
	 * KEYS: ns, run, tasks, journal, outbox. ARGV: tuple. Returns OK, nsExists,
	 * reservation, leaseOwner, leaseFence, leaseExpires ('' when absent), then four
	 * HGETALL sections, each a count followed by the flat field/value list.
	 */
	static final String READ = PRELUDE + """
			local bad = wrong_type()
			if bad then
			  return corrupt(bad, 'type')
			end
			local h, err = load_ns(KEYS[1], ARGV[1])
			if err then
			  return corrupt(KEYS[1], err)
			end
			local function opt(v)
			  if v then
			    return v
			  end
			  return ''
			end
			local result = {'OK', '0', '', '', '', ''}
			if h then
			  result = {'OK', '1', opt(h.reservation), opt(h.owner), opt(h.lease_fence), opt(h.expires)}
			end
			for i = 2, 5 do
			  local all = redis.call('HGETALL', KEYS[i])
			  table.insert(result, tostring(#all))
			  for _, v in ipairs(all) do
			    table.insert(result, v)
			  end
			end
			return result
			""";

	/** KEYS: ns. ARGV: tuple. */
	static final String ACTIVE = PRELUDE + """
			local bad = wrong_type()
			if bad then
			  return corrupt(bad, 'type')
			end
			local h, err = load_ns(KEYS[1], ARGV[1])
			if err then
			  return corrupt(KEYS[1], err)
			end
			if h and h.reservation then
			  return {'OK', h.reservation}
			end
			return {'NONE'}
			""";

	private RedisRunScripts() {
	}

}
