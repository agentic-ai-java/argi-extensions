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
 * Lua scripts of {@link RedisLeasedProgressStore}. Every script touches only keys
 * declared in KEYS, rejects any declared key that is not a hash before reading or
 * writing, and never sets a TTL. Lease expiry uses the Redis server {@code TIME}
 * as the single authoritative clock, stored and compared as exact decimal epoch
 * millisecond strings. Stored int64 counters are never compared numerically in
 * Lua: they are compared as canonical decimal strings, and the fence increment
 * uses {@code HINCRBY} only after an explicit {@code MAX_LONG} guard. Every guard
 * (owner, fence, expiry, run/checkpoint revision, append-only entry ids) runs
 * before the first write, so a rejected script leaves no partial progress.
 */
final class RedisProgressScripts {

	static final String OK = "OK";

	static final String CORRUPT = "CORRUPT";

	static final String BUSY = "BUSY";

	static final String LOST = "LOST";

	static final String OVERFLOW = "OVERFLOW";

	static final String C_NOT_RESERVED = "C_NOT_RESERVED";

	static final String C_REVISION = "C_REVISION";

	static final String C_CP_REVISION = "C_CP_REVISION";

	static final String C_ENTRY = "C_ENTRY";

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
			-- The only time source: Redis server epoch milliseconds as a decimal string.
			local function now_millis()
			  local t = redis.call('TIME')
			  return tostring((t[1] * 1000) + math.floor(t[2] / 1000))
			end
			-- Returns nil, nil when absent; hash, nil when valid; nil, field when corrupt.
			local function load_session(key, session)
			  if redis.call('EXISTS', key) == 0 then
			    return nil, nil
			  end
			  local raw = redis.call('HMGET', key, 'v', 'session', 'reservation', 'fence', 'owner', 'expires')
			  local h = {v = raw[1], session = raw[2], reservation = raw[3], fence = raw[4],
			    owner = raw[5], expires = raw[6]}
			  if h.v ~= '1' then
			    return nil, 'v'
			  end
			  if h.session ~= session then
			    return nil, 'session'
			  end
			  if not h.fence or not is_long(h.fence) or h.fence == '0' then
			    return nil, 'fence'
			  end
			  if h.reservation == '' then
			    return nil, 'reservation'
			  end
			  local present = 0
			  if h.owner then present = present + 1 end
			  if h.expires then present = present + 1 end
			  if present ~= 0 and present ~= 2 then
			    return nil, 'lease'
			  end
			  if present == 2 then
			    if h.owner == '' then
			      return nil, 'owner'
			    end
			    if not is_long(h.expires) then
			      return nil, 'expires'
			    end
			  end
			  return h, nil
			end
			-- Returns nil, nil when absent; record, nil when valid; nil, field when corrupt.
			local function load_run(key, runId)
			  if redis.call('EXISTS', key) == 0 then
			    return nil, nil
			  end
			  local raw = redis.call('HMGET', key, 'v', 'run', 'revision', 'state', 'checkpoint', 'cp_revision')
			  local r = {v = raw[1], run = raw[2], revision = raw[3], state = raw[4],
			    checkpoint = raw[5], cp_revision = raw[6]}
			  if r.v ~= '1' then
			    return nil, 'v'
			  end
			  if r.run ~= runId then
			    return nil, 'run'
			  end
			  if not r.revision or not is_long(r.revision) then
			    return nil, 'revision'
			  end
			  if r.revision == '0' then
			    if r.state or r.checkpoint or r.cp_revision then
			      return nil, 'revision'
			    end
			  else
			    if not r.state or r.state == '' then
			      return nil, 'state'
			    end
			    if r.checkpoint and not r.cp_revision then
			      return nil, 'cp_revision'
			    end
			    if r.cp_revision and not r.checkpoint then
			      return nil, 'checkpoint'
			    end
			    if r.cp_revision and not is_long(r.cp_revision) then
			      return nil, 'cp_revision'
			    end
			  end
			  return r, nil
			end
			local function lease_live(h, owner, fence, now)
			  return h.owner == owner and h.fence == fence and cmp_decimal(now, h.expires) < 0
			end
			""";

	/**
	 * KEYS: ns, run. ARGV: session, runId, owner, ttlMillis. Claims the session
	 * when no lease is live, increments the retained fence, binds the reservation
	 * to the run and bootstraps the run record at revision zero when absent.
	 */
	static final String CLAIM = PRELUDE + """
			local bad = wrong_type()
			if bad then
			  return corrupt(bad, 'type')
			end
			local h, err = load_session(KEYS[1], ARGV[1])
			if err then
			  return corrupt(KEYS[1], err)
			end
			local now = now_millis()
			if h and h.owner and cmp_decimal(now, h.expires) < 0 then
			  return {'BUSY'}
			end
			if h and h.fence == MAX_LONG then
			  return {'OVERFLOW'}
			end
			local r, rerr = load_run(KEYS[2], ARGV[2])
			if rerr then
			  return corrupt(KEYS[2], rerr)
			end
			local fence
			if h then
			  redis.call('HINCRBY', KEYS[1], 'fence', 1)
			  fence = redis.call('HGET', KEYS[1], 'fence')
			else
			  fence = '1'
			  redis.call('HSET', KEYS[1], 'v', '1', 'session', ARGV[1], 'fence', fence)
			end
			local expires = tostring(tonumber(now) + tonumber(ARGV[4]))
			redis.call('HSET', KEYS[1], 'reservation', ARGV[2], 'owner', ARGV[3], 'expires', expires)
			if not r then
			  redis.call('HSET', KEYS[2], 'v', '1', 'run', ARGV[2], 'revision', '0')
			end
			return {'OK', fence, expires}
			""";

	/** KEYS: ns. ARGV: session, owner, fence, ttlMillis. Heartbeat of a live lease. */
	static final String RENEW = PRELUDE + """
			local bad = wrong_type()
			if bad then
			  return corrupt(bad, 'type')
			end
			local h, err = load_session(KEYS[1], ARGV[1])
			if err then
			  return corrupt(KEYS[1], err)
			end
			local now = now_millis()
			if not h or not lease_live(h, ARGV[2], ARGV[3], now) then
			  return {'LOST'}
			end
			local expires = tostring(tonumber(now) + tonumber(ARGV[4]))
			redis.call('HSET', KEYS[1], 'expires', expires)
			return {'OK', expires}
			""";

	/** KEYS: ns. ARGV: session, owner, fence. Only a live, matching lease releases. */
	static final String RELEASE = PRELUDE + """
			local bad = wrong_type()
			if bad then
			  return corrupt(bad, 'type')
			end
			local h, err = load_session(KEYS[1], ARGV[1])
			if err then
			  return corrupt(KEYS[1], err)
			end
			local now = now_millis()
			if h and lease_live(h, ARGV[2], ARGV[3], now) then
			  redis.call('HDEL', KEYS[1], 'owner', 'expires')
			  return {'OK', '1'}
			end
			return {'OK', '0'}
			""";

	/**
	 * KEYS: ns, run, journal, outbox. ARGV: session, runId, owner, fence,
	 * expectedRunRev, nextRunRev, state, cpMode(KEEP|SET), expectedCpRev,
	 * nextCpRev, cpPayload, then two sections (journal, outbox), each a count
	 * followed by id/payload pairs. Re-validates reservation, live owner/fence,
	 * Redis time, both revisions and entry ids, then writes everything at once.
	 */
	static final String COMMIT = PRELUDE + """
			local bad = wrong_type()
			if bad then
			  return corrupt(bad, 'type')
			end
			local h, err = load_session(KEYS[1], ARGV[1])
			if err then
			  return corrupt(KEYS[1], err)
			end
			if not h or h.reservation ~= ARGV[2] then
			  return {'C_NOT_RESERVED'}
			end
			local now = now_millis()
			if not lease_live(h, ARGV[3], ARGV[4], now) then
			  return {'LOST'}
			end
			local r, rerr = load_run(KEYS[2], ARGV[2])
			if rerr then
			  return corrupt(KEYS[2], rerr)
			end
			if not r then
			  return corrupt(KEYS[2], 'run')
			end
			if r.revision ~= ARGV[5] then
			  return {'C_REVISION', r.revision}
			end
			if ARGV[8] == 'SET' then
			  local current_cp = r.cp_revision or '0'
			  if current_cp ~= ARGV[9] then
			    return {'C_CP_REVISION', current_cp}
			  end
			end
			local index = 12
			for s = 1, 2 do
			  local count = tonumber(ARGV[index])
			  for i = 0, count - 1 do
			    local id = ARGV[index + 1 + i * 2]
			    if redis.call('HEXISTS', KEYS[s + 2], id) == 1 then
			      return {'C_ENTRY', id}
			    end
			  end
			  index = index + 1 + count * 2
			end
			redis.call('HSET', KEYS[2], 'revision', ARGV[6], 'state', ARGV[7])
			if ARGV[8] == 'SET' then
			  redis.call('HSET', KEYS[2], 'checkpoint', ARGV[11], 'cp_revision', ARGV[10])
			end
			index = 12
			for s = 1, 2 do
			  local count = tonumber(ARGV[index])
			  for i = 0, count - 1 do
			    redis.call('HSET', KEYS[s + 2], ARGV[index + 1 + i * 2], ARGV[index + 2 + i * 2])
			  end
			  index = index + 1 + count * 2
			end
			return {'OK'}
			""";

	/**
	 * KEYS: ns, run, journal, outbox. ARGV: session, runId. Returns OK, nsExists,
	 * runExists, revision, state, checkpoint, cpRevision ('' when absent), then two
	 * HGETALL sections, each a count followed by the flat field/value list.
	 */
	static final String READ = PRELUDE + """
			local bad = wrong_type()
			if bad then
			  return corrupt(bad, 'type')
			end
			local h, err = load_session(KEYS[1], ARGV[1])
			if err then
			  return corrupt(KEYS[1], err)
			end
			local r, rerr = load_run(KEYS[2], ARGV[2])
			if rerr then
			  return corrupt(KEYS[2], rerr)
			end
			local function opt(v)
			  if v then
			    return v
			  end
			  return ''
			end
			local result = {'OK', '0', '0', '', '', '', ''}
			if h then
			  result[2] = '1'
			end
			if r then
			  result[3] = '1'
			  result[4] = r.revision
			  result[5] = opt(r.state)
			  result[6] = opt(r.checkpoint)
			  result[7] = opt(r.cp_revision)
			end
			for i = 3, 4 do
			  local all = redis.call('HGETALL', KEYS[i])
			  table.insert(result, tostring(#all))
			  for _, v in ipairs(all) do
			    table.insert(result, v)
			  end
			end
			return result
			""";

	/** KEYS: ns, run, journal, outbox. Read-only deployment probe; writes nothing. */
	static final String SLOT_PROBE = PRELUDE + """
			local bad = wrong_type()
			if bad then
			  return corrupt(bad, 'type')
			end
			return {'OK'}
			""";

	private RedisProgressScripts() {
	}

}
