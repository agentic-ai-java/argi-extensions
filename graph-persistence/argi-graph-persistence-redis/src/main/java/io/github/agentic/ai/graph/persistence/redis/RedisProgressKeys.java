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
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.List;

/**
 * Derives every Redis key in Java. All keys of one session share the hash tag
 * {@code {T}}, so a single script can declare all of them in KEYS even on a cluster.
 */
final class RedisProgressKeys {

	private final String prefix;

	RedisProgressKeys(String prefix) {
		this.prefix = prefix;
	}

	/** Returns {@code [session, run, journal, outbox]} for one session and run. */
	List<Object> keys(String sessionId, String runId) {
		String base = base(sessionId);
		String run = base + ":run:" + Base64.getUrlEncoder().withoutPadding().encodeToString(sha256(runId));
		return List.of(base + ":ns", run, run + ":journal", run + ":outbox");
	}

	String sessionKey(String sessionId) {
		return base(sessionId) + ":ns";
	}

	private String base(String sessionId) {
		return prefix + ":{" + Base64.getUrlEncoder().withoutPadding().encodeToString(sha256(sessionId)) + "}";
	}

	private static byte[] sha256(String value) {
		try {
			return MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
		}
		catch (NoSuchAlgorithmException ex) {
			throw new IllegalStateException("SHA-256 is required by the Java platform", ex);
		}
	}

}
