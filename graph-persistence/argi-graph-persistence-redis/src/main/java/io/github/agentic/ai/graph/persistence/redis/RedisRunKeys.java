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

import io.github.agentic.ai.graph.execution.run.RunNamespace;
import io.github.agentic.ai.graph.execution.run.RunSpec;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;

/**
 * Derives every Redis key in Java. All keys of one namespace share the hash tag
 * {@code {T}}, so a single script can declare all of them in KEYS even on a cluster.
 */
final class RedisRunKeys {

	private final String prefix;

	RedisRunKeys(String prefix) {
		this.prefix = prefix;
	}

	/** Length-prefixed, injective encoding of the namespace identity. */
	static String tuple(RunNamespace namespace) {
		StringBuilder result = new StringBuilder();
		append(result, namespace.tenant().authority());
		append(result, namespace.tenant().id());
		append(result, namespace.applicationId());
		append(result, namespace.sessionId());
		return result.toString();
	}

	/** Dedup fingerprint over everything a retry with the same submission key must match. */
	static String fingerprint(RunSpec spec) {
		StringBuilder result = new StringBuilder();
		append(result, spec.inputHash());
		append(result, spec.graphId());
		append(result, spec.graphVersion());
		append(result, spec.context().principal().authority());
		append(result, spec.context().principal().id());
		return HexFormat.of().formatHex(sha256(result.toString()));
	}

	String namespaceKey(RunNamespace namespace) {
		return base(namespace) + ":ns";
	}

	/** Returns {@code [ns, run, tasks, journal, outbox]}. */
	List<Object> runKeys(RunNamespace namespace, String runId) {
		String base = base(namespace);
		String run = base + ":run:" + Base64.getUrlEncoder().withoutPadding().encodeToString(sha256(runId));
		return List.of(base + ":ns", run, run + ":tasks", run + ":journal", run + ":outbox");
	}

	private String base(RunNamespace namespace) {
		return prefix + ":{" + Base64.getUrlEncoder().withoutPadding().encodeToString(sha256(tuple(namespace)))
				+ "}";
	}

	private static void append(StringBuilder target, String value) {
		target.append(value.getBytes(StandardCharsets.UTF_8).length).append(':').append(value);
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
