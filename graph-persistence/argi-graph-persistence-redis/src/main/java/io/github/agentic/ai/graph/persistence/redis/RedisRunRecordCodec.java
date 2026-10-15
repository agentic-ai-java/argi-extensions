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

import io.github.agentic.ai.graph.execution.run.RunEntry;
import io.github.agentic.ai.graph.execution.run.RunNamespace;
import io.github.agentic.ai.graph.execution.run.RunPayload;
import io.github.agentic.ai.graph.execution.run.RunSnapshot;
import io.github.agentic.ai.graph.execution.run.RunSpec;
import io.github.agentic.ai.graph.execution.run.RunSpecCodec;
import io.github.agentic.ai.graph.execution.run.RunState;
import io.github.agentic.ai.graph.execution.run.TaskRecord;
import io.github.agentic.ai.graph.execution.run.TaskSpec;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;

/**
 * Strict data-only codec for stored run records. Every decode goes through the record
 * constructors, so canonical JSON and core validation are re-applied, and integrity
 * failures surface as {@link IllegalStateException} instead of being repaired.
 */
final class RedisRunRecordCodec {

	static final String V = "v";

	static final String SPEC = "spec";

	static final String FP = "fp";

	static final String REVISION = "revision";

	static final String STATE = "state";

	static final String CHECKPOINT = "checkpoint";

	private static final Set<String> RUN_FIELDS = Set.of(V, SPEC, FP, REVISION, STATE, CHECKPOINT);

	private static final JsonMapper MAPPER = JsonMapper.builder()
		.enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
		.disable(MapperFeature.ALLOW_COERCION_OF_SCALARS)
		.enable(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY)
		.enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
		.enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
		.enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
		.enable(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES)
		.enable(DeserializationFeature.FAIL_ON_MISSING_CREATOR_PROPERTIES)
		.disable(DeserializationFeature.ACCEPT_FLOAT_AS_INT)
		.enable(DeserializationFeature.FAIL_ON_NUMBERS_FOR_ENUMS)
		.build();

	private final RunSpecCodec specs = new RunSpecCodec();

	String encodeSpec(RunSpec spec) {
		return specs.encodeRun(spec);
	}

	String encodeTask(TaskRecord task) {
		return write(new StoredTask(task.revision(), specs.encodeTask(task.spec()), task.state()));
	}

	String encodeEntry(RunEntry entry) {
		return write(entry);
	}

	String encodePayload(RunPayload payload) {
		return write(payload);
	}

	/**
	 * Decodes and cross-checks one run. The hashes are the raw field maps of the run,
	 * tasks, journal and outbox keys; keys are only used in error messages.
	 */
	RunSnapshot decode(RunNamespace namespace, String runId, List<Object> keys, Map<String, String> run,
			Map<String, String> tasks, Map<String, String> journal, Map<String, String> outbox) {
		String runKey = (String) keys.get(1);
		for (String field : run.keySet()) {
			if (!RUN_FIELDS.contains(field)) {
				throw corrupt(runKey, field, null);
			}
		}
		if (!"1".equals(run.get(V))) {
			throw corrupt(runKey, V, null);
		}
		RunSpec spec = guard(runKey, SPEC, () -> specs.decodeRun(require(run, SPEC)));
		if (!spec.namespace().equals(namespace) || !spec.runId().equals(runId)) {
			throw corrupt(runKey, SPEC, null);
		}
		if (!RedisRunKeys.fingerprint(spec).equals(run.get(FP))) {
			throw corrupt(runKey, FP, null);
		}
		long revision = parseRevision(runKey, REVISION, run.get(REVISION));
		RunState state = guard(runKey, STATE, () -> RunState.valueOf(require(run, STATE)));
		String checkpointJson = run.get(CHECKPOINT);
		RunPayload checkpoint = checkpointJson == null ? null
				: guard(runKey, CHECKPOINT, () -> read(checkpointJson, RunPayload.class));
		String tasksKey = (String) keys.get(2);
		Map<String, TaskRecord> decodedTasks = new HashMap<>();
		tasks.forEach((taskId, json) -> {
			TaskRecord task = guard(tasksKey, taskId, () -> {
				StoredTask stored = read(json, StoredTask.class);
				TaskSpec taskSpec = specs.decodeTask(stored.spec());
				return new TaskRecord(taskSpec, stored.revision(), stored.state());
			});
			if (!task.spec().taskId().equals(taskId) || !task.spec().runId().equals(runId)) {
				throw corrupt(tasksKey, taskId, null);
			}
			decodedTasks.put(taskId, task);
		});
		Map<String, RunEntry> decodedJournal = decodeEntries((String) keys.get(3), journal, false);
		Map<String, RunEntry> decodedOutbox = decodeEntries((String) keys.get(4), outbox, true);
		return guard(runKey, REVISION, () -> new RunSnapshot(spec, revision, state, checkpoint, decodedTasks,
				decodedJournal, decodedOutbox));
	}

	static IllegalStateException corrupt(String key, String field, Throwable cause) {
		return new IllegalStateException("Redis run store data is corrupt: " + key + "/" + field, cause);
	}

	static long parseRevision(String key, String field, String value) {
		if (value == null || !value.matches("0|[1-9][0-9]{0,18}")) {
			throw corrupt(key, field, null);
		}
		try {
			return Long.parseLong(value);
		}
		catch (NumberFormatException ex) {
			throw corrupt(key, field, ex);
		}
	}

	private Map<String, RunEntry> decodeEntries(String key, Map<String, String> raw, boolean outbox) {
		Map<String, RunEntry> result = new HashMap<>();
		raw.forEach((id, json) -> {
			RunEntry entry = guard(key, id, () -> read(json, RunEntry.class));
			if (!entry.id().equals(id) || (outbox && entry.kind() != RunEntry.Kind.EVENT)) {
				throw corrupt(key, id, null);
			}
			result.put(id, entry);
		});
		return result;
	}

	private static String require(Map<String, String> hash, String field) {
		String value = hash.get(field);
		if (value == null) {
			throw new IllegalArgumentException(field + " is missing");
		}
		return value;
	}

	private static <T> T guard(String key, String field, Decoder<T> decoder) {
		try {
			return decoder.decode();
		}
		catch (JsonProcessingException | RuntimeException ex) {
			throw corrupt(key, field, ex);
		}
	}

	private static <T> T read(String json, Class<T> type) throws JsonProcessingException {
		T result = MAPPER.readValue(json, type);
		if (result == null) {
			throw new IllegalArgumentException("record cannot be null");
		}
		return result;
	}

	private static String write(Object value) {
		try {
			return MAPPER.writeValueAsString(value);
		}
		catch (JsonProcessingException ex) {
			throw new IllegalArgumentException("Cannot encode run record", ex);
		}
	}

	@FunctionalInterface
	private interface Decoder<T> {

		T decode() throws JsonProcessingException;

	}

	/** Stored task envelope; the spec keeps the core {@link RunSpecCodec} encoding. */
	record StoredTask(long revision, String spec, RunState state) {

		StoredTask {
			if (spec == null || state == null) {
				throw new IllegalArgumentException("task record is incomplete");
			}
		}
	}

}
