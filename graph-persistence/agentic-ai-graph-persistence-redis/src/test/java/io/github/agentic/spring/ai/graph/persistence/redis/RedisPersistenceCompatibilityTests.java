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
package io.github.agentic.spring.ai.graph.persistence.redis;

import io.github.agentic.spring.ai.graph.OverAllState;
import io.github.agentic.spring.ai.graph.RunnableConfig;
import io.github.agentic.spring.ai.graph.checkpoint.BaseCheckpointSaver;
import io.github.agentic.spring.ai.graph.checkpoint.Checkpoint;
import io.github.agentic.spring.ai.graph.serializer.Serializer;
import io.github.agentic.spring.ai.graph.serializer.StateSerializer;
import io.github.agentic.spring.ai.graph.serializer.check_point.CheckPointSerializer;
import io.github.agentic.spring.ai.graph.serializer.plain_text.jackson.SpringAIJacksonStateSerializer;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.util.Base64;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.redisson.Redisson;
import org.redisson.api.RBucket;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Real Redis compatibility tests between Core RedisSaver and the copied Redis
 * persistence extension.
 */
@Testcontainers(disabledWithoutDocker = true)
class RedisPersistenceCompatibilityTests {

	private static final String CHECKPOINT_PREFIX = "graph:checkpoint:content:";

	private static final String THREAD_META_PREFIX = "graph:thread:meta:";

	private static final String FIELD_THREAD_ID = "thread_id";

	private static final StateSerializer STATE_SERIALIZER = new SpringAIJacksonStateSerializer(OverAllState::new);

	private static final Serializer<Checkpoint> CHECKPOINT_SERIALIZER = new CheckPointSerializer(STATE_SERIALIZER);

	@Container
	private static final GenericContainer<?> REDIS = new GenericContainer<>(DockerImageName.parse("valkey/valkey:8.1.2"))
		.withExposedPorts(6379);

	private static RedissonClient redisson;

	@BeforeAll
	static void setup() {
		Config config = new Config();
		config.useSingleServer().setAddress("redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379));
		redisson = Redisson.create(config);
	}

	@AfterAll
	static void tearDown() {
		if (redisson != null) {
			redisson.shutdown();
		}
	}

	@Test
	void extensionReadsCoreRedisSaverWritesWithByteCompatibleKeysAndContent() throws Exception {
		var oldWriter = io.github.agentic.spring.ai.graph.checkpoint.savers.redis.RedisSaver.builder()
			.redisson(redisson)
			.stateSerializer(STATE_SERIALIZER)
			.build();
		var newReader = RedisSaver.builder().redisson(redisson).stateSerializer(STATE_SERIALIZER).build();
		String threadName = "redis-old-new-" + UUID.randomUUID();
		Checkpoint checkpoint = checkpoint("core");

		assertRoundTrip(oldWriter, newReader, threadName, checkpoint);
		assertRedisKeysAndSerializedContent(threadName, checkpoint);
	}

	@Test
	void coreRedisSaverReadsExtensionWritesWithByteCompatibleKeysAndContent() throws Exception {
		var newWriter = RedisSaver.builder().redisson(redisson).stateSerializer(STATE_SERIALIZER).build();
		var oldReader = io.github.agentic.spring.ai.graph.checkpoint.savers.redis.RedisSaver.builder()
			.redisson(redisson)
			.stateSerializer(STATE_SERIALIZER)
			.build();
		String threadName = "redis-new-old-" + UUID.randomUUID();
		Checkpoint checkpoint = checkpoint("extension");

		assertRoundTrip(newWriter, oldReader, threadName, checkpoint);
		assertRedisKeysAndSerializedContent(threadName, checkpoint);
	}

	@Test
	void extensionRedisSaverRetainsLatestCheckpoints() throws Exception {
		var saver = RedisSaver.builder().redisson(redisson).stateSerializer(STATE_SERIALIZER).build();
		String threadName = "redis-retention-" + UUID.randomUUID();
		RunnableConfig config = RunnableConfig.builder()
			.threadId(threadName)
			.checkpointsNumRetained(2)
			.build();

		Checkpoint checkpoint1 = checkpointWithId("cp1", 1);
		Checkpoint checkpoint2 = checkpointWithId("cp2", 2);
		Checkpoint checkpoint3 = checkpointWithId("cp3", 3);

		saver.put(config, checkpoint1);
		saver.put(config, checkpoint2);
		saver.put(config, checkpoint3);

		assertThat(saver.list(config)).extracting(Checkpoint::getId).containsExactly("cp3", "cp2");
		assertThat(saver.get(RunnableConfig.builder(config).checkPointId("cp1").build())).isEmpty();
	}

	@Test
	void extensionRedisSaverRetainsLatestCheckpointsAfterUpdate() throws Exception {
		var saver = RedisSaver.builder().redisson(redisson).stateSerializer(STATE_SERIALIZER).build();
		String threadName = "redis-retention-update-" + UUID.randomUUID();
		RunnableConfig config = RunnableConfig.builder().threadId(threadName).build();

		saver.put(config, checkpointWithId("cp1", 1));
		saver.put(config, checkpointWithId("cp2", 2));
		saver.put(config, checkpointWithId("cp3", 3));

		RunnableConfig updateConfig = RunnableConfig.builder(config)
			.checkPointId("cp2")
			.checkpointsNumRetained(2)
			.build();
		Checkpoint updatedCheckpoint = checkpointWithId("cp2", 20);

		saver.put(updateConfig, updatedCheckpoint);

		assertThat(saver.list(config)).extracting(Checkpoint::getId).containsExactly("cp3", "cp2");
		assertThat(saver.get(updateConfig)).hasValueSatisfying(actual -> {
			assertThat(actual.getId()).isEqualTo("cp2");
			assertThat(actual.getState()).containsEntry("version", 20);
		});
		assertThat(saver.get(RunnableConfig.builder(config).checkPointId("cp1").build())).isEmpty();
	}

	private static void assertRoundTrip(BaseCheckpointSaver writer, BaseCheckpointSaver reader, String threadName,
			Checkpoint checkpoint) throws Exception {
		RunnableConfig config = RunnableConfig.builder().threadId(threadName).build();

		RunnableConfig savedConfig = writer.put(config, checkpoint);

		assertThat(savedConfig.checkPointId()).contains(checkpoint.getId());
		Optional<Checkpoint> restored = reader.get(config);
		assertThat(restored).isPresent();
		assertCheckpoint(restored.get(), checkpoint);
		assertThat(reader.list(config)).singleElement().satisfies(actual -> assertCheckpoint(actual, checkpoint));
	}

	private static void assertRedisKeysAndSerializedContent(String threadName, Checkpoint checkpoint) throws Exception {
		RMap<String, String> meta = redisson.getMap(THREAD_META_PREFIX + threadName);
		assertThat(meta.isExists()).isTrue();
		String threadId = meta.get(FIELD_THREAD_ID);
		assertThat(threadId).isNotBlank();

		RBucket<String> contentBucket = redisson.getBucket(CHECKPOINT_PREFIX + threadId);
		assertThat(contentBucket.isExists()).isTrue();
		String serializedContent = contentBucket.get();
		assertThat(serializedContent).isNotBlank();
		assertThat(Base64.getDecoder().decode(serializedContent)).isNotEmpty();

		LinkedList<Checkpoint> serializedCheckpoints = deserializeCheckpoints(serializedContent);
		assertThat(serializedCheckpoints).singleElement().satisfies(actual -> assertCheckpoint(actual, checkpoint));
	}

	private static LinkedList<Checkpoint> deserializeCheckpoints(String content) throws Exception {
		byte[] bytes = Base64.getDecoder().decode(content);
		try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes); ObjectInputStream ois = new ObjectInputStream(bais)) {
			int size = ois.readInt();
			LinkedList<Checkpoint> checkpoints = new LinkedList<>();
			for (int i = 0; i < size; i++) {
				checkpoints.add(CHECKPOINT_SERIALIZER.read(ois));
			}
			return checkpoints;
		}
	}

	private static Checkpoint checkpoint(String writerName) {
		return Checkpoint.builder()
			.id("checkpoint-" + UUID.randomUUID())
			.nodeId("node-" + writerName)
			.nextNodeId("next")
			.state(Map.of("writer", writerName, "dialect", "redis", "sequence", 1, "tags", List.of("compat", "redis")))
			.build();
	}

	private static Checkpoint checkpointWithId(String id, int version) {
		return Checkpoint.builder()
			.id(id)
			.nodeId("node-" + id)
			.nextNodeId("next-" + id)
			.state(Map.of("version", version))
			.build();
	}

	private static void assertCheckpoint(Checkpoint actual, Checkpoint expected) {
		assertThat(actual.getId()).isEqualTo(expected.getId());
		assertThat(actual.getNodeId()).isEqualTo(expected.getNodeId());
		assertThat(actual.getNextNodeId()).isEqualTo(expected.getNextNodeId());
		assertThat(actual.getState()).isEqualTo(expected.getState());
	}

}
