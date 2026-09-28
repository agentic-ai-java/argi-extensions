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
package io.github.agentic.spring.ai.graph.persistence.mongodb;

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

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Real MongoDB compatibility tests between Core MongoSaver and the copied MongoDB
 * persistence extension.
 */
@Testcontainers(disabledWithoutDocker = true)
class MongoPersistenceCompatibilityTests {

	private static final String DB_NAME = "check_point_db";

	private static final String THREAD_META_COLLECTION = "thread_meta";

	private static final String CHECKPOINT_COLLECTION = "checkpoint_collection";

	private static final String THREAD_META_PREFIX = "mongo:thread:meta:";

	private static final String CHECKPOINT_PREFIX = "mongo:checkpoint:content:";

	private static final String DOCUMENT_CONTENT_KEY = "checkpoint_content";

	private static final String FIELD_THREAD_ID = "thread_id";

	private static final String FIELD_IS_RELEASED = "is_released";

	private static final StateSerializer STATE_SERIALIZER = new SpringAIJacksonStateSerializer(OverAllState::new);

	private static final Serializer<Checkpoint> CHECKPOINT_SERIALIZER = new CheckPointSerializer(STATE_SERIALIZER);

	@Container
	private static final MongoDBContainer MONGODB = new MongoDBContainer(DockerImageName.parse("mongo:6.0.24"));

	private static MongoClient mongoClient;

	@AfterAll
	static void tearDown() {
		if (mongoClient != null) {
			mongoClient.close();
		}
	}

	@Test
	void extensionReadsCoreMongoSaverWritesWithCompatibleDocuments() throws Exception {
		MongoClient client = mongoClient();
		var oldWriter = io.github.agentic.spring.ai.graph.checkpoint.savers.mongo.MongoSaver.builder()
			.client(client)
			.stateSerializer(STATE_SERIALIZER)
			.build();
		var newReader = MongoSaver.builder().client(client).stateSerializer(STATE_SERIALIZER).build();
		String threadName = "mongo-old-new-" + UUID.randomUUID();
		Checkpoint checkpoint = checkpoint("core");

		assertRoundTrip(oldWriter, newReader, threadName, checkpoint);
		assertPersistedDocuments(threadName, checkpoint);
	}

	@Test
	void coreMongoSaverReadsExtensionWritesWithCompatibleDocuments() throws Exception {
		MongoClient client = mongoClient();
		var newWriter = MongoSaver.builder().client(client).stateSerializer(STATE_SERIALIZER).build();
		var oldReader = io.github.agentic.spring.ai.graph.checkpoint.savers.mongo.MongoSaver.builder()
			.client(client)
			.stateSerializer(STATE_SERIALIZER)
			.build();
		String threadName = "mongo-new-old-" + UUID.randomUUID();
		Checkpoint checkpoint = checkpoint("extension");

		assertRoundTrip(newWriter, oldReader, threadName, checkpoint);
		assertPersistedDocuments(threadName, checkpoint);
	}

	private static MongoClient mongoClient() {
		if (mongoClient == null) {
			mongoClient = MongoClients.create(MONGODB.getReplicaSetUrl());
		}
		return mongoClient;
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

	private static void assertPersistedDocuments(String threadName, Checkpoint checkpoint) throws Exception {
		MongoCollection<Document> threadMeta = mongoClient().getDatabase(DB_NAME).getCollection(THREAD_META_COLLECTION);
		Document metaDocument = threadMeta.find(new Document("_id", THREAD_META_PREFIX + threadName)).first();
		assertThat(metaDocument).isNotNull();
		assertThat(metaDocument.getString("_id")).isEqualTo(THREAD_META_PREFIX + threadName);
		assertThat(metaDocument.getString(FIELD_THREAD_ID)).isNotBlank();
		assertThat(metaDocument.getBoolean(FIELD_IS_RELEASED, true)).isFalse();

		String checkpointDocumentId = CHECKPOINT_PREFIX + metaDocument.getString(FIELD_THREAD_ID);
		MongoCollection<Document> checkpoints = mongoClient().getDatabase(DB_NAME).getCollection(CHECKPOINT_COLLECTION);
		Document checkpointDocument = checkpoints.find(new Document("_id", checkpointDocumentId)).first();
		assertThat(checkpointDocument).isNotNull();
		assertThat(checkpointDocument.getString("_id")).isEqualTo(checkpointDocumentId);
		assertThat(checkpointDocument.containsKey(DOCUMENT_CONTENT_KEY)).isTrue();
		assertThat(checkpointDocument.keySet()).containsExactlyInAnyOrder("_id", DOCUMENT_CONTENT_KEY);

		String serializedContent = checkpointDocument.getString(DOCUMENT_CONTENT_KEY);
		assertThat(serializedContent).isNotBlank();
		assertThat(Base64.getDecoder().decode(serializedContent)).isNotEmpty();
		LinkedList<Checkpoint> serializedCheckpoints = deserializeCheckpoints(serializedContent);
		assertThat(serializedCheckpoints).singleElement().satisfies(actual -> assertCheckpoint(actual, checkpoint));
	}

	private static LinkedList<Checkpoint> deserializeCheckpoints(String content) throws Exception {
		byte[] bytes = Base64.getDecoder().decode(content);
		try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
				ObjectInputStream ois = new ObjectInputStream(bais)) {
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
			.state(Map.of("writer", writerName, "store", "mongodb", "sequence", 1, "tags", List.of("compat", "mongo")))
			.build();
	}

	private static void assertCheckpoint(Checkpoint actual, Checkpoint expected) {
		assertThat(actual.getId()).isEqualTo(expected.getId());
		assertThat(actual.getNodeId()).isEqualTo(expected.getNodeId());
		assertThat(actual.getNextNodeId()).isEqualTo(expected.getNextNodeId());
		assertThat(actual.getState()).isEqualTo(expected.getState());
	}

}
