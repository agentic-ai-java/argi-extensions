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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.redisson.Redisson;
import org.redisson.api.RList;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.ai.chat.messages.Message;
import org.springframework.ai.chat.messages.UserMessage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class RedissonSaveAllFailureTest {

	private final RedissonClient client = mock(RedissonClient.class);

	@SuppressWarnings("unchecked")
	private final RList<String> redisList = mock(RList.class);

	private final List<String> stored = new ArrayList<>();

	private MockedStatic<Redisson> redisson;

	private RedissonRedisChatMemoryRepository repository;

	@BeforeEach
	void setUp() {
		redisson = mockStatic(Redisson.class);
		redisson.when(() -> Redisson.create(any(Config.class))).thenReturn(client);
		repository = RedissonRedisChatMemoryRepository.builder().build();
		when(client.<String>getList("argi_chat_memory:conversation")).thenReturn(redisList);
		when(redisList.readAll()).thenAnswer(invocation -> new ArrayList<>(stored));
		when(redisList.delete()).thenAnswer(invocation -> {
			boolean existed = !stored.isEmpty();
			stored.clear();
			return existed;
		});
		doAnswer(invocation -> stored.addAll(invocation.getArgument(0))).when(redisList).addAll(any());
		stored.add(repository.serializeMessage(new UserMessage("old history")));
	}

	@AfterEach
	void tearDown() {
		redisson.close();
	}

	@ParameterizedTest
	@ValueSource(ints = { 0, 1 })
	void serializationFailurePreservesOldHistory(int failureIndex) {
		var replacement = new ArrayList<Message>(List.of(new UserMessage("valid replacement")));
		replacement.add(failureIndex,
				UserMessage.builder()
					.text("unserializable replacement")
					.metadata(Map.of("cycle", new SelfReference()))
					.build());

		assertThatThrownBy(() -> repository.saveAll("conversation", replacement)).isInstanceOf(RuntimeException.class)
			.hasMessage("Error serializing message");

		assertThat(repository.findByConversationId("conversation")).extracting(Message::getText)
			.containsExactly("old history");
		verify(redisList, never()).delete();
		verify(redisList, never()).addAll(any());
	}

	@Test
	void invalidMessagesDoNotAccessRedis() {
		assertThatThrownBy(() -> repository.saveAll("conversation", null)).isInstanceOf(IllegalArgumentException.class);
		assertThatThrownBy(() -> repository.saveAll("conversation", Arrays.asList(new UserMessage("valid"), null)))
			.isInstanceOf(IllegalArgumentException.class);
		verifyNoInteractions(client, redisList);
	}

	@ParameterizedTest
	@ValueSource(strings = { "", " " })
	void invalidConversationIdDoesNotAccessRedis(String conversationId) {
		assertThatThrownBy(() -> repository.saveAll(conversationId, List.of(new UserMessage("valid"))))
			.isInstanceOf(IllegalArgumentException.class);
		verifyNoInteractions(client, redisList);
	}

	@Test
	void validMessagesReplaceOldHistoryInOrder() {
		repository.saveAll("conversation", List.of(new UserMessage("first"), new UserMessage("second")));

		assertThat(repository.findByConversationId("conversation")).extracting(Message::getText)
			.containsExactly("first", "second");
	}

	@Test
	void emptyMessagesClearOldHistory() {
		repository.saveAll("conversation", List.of());

		assertThat(repository.findByConversationId("conversation")).isEmpty();
		verify(redisList).delete();
	}

	static class SelfReference {

		public final SelfReference self = this;

	}

}
