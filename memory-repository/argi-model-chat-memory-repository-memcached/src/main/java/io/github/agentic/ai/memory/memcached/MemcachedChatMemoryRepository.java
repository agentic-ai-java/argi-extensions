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
package io.github.agentic.ai.memory.memcached;

import io.github.agentic.ai.memory.memcached.serializer.MessageDeserializer;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import net.spy.memcached.MemcachedClient;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.module.SimpleModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ai.chat.memory.ChatMemoryRepository;
import org.springframework.ai.chat.messages.Message;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Memcached implementation of ChatMemoryRepository auth: dahua
 */
public class MemcachedChatMemoryRepository implements ChatMemoryRepository, AutoCloseable {

	private static final Logger logger = LoggerFactory.getLogger(MemcachedChatMemoryRepository.class);

	private final MemcachedClient memcachedClient;

	private final JsonMapper jsonMapper;

	private static final String DEFAULT_CONVERSATION = "argi_chat_memory_conversation";

	private static final String DEFAULT_KEY_PREFIX = "argi_chat_memory:";

	public MemcachedChatMemoryRepository(MemcachedClient memcachedClient) {
		this.memcachedClient = memcachedClient;
		SimpleModule module = new SimpleModule();
		module.addDeserializer(Message.class, new MessageDeserializer());
		this.jsonMapper = JsonMapper.builder()
				.changeDefaultVisibility(vc -> vc.withGetterVisibility(JsonAutoDetect.Visibility.NONE)
						.withSetterVisibility(JsonAutoDetect.Visibility.NONE)
						.withFieldVisibility(JsonAutoDetect.Visibility.ANY))
				.addModule(module)
				.build();
	}

	@Override
	public void close() {
		if (this.memcachedClient != null) {
			this.memcachedClient.shutdown();
		}
	}

	@Override
	public List<String> findConversationIds() {
		try {
			Object result = this.memcachedClient.get(DEFAULT_CONVERSATION);
			if (result instanceof List<?> conversationIds) {
				return conversationIds.stream().filter(String.class::isInstance).map(String.class::cast).toList();
			}
		}
		catch (Exception e) {
			logger.error("Get conversation IDs from memcached failed: {}", e.getMessage(), e);
		}
		return List.of();
	}

	@Override
	@SuppressWarnings("unchecked")
	public List<Message> findByConversationId(String conversationId) {
		try {
			Object apply = this.memcachedClient.get(DEFAULT_KEY_PREFIX + conversationId);
			if (apply instanceof List<?> messageList) {
				return messageList.stream()
						.filter(String.class::isInstance)
						.map(String.class::cast)
						.map(messageStr -> jsonMapper.readValue(messageStr, Message.class))
						.toList();
			}
		}
		catch (Exception e) {
			logger.error("Get messages for conversation {} failed: {}", conversationId, e.getMessage(), e);
		}
		return List.of();
	}

	@Override
	public void saveAll(String conversationId, List<Message> messages) {
		try {
			List<String> conversationIds = new ArrayList<>(findConversationIds());
			conversationIds.remove(conversationId);
			conversationIds.add(conversationId);
			this.memcachedClient.set(DEFAULT_CONVERSATION, 0, conversationIds);
			List<String> serializingMessage = messages.stream().map(this.jsonMapper::writeValueAsString).toList();
			this.memcachedClient.set(DEFAULT_KEY_PREFIX + conversationId, 0, serializingMessage);
		}
		catch (Exception e) {
			logger.error("Save messages for conversation {} failed: {}", conversationId, e.getMessage(), e);
		}
	}

	@Override
	public void deleteByConversationId(String conversationId) {
		try {
			List<String> conversationIds = new ArrayList<>(findConversationIds());
			conversationIds.remove(conversationId);
			this.memcachedClient.set(DEFAULT_CONVERSATION, 0, conversationIds);
			this.memcachedClient.delete(DEFAULT_KEY_PREFIX + conversationId);
		}
		catch (Exception e) {
			logger.error("Delete conversation {} failed: {}", conversationId, e.getMessage(), e);
		}
	}

	public void clearOverLimit(String conversationId, int maxLimit, int deleteSize) {
		final int finalDeleteSize = Math.min(deleteSize, maxLimit);
		List<Message> messages = findByConversationId(conversationId);
		List<Message> lastMessages = new ArrayList<>();
		AtomicInteger index = new AtomicInteger(0);
		if (messages.size() >= maxLimit) {
			messages.forEach(message -> {
				if (index.get() >= finalDeleteSize) {
					lastMessages.add(message);
				}
				index.incrementAndGet();
			});
		}
		saveAll(conversationId, lastMessages);
	}

}
