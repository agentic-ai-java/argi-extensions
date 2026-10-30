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

import io.github.agentic.ai.memory.redis.serializer.MessageDeserializer;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.module.SimpleModule;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ai.chat.memory.ChatMemoryRepository;
import org.springframework.ai.chat.messages.Message;

/**
 * Base class for Redis-based chat memory repositories
 *
 * @author benym
 * @since 2025/7/31 0:05
 */
public abstract class BaseRedisChatMemoryRepository implements ChatMemoryRepository, AutoCloseable {

	protected static final Logger logger = LoggerFactory.getLogger(BaseRedisChatMemoryRepository.class);

	protected static final String DEFAULT_KEY_PREFIX = "argi_chat_memory:";

	private final String keyPrefix;

	protected final JsonMapper jsonMapper;

	public BaseRedisChatMemoryRepository() {
		this(null);
	}

	/**
	 * Capture this repository's namespace without changing other repository instances.
	 * @param keyPrefix the key prefix, or null to use the default
	 */
	protected BaseRedisChatMemoryRepository(@Nullable String keyPrefix) {
		this.keyPrefix = keyPrefix != null ? keyPrefix : DEFAULT_KEY_PREFIX;
        SimpleModule module = new SimpleModule();
        module.addDeserializer(Message.class, new MessageDeserializer());
		this.jsonMapper = JsonMapper.builder()
            .changeDefaultVisibility(vc -> vc.withGetterVisibility(JsonAutoDetect.Visibility.NONE)
                .withSetterVisibility(JsonAutoDetect.Visibility.NONE)
                .withFieldVisibility(JsonAutoDetect.Visibility.ANY))
            .addModule(module)
			.build();
	}

	protected @Nullable Message deserializeMessage(String messageStr) {
		try {
			return jsonMapper.readValue(messageStr, Message.class);
		}
		catch (JacksonException e) {
			// Parser messages and their cause chains can contain the stored message.
			logger.error("Deserialization error for Redis message (length={}, errorType={})",
					messageStr.length(), e.getClass().getSimpleName());
			return null;
		}
	}

	protected String serializeMessage(Message message) {
		try {
			return jsonMapper.writeValueAsString(message);
		}
		catch (JacksonException e) {
			throw new RuntimeException("Error serializing message", e);
		}
	}

	protected String getKeyPrefix() {
		return this.keyPrefix;
	}
}
