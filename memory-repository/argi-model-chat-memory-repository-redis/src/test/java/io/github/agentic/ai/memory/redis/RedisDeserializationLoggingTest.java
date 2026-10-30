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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.ThrowableProxyUtil;
import ch.qos.logback.core.read.ListAppender;
import io.github.agentic.ai.memory.redis.serializer.MessageDeserializer;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.LoggerFactory;
import org.springframework.ai.chat.messages.AssistantMessage;
import org.springframework.ai.chat.messages.Message;
import org.springframework.ai.chat.messages.ToolResponseMessage;
import org.springframework.ai.chat.messages.UserMessage;
import tools.jackson.core.JsonParser;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.JsonNode;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;
import static org.mockito.Mockito.when;

@ResourceLock("redis-deserialization-loggers")
class RedisDeserializationLoggingTest {

	static final String SECRET = "SYNTHETIC_PRIVATE_MARKER_54";

	private final BaseRedisChatMemoryRepository repository = mock(BaseRedisChatMemoryRepository.class,
			withSettings().useConstructor().defaultAnswer(CALLS_REAL_METHODS));

	static Stream<Level> logLevels() {
		return Stream.of(Level.INFO, Level.DEBUG);
	}

	@ParameterizedTest
	@MethodSource("logLevels")
	void validMessagesRetainContentWithoutLoggingIt(Level level) {
		List<Message> messages = List.of(
				UserMessage.builder().text(SECRET + "-body").metadata(Map.of("private", SECRET + "-metadata")).build(),
				AssistantMessage.builder().content(SECRET + "-assistant")
					.toolCalls(List.of(new AssistantMessage.ToolCall("id", "function", "tool", SECRET + "-arguments")))
					.build(),
				ToolResponseMessage.builder()
					.responses(List.of(new ToolResponseMessage.ToolResponse("id", "tool", SECRET + "-result")))
					.build());
		try (CapturedLogs logs = new CapturedLogs(level)) {
			for (Message message : messages) {
				Message restored = repository.deserializeMessage(repository.serializeMessage(message));
				assertThat(restored).isEqualTo(message);
			}
			assertThat(repository.deserializeMessage('"' + SECRET + '"').getText()).isEqualTo(SECRET);
			logs.assertSafe();
		}
	}

	static Stream<Arguments> malformedMessages() {
		return logLevels().flatMap(level -> Stream.of(
				"{\"content\":\"" + SECRET + "\",",
				"{\"content\":" + SECRET + "}",
				"{\"content\":\"" + "x".repeat(8192) + SECRET + "\",")
			.map(json -> Arguments.of(level, json)));
	}

	@ParameterizedTest
	@MethodSource("malformedMessages")
	void malformedMessageIsStillRejectedWithoutLoggingSourceOrException(Level level, String json) {
		try (CapturedLogs logs = new CapturedLogs(level)) {
			assertThat(repository.deserializeMessage(json)).isNull();
			logs.assertDiagnostic("Deserialization error");
			logs.assertSafe();
		}
	}

	static Stream<Arguments> malformedFields() {
		return logLevels().flatMap(level -> Stream.of(
				Arguments.of(level, "metadata", '"' + SECRET + '"', "metadata"),
				Arguments.of(level, "toolCalls", "[\"" + SECRET + "\"]", "toolCalls"),
				Arguments.of(level, "responses", "[\"" + SECRET + "\"]", "toolResponses")));
	}

	@ParameterizedTest
	@MethodSource("malformedFields")
	void malformedFieldsKeepRejectionWithoutLoggingExceptionMessages(Level level, String field, String value,
			String diagnostic) {
		String json = "{\"messageType\":\"USER\",\"content\":\"" + SECRET + "\",\"" + field + "\":" + value + "}";
		try (CapturedLogs logs = new CapturedLogs(level)) {
			Message restored = repository.deserializeMessage(json);
			assertThat(restored).isNull();
			logs.assertDiagnostic("Deserialization error");
			logs.assertSafe();
		}
	}

	@ParameterizedTest
	@MethodSource("malformedFields")
	void defensiveFieldFallbackDoesNotLogExceptionMessageOrCause(Level level, String field, String value,
			String diagnostic) {
		JsonNode root = mock(JsonNode.class);
		when(root.get("messageType")).thenReturn(repository.jsonMapper.valueToTree("USER"));
		when(root.get("content")).thenReturn(repository.jsonMapper.valueToTree(SECRET));
		// Exercise the existing IllegalArgumentException fallback independently of
		// Jackson's malformed-input exceptions, which are rejected by the repository.
		JsonNode failingNode = mock(JsonNode.class, invocation -> {
			if (invocation.getMethod().getName().equals("isArray")) {
				return true;
			}
			throw new IllegalArgumentException(SECRET, new IllegalStateException(SECRET + "-cause"));
		});
		when(root.get(field)).thenReturn(failingNode);
		JsonParser parser = mock(JsonParser.class);
		DeserializationContext context = mock(DeserializationContext.class);
		when(context.readTree(parser)).thenReturn(root);
		try (CapturedLogs logs = new CapturedLogs(level)) {
			Message restored = new MessageDeserializer().deserialize(parser, context);
			assertThat(restored).isInstanceOf(UserMessage.class);
			assertThat(restored.getText()).isEqualTo(SECRET);
			logs.assertDiagnostic("Failed to deserialize " + diagnostic);
			logs.assertSafe();
		}
	}

	@ParameterizedTest
	@MethodSource("logLevels")
	void unknownAndMissingTypesKeepFallbackWithoutEchoingUntrustedType(Level level) {
		try (CapturedLogs logs = new CapturedLogs(level)) {
			Message unknown = repository.deserializeMessage(
					"{\"messageType\":\"" + SECRET + "\",\"content\":\"" + SECRET + "\"}");
			Message missing = repository.deserializeMessage("{\"content\":\"" + SECRET + "\"}");
			assertThat(unknown).isInstanceOf(UserMessage.class);
			assertThat(missing).isInstanceOf(UserMessage.class);
			assertThat(unknown.getText()).isEqualTo(SECRET);
			assertThat(missing.getText()).isEqualTo(SECRET);
			logs.assertDiagnostic("Unknown message type");
			logs.assertDiagnostic("Message type not found");
			logs.assertSafe();
		}
	}

	static final class CapturedLogs implements AutoCloseable {

		private final List<Logger> loggers = List.of(
				(Logger) LoggerFactory.getLogger(BaseRedisChatMemoryRepository.class),
				(Logger) LoggerFactory.getLogger(MessageDeserializer.class));

		private final Level[] previousLevels = new Level[loggers.size()];

		private final ListAppender<ILoggingEvent> appender = new ListAppender<>();

		CapturedLogs(Level level) {
			appender.start();
			for (int i = 0; i < loggers.size(); i++) {
				Logger logger = loggers.get(i);
				previousLevels[i] = logger.getLevel();
				logger.setLevel(level);
				logger.addAppender(appender);
			}
		}

		void assertDiagnostic(String text) {
			assertThat(appender.list).anySatisfy(event -> assertThat(event.getFormattedMessage()).contains(text));
		}

		void assertSafe() {
			for (ILoggingEvent event : appender.list) {
				assertThat(event.getFormattedMessage()).doesNotContain(SECRET);
				assertThat(Arrays.deepToString(event.getArgumentArray())).doesNotContain(SECRET);
				if (event.getThrowableProxy() != null) {
					assertThat(ThrowableProxyUtil.asString(event.getThrowableProxy())).doesNotContain(SECRET);
				}
				// Parser exception chains can expose source text even with a safe log template.
				assertThat(event.getThrowableProxy()).isNull();
			}
		}

		@Override
		public void close() {
			for (int i = 0; i < loggers.size(); i++) {
				loggers.get(i).detachAppender(appender);
				loggers.get(i).setLevel(previousLevels[i]);
			}
			appender.stop();
		}
	}

}
