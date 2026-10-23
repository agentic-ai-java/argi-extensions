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
package io.github.agentic.ai.observation.tool;

import io.micrometer.observation.Observation.Context;
import org.jspecify.annotations.Nullable;
import org.springframework.ai.chat.messages.AssistantMessage.ToolCall;
import org.springframework.util.Assert;

public class ToolCallingObservationContext extends Context {

	private ToolCall toolCall;

	private final @Nullable String description;

	private final boolean returnDirect;

	private @Nullable String toolResult;

	public static Builder builder() {
		return new Builder();
	}

	public ToolCall getToolCall() {
		return toolCall;
	}

	public @Nullable String getDescription() {
		return description;
	}

	public boolean isReturnDirect() {
		return returnDirect;
	}

	public void setToolResult(@Nullable String toolResult) {
		this.toolResult = toolResult;
	}

	public @Nullable String getToolResult() {
		return toolResult;
	}

	public ToolCallingObservationContext(ToolCall toolCall, @Nullable String description, boolean returnDirect) {
		Assert.notNull(toolCall, "toolCall cannot be null");
		this.toolCall = toolCall;
		this.description = description;
		this.returnDirect = returnDirect;
	}

	public static final class Builder {

		private @Nullable ToolCall toolCall;

		private @Nullable String description;

		private boolean returnDirect;

		private Builder() {
		}

		public Builder toolCall(@Nullable ToolCall toolCall) {
			this.toolCall = toolCall;
			return this;
		}

		public Builder description(@Nullable String description) {
			this.description = description;
			return this;
		}

		public Builder returnDirect(boolean returnDirect) {
			this.returnDirect = returnDirect;
			return this;
		}

		public ToolCallingObservationContext build() {
			Assert.notNull(this.toolCall, "toolCall cannot be null");
			return new ToolCallingObservationContext(this.toolCall, this.description, this.returnDirect);
		}

	}

}
