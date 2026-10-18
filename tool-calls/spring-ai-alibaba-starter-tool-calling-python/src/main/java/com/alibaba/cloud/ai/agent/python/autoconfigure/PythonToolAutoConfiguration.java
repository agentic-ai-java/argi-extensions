/*
 * Copyright 2024-2025 the original author or authors.
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
package com.alibaba.cloud.ai.agent.python.autoconfigure;

import com.alibaba.cloud.ai.agent.python.tool.PythonTool;
import org.graalvm.polyglot.Engine;
import org.springframework.ai.tool.ToolCallback;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;

/**
 * Auto-configuration for Python Tool.
 *
 * This configuration automatically registers the PythonTool as a ToolCallback
 * when GraalVM polyglot is available on the classpath.
 *
 * @author Spring AI Alibaba
 */
@AutoConfiguration
@ConditionalOnClass({ Engine.class })
@ConditionalOnProperty(prefix = "spring.ai.alibaba.python.tool", name = "enabled", havingValue = "true", matchIfMissing = true)
public class PythonToolAutoConfiguration {

	@Bean
	@ConditionalOnMissingBean
	public PythonTool pythonTool() {
		return new PythonTool();
	}

	@Bean
	@ConditionalOnMissingBean(name = "pythonToolCallback")
	public ToolCallback pythonToolCallback(PythonTool pythonTool) {
		return PythonTool.createPythonToolCallback(PythonTool.DESCRIPTION);
	}

}

