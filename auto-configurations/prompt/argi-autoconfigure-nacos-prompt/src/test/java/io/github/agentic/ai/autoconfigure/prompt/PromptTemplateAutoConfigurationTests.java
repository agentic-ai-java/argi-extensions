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
package io.github.agentic.ai.autoconfigure.prompt;

import java.util.stream.Stream;

import io.github.agentic.ai.prompt.ConfigurablePromptTemplateFactory;
import io.github.agentic.ai.prompt.PromptTemplateBuilderConfigure;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

/**
 * @author yuluo
 * @author <a href="mailto:yuluo08290126@gmail.com">yuluo</a>
 */

@ResourceLock(Resources.SYSTEM_PROPERTIES)
class PromptTemplateAutoConfigurationTests {

	private static final String GLOBAL_ENABLED = "spring.nacos.config.enabled";

	private static final String PROMPT_ENABLED = "argi.nacos.prompt.template.enabled";

	private String originalGlobalEnabled;

	private String originalPromptEnabled;

	private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
		.withUserConfiguration(PromptTemplateAutoConfiguration.class);

	@BeforeEach
	void saveSystemProperties() {
		originalGlobalEnabled = System.getProperty(GLOBAL_ENABLED);
		originalPromptEnabled = System.getProperty(PROMPT_ENABLED);
		System.clearProperty(PROMPT_ENABLED);
	}

	@AfterEach
	void restoreSystemProperties() {
		setSystemProperty(GLOBAL_ENABLED, originalGlobalEnabled);
		setSystemProperty(PROMPT_ENABLED, originalPromptEnabled);
	}

	private static void setSystemProperty(String name, String value) {
		if (value == null) {
			System.clearProperty(name);
		}
		else {
			System.setProperty(name, value);
		}
	}

	static Stream<Arguments> switchCombinations() {
		return Stream.of("true", "false", null)
			.flatMap(prompt -> Stream.of("true", "false", null).map(global -> Arguments.of(prompt, global)));
	}

	@ParameterizedTest
	@MethodSource("switchCombinations")
	void promptSwitchDoesNotChangeGlobalNacosConfig(String prompt, String global) {
		setSystemProperty(GLOBAL_ENABLED, global);
		ApplicationContextRunner runner = this.contextRunner;
		if (prompt != null) {
			runner = runner.withPropertyValues(PROMPT_ENABLED + "=" + prompt);
		}
		runner.run(context -> {
			assertThat(context).hasNotFailed();
			if ("true".equals(prompt)) {
				assertThat(context).hasSingleBean(ConfigurablePromptTemplateFactory.class);
				assertThat(context).hasSingleBean(PromptTemplateBuilderConfigure.class);
			}
			else {
				assertThat(context).doesNotHaveBean(ConfigurablePromptTemplateFactory.class);
				assertThat(context).doesNotHaveBean(PromptTemplateBuilderConfigure.class);
			}
			assertThat(System.getProperty(GLOBAL_ENABLED)).isEqualTo(global);
		});
		assertThat(System.getProperty(GLOBAL_ENABLED)).isEqualTo(global);
	}

	@Test
	void contextsWithDifferentPromptSettingsAndRecreationRemainIndependent() {
		System.setProperty(GLOBAL_ENABLED, "true");
		this.contextRunner.withPropertyValues(PROMPT_ENABLED + "=true").run(enabled -> {
			ConfigurablePromptTemplateFactory factory = enabled.getBean(ConfigurablePromptTemplateFactory.class);
			this.contextRunner.withPropertyValues(PROMPT_ENABLED + "=false").run(disabled -> {
				assertThat(disabled).doesNotHaveBean(ConfigurablePromptTemplateFactory.class);
				assertThat(enabled.getBean(ConfigurablePromptTemplateFactory.class)).isSameAs(factory);
				assertThat(System.getProperty(GLOBAL_ENABLED)).isEqualTo("true");
			});
			this.contextRunner.run(recreated -> {
				assertThat(recreated).doesNotHaveBean(ConfigurablePromptTemplateFactory.class);
				assertThat(System.getProperty(GLOBAL_ENABLED)).isEqualTo("true");
			});
		});
		this.contextRunner.withPropertyValues(PROMPT_ENABLED + "=true").run(recreated -> {
			assertThat(recreated).hasSingleBean(ConfigurablePromptTemplateFactory.class);
			assertThat(System.getProperty(GLOBAL_ENABLED)).isEqualTo("true");
		});
	}

	@Test
	void customBeansArePreservedWithoutChangingGlobalConfig() {
		System.setProperty(GLOBAL_ENABLED, "true");
		PromptTemplateBuilderConfigure configure = new PromptTemplateBuilderConfigure();
		ConfigurablePromptTemplateFactory factory = new ConfigurablePromptTemplateFactory(configure);
		for (boolean enabled : new boolean[] { true, false }) {
			this.contextRunner.withPropertyValues(PROMPT_ENABLED + "=" + enabled)
				.withBean(PromptTemplateBuilderConfigure.class, () -> configure)
				.withBean(ConfigurablePromptTemplateFactory.class, () -> factory)
				.run(context -> {
					assertThat(context).hasNotFailed();
					assertThat(context).hasSingleBean(PromptTemplateBuilderConfigure.class);
					assertThat(context).hasSingleBean(ConfigurablePromptTemplateFactory.class);
					assertThat(context.getBean(ConfigurablePromptTemplateFactory.class)).isSameAs(factory);
					assertThat(context.getBean(PromptTemplateBuilderConfigure.class)).isSameAs(configure);
					assertThat(System.getProperty(GLOBAL_ENABLED)).isEqualTo("true");
				});
		}
	}

	@Test
	void whenEnabledPropertyIsTrue_thenBeanShouldBeCreated() {

		this.contextRunner.withPropertyValues("argi.nacos.prompt.template.enabled=true")
			.run(context -> assertThat(context).hasSingleBean(ConfigurablePromptTemplateFactory.class));
	}

	/**
	 * Nacos autoconfigure default value is false.
	 */
	@Test
	void whenNoPropertiesConfigured_thenBeanShouldNotBeCreated() {

		this.contextRunner.run(context -> assertThat(context).doesNotHaveBean(ConfigurablePromptTemplateFactory.class));
	}

	@Test
	void whenEnabledPropertyIsFalse_thenBeanShouldNotBeCreated() {

		this.contextRunner.withPropertyValues("argi.nacos.prompt.template.enabled=false")
			.run(context -> assertThat(context).doesNotHaveBean(ConfigurablePromptTemplateFactory.class));
	}

}
