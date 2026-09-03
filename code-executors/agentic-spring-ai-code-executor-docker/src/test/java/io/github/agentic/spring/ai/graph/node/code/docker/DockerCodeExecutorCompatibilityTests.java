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
package io.github.agentic.spring.ai.graph.node.code.docker;

import io.github.agentic.spring.ai.graph.node.code.CodeExecutor;
import io.github.agentic.spring.ai.graph.node.code.entity.CodeBlock;
import io.github.agentic.spring.ai.graph.node.code.entity.CodeExecutionConfig;
import io.github.agentic.spring.ai.graph.node.code.entity.CodeExecutionResult;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.async.ResultCallback;
import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.command.InspectContainerCmd;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.command.LogContainerCmd;
import com.github.dockerjava.api.command.RemoveContainerCmd;
import com.github.dockerjava.api.command.StartContainerCmd;
import com.github.dockerjava.api.command.WaitContainerCmd;
import com.github.dockerjava.api.command.WaitContainerResultCallback;
import com.github.dockerjava.api.model.Bind;
import com.github.dockerjava.api.model.Capability;
import com.github.dockerjava.api.model.Frame;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.StreamType;
import com.github.dockerjava.core.DockerClientBuilder;
import com.github.dockerjava.transport.DockerHttpClient;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Observable compatibility contract for the Core and Extensions Docker code
 * executors.
 */
class DockerCodeExecutorCompatibilityTests {

	private static Stream<Arguments> executors() {
		return Stream.of(
				Arguments.of("core", (Supplier<CodeExecutor>) io.github.agentic.spring.ai.graph.node.code.DockerCodeExecutor::new),
				Arguments.of("extension", (Supplier<CodeExecutor>) DockerCodeExecutor::new));
	}

	private static Stream<Arguments> languageTemplates() {
		return Stream.of(
				Arguments.of("python3", "python3", "py"),
				Arguments.of("python", "python", "py"),
				Arguments.of("bash", "sh", "sh"),
				Arguments.of("shell", "sh", "sh"),
				Arguments.of("sh", "sh", "sh"),
				Arguments.of("powershell", "sh", "sh"),
				Arguments.of("nodejs", "node", "js"),
				Arguments.of("java", "java", "java"));
	}

	@ParameterizedTest(name = "{0} applies default isolation")
	@MethodSource("executors")
	void executeCodeBlocksAppliesDefaultContainerLimits(String name, Supplier<CodeExecutor> executorFactory,
			@TempDir Path workDir) throws Exception {
		DockerClientFixture docker = DockerClientFixture.success("limited-container");
		CodeExecutionConfig config = baseConfig(workDir, "python:3.10", "docker-code-exec-limits-test")
			.setTimeout(1)
			.setContainerUser("65534:65534");

		try (MockedStatic<DockerClientBuilder> dockerClientBuilder = docker.mockBuilder()) {
			CodeExecutionResult result = executorFactory.get()
				.executeCodeBlocks(List.of(new CodeBlock("python3", "print('limited')")), config);

			assertThat(result.exitCode()).isZero();
		}

		HostConfig hostConfig = docker.capturedHostConfig();
		assertThat(hostConfig.getNetworkMode()).isEqualTo("none");
		assertThat(hostConfig.getReadonlyRootfs()).isTrue();
		assertThat(hostConfig.getTmpFs()).containsEntry("/tmp", "rw,nosuid,size=64m,mode=1777");
		assertThat(hostConfig.getMemory()).isEqualTo(256L * 1024L * 1024L);
		assertThat(hostConfig.getMemorySwap()).isEqualTo(256L * 1024L * 1024L);
		assertThat(hostConfig.getCpuPeriod()).isEqualTo(100_000L);
		assertThat(hostConfig.getCpuQuota()).isEqualTo(100_000L);
		assertThat(hostConfig.getPidsLimit()).isEqualTo(128L);
		assertThat(Arrays.asList(hostConfig.getCapDrop())).contains(Capability.ALL);
		assertThat(hostConfig.getSecurityOpts()).contains("no-new-privileges");
		verify(docker.createContainerCmd).withUser("65534:65534");
		assertWorkspaceBindAndCleanup(hostConfig, workDir);
	}

	@ParameterizedTest(name = "{0} respects timeout")
	@MethodSource("executors")
	void executeCodeBlocksFailsFastWhenContainerDoesNotFinishBeforeTimeout(String name,
			Supplier<CodeExecutor> executorFactory, @TempDir Path workDir) throws Exception {
		DockerClientFixture docker = DockerClientFixture.timeout("timed-out-container");
		CodeExecutionConfig config = baseConfig(workDir, "python:3.10", "docker-code-exec-timeout-test").setTimeout(1);

		try (MockedStatic<DockerClientBuilder> dockerClientBuilder = docker.mockBuilder()) {
			assertThatThrownBy(() -> executorFactory.get()
				.executeCodeBlocks(List.of(new CodeBlock("python3", "print('still running')")), config))
				.isInstanceOf(RuntimeException.class)
				.hasMessageContaining("timed out");
		}

		verify(docker.dockerClient, never()).logContainerCmd("timed-out-container");
		verify(docker.dockerClient, never()).inspectContainerCmd("timed-out-container");
		verify(docker.removeContainerCmd).exec();
	}

	@ParameterizedTest(name = "{0} truncates output")
	@MethodSource("executors")
	void executeCodeBlocksTruncatesContainerLogs(String name, Supplier<CodeExecutor> executorFactory, @TempDir Path workDir)
			throws Exception {
		DockerClientFixture docker = DockerClientFixture.success("limited-output-container", "x".repeat(1024));
		CodeExecutionConfig config = baseConfig(workDir, "python:3.10", "docker-output-limit-test").setMaxOutputBytes(16);

		try (MockedStatic<DockerClientBuilder> dockerClientBuilder = docker.mockBuilder()) {
			CodeExecutionResult result = executorFactory.get()
				.executeCodeBlocks(List.of(new CodeBlock("python3", "print('x')")), config);

			assertThat(result.logs()).contains("output truncated");
			assertThat(result.logs().length()).isLessThan(100);
		}
	}

	@ParameterizedTest(name = "{0} maps {1}")
	@MethodSource("executorLanguageTemplates")
	void executeCodeBlocksUsesExpectedLanguageTemplate(String name, String language, String executable,
			String extension, Supplier<CodeExecutor> executorFactory, @TempDir Path workDir) throws Exception {
		DockerClientFixture docker = DockerClientFixture.success("language-container");
		CodeExecutionConfig config = baseConfig(workDir, "python:3.10", "docker-language-test");

		try (MockedStatic<DockerClientBuilder> dockerClientBuilder = docker.mockBuilder()) {
			CodeExecutionResult result = executorFactory.get()
				.executeCodeBlocks(List.of(new CodeBlock(language, "print('language')")), config);

			assertThat(result.exitCode()).isZero();
		}

		if ("java".equals(language)) {
			verify(docker.createContainerCmd).withCmd(eq(executable), eq("-cp"), anyString(),
					org.mockito.ArgumentMatchers.argThat(filename -> filename.startsWith("tmp_code_")
							&& filename.endsWith("." + extension)));
		}
		else {
			verify(docker.createContainerCmd).withCmd(eq(executable),
					org.mockito.ArgumentMatchers.argThat(filename -> filename.startsWith("tmp_code_")
							&& filename.endsWith("." + extension)));
		}
	}

	@ParameterizedTest(name = "{0} restart is no-op")
	@MethodSource("executors")
	void restartIsNoOp(String name, Supplier<CodeExecutor> executorFactory) {
		assertThatCode(() -> executorFactory.get().restart()).doesNotThrowAnyException();
	}

	private static Stream<Arguments> executorLanguageTemplates() {
		return executors().flatMap(executor -> languageTemplates().map(language -> Arguments.of(
				executor.get()[0],
				language.get()[0],
				language.get()[1],
				language.get()[2],
				executor.get()[1])));
	}

	private static CodeExecutionConfig baseConfig(Path workDir, String image, String containerName) {
		return new CodeExecutionConfig()
			.setDocker(image)
			.setWorkDir(workDir.toAbsolutePath().toString())
			.setContainerName(containerName)
			.setDockerHost("unix:///var/run/docker.sock");
	}

	private static void assertWorkspaceBindAndCleanup(HostConfig hostConfig, Path workDir) throws Exception {
		assertThat(hostConfig.getBinds()).hasSize(1);
		Bind bind = hostConfig.getBinds()[0];
		assertThat(bind.getPath()).startsWith(workDir.toAbsolutePath().normalize().toString());
		assertThat(bind.getVolume().getPath()).isEqualTo("/workspace");
		try (var children = Files.list(workDir)) {
			assertThat(children).isEmpty();
		}
	}

	private static final class DockerClientFixture {

		private final DockerClientBuilder dockerClientBuilder = mock(DockerClientBuilder.class);

		private final DockerClient dockerClient = mock(DockerClient.class);

		private final CreateContainerCmd createContainerCmd = mock(CreateContainerCmd.class);

		private final StartContainerCmd startContainerCmd = mock(StartContainerCmd.class);

		private final WaitContainerCmd waitContainerCmd = mock(WaitContainerCmd.class);

		private final WaitContainerResultCallback waitCallback = mock(WaitContainerResultCallback.class);

		private final LogContainerCmd logContainerCmd = mock(LogContainerCmd.class);

		private final InspectContainerCmd inspectContainerCmd = mock(InspectContainerCmd.class);

		private final InspectContainerResponse inspectContainerResponse = mock(InspectContainerResponse.class);

		private final InspectContainerResponse.ContainerState containerState = mock(
				InspectContainerResponse.ContainerState.class);

		private final RemoveContainerCmd removeContainerCmd = mock(RemoveContainerCmd.class);

		private final CreateContainerResponse container = new CreateContainerResponse();

		private final ArgumentCaptor<HostConfig> hostConfigCaptor = ArgumentCaptor.forClass(HostConfig.class);

		private DockerClientFixture(String containerId, boolean completed, String output) throws Exception {
			container.setId(containerId);
			when(dockerClientBuilder.withDockerHttpClient(any(DockerHttpClient.class))).thenReturn(dockerClientBuilder);
			when(dockerClientBuilder.build()).thenReturn(dockerClient);
			when(dockerClient.createContainerCmd(anyString())).thenReturn(createContainerCmd);
			when(createContainerCmd.withName(anyString())).thenReturn(createContainerCmd);
			when(createContainerCmd.withWorkingDir("/workspace")).thenReturn(createContainerCmd);
			when(createContainerCmd.withHostConfig(any())).thenReturn(createContainerCmd);
			when(createContainerCmd.withUser(anyString())).thenReturn(createContainerCmd);
			when(createContainerCmd.withCmd(anyString(), anyString())).thenReturn(createContainerCmd);
			when(createContainerCmd.withCmd(anyString(), anyString(), anyString(), anyString()))
				.thenReturn(createContainerCmd);
			when(createContainerCmd.exec()).thenReturn(container);
			when(dockerClient.startContainerCmd(containerId)).thenReturn(startContainerCmd);
			when(dockerClient.waitContainerCmd(containerId)).thenReturn(waitContainerCmd);
			when(waitContainerCmd.start()).thenReturn(waitCallback);
			when(waitCallback.awaitCompletion(anyLong(), any())).thenReturn(completed);
			when(dockerClient.logContainerCmd(containerId)).thenReturn(logContainerCmd);
			when(logContainerCmd.withStdOut(true)).thenReturn(logContainerCmd);
			when(logContainerCmd.withStdErr(true)).thenReturn(logContainerCmd);
			doAnswer(invocation -> {
				@SuppressWarnings("unchecked")
				ResultCallback<Frame> callback = invocation.getArgument(0);
				callback.onNext(new Frame(StreamType.STDOUT, output.getBytes()));
				callback.onComplete();
				return callback;
			}).when(logContainerCmd).exec(any());
			when(dockerClient.inspectContainerCmd(containerId)).thenReturn(inspectContainerCmd);
			when(inspectContainerCmd.exec()).thenReturn(inspectContainerResponse);
			when(inspectContainerResponse.getState()).thenReturn(containerState);
			when(containerState.getExitCodeLong()).thenReturn(0L);
			when(dockerClient.removeContainerCmd(containerId)).thenReturn(removeContainerCmd);
			when(removeContainerCmd.withForce(true)).thenReturn(removeContainerCmd);
		}

		static DockerClientFixture success(String containerId) throws Exception {
			return success(containerId, "ok");
		}

		static DockerClientFixture success(String containerId, String output) throws Exception {
			return new DockerClientFixture(containerId, true, output);
		}

		static DockerClientFixture timeout(String containerId) throws Exception {
			return new DockerClientFixture(containerId, false, "");
		}

		MockedStatic<DockerClientBuilder> mockBuilder() {
			MockedStatic<DockerClientBuilder> mocked = mockStatic(DockerClientBuilder.class);
			mocked.when(DockerClientBuilder::getInstance).thenReturn(dockerClientBuilder);
			return mocked;
		}

		HostConfig capturedHostConfig() {
			verify(createContainerCmd).withHostConfig(hostConfigCaptor.capture());
			return hostConfigCaptor.getValue();
		}

	}

}
