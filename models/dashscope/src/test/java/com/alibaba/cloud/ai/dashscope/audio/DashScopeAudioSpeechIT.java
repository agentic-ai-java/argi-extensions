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
package com.alibaba.cloud.ai.dashscope.audio;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.alibaba.cloud.ai.dashscope.api.DashScopeAudioSpeechApi;
import com.alibaba.cloud.ai.dashscope.spec.DashScopeModel;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.ai.audio.tts.TextToSpeechPrompt;
import org.springframework.ai.audio.tts.TextToSpeechResponse;
import reactor.core.publisher.Flux;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for DashScope Audio Speech functionality. These tests will only run
 * if AI_DASHSCOPE_API_KEY environment variable is set.
 *
 * @author yingzi
 * @since 1.1.0.0
 */
@Tag("integration")
@EnabledIfEnvironmentVariable(named = "AI_DASHSCOPE_API_KEY", matches = ".+")
class DashScopeAudioSpeechIT {

    // Test constants
    private static final String TEST_MODEL = DashScopeModel.AudioModel.COSYVOICE_V1.getValue();

    private static final String TEST_TEXT = "你好，这是一个语音合成测试。";

    private static final String API_KEY_ENV = "AI_DASHSCOPE_API_KEY";

    private String apiKey;

    @BeforeEach
    void setUp() {
        // Get API key from environment variable
        apiKey = System.getenv(API_KEY_ENV);
        // Skip tests if API key is not set
        Assumptions.assumeTrue(
                apiKey != null && !apiKey.trim().isEmpty(),
                "Skipping tests because " + API_KEY_ENV + " environment variable is not set");
    }

    /**
     * Get file extension based on response format.
     *
     * @param format the response format
     *
     * @return file extension without dot
     */
    private String getFileExtension(DashScopeAudioSpeechApi.ResponseFormat format) {
        return switch (format) {
            case MP3 -> "mp3";
            case WAV -> "wav";
            case PCM -> "pcm";
            default -> "bin";
        };
    }

    /**
     * Save audio file to test resources directory using relative path.
     *
     * @param audioData the audio data bytes
     * @param testName  the test name for filename
     * @param format    the audio format
     *
     * @return the saved file path
     *
     * @throws IOException if file writing fails
     */
    private Path saveAudioFile(
            byte[] audioData,
            String testName,
            DashScopeAudioSpeechApi.ResponseFormat format) throws IOException {
        // Use relative path from module root
        Path outputDir = Paths.get("src/test/resources/audio");
        Files.createDirectories(outputDir);

        String extension = getFileExtension(format);
        String filename = String.format("speech_%s_%d.%s", testName, System.currentTimeMillis(), extension);
        Path outputFile = outputDir.resolve(filename);

        Files.write(outputFile, audioData);
        System.out.println("✓ Audio file saved: " + outputFile.toAbsolutePath());

        return outputFile;
    }

    /**
     * Test basic speech synthesis functionality with real API call using streaming API.
     */
    @Test
    void testBasicSpeechSynthesis() throws IOException {
        // Create real API client with API key from environment
        DashScopeAudioSpeechApi realApi = new DashScopeAudioSpeechApi(apiKey);

        // Create speech model with default options
        DashScopeAudioSpeechOptions options = DashScopeAudioSpeechOptions.builder()
                .model(TEST_MODEL)
                .voice("longhua")
                .speed(1.0)
                .responseFormat(DashScopeAudioSpeechApi.ResponseFormat.MP3)
                .build();

        DashScopeAudioSpeechModel speechModel = new DashScopeAudioSpeechModel(realApi, options);

        // Create prompt and use streaming API
        TextToSpeechPrompt prompt = new TextToSpeechPrompt(TEST_TEXT);
        Flux<TextToSpeechResponse> responseFlux = speechModel.stream(prompt);

        // Collect all responses and merge audio data
        var responses = responseFlux.collectList().block();
        assertThat(responses).isNotEmpty();

        // Merge all audio chunks
        int totalBytes = responses.stream().mapToInt(r -> r.getResult().getOutput().length).sum();
        byte[] audioData = new byte[totalBytes];
        int offset = 0;
        for (TextToSpeechResponse r : responses) {
            byte[] chunk = r.getResult().getOutput();
            System.arraycopy(chunk, 0, audioData, offset, chunk.length);
            offset += chunk.length;
        }

        assertThat(totalBytes).isGreaterThan(0);

        // Save audio file to test resources
        Path savedFile = saveAudioFile(audioData, "basic", options.getResponseFormat());
        assertThat(savedFile).exists();

        System.out.println("Speech synthesis successful! Audio data size: " + audioData.length + " bytes");
    }

    /**
     * Test streaming speech synthesis.
     */
    @Test
    void testStreamingSpeechSynthesis() throws IOException {
        // Create real API client
        DashScopeAudioSpeechApi realApi = new DashScopeAudioSpeechApi(apiKey);

        // Create speech model
        DashScopeAudioSpeechOptions options = DashScopeAudioSpeechOptions.builder()
                .model(TEST_MODEL)
                .voice("longhua")
                .speed(1.0)
                .responseFormat(DashScopeAudioSpeechApi.ResponseFormat.MP3)
                .build();

        DashScopeAudioSpeechModel speechModel = new DashScopeAudioSpeechModel(realApi, options);

        // Create prompt and stream
        TextToSpeechPrompt prompt = new TextToSpeechPrompt("这是一个流式语音合成测试。");
        Flux<TextToSpeechResponse> responseFlux = speechModel.stream(prompt);

        // Collect all responses and merge audio data
        var responses = responseFlux.collectList().block();
        assertThat(responses).isNotEmpty();

        // Merge all audio chunks
        int totalBytes = responses.stream().mapToInt(r -> r.getResult().getOutput().length).sum();
        byte[] mergedAudio = new byte[totalBytes];
        int offset = 0;
        for (TextToSpeechResponse r : responses) {
            byte[] chunk = r.getResult().getOutput();
            System.arraycopy(chunk, 0, mergedAudio, offset, chunk.length);
            offset += chunk.length;
        }

        assertThat(totalBytes).isGreaterThan(0);

        // Save merged audio file to test resources
        Path savedFile = saveAudioFile(mergedAudio, "stream", options.getResponseFormat());
        assertThat(savedFile).exists();

        System.out.println("Streaming speech synthesis successful! Total audio data: " + totalBytes + " bytes");
    }

}
