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

import java.util.List;

import com.alibaba.cloud.ai.dashscope.api.DashScopeAudioTranscriptionApi;
import com.alibaba.cloud.ai.dashscope.spec.DashScopeModel;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.ai.audio.transcription.AudioTranscriptionPrompt;
import org.springframework.ai.audio.transcription.AudioTranscriptionResponse;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for DashScope Audio Transcription functionality. These tests will
 * only run if AI_DASHSCOPE_API_KEY environment variable is set.
 *
 * @author yingzi
 * @since 1.1.0.0
 */
@Tag("integration")
@EnabledIfEnvironmentVariable(named = "AI_DASHSCOPE_API_KEY", matches = ".+")
class DashScopeAudioTranscriptionIT {

    // Test constants
    private static final String TEST_MODEL = DashScopeModel.AudioModel.PARAFORMER_V1.getValue();

    private static final String AUDIO_FILE_URL = "https://dashscope.oss-cn-beijing.aliyuncs.com/samples/audio/paraformer/hello_world_female2.wav";

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
     * Test basic audio transcription functionality with real API call.
     */
    @Test
    void testBasicTranscription() throws Exception {
        // Create real API client with API key from environment
        DashScopeAudioTranscriptionApi realApi = DashScopeAudioTranscriptionApi.builder()
                .apiKey(apiKey)
                .model(TEST_MODEL)
                .build();

        // Create transcription model with default options
        DashScopeAudioTranscriptionOptions options = DashScopeAudioTranscriptionOptions.builder()
                .model(TEST_MODEL)
                .build();

        DashScopeAudioTranscriptionModel transcriptionModel = new DashScopeAudioTranscriptionModel(realApi, options);

        // Create prompt with audio URL
        Resource audioResource = new UrlResource(AUDIO_FILE_URL);
        AudioTranscriptionPrompt prompt = new AudioTranscriptionPrompt(audioResource);

        // Call API
        AudioTranscriptionResponse response = transcriptionModel.call(prompt);

        // Verify response
        assertThat(response).isNotNull();
        assertThat(response.getResult()).isNotNull();
        assertThat(response.getResult().getOutput()).isNotNull();
        assertThat(response.getResult().getOutput()).isNotEmpty();

        System.out.println("Transcription successful! Text: " + response.getResult().getOutput());
        System.out.println("Metadata: " + response.getMetadata());
    }

    /**
     * Test transcription with custom options.
     */
    @Test
    void testTranscriptionWithCustomOptions() throws Exception {
        // Create real API client
        DashScopeAudioTranscriptionApi realApi = DashScopeAudioTranscriptionApi.builder()
                .apiKey(apiKey)
                .model(TEST_MODEL)
                .build();

        // Create transcription model with custom options
        DashScopeAudioTranscriptionOptions options = DashScopeAudioTranscriptionOptions.builder()
                .model(TEST_MODEL)
                .disfluencyRemovalEnabled(true)
                .timestampAlignmentEnabled(true)
                .languageHints(List.of("zh", "en"))
                .build();

        DashScopeAudioTranscriptionModel transcriptionModel = new DashScopeAudioTranscriptionModel(realApi, options);

        // Create prompt with audio URL
        Resource audioResource = new UrlResource(AUDIO_FILE_URL);
        AudioTranscriptionPrompt prompt = new AudioTranscriptionPrompt(audioResource);

        // Call API
        AudioTranscriptionResponse response = transcriptionModel.call(prompt);

        // Verify response
        assertThat(response).isNotNull();
        assertThat(response.getResult()).isNotNull();
        assertThat(response.getResult().getOutput()).isNotNull();
        assertThat(response.getResult().getOutput()).isNotEmpty();

        System.out.println("Transcription with custom options successful! Text: " + response.getResult().getOutput());
    }

    /**
     * Test transcription with Paraformer 8k model.
     */
    @Test
    void testTranscriptionWithParaformer8k() throws Exception {
        // Create real API client
        DashScopeAudioTranscriptionApi realApi = DashScopeAudioTranscriptionApi.builder()
                .apiKey(apiKey)
                .model(DashScopeModel.AudioModel.PARAFORMER_8K_V1.getValue())
                .build();

        // Create transcription model with Paraformer 8k model
        DashScopeAudioTranscriptionOptions options = DashScopeAudioTranscriptionOptions.builder()
                .model(DashScopeModel.AudioModel.PARAFORMER_8K_V1.getValue())
                .channelId(List.of(0))
                .build();

        DashScopeAudioTranscriptionModel transcriptionModel = new DashScopeAudioTranscriptionModel(realApi, options);

        // Create prompt with audio URL
        Resource audioResource = new UrlResource(AUDIO_FILE_URL);
        AudioTranscriptionPrompt prompt = new AudioTranscriptionPrompt(audioResource);

        // Call API
        AudioTranscriptionResponse response = transcriptionModel.call(prompt);

        // Verify response
        assertThat(response).isNotNull();
        assertThat(response.getResult()).isNotNull();

        System.out.println("Paraformer 8k transcription successful!");
        if (response.getResult().getOutput() != null) {
            System.out.println("Text: " + response.getResult().getOutput());
        }
    }

}
