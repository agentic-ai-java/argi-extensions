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
package com.alibaba.cloud.ai.dashscope.audio.tts;

import com.alibaba.cloud.ai.dashscope.api.DashScopeAudioSpeechApi;
import com.alibaba.cloud.ai.dashscope.common.DashScopeAudioApiConstants;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ai.audio.tts.Speech;
import org.springframework.ai.audio.tts.TextToSpeechModel;
import org.springframework.ai.audio.tts.TextToSpeechOptions;
import org.springframework.ai.audio.tts.TextToSpeechPrompt;
import org.springframework.ai.audio.tts.TextToSpeechResponse;
import org.springframework.ai.model.ModelOptionsUtils;
import org.springframework.ai.retry.RetryUtils;
import org.springframework.retry.support.RetryTemplate;
import reactor.core.publisher.Flux;

import java.util.List;

/**
 * Audio Speech: input text, output audio.
 *
 * <p>This model supports two TTS modes:
 * <ul>
 *   <li>File-based synthesis using {@link #stream(TextToSpeechPrompt)} - processes complete text at once</li>
 *   <li>Streaming synthesis using {@link #stream(Flux, DashScopeAudioSpeechOptions)} - processes text chunks in real-time</li>
 * </ul>
 *
 * <p><b>Streaming Text Example:</b>
 * <pre>{@code
 * // Create a Flux of text chunks from a source (e.g., LLM output, user input)
 * Flux<String> textStream = Flux.just("Hello", " world", "!");
 *
 * // Configure TTS options for CosyVoice model
 * DashScopeAudioSpeechOptions options = DashScopeAudioSpeechOptions.builder()
 *     .model("cosyvoice-v1")
 *     .voice("longxiaochun")
 *     .build();
 *
 * // Stream synthesis
 * Flux<TextToSpeechResponse> responses = speechModel.stream(textStream, options);
 *
 * // Subscribe to process audio data
 * responses.subscribe(response -> {
 *     response.getSpeech().forEach(speech -> {
 *         Files.write(Paths.get("output.mp3"), speech.getBytes());
 *     });
 * });
 * }</pre>
 *
 * <p><b>Recommended Text Chunk Sizes:</b>
 * For optimal streaming performance, send text chunks representing complete phrases or sentences.
 * CosyVoice handles word boundaries internally, so chunk boundaries should align with natural pauses
 * (e.g., punctuation marks, clause boundaries).
 *
 * <p><b>Model Requirements:</b>
 * Streaming text input is only supported for CosyVoice models that support duplex mode:
 * <ul>
 *   <li>cosyvoice-v1</li>
 *   <li>cosyvoice-v1-longxiaochun</li>
 *   <li>cosyvoice-v1-zhibo</li>
 *   <li>cosyvoice-v1-ruiling</li>
 * </ul>
 *
 * @author kevinlin09, xuguan, yingzi
 */
public class DashScopeAudioSpeechModel implements TextToSpeechModel {

	private static final Logger logger = LoggerFactory.getLogger(DashScopeAudioSpeechModel.class);

	private final DashScopeAudioSpeechApi audioSpeechApi;

	private final DashScopeAudioSpeechOptions defaultOptions;

	private final RetryTemplate retryTemplate;

	public DashScopeAudioSpeechModel(DashScopeAudioSpeechApi audioSpeechApi) {
		this(audioSpeechApi, DashScopeAudioSpeechOptions.builder()
			.build());
	}

	public DashScopeAudioSpeechModel(DashScopeAudioSpeechApi audioSpeechApi, DashScopeAudioSpeechOptions defaultOptions) {
		this(audioSpeechApi, defaultOptions, RetryUtils.DEFAULT_RETRY_TEMPLATE);
	}

	public DashScopeAudioSpeechModel(DashScopeAudioSpeechApi audioSpeechApi, DashScopeAudioSpeechOptions defaultOptions,
		RetryTemplate retryTemplate) {
		this.audioSpeechApi = audioSpeechApi;
		this.defaultOptions = defaultOptions;
		this.retryTemplate = retryTemplate;
	}

	@NonNull
    @Override
	public TextToSpeechResponse call(TextToSpeechPrompt prompt) {
        DashScopeAudioSpeechOptions options = this.mergeOptions(prompt);
        if (DashScopeAudioApiConstants.isQwenTTSModel(options.getModel())) {
            return this.audioSpeechApi.callQwenTTS(prompt.getInstructions().getText(), options);
        }

        throw new IllegalArgumentException("Model " + options.getModel() + " is not supported call method.");
    }

    @Override
	public Flux<TextToSpeechResponse> stream(TextToSpeechPrompt prompt) {
        DashScopeAudioSpeechOptions options = this.mergeOptions(prompt);
        if (DashScopeAudioApiConstants.isQwenTTSModel(options.getModel())) {
            return this.audioSpeechApi.streamQwenTTS(prompt.getInstructions().getText(), options)
                    .map(response -> (TextToSpeechResponse) response);
        }

        if (!DashScopeAudioApiConstants.isWebsocketByTTSModelName(options.getModel())) {
            throw new IllegalArgumentException("Model " + options.getModel() + " is not supported.");
        }

        // 下面是websocket任务
        return this.audioSpeechApi.createWebSocketTask(prompt.getInstructions().getText(), options)
                .map(byteBuffer -> {
            byte[] data = new byte[byteBuffer.remaining()];
            byteBuffer.get(data);
            return new TextToSpeechResponse(List.of(new Speech(data)));
        });
	}

    /**
     * Stream text-to-speech with real-time text input.
     * This method accepts a Flux of String for streaming TTS,
     * enabling lower latency and reduced memory footprint for real-time scenarios.
     *
     * @param textFlux the streaming text data as Flux&lt;String&gt;
     * @param options the TTS options
     * @return Flux of TextToSpeechResponse with audio data
     * @throws IllegalArgumentException if model is not a CosyVoice duplex model
     */
    public Flux<TextToSpeechResponse> stream(Flux<String> textFlux, DashScopeAudioSpeechOptions options) {
        // Merge with default options
        DashScopeAudioSpeechOptions mergedOptions = ModelOptionsUtils.merge(
                options, this.defaultOptions, DashScopeAudioSpeechOptions.class);

        if (!DashScopeAudioApiConstants.COSY_VOICE_MODEL_LIST.contains(options.getModel())) {
            throw new IllegalArgumentException(
                    "Model " + options.getModel() + " does not support streaming text input. " +
                            "Only CosyVoice models support duplex mode for streaming text.");
        }
        // Delegate to API layer
        return audioSpeechApi.createStreamingWebSocketTask(textFlux, mergedOptions)
                .map(byteBuffer -> {
                    byte[] data = new byte[byteBuffer.remaining()];
                    byteBuffer.get(data);
                    return new TextToSpeechResponse(List.of(new Speech(data)));
                });
    }

	private DashScopeAudioSpeechOptions mergeOptions(TextToSpeechPrompt prompt) {
		DashScopeAudioSpeechOptions options = DashScopeAudioSpeechOptions.builder().build();
        DashScopeAudioSpeechOptions runtimeOptions = ModelOptionsUtils.copyToTarget(prompt.getOptions(), TextToSpeechOptions.class, DashScopeAudioSpeechOptions.class);

        options = ModelOptionsUtils.merge(runtimeOptions, options, DashScopeAudioSpeechOptions.class);

        return ModelOptionsUtils.merge(options, this.defaultOptions, DashScopeAudioSpeechOptions.class);
	}

    /**
     * Returns a builder pre-populated with the current configuration for mutation.
     */
    public Builder mutate() {
        return new Builder(this);
    }

    @Override
    public DashScopeAudioSpeechModel clone() {
        return this.mutate().build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private DashScopeAudioSpeechApi audioSpeechApi;

        private DashScopeAudioSpeechOptions defaultOptions = DashScopeAudioSpeechOptions.builder().build();

        private RetryTemplate retryTemplate = RetryUtils.DEFAULT_RETRY_TEMPLATE;

        private Builder() {
        }

        private Builder(DashScopeAudioSpeechModel audioSpeechModel) {
            this.audioSpeechApi = audioSpeechModel.audioSpeechApi;
            this.defaultOptions = audioSpeechModel.defaultOptions;
            this.retryTemplate = audioSpeechModel.retryTemplate;
        }

        public Builder audioSpeechApi(DashScopeAudioSpeechApi audioSpeechApi) {
            this.audioSpeechApi = audioSpeechApi;
            return this;
        }

        public Builder defaultOptions(DashScopeAudioSpeechOptions defaultOptions) {
            this.defaultOptions = defaultOptions;
            return this;
        }

        public Builder retryTemplate(RetryTemplate retryTemplate) {
            this.retryTemplate = retryTemplate;
            return this;
        }

        public DashScopeAudioSpeechModel build() {
            return new DashScopeAudioSpeechModel(this.audioSpeechApi, this.defaultOptions, this.retryTemplate);
        }
    }

}
