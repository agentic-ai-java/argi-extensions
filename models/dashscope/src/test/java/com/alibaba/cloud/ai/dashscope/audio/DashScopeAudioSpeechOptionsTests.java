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

import com.alibaba.cloud.ai.dashscope.api.DashScopeAudioSpeechApi;
import com.alibaba.cloud.ai.dashscope.spec.DashScopeModel;
import org.junit.jupiter.api.Test;
import org.springframework.ai.audio.tts.TextToSpeechOptions;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test cases for DashScopeAudioSpeechOptions. Tests cover builder pattern, getters/setters,
 * copy functionality, and various edge cases.
 *
 * @author yingzi
 * @since 1.1.0.0
 */
class DashScopeAudioSpeechOptionsTests {

    // Test constants
    private static final String TEST_MODEL = DashScopeModel.AudioModel.COSYVOICE_V1.getValue();

    private static final String TEST_TEXT = "Test audio text";

    private static final String TEST_VOICE = "longhua";

    private static final DashScopeAudioSpeechApi.RequestTextType TEST_REQUEST_TEXT_TYPE = DashScopeAudioSpeechApi.RequestTextType.PLAIN_TEXT;

    private static final Integer TEST_SAMPLE_RATE = 24000;

    private static final Integer TEST_VOLUME = 80;

    private static final Double TEST_SPEED = 1.5;

    private static final Double TEST_PITCH = 1.2;

    private static final Boolean TEST_ENABLE_WORD_TIMESTAMP = true;

    private static final Boolean TEST_ENABLE_PHONEME_TIMESTAMP = true;

    private static final Boolean TEST_ENABLE_SSML = true;

    private static final Integer TEST_BIT_RATE = 128000;

    private static final Integer TEST_SEED = 42;

    private static final List<String> TEST_LANGUAGE_HINTS = List.of("zh", "en");

    private static final String TEST_INSTRUCTION = "happy";

    private static final DashScopeAudioSpeechApi.ResponseFormat TEST_RESPONSE_FORMAT = DashScopeAudioSpeechApi.ResponseFormat.WAV;

    @Test
    void testBuilderAndGetters() {
        // Test building DashScopeAudioSpeechOptions using builder pattern and verify
        // getters
        DashScopeAudioSpeechOptions options = DashScopeAudioSpeechOptions.builder()
                .model(TEST_MODEL)
                .text(TEST_TEXT)
                .voice(TEST_VOICE)
                .requestTextType(TEST_REQUEST_TEXT_TYPE)
                .sampleRate(TEST_SAMPLE_RATE)
                .volume(TEST_VOLUME)
                .speed(TEST_SPEED)
                .pitch(TEST_PITCH)
                .enableWordTimestamp(TEST_ENABLE_WORD_TIMESTAMP)
                .enablePhonemeTimestamp(TEST_ENABLE_PHONEME_TIMESTAMP)
                .enableSsml(TEST_ENABLE_SSML)
                .bitRate(TEST_BIT_RATE)
                .seed(TEST_SEED)
                .languageHints(TEST_LANGUAGE_HINTS)
                .instruction(TEST_INSTRUCTION)
                .responseFormat(TEST_RESPONSE_FORMAT)
                .build();

        // Verify all fields are set correctly
        assertThat(options.getModel()).isEqualTo(TEST_MODEL);
        assertThat(options.getText()).isEqualTo(TEST_TEXT);
        assertThat(options.getVoice()).isEqualTo(TEST_VOICE);
        assertThat(options.getRequestTextType()).isEqualTo(TEST_REQUEST_TEXT_TYPE);
        assertThat(options.getSampleRate()).isEqualTo(TEST_SAMPLE_RATE);
        assertThat(options.getVolume()).isEqualTo(TEST_VOLUME);
        assertThat(options.getSpeed()).isEqualTo(TEST_SPEED);
        assertThat(options.getPitch()).isEqualTo(TEST_PITCH);
        assertThat(options.getEnableWordTimestamp()).isEqualTo(TEST_ENABLE_WORD_TIMESTAMP);
        assertThat(options.getEnablePhonemeTimestamp()).isEqualTo(TEST_ENABLE_PHONEME_TIMESTAMP);
        assertThat(options.getEnableSsml()).isEqualTo(TEST_ENABLE_SSML);
        assertThat(options.getBitRate()).isEqualTo(TEST_BIT_RATE);
        assertThat(options.getSeed()).isEqualTo(TEST_SEED);
        assertThat(options.getLanguageHints()).isEqualTo(TEST_LANGUAGE_HINTS);
        assertThat(options.getInstruction()).isEqualTo(TEST_INSTRUCTION);
        assertThat(options.getResponseFormat()).isEqualTo(TEST_RESPONSE_FORMAT);
    }

    @Test
    void testSettersAndGetters() {
        // Test setters and getters
        DashScopeAudioSpeechOptions options = new DashScopeAudioSpeechOptions();

        options.setModel(TEST_MODEL);
        options.setText(TEST_TEXT);
        options.setVoice(TEST_VOICE);
        options.setRequestTextType(TEST_REQUEST_TEXT_TYPE);
        options.setSampleRate(TEST_SAMPLE_RATE);
        options.setVolume(TEST_VOLUME);
        options.setSpeed(TEST_SPEED);
        options.setPitch(TEST_PITCH);
        options.setEnableWordTimestamp(TEST_ENABLE_WORD_TIMESTAMP);
        options.setEnablePhonemeTimestamp(TEST_ENABLE_PHONEME_TIMESTAMP);
        options.setEnableSsml(TEST_ENABLE_SSML);
        options.setBitRate(TEST_BIT_RATE);
        options.setSeed(TEST_SEED);
        options.setLanguageHints(TEST_LANGUAGE_HINTS);
        options.setInstruction(TEST_INSTRUCTION);
        options.setResponseFormat(TEST_RESPONSE_FORMAT);

        // Verify all fields are set correctly
        assertThat(options.getModel()).isEqualTo(TEST_MODEL);
        assertThat(options.getText()).isEqualTo(TEST_TEXT);
        assertThat(options.getVoice()).isEqualTo(TEST_VOICE);
        assertThat(options.getRequestTextType()).isEqualTo(TEST_REQUEST_TEXT_TYPE);
        assertThat(options.getSampleRate()).isEqualTo(TEST_SAMPLE_RATE);
        assertThat(options.getVolume()).isEqualTo(TEST_VOLUME);
        assertThat(options.getSpeed()).isEqualTo(TEST_SPEED);
        assertThat(options.getPitch()).isEqualTo(TEST_PITCH);
        assertThat(options.getEnableWordTimestamp()).isEqualTo(TEST_ENABLE_WORD_TIMESTAMP);
        assertThat(options.getEnablePhonemeTimestamp()).isEqualTo(TEST_ENABLE_PHONEME_TIMESTAMP);
        assertThat(options.getEnableSsml()).isEqualTo(TEST_ENABLE_SSML);
        assertThat(options.getBitRate()).isEqualTo(TEST_BIT_RATE);
        assertThat(options.getSeed()).isEqualTo(TEST_SEED);
        assertThat(options.getLanguageHints()).isEqualTo(TEST_LANGUAGE_HINTS);
        assertThat(options.getInstruction()).isEqualTo(TEST_INSTRUCTION);
        assertThat(options.getResponseFormat()).isEqualTo(TEST_RESPONSE_FORMAT);
    }

    @Test
    void testDefaultValues() {
        // Test default values when creating a new instance
        DashScopeAudioSpeechOptions options = new DashScopeAudioSpeechOptions();

        // Verify default values
        assertThat(options.getModel()).isNull();
        assertThat(options.getText()).isNull();
        assertThat(options.getVoice()).isNull();
        assertThat(options.getRequestTextType()).isEqualTo(DashScopeAudioSpeechApi.RequestTextType.PLAIN_TEXT);
        assertThat(options.getSampleRate()).isEqualTo(48000);
        assertThat(options.getVolume()).isEqualTo(50);
        assertThat(options.getSpeed()).isEqualTo(1.0);
        assertThat(options.getPitch()).isEqualTo(1.0);
        assertThat(options.getEnableWordTimestamp()).isFalse();
        assertThat(options.getEnablePhonemeTimestamp()).isFalse();
        assertThat(options.getEnableSsml()).isNull();
        assertThat(options.getBitRate()).isNull();
        assertThat(options.getSeed()).isNull();
        assertThat(options.getLanguageHints()).isNull();
        assertThat(options.getInstruction()).isNull();
        assertThat(options.getResponseFormat()).isEqualTo(DashScopeAudioSpeechApi.ResponseFormat.MP3);
    }

    @Test
    void testCopyFunctionality() {
        // Test copy method
        DashScopeAudioSpeechOptions original = DashScopeAudioSpeechOptions.builder()
                .model(TEST_MODEL)
                .text(TEST_TEXT)
                .voice(TEST_VOICE)
                .speed(TEST_SPEED)
                .pitch(TEST_PITCH)
                .volume(TEST_VOLUME)
                .sampleRate(TEST_SAMPLE_RATE)
                .responseFormat(TEST_RESPONSE_FORMAT)
                .enableSsml(TEST_ENABLE_SSML)
                .bitRate(TEST_BIT_RATE)
                .seed(TEST_SEED)
                .languageHints(TEST_LANGUAGE_HINTS)
                .instruction(TEST_INSTRUCTION)
                .build();

        DashScopeAudioSpeechOptions copied = original.copy();

        // Verify copied options match original
        assertThat(copied.getModel()).isEqualTo(original.getModel());
        assertThat(copied.getText()).isEqualTo(original.getText());
        assertThat(copied.getVoice()).isEqualTo(original.getVoice());
        assertThat(copied.getSpeed()).isEqualTo(original.getSpeed());
        assertThat(copied.getPitch()).isEqualTo(original.getPitch());
        assertThat(copied.getVolume()).isEqualTo(original.getVolume());
        assertThat(copied.getSampleRate()).isEqualTo(original.getSampleRate());
        assertThat(copied.getResponseFormat()).isEqualTo(original.getResponseFormat());
        assertThat(copied.getEnableSsml()).isEqualTo(original.getEnableSsml());
        assertThat(copied.getBitRate()).isEqualTo(original.getBitRate());
        assertThat(copied.getSeed()).isEqualTo(original.getSeed());
        assertThat(copied.getLanguageHints()).isEqualTo(original.getLanguageHints());
        assertThat(copied.getInstruction()).isEqualTo(original.getInstruction());

        // Verify it's a different instance
        assertThat(copied).isNotSameAs(original);
    }

    @Test
    void testGetFormat() {
        // Test getFormat method
        DashScopeAudioSpeechOptions options = DashScopeAudioSpeechOptions.builder()
                .responseFormat(DashScopeAudioSpeechApi.ResponseFormat.WAV)
                .build();

        assertThat(options.getFormat()).isEqualTo("wav");
    }

    @Test
    void testGetFormatWithNull() {
        // Test getFormat with null response format
        DashScopeAudioSpeechOptions options = new DashScopeAudioSpeechOptions();
        options.setResponseFormat(null);

        assertThat(options.getFormat()).isNull();
    }

    @Test
    void testImplementsTextToSpeechOptions() {
        // Test that DashScopeAudioSpeechOptions implements TextToSpeechOptions interface
        DashScopeAudioSpeechOptions options = new DashScopeAudioSpeechOptions();

        assertThat(options).isInstanceOf(TextToSpeechOptions.class);
    }

    @Test
    void testResponseFormats() {
        // Test different response formats
        DashScopeAudioSpeechOptions mp3Options = DashScopeAudioSpeechOptions.builder()
                .responseFormat(DashScopeAudioSpeechApi.ResponseFormat.MP3)
                .build();

        DashScopeAudioSpeechOptions wavOptions = DashScopeAudioSpeechOptions.builder()
                .responseFormat(DashScopeAudioSpeechApi.ResponseFormat.WAV)
                .build();

        DashScopeAudioSpeechOptions pcmOptions = DashScopeAudioSpeechOptions.builder()
                .responseFormat(DashScopeAudioSpeechApi.ResponseFormat.PCM)
                .build();

        assertThat(mp3Options.getFormat()).isEqualTo("mp3");
        assertThat(wavOptions.getFormat()).isEqualTo("wav");
        assertThat(pcmOptions.getFormat()).isEqualTo("pcm");
    }

    @Test
    void testRequestTextTypes() {
        // Test different request text types
        DashScopeAudioSpeechOptions plainTextOptions = DashScopeAudioSpeechOptions.builder()
                .requestTextType(DashScopeAudioSpeechApi.RequestTextType.PLAIN_TEXT)
                .build();

        DashScopeAudioSpeechOptions ssmlOptions = DashScopeAudioSpeechOptions.builder()
                .requestTextType(DashScopeAudioSpeechApi.RequestTextType.SSML)
                .build();

        assertThat(plainTextOptions.getRequestTextType()).isEqualTo(DashScopeAudioSpeechApi.RequestTextType.PLAIN_TEXT);
        assertThat(ssmlOptions.getRequestTextType()).isEqualTo(DashScopeAudioSpeechApi.RequestTextType.SSML);
    }

    @Test
    void testTimestampOptions() {
        // Test timestamp options
        DashScopeAudioSpeechOptions options = DashScopeAudioSpeechOptions.builder()
                .enableWordTimestamp(true)
                .enablePhonemeTimestamp(true)
                .build();

        assertThat(options.getEnableWordTimestamp()).isTrue();
        assertThat(options.getEnablePhonemeTimestamp()).isTrue();
    }

}
