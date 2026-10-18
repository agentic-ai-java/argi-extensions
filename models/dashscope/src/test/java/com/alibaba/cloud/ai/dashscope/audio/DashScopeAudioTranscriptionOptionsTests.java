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
import org.junit.jupiter.api.Test;
import org.springframework.ai.audio.transcription.AudioTranscriptionOptions;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test cases for DashScopeAudioTranscriptionOptions. Tests cover builder pattern,
 * getters/setters, deprecated methods, and various edge cases.
 *
 * @author yingzi
 * @since 1.1.0.0
 */
class DashScopeAudioTranscriptionOptionsTests {

    // Test constants
    private static final String TEST_MODEL = DashScopeModel.AudioModel.PARAFORMER_V1.getValue();

    private static final String TEST_VOCABULARY_ID = "test-vocab-id";

    private static final String TEST_RESOURCE_ID = "test-resource-id";

    private static final Integer TEST_SAMPLE_RATE = 16000;

    private static final DashScopeAudioTranscriptionApi.AudioFormat TEST_FORMAT = DashScopeAudioTranscriptionApi.AudioFormat.WAV;

    private static final List<Integer> TEST_CHANNEL_ID = List.of(0, 1);

    private static final Boolean TEST_DISFLUENCY_REMOVAL = true;

    private static final Boolean TEST_TIMESTAMP_ALIGNMENT = true;

    private static final String TEST_SPECIAL_WORD_FILTER = "test-filter";

    private static final List<String> TEST_LANGUAGE_HINTS = List.of("zh", "en");

    private static final Boolean TEST_DIARIZATION_ENABLED = true;

    private static final Integer TEST_SPEAKER_COUNT = 2;

    private static final Boolean TEST_SEMANTIC_PUNCTUATION = true;

    private static final Integer TEST_MAX_SENTENCE_SILENCE = 1000;

    private static final Boolean TEST_MULTI_THRESHOLD_MODE = true;

    private static final Boolean TEST_PUNCTUATION_PREDICTION = false;

    private static final Boolean TEST_HEARTBEAT = true;

    private static final Boolean TEST_INVERSE_TEXT_NORMALIZATION = false;

    private static final String TEST_SOURCE_LANGUAGE = "zh";

    private static final Boolean TEST_TRANSCRIPTION_ENABLED = true;

    private static final Boolean TEST_TRANSLATION_ENABLED = true;

    private static final List<String> TEST_TRANSLATION_TARGET_LANGUAGES = List.of("en");

    private static final Integer TEST_MAX_END_SILENCE = 1000;

    @Test
    void testBuilderAndGetters() {
        // Test building DashScopeAudioTranscriptionOptions using builder pattern and
        // verify getters
        DashScopeAudioTranscriptionOptions options = DashScopeAudioTranscriptionOptions.builder()
                .model(TEST_MODEL)
                .vocabularyId(TEST_VOCABULARY_ID)
                .resourceId(TEST_RESOURCE_ID)
                .sampleRate(TEST_SAMPLE_RATE)
                .format(TEST_FORMAT)
                .channelId(TEST_CHANNEL_ID)
                .disfluencyRemovalEnabled(TEST_DISFLUENCY_REMOVAL)
                .timestampAlignmentEnabled(TEST_TIMESTAMP_ALIGNMENT)
                .specialWordFilter(TEST_SPECIAL_WORD_FILTER)
                .languageHints(TEST_LANGUAGE_HINTS)
                .diarizationEnabled(TEST_DIARIZATION_ENABLED)
                .speakerCount(TEST_SPEAKER_COUNT)
                .semanticPunctuationEnabled(TEST_SEMANTIC_PUNCTUATION)
                .maxSentenceSilence(TEST_MAX_SENTENCE_SILENCE)
                .multiThresholdModeEnabled(TEST_MULTI_THRESHOLD_MODE)
                .punctuationPredictionEnabled(TEST_PUNCTUATION_PREDICTION)
                .heartbeat(TEST_HEARTBEAT)
                .inverseTextNormalizationEnabled(TEST_INVERSE_TEXT_NORMALIZATION)
                .sourceLanguage(TEST_SOURCE_LANGUAGE)
                .transcriptionEnabled(TEST_TRANSCRIPTION_ENABLED)
                .translationEnabled(TEST_TRANSLATION_ENABLED)
                .translationTargetLanguages(TEST_TRANSLATION_TARGET_LANGUAGES)
                .maxEndSilence(TEST_MAX_END_SILENCE)
                .build();

        // Verify all fields are set correctly
        assertThat(options.getModel()).isEqualTo(TEST_MODEL);
        assertThat(options.getVocabularyId()).isEqualTo(TEST_VOCABULARY_ID);
        assertThat(options.getResourceId()).isEqualTo(TEST_RESOURCE_ID);
        assertThat(options.getSampleRate()).isEqualTo(TEST_SAMPLE_RATE);
        assertThat(options.getFormat()).isEqualTo(TEST_FORMAT);
        assertThat(options.getChannelId()).isEqualTo(TEST_CHANNEL_ID);
        assertThat(options.getDisfluencyRemovalEnabled()).isEqualTo(TEST_DISFLUENCY_REMOVAL);
        assertThat(options.getTimestampAlignmentEnabled()).isEqualTo(TEST_TIMESTAMP_ALIGNMENT);
        assertThat(options.getSpecialWordFilter()).isEqualTo(TEST_SPECIAL_WORD_FILTER);
        assertThat(options.getLanguageHints()).isEqualTo(TEST_LANGUAGE_HINTS);
        assertThat(options.getDiarizationEnabled()).isEqualTo(TEST_DIARIZATION_ENABLED);
        assertThat(options.getSpeakerCount()).isEqualTo(TEST_SPEAKER_COUNT);
        assertThat(options.getSemanticPunctuationEnabled()).isEqualTo(TEST_SEMANTIC_PUNCTUATION);
        assertThat(options.getMaxSentenceSilence()).isEqualTo(TEST_MAX_SENTENCE_SILENCE);
        assertThat(options.getMultiThresholdModeEnabled()).isEqualTo(TEST_MULTI_THRESHOLD_MODE);
        assertThat(options.getPunctuationPredictionEnabled()).isEqualTo(TEST_PUNCTUATION_PREDICTION);
        assertThat(options.getHeartbeat()).isEqualTo(TEST_HEARTBEAT);
        assertThat(options.getInverseTextNormalizationEnabled()).isEqualTo(TEST_INVERSE_TEXT_NORMALIZATION);
        assertThat(options.getSourceLanguage()).isEqualTo(TEST_SOURCE_LANGUAGE);
        assertThat(options.getTranscriptionEnabled()).isEqualTo(TEST_TRANSCRIPTION_ENABLED);
        assertThat(options.getTranslationEnabled()).isEqualTo(TEST_TRANSLATION_ENABLED);
        assertThat(options.getTranslationTargetLanguages()).isEqualTo(TEST_TRANSLATION_TARGET_LANGUAGES);
        assertThat(options.getMaxEndSilence()).isEqualTo(TEST_MAX_END_SILENCE);
    }

    @Test
    void testSettersAndGetters() {
        // Test setters and getters
        DashScopeAudioTranscriptionOptions options = new DashScopeAudioTranscriptionOptions();

        options.setModel(TEST_MODEL);
        options.setVocabularyId(TEST_VOCABULARY_ID);
        options.setResourceId(TEST_RESOURCE_ID);
        options.setSampleRate(TEST_SAMPLE_RATE);
        options.setFormat(TEST_FORMAT);
        options.setChannelId(TEST_CHANNEL_ID);
        options.setDisfluencyRemovalEnabled(TEST_DISFLUENCY_REMOVAL);
        options.setTimestampAlignmentEnabled(TEST_TIMESTAMP_ALIGNMENT);
        options.setSpecialWordFilter(TEST_SPECIAL_WORD_FILTER);
        options.setLanguageHints(TEST_LANGUAGE_HINTS);
        options.setDiarizationEnabled(TEST_DIARIZATION_ENABLED);
        options.setSpeakerCount(TEST_SPEAKER_COUNT);
        options.setSemanticPunctuationEnabled(TEST_SEMANTIC_PUNCTUATION);
        options.setMaxSentenceSilence(TEST_MAX_SENTENCE_SILENCE);
        options.setMultiThresholdModeEnabled(TEST_MULTI_THRESHOLD_MODE);
        options.setPunctuationPredictionEnabled(TEST_PUNCTUATION_PREDICTION);
        options.setHeartbeat(TEST_HEARTBEAT);
        options.setInverseTextNormalizationEnabled(TEST_INVERSE_TEXT_NORMALIZATION);
        options.setSourceLanguage(TEST_SOURCE_LANGUAGE);
        options.setTranscriptionEnabled(TEST_TRANSCRIPTION_ENABLED);
        options.setTranslationEnabled(TEST_TRANSLATION_ENABLED);
        options.setTranslationTargetLanguages(TEST_TRANSLATION_TARGET_LANGUAGES);
        options.setMaxEndSilence(TEST_MAX_END_SILENCE);

        // Verify all fields are set correctly
        assertThat(options.getModel()).isEqualTo(TEST_MODEL);
        assertThat(options.getVocabularyId()).isEqualTo(TEST_VOCABULARY_ID);
        assertThat(options.getResourceId()).isEqualTo(TEST_RESOURCE_ID);
        assertThat(options.getSampleRate()).isEqualTo(TEST_SAMPLE_RATE);
        assertThat(options.getFormat()).isEqualTo(TEST_FORMAT);
        assertThat(options.getChannelId()).isEqualTo(TEST_CHANNEL_ID);
        assertThat(options.getDisfluencyRemovalEnabled()).isEqualTo(TEST_DISFLUENCY_REMOVAL);
        assertThat(options.getTimestampAlignmentEnabled()).isEqualTo(TEST_TIMESTAMP_ALIGNMENT);
        assertThat(options.getSpecialWordFilter()).isEqualTo(TEST_SPECIAL_WORD_FILTER);
        assertThat(options.getLanguageHints()).isEqualTo(TEST_LANGUAGE_HINTS);
        assertThat(options.getDiarizationEnabled()).isEqualTo(TEST_DIARIZATION_ENABLED);
        assertThat(options.getSpeakerCount()).isEqualTo(TEST_SPEAKER_COUNT);
        assertThat(options.getSemanticPunctuationEnabled()).isEqualTo(TEST_SEMANTIC_PUNCTUATION);
        assertThat(options.getMaxSentenceSilence()).isEqualTo(TEST_MAX_SENTENCE_SILENCE);
        assertThat(options.getMultiThresholdModeEnabled()).isEqualTo(TEST_MULTI_THRESHOLD_MODE);
        assertThat(options.getPunctuationPredictionEnabled()).isEqualTo(TEST_PUNCTUATION_PREDICTION);
        assertThat(options.getHeartbeat()).isEqualTo(TEST_HEARTBEAT);
        assertThat(options.getInverseTextNormalizationEnabled()).isEqualTo(TEST_INVERSE_TEXT_NORMALIZATION);
        assertThat(options.getSourceLanguage()).isEqualTo(TEST_SOURCE_LANGUAGE);
        assertThat(options.getTranscriptionEnabled()).isEqualTo(TEST_TRANSCRIPTION_ENABLED);
        assertThat(options.getTranslationEnabled()).isEqualTo(TEST_TRANSLATION_ENABLED);
        assertThat(options.getTranslationTargetLanguages()).isEqualTo(TEST_TRANSLATION_TARGET_LANGUAGES);
        assertThat(options.getMaxEndSilence()).isEqualTo(TEST_MAX_END_SILENCE);
    }

    @Test
    void testDefaultValues() {
        // Test default values when creating a new instance
        DashScopeAudioTranscriptionOptions options = new DashScopeAudioTranscriptionOptions();

        // Verify default values
        assertThat(options.getModel()).isNull();
        assertThat(options.getVocabularyId()).isNull();
        assertThat(options.getResourceId()).isNull();
        assertThat(options.getSampleRate()).isNull();
        assertThat(options.getFormat()).isEqualTo(DashScopeAudioTranscriptionApi.AudioFormat.PCM);
        assertThat(options.getChannelId()).isEqualTo(List.of(0));
        assertThat(options.getDisfluencyRemovalEnabled()).isFalse();
        assertThat(options.getTimestampAlignmentEnabled()).isFalse();
        assertThat(options.getSpecialWordFilter()).isNull();
        assertThat(options.getLanguageHints()).isEqualTo(List.of("zh", "en"));
        assertThat(options.getDiarizationEnabled()).isFalse();
        assertThat(options.getSpeakerCount()).isNull();
        assertThat(options.getSemanticPunctuationEnabled()).isFalse();
        assertThat(options.getMaxSentenceSilence()).isEqualTo(800);
        assertThat(options.getMultiThresholdModeEnabled()).isFalse();
        assertThat(options.getPunctuationPredictionEnabled()).isTrue();
        assertThat(options.getHeartbeat()).isFalse();
        assertThat(options.getInverseTextNormalizationEnabled()).isTrue();
        assertThat(options.getSourceLanguage()).isNull();
        assertThat(options.getTranscriptionEnabled()).isTrue();
        assertThat(options.getTranslationEnabled()).isFalse();
        assertThat(options.getTranslationTargetLanguages()).isNull();
        assertThat(options.getMaxEndSilence()).isEqualTo(800);
    }

    @Test
    void testImplementsAudioTranscriptionOptions() {
        // Test that DashScopeAudioTranscriptionOptions implements AudioTranscriptionOptions
        // interface
        DashScopeAudioTranscriptionOptions options = new DashScopeAudioTranscriptionOptions();

        assertThat(options).isInstanceOf(AudioTranscriptionOptions.class);
    }

    @Test
    void testAudioFormats() {
        // Test different audio formats
        DashScopeAudioTranscriptionOptions pcmOptions = DashScopeAudioTranscriptionOptions.builder()
                .format(DashScopeAudioTranscriptionApi.AudioFormat.PCM)
                .build();

        DashScopeAudioTranscriptionOptions wavOptions = DashScopeAudioTranscriptionOptions.builder()
                .format(DashScopeAudioTranscriptionApi.AudioFormat.WAV)
                .build();

        DashScopeAudioTranscriptionOptions mp3Options = DashScopeAudioTranscriptionOptions.builder()
                .format(DashScopeAudioTranscriptionApi.AudioFormat.MP3)
                .build();

        assertThat(pcmOptions.getFormat()).isEqualTo(DashScopeAudioTranscriptionApi.AudioFormat.PCM);
        assertThat(wavOptions.getFormat()).isEqualTo(DashScopeAudioTranscriptionApi.AudioFormat.WAV);
        assertThat(mp3Options.getFormat()).isEqualTo(DashScopeAudioTranscriptionApi.AudioFormat.MP3);
    }

    @Test
    void testRealtimeTranscriptionOptions() {
        // Test realtime transcription specific options
        DashScopeAudioTranscriptionOptions options = DashScopeAudioTranscriptionOptions.builder()
                .model(DashScopeModel.AudioModel.PARAFORMER_REALTIME_V1.getValue())
                .semanticPunctuationEnabled(true)
                .maxSentenceSilence(1000)
                .multiThresholdModeEnabled(true)
                .punctuationPredictionEnabled(true)
                .heartbeat(true)
                .build();

        assertThat(options.getModel()).isEqualTo(DashScopeModel.AudioModel.PARAFORMER_REALTIME_V1.getValue());
        assertThat(options.getSemanticPunctuationEnabled()).isTrue();
        assertThat(options.getMaxSentenceSilence()).isEqualTo(1000);
        assertThat(options.getMultiThresholdModeEnabled()).isTrue();
        assertThat(options.getPunctuationPredictionEnabled()).isTrue();
        assertThat(options.getHeartbeat()).isTrue();
    }

    @Test
    void testTranslationOptions() {
        // Test translation specific options
        DashScopeAudioTranscriptionOptions options = DashScopeAudioTranscriptionOptions.builder()
                .sourceLanguage("zh")
                .transcriptionEnabled(true)
                .translationEnabled(true)
                .translationTargetLanguages(List.of("en", "ja"))
                .build();

        assertThat(options.getSourceLanguage()).isEqualTo("zh");
        assertThat(options.getTranscriptionEnabled()).isTrue();
        assertThat(options.getTranslationEnabled()).isTrue();
        assertThat(options.getTranslationTargetLanguages()).containsExactly("en", "ja");
    }

    @Test
    void testDiarizationOptions() {
        // Test diarization (speaker separation) options
        DashScopeAudioTranscriptionOptions options = DashScopeAudioTranscriptionOptions.builder()
                .diarizationEnabled(true)
                .speakerCount(3)
                .build();

        assertThat(options.getDiarizationEnabled()).isTrue();
        assertThat(options.getSpeakerCount()).isEqualTo(3);
    }

    @Test
    void testChannelOptions() {
        // Test multi-channel audio options
        DashScopeAudioTranscriptionOptions singleChannel = DashScopeAudioTranscriptionOptions.builder()
                .channelId(List.of(0))
                .build();

        DashScopeAudioTranscriptionOptions multiChannel = DashScopeAudioTranscriptionOptions.builder()
                .channelId(List.of(0, 1))
                .build();

        assertThat(singleChannel.getChannelId()).containsExactly(0);
        assertThat(multiChannel.getChannelId()).containsExactly(0, 1);
    }

}
