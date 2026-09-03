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
package io.github.agentic.spring.ai.graph.node.rag;

import io.github.agentic.spring.ai.document.DocumentWithScore;
import io.github.agentic.spring.ai.graph.OverAllState;
import io.github.agentic.spring.ai.model.RerankModel;
import io.github.agentic.spring.ai.model.RerankOptions;
import io.github.agentic.spring.ai.model.RerankRequest;
import io.github.agentic.spring.ai.model.RerankResponse;
import org.junit.jupiter.api.Test;

import org.springframework.ai.document.Document;
import org.springframework.ai.vectorstore.SearchRequest;
import org.springframework.ai.vectorstore.VectorStore;
import org.springframework.ai.vectorstore.filter.Filter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KnowledgeRetrievalNodeCompatibilityTests {

	@Test
	void publicApiMatchesCoreCompatibilityClass() {
		PublicApiParity.assertParity(io.github.agentic.spring.ai.graph.node.KnowledgeRetrievalNode.class,
				KnowledgeRetrievalNode.class);
	}

	@Test
	void disabledRankerRetrievesAndAugmentsPromptWithDefaultOutputKeysLikeCore() throws Exception {
		Filter.Expression filter = filter("kind", "guide");
		List<Document> documents = List.of(document("doc-1", "alpha"), document("doc-2", "beta"));

		Invocation core = invokeCore(builder -> builder.userPrompt("query")
			.topK(3)
			.similarityThreshold(0.72)
			.filterExpression(filter)
			.enableRanker(false), new OverAllState(), documents, List.of("doc-2", "doc-1"));
		Invocation extension = invokeExtension(builder -> builder.userPrompt("query")
			.topK(3)
			.similarityThreshold(0.72)
			.filterExpression(filter)
			.enableRanker(false), new OverAllState(), documents, List.of("doc-2", "doc-1"));

		assertSearchRequestsEqual(core.vectorStore().lastSearchRequest(), extension.vectorStore().lastSearchRequest());
		assertEquals("query", extension.vectorStore().lastSearchRequest().getQuery());
		assertEquals(3, extension.vectorStore().lastSearchRequest().getTopK());
		assertEquals(0.72, extension.vectorStore().lastSearchRequest().getSimilarityThreshold());
		assertEquals(filter, extension.vectorStore().lastSearchRequest().getFilterExpression());
		assertEquals(List.of("doc-1", "doc-2"), documentIds(extension.output(), "output"));
		assertEquals("queryDocument: \n\nalpha\nDocument: \n\nbeta\n", extension.output().get("user_prompt"));
		assertEquals(core.output(), extension.output());
		assertNull(core.rerankModel().lastRequest());
		assertNull(extension.rerankModel().lastRequest());
	}

	@Test
	void stateKeysOverridePresetValuesAndEnabledRankerMatchesCore() throws Exception {
		Filter.Expression presetFilter = filter("kind", "preset");
		Filter.Expression stateFilter = filter("kind", "state");
		CapturingRerankOptions presetOptions = new CapturingRerankOptions("preset-rerank", 1);
		CapturingRerankOptions stateOptions = new CapturingRerankOptions("state-rerank", 2);
		List<Document> presetDocuments = List.of(document("preset-doc", "preset"));
		List<Document> stateDocuments = List.of(document("doc-1", "alpha"), document("doc-2", "beta"));
		Map<String, Object> stateData = new HashMap<>();
		stateData.put("prompt_key", "state query");
		stateData.put("top_k_key", 1);
		stateData.put("similarity_key", 0.51);
		stateData.put("filter_key", stateFilter);
		stateData.put("enable_ranker_key", true);
		stateData.put("rerank_options_key", stateOptions);

		CapturingVectorStore coreStateVectorStore = new CapturingVectorStore(stateDocuments);
		CapturingRerankModel coreStateRerankModel = new CapturingRerankModel(List.of("doc-2", "doc-1"));
		Map<String, Object> coreStateData = new HashMap<>(stateData);
		coreStateData.put("rerank_model_key", coreStateRerankModel);
		coreStateData.put("vector_store_key", coreStateVectorStore);
		io.github.agentic.spring.ai.graph.node.KnowledgeRetrievalNode.Builder coreBuilder = io.github.agentic.spring.ai.graph.node.KnowledgeRetrievalNode
			.builder()
			.vectorStore(new CapturingVectorStore(presetDocuments))
			.rerankModel(new CapturingRerankModel(List.of("doc-1")))
			.userPromptKey("prompt_key")
			.userPrompt("preset query")
			.topKKey("top_k_key")
			.topK(7)
			.similarityThresholdKey("similarity_key")
			.similarityThreshold(0.91)
			.filterExpressionKey("filter_key")
			.filterExpression(presetFilter)
			.enableRankerKey("enable_ranker_key")
			.enableRanker(false)
			.rerankModelKey("rerank_model_key")
			.rerankOptionsKey("rerank_options_key")
			.rerankOptions(presetOptions)
			.vectorStoreKey("vector_store_key");
		Invocation core = new Invocation(coreBuilder.build().apply(new OverAllState(coreStateData)), coreStateVectorStore,
				coreStateRerankModel);

		CapturingVectorStore extensionStateVectorStore = new CapturingVectorStore(stateDocuments);
		CapturingRerankModel extensionStateRerankModel = new CapturingRerankModel(List.of("doc-2", "doc-1"));
		Map<String, Object> extensionStateData = new HashMap<>(stateData);
		extensionStateData.put("rerank_model_key", extensionStateRerankModel);
		extensionStateData.put("vector_store_key", extensionStateVectorStore);
		KnowledgeRetrievalNode.Builder extensionBuilder = KnowledgeRetrievalNode.builder()
			.vectorStore(new CapturingVectorStore(presetDocuments))
			.rerankModel(new CapturingRerankModel(List.of("doc-1")))
			.userPromptKey("prompt_key")
			.userPrompt("preset query")
			.topKKey("top_k_key")
			.topK(7)
			.similarityThresholdKey("similarity_key")
			.similarityThreshold(0.91)
			.filterExpressionKey("filter_key")
			.filterExpression(presetFilter)
			.enableRankerKey("enable_ranker_key")
			.enableRanker(false)
			.rerankModelKey("rerank_model_key")
			.rerankOptionsKey("rerank_options_key")
			.rerankOptions(presetOptions)
			.vectorStoreKey("vector_store_key");
		Invocation extension = new Invocation(extensionBuilder.build().apply(new OverAllState(extensionStateData)),
				extensionStateVectorStore, extensionStateRerankModel);

		assertSearchRequestsEqual(core.vectorStore().lastSearchRequest(), extension.vectorStore().lastSearchRequest());
		assertEquals("state query", extension.vectorStore().lastSearchRequest().getQuery());
		assertEquals(1, extension.vectorStore().lastSearchRequest().getTopK());
		assertEquals(0.51, extension.vectorStore().lastSearchRequest().getSimilarityThreshold());
		assertEquals(stateFilter, extension.vectorStore().lastSearchRequest().getFilterExpression());
		assertRerankRequestsEqual(core.rerankModel().lastRequest(), extension.rerankModel().lastRequest());
		assertEquals("state query", extension.rerankModel().lastRequest().getQuery());
		assertEquals(stateDocuments, extension.rerankModel().lastRequest().getInstructions());
		assertSame(stateOptions, extension.rerankModel().lastRequest().getOptions());
		assertEquals(List.of("doc-2", "doc-1"), documentIds(extension.output(), "output"));
		assertEquals("state queryDocument: \n\nbeta\nDocument: \n\nalpha\n", extension.output().get("prompt_key"));
		assertEquals(core.output(), extension.output());
	}

	@Test
	void presetValuesWinWhenKeyFirstIsFalseAndConfiguredOutputKeysMatchCore() throws Exception {
		Filter.Expression presetFilter = filter("kind", "preset");
		Filter.Expression stateFilter = filter("kind", "state");
		List<Document> presetDocuments = List.of(document("doc-1", "alpha"), document("doc-2", "beta"));
		CapturingVectorStore stateVectorStore = new CapturingVectorStore(List.of(document("state-doc", "state")));
		Map<String, Object> stateData = new HashMap<>();
		stateData.put("prompt_key", "state query");
		stateData.put("top_k_key", 1);
		stateData.put("similarity_key", 0.51);
		stateData.put("filter_key", stateFilter);
		stateData.put("enable_ranker_key", true);
		stateData.put("rerank_model_key", new CapturingRerankModel(List.of("state-doc")));
		stateData.put("rerank_options_key", new CapturingRerankOptions("state-rerank", 1));
		stateData.put("vector_store_key", stateVectorStore);

		Invocation core = invokeCore(builder -> builder.userPromptKey("prompt_key")
			.userPrompt("preset query")
			.topKKey("top_k_key")
			.topK(4)
			.similarityThresholdKey("similarity_key")
			.similarityThreshold(0.81)
			.filterExpressionKey("filter_key")
			.filterExpression(presetFilter)
			.enableRankerKey("enable_ranker_key")
			.enableRanker(false)
			.vectorStoreKey("vector_store_key")
			.outputKey("documents")
			.isKeyFirst(false), new OverAllState(stateData), presetDocuments, List.of("doc-2", "doc-1"));
		Invocation extension = invokeExtension(builder -> builder.userPromptKey("prompt_key")
			.userPrompt("preset query")
			.topKKey("top_k_key")
			.topK(4)
			.similarityThresholdKey("similarity_key")
			.similarityThreshold(0.81)
			.filterExpressionKey("filter_key")
			.filterExpression(presetFilter)
			.enableRankerKey("enable_ranker_key")
			.enableRanker(false)
			.vectorStoreKey("vector_store_key")
			.outputKey("documents")
			.isKeyFirst(false), new OverAllState(stateData), presetDocuments, List.of("doc-2", "doc-1"));

		assertSearchRequestsEqual(core.vectorStore().lastSearchRequest(), extension.vectorStore().lastSearchRequest());
		assertEquals("preset query", extension.vectorStore().lastSearchRequest().getQuery());
		assertEquals(4, extension.vectorStore().lastSearchRequest().getTopK());
		assertEquals(0.81, extension.vectorStore().lastSearchRequest().getSimilarityThreshold());
		assertEquals(presetFilter, extension.vectorStore().lastSearchRequest().getFilterExpression());
		assertEquals(List.of("doc-1", "doc-2"), documentIds(extension.output(), "documents"));
		assertEquals("preset queryDocument: \n\nalpha\nDocument: \n\nbeta\n", extension.output().get("prompt_key"));
		assertFalse(extension.output().containsKey("output"));
		assertEquals(core.output(), extension.output());
		assertEquals(0, stateVectorStore.searchCount());
	}

	@Test
	void missingVectorStoreFailsLikeCore() {
		Throwable core = thrownBy(
				() -> io.github.agentic.spring.ai.graph.node.KnowledgeRetrievalNode.builder().userPrompt("query").build()
					.apply(new OverAllState()));
		Throwable extension = thrownBy(() -> KnowledgeRetrievalNode.builder().userPrompt("query").build()
			.apply(new OverAllState()));

		assertNotNull(core);
		assertNotNull(extension);
		assertEquals(core.getClass(), extension.getClass());
		assertMessageContains(core, extension, "vectorStore");
	}

	@Test
	void invalidTopKFailsLikeCore() {
		Throwable core = thrownBy(() -> io.github.agentic.spring.ai.graph.node.KnowledgeRetrievalNode.builder()
			.userPrompt("query")
			.topK(0)
			.vectorStore(new CapturingVectorStore(List.of(document("doc-1", "alpha"))))
			.build()
			.apply(new OverAllState()));
		Throwable extension = thrownBy(() -> KnowledgeRetrievalNode.builder()
			.userPrompt("query")
			.topK(0)
			.vectorStore(new CapturingVectorStore(List.of(document("doc-1", "alpha"))))
			.build()
			.apply(new OverAllState()));

		assertNotNull(core);
		assertNotNull(extension);
		assertEquals(core.getClass(), extension.getClass());
		assertMessageContains(core, extension, "topK");
	}

	private static Invocation invokeCore(CoreBuilderConfigurer configurer, OverAllState state, List<Document> documents,
			List<String> rerankedIds) throws Exception {
		CapturingVectorStore vectorStore = new CapturingVectorStore(documents);
		CapturingRerankModel rerankModel = new CapturingRerankModel(rerankedIds);
		io.github.agentic.spring.ai.graph.node.KnowledgeRetrievalNode.Builder builder = io.github.agentic.spring.ai.graph.node.KnowledgeRetrievalNode
			.builder()
			.vectorStore(vectorStore)
			.rerankModel(rerankModel);
		configurer.configure(builder);
		return new Invocation(builder.build().apply(state), vectorStore, rerankModel);
	}

	private static Invocation invokeExtension(ExtensionBuilderConfigurer configurer, OverAllState state,
			List<Document> documents, List<String> rerankedIds) throws Exception {
		CapturingVectorStore vectorStore = new CapturingVectorStore(documents);
		CapturingRerankModel rerankModel = new CapturingRerankModel(rerankedIds);
		KnowledgeRetrievalNode.Builder builder = KnowledgeRetrievalNode.builder()
			.vectorStore(vectorStore)
			.rerankModel(rerankModel);
		configurer.configure(builder);
		return new Invocation(builder.build().apply(state), vectorStore, rerankModel);
	}

	private static void assertSearchRequestsEqual(SearchRequest expected, SearchRequest actual) {
		assertNotNull(expected);
		assertNotNull(actual);
		assertEquals(expected.getQuery(), actual.getQuery());
		assertEquals(expected.getTopK(), actual.getTopK());
		assertEquals(expected.getSimilarityThreshold(), actual.getSimilarityThreshold());
		assertEquals(expected.getFilterExpression(), actual.getFilterExpression());
	}

	private static void assertRerankRequestsEqual(RerankRequest expected, RerankRequest actual) {
		assertNotNull(expected);
		assertNotNull(actual);
		assertEquals(expected.getQuery(), actual.getQuery());
		assertEquals(expected.getInstructions(), actual.getInstructions());
		assertEquals(expected.getOptions(), actual.getOptions());
	}

	private static void assertMessageContains(Throwable core, Throwable extension, String fragment) {
		assertNotNull(core.getMessage());
		assertNotNull(extension.getMessage());
		assertEquals(core.getMessage(), extension.getMessage());
		assertTrue(extension.getMessage().contains(fragment), extension.getMessage());
	}

	private static Throwable thrownBy(ThrowingRunnable runnable) {
		try {
			runnable.run();
			return null;
		}
		catch (Throwable ex) {
			return ex;
		}
	}

	@SuppressWarnings("unchecked")
	private static List<String> documentIds(Map<String, Object> output, String outputKey) {
		Object value = output.get(outputKey);
		assertInstanceOf(List.class, value);
		return ((List<Document>) value).stream().map(Document::getId).toList();
	}

	private static Filter.Expression filter(String key, String value) {
		return new Filter.Expression(Filter.ExpressionType.EQ, new Filter.Key(key), new Filter.Value(value));
	}

	private static Document document(String id, String text) {
		return Document.builder().id(id).text(text).build();
	}

	private static DocumentWithScore scored(Document document, double score) {
		return DocumentWithScore.builder().withDocument(document).withScore(score).build();
	}

	@FunctionalInterface
	private interface CoreBuilderConfigurer {

		void configure(io.github.agentic.spring.ai.graph.node.KnowledgeRetrievalNode.Builder builder);

	}

	@FunctionalInterface
	private interface ExtensionBuilderConfigurer {

		void configure(KnowledgeRetrievalNode.Builder builder);

	}

	@FunctionalInterface
	private interface ThrowingRunnable {

		void run() throws Exception;

	}

	private record Invocation(Map<String, Object> output, CapturingVectorStore vectorStore,
			CapturingRerankModel rerankModel) {
	}

	private record CapturingRerankOptions(String model, Integer topN) implements RerankOptions {

		@Override
		public String getModel() {
			return this.model;
		}

		@Override
		public Integer getTopN() {
			return this.topN;
		}

	}

	private static final class CapturingRerankModel implements RerankModel {

		private final List<String> rerankedIds;

		private RerankRequest lastRequest;

		private CapturingRerankModel(List<String> rerankedIds) {
			this.rerankedIds = rerankedIds;
		}

		@Override
		public RerankResponse call(RerankRequest request) {
			this.lastRequest = request;
			Map<String, Document> documentsById = request.getInstructions()
				.stream()
				.collect(HashMap::new, (documents, document) -> documents.put(document.getId(), document),
						HashMap::putAll);
			List<DocumentWithScore> results = this.rerankedIds.stream()
				.map(documentsById::get)
				.filter(document -> document != null)
				.map(document -> scored(document, 1.0))
				.toList();
			return new RerankResponse(results);
		}

		private RerankRequest lastRequest() {
			return this.lastRequest;
		}

	}

	private static final class CapturingVectorStore implements VectorStore {

		private final List<Document> documents;

		private SearchRequest lastSearchRequest;

		private int searchCount;

		private CapturingVectorStore(List<Document> documents) {
			this.documents = documents;
		}

		@Override
		public void add(List<Document> documents) {
		}

		@Override
		public void delete(List<String> idList) {
		}

		@Override
		public void delete(Filter.Expression filterExpression) {
		}

		@Override
		public List<Document> similaritySearch(SearchRequest request) {
			this.lastSearchRequest = request;
			this.searchCount++;
			return this.documents;
		}

		private SearchRequest lastSearchRequest() {
			return this.lastSearchRequest;
		}

		private int searchCount() {
			return this.searchCount;
		}

	}

}
