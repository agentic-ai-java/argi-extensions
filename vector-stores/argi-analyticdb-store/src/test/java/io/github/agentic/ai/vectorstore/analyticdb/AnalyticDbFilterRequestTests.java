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
package io.github.agentic.ai.vectorstore.analyticdb;

import java.util.List;

import com.aliyun.gpdb20160503.Client;
import com.aliyun.gpdb20160503.models.DeleteCollectionDataRequest;
import com.aliyun.gpdb20160503.models.DeleteCollectionDataResponse;
import com.aliyun.gpdb20160503.models.QueryCollectionDataRequest;
import com.aliyun.gpdb20160503.models.QueryCollectionDataResponse;
import com.aliyun.gpdb20160503.models.QueryCollectionDataResponseBody;
import com.aliyun.gpdb20160503.models.QueryCollectionDataResponseBody.QueryCollectionDataResponseBodyMatches;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import org.springframework.ai.embedding.EmbeddingModel;
import org.springframework.ai.vectorstore.SearchRequest;
import org.springframework.ai.vectorstore.filter.FilterExpressionBuilder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AnalyticDbFilterRequestTests {

	@Test
	void searchAndDeleteSendTheSameMetadataPredicateToTheSdk() throws Exception {
		Client client = mock(Client.class);
		EmbeddingModel embeddings = mock(EmbeddingModel.class);
		when(embeddings.embed("query")).thenReturn(new float[] { 1f });
		var matches = new QueryCollectionDataResponseBodyMatches().setMatch(List.of());
		when(client.queryCollectionData(any())).thenReturn(
				new QueryCollectionDataResponse().setBody(new QueryCollectionDataResponseBody().setMatches(matches)));
		when(client.deleteCollectionData(any())).thenReturn(new DeleteCollectionDataResponse());
		var store = AnalyticDbVectorStore.builder("docs", new AnalyticDbConfig(), client, embeddings).build();
		var expression = new FilterExpressionBuilder().eq("tenantId", "t001").build();

		store.similaritySearch(SearchRequest.builder().query("query").filterExpression(expression).build());
		store.delete(expression);

		var search = ArgumentCaptor.forClass(QueryCollectionDataRequest.class);
		var delete = ArgumentCaptor.forClass(DeleteCollectionDataRequest.class);
		verify(client).queryCollectionData(search.capture());
		verify(client).deleteCollectionData(delete.capture());
		assertThat(search.getValue().getFilter()).isEqualTo("(metadata -> E'tenantId') = E'\"t001\"'::jsonb");
		assertThat(delete.getValue().getCollectionDataFilter()).isEqualTo(search.getValue().getFilter());
	}

}
