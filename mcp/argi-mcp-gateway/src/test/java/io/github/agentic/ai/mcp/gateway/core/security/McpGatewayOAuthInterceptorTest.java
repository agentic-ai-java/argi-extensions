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

package io.github.agentic.ai.mcp.gateway.core.security;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.web.reactive.function.client.ClientRequest;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.ExchangeFunction;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link McpGatewayOAuthInterceptor} fail-closed and single-send semantics.
 */
class McpGatewayOAuthInterceptorTest {

	private static final String TOKEN = "test-access-token";

	private static final RuntimeException TOKEN_FAILURE = new RuntimeException("token endpoint unavailable");

	private static final RuntimeException DOWNSTREAM_FAILURE = new RuntimeException("connection reset");

	private McpGatewayOAuthTokenManager tokenManager;

	private McpGatewayOAuthProperties properties;

	private ClientRequest request;

	@BeforeEach
	void setUp() {
		tokenManager = mock(McpGatewayOAuthTokenManager.class);
		properties = new McpGatewayOAuthProperties();
		properties.setEnabled(true);
		request = ClientRequest.create(HttpMethod.POST, URI.create("http://mcp.example/tools/call")).build();
	}

	@Test
	void strictModeIsEnabledByDefault() {
		assertTrue(new McpGatewayOAuthProperties().isStrictMode());
	}

	@Test
	void disabledOAuthPassesRequestThroughUnchanged() {
		properties.setEnabled(false);
		RecordingExchange exchange = RecordingExchange.returning(HttpStatus.OK);

		ClientResponse response = filter(exchange).block();

		assertNotNull(response);
		assertEquals(1, exchange.callCount());
		assertSame(request, exchange.lastRequest());
		assertNull(exchange.lastRequest().headers().getFirst(HttpHeaders.AUTHORIZATION));
		verify(tokenManager, never()).getAccessToken();
	}

	@Test
	void validTokenIsAttachedAsBearerHeaderAndSentOnce() {
		when(tokenManager.getAccessToken()).thenReturn(Mono.just(TOKEN));
		RecordingExchange exchange = RecordingExchange.returning(HttpStatus.OK);

		ClientResponse response = filter(exchange).block();

		assertNotNull(response);
		assertEquals(1, exchange.callCount());
		assertEquals("Bearer " + TOKEN, exchange.lastRequest().headers().getFirst(HttpHeaders.AUTHORIZATION));
	}

	@Test
	void strictModeEmptyTokenNeverCallsDownstream() {
		when(tokenManager.getAccessToken()).thenReturn(Mono.empty());
		RecordingExchange exchange = RecordingExchange.returning(HttpStatus.OK);

		assertThrows(IllegalStateException.class, () -> filter(exchange).block());

		assertEquals(0, exchange.callCount());
	}

	@Test
	void strictModeBlankTokenNeverCallsDownstream() {
		when(tokenManager.getAccessToken()).thenReturn(Mono.just("   "));
		RecordingExchange exchange = RecordingExchange.returning(HttpStatus.OK);

		assertThrows(IllegalStateException.class, () -> filter(exchange).block());

		assertEquals(0, exchange.callCount());
	}

	@Test
	void strictModeTokenErrorNeverCallsDownstreamAndKeepsCause() {
		when(tokenManager.getAccessToken()).thenReturn(Mono.error(TOKEN_FAILURE));
		RecordingExchange exchange = RecordingExchange.returning(HttpStatus.OK);

		IllegalStateException error = assertThrows(IllegalStateException.class, () -> filter(exchange).block());

		assertEquals(0, exchange.callCount());
		assertSame(TOKEN_FAILURE, error.getCause());
	}

	@Test
	void downstreamErrorAfterValidTokenPropagatesWithoutResend() {
		when(tokenManager.getAccessToken()).thenReturn(Mono.just(TOKEN));
		RecordingExchange exchange = RecordingExchange.failing(DOWNSTREAM_FAILURE);

		RuntimeException error = assertThrows(RuntimeException.class, () -> filter(exchange).block());

		assertSame(DOWNSTREAM_FAILURE, error);
		assertEquals(1, exchange.callCount());
		assertEquals("Bearer " + TOKEN, exchange.lastRequest().headers().getFirst(HttpHeaders.AUTHORIZATION));
	}

	@Test
	void compatibilityModeDownstreamErrorAfterValidTokenAlsoPropagatesWithoutResend() {
		properties.setStrictMode(false);
		when(tokenManager.getAccessToken()).thenReturn(Mono.just(TOKEN));
		RecordingExchange exchange = RecordingExchange.failing(DOWNSTREAM_FAILURE);

		RuntimeException error = assertThrows(RuntimeException.class, () -> filter(exchange).block());

		assertSame(DOWNSTREAM_FAILURE, error);
		assertEquals(1, exchange.callCount());
		assertEquals("Bearer " + TOKEN, exchange.lastRequest().headers().getFirst(HttpHeaders.AUTHORIZATION));
	}

	@Test
	void compatibilityModeEmptyTokenSendsUnauthenticatedRequestOnce() {
		properties.setStrictMode(false);
		when(tokenManager.getAccessToken()).thenReturn(Mono.empty());
		RecordingExchange exchange = RecordingExchange.returning(HttpStatus.OK);

		ClientResponse response = filter(exchange).block();

		assertNotNull(response);
		assertEquals(1, exchange.callCount());
		assertNull(exchange.lastRequest().headers().getFirst(HttpHeaders.AUTHORIZATION));
	}

	@Test
	void compatibilityModeTokenErrorSendsUnauthenticatedRequestOnce() {
		properties.setStrictMode(false);
		when(tokenManager.getAccessToken()).thenReturn(Mono.error(TOKEN_FAILURE));
		RecordingExchange exchange = RecordingExchange.returning(HttpStatus.OK);

		ClientResponse response = filter(exchange).block();

		assertNotNull(response);
		assertEquals(1, exchange.callCount());
		assertNull(exchange.lastRequest().headers().getFirst(HttpHeaders.AUTHORIZATION));
	}

	@Test
	void unauthorizedResponseClearsCachedTokenWithoutReplay() {
		when(tokenManager.getAccessToken()).thenReturn(Mono.just(TOKEN));
		RecordingExchange exchange = RecordingExchange.returning(HttpStatus.UNAUTHORIZED);

		ClientResponse response = filter(exchange).block();

		assertNotNull(response);
		assertEquals(HttpStatus.UNAUTHORIZED, response.statusCode());
		assertEquals(1, exchange.callCount());
		verify(tokenManager, times(1)).clearCachedToken();
	}

	@Test
	void successfulResponseDoesNotClearCachedToken() {
		when(tokenManager.getAccessToken()).thenReturn(Mono.just(TOKEN));
		RecordingExchange exchange = RecordingExchange.returning(HttpStatus.OK);

		filter(exchange).block();

		verify(tokenManager, never()).clearCachedToken();
	}

	private Mono<ClientResponse> filter(ExchangeFunction exchange) {
		return new McpGatewayOAuthInterceptor(tokenManager, properties).filter(request, exchange);
	}

	/**
	 * Downstream stub that records every request it receives.
	 */
	private static final class RecordingExchange implements ExchangeFunction {

		private final AtomicInteger calls = new AtomicInteger();

		private final List<ClientRequest> requests = new ArrayList<>();

		private final HttpStatus status;

		private final @Nullable RuntimeException failure;

		private RecordingExchange(HttpStatus status, @Nullable RuntimeException failure) {
			this.status = status;
			this.failure = failure;
		}

		static RecordingExchange returning(HttpStatus status) {
			return new RecordingExchange(status, null);
		}

		static RecordingExchange failing(RuntimeException failure) {
			return new RecordingExchange(HttpStatus.OK, failure);
		}

		@Override
		public Mono<ClientResponse> exchange(ClientRequest request) {
			calls.incrementAndGet();
			requests.add(request);
			if (failure != null) {
				return Mono.error(failure);
			}
			return Mono.just(ClientResponse.create(status).build());
		}

		int callCount() {
			return calls.get();
		}

		ClientRequest lastRequest() {
			return requests.get(requests.size() - 1);
		}

	}

}
