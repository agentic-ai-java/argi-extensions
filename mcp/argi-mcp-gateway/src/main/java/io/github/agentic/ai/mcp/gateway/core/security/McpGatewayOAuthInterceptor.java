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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.util.StringUtils;
import org.springframework.web.reactive.function.client.ClientRequest;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.ExchangeFilterFunction;
import org.springframework.web.reactive.function.client.ExchangeFunction;
import reactor.core.publisher.Mono;

/**
 * OAuth认证拦截器 自动为MCP Gateway的HTTP请求添加OAuth认证头
 *
 * <p>
 * token 获取阶段与下游请求阶段相互隔离：只有 token 为空或获取异常会按严格/兼容模式处理；
 * 下游 {@link ExchangeFunction#exchange(ClientRequest)} 抛出的异常原样向上传播，同一请求不会被重复发送。
 * 严格模式（默认开启）下拿不到 token 时直接失败，不会发出无认证请求。
 */
public class McpGatewayOAuthInterceptor implements ExchangeFilterFunction {

	private static final Logger logger = LoggerFactory.getLogger(McpGatewayOAuthInterceptor.class);

	private final McpGatewayOAuthTokenManager tokenManager;

	private final McpGatewayOAuthProperties oauthProperties;

	public McpGatewayOAuthInterceptor(McpGatewayOAuthTokenManager tokenManager,
			McpGatewayOAuthProperties oauthProperties) {
		this.tokenManager = tokenManager;
		this.oauthProperties = oauthProperties;
	}

	@Override
	public Mono<ClientResponse> filter(ClientRequest request, ExchangeFunction next) {
		if (!oauthProperties.isEnabled()) {
			logger.debug("OAuth 身份验证未开启");
			return next.exchange(request);
		}

		logger.debug("OAuth 认证 URL: {}", request.url());

		// 先完成 token 阶段再调用下游，保证下游异常不会落入 token 阶段的错误处理而被重发
		return resolveAuthenticatedRequest(request).flatMap(next::exchange).doOnNext(this::evictTokenOnUnauthorized);
	}

	/**
	 * token 获取阶段：成功时返回携带 Bearer 头的请求；token 为空或获取失败时按严格/兼容模式处理
	 */
	private Mono<ClientRequest> resolveAuthenticatedRequest(ClientRequest request) {
		return tokenManager.getAccessToken()
			.filter(StringUtils::hasText)
			.map(accessToken -> withBearerToken(request, accessToken))
			.onErrorResume(error -> handleTokenUnavailable(request, "OAuth token 获取失败", error))
			.switchIfEmpty(Mono.defer(() -> handleTokenUnavailable(request, "没有可用的 OAuth token", null)));
	}

	private ClientRequest withBearerToken(ClientRequest request, String accessToken) {
		logger.debug("成功添加 OAuth 认证");
		return ClientRequest.from(request).header(HttpHeaders.AUTHORIZATION, "Bearer " + accessToken).build();
	}

	private Mono<ClientRequest> handleTokenUnavailable(ClientRequest request, String reason,
			@Nullable Throwable cause) {
		if (oauthProperties.isStrictMode()) {
			String message = reason + "，严格模式下已拒绝发送请求: " + request.url();
			logger.error(message, cause);
			return Mono.error(new IllegalStateException(message, cause));
		}

		logger.warn("{}，兼容模式下继续发送无认证请求: {}", reason, request.url(), cause);
		return Mono.just(request);
	}

	private void evictTokenOnUnauthorized(ClientResponse response) {
		if (response.statusCode().value() != HttpStatus.UNAUTHORIZED.value()) {
			return;
		}

		// 只清理缓存，不自动重放请求，避免复制有副作用的调用
		logger.warn("收到401未授权响应，OAuth token 无效");
		try {
			tokenManager.clearCachedToken();
			logger.info("已清除无效的缓存token");
		}
		catch (Exception e) {
			logger.debug("清除缓存token失败", e);
		}
	}

}
