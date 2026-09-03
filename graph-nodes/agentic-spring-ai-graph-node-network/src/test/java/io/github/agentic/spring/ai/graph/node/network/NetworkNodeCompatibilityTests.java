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
package io.github.agentic.spring.ai.graph.node.network;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import io.github.agentic.spring.ai.document.DocumentParser;
import io.github.agentic.spring.ai.graph.OverAllState;
import io.github.agentic.spring.ai.graph.action.NodeAction;
import io.github.agentic.spring.ai.graph.utils.InMemoryFileStorage;
import okhttp3.HttpUrl;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import okhttp3.mockwebserver.SocketPolicy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.ai.document.Document;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.WebClient;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NetworkNodeCompatibilityTests {

	private final List<MockWebServer> servers = new java.util.ArrayList<>();

	@AfterEach
	void tearDown() throws IOException {
		for (MockWebServer server : this.servers) {
			server.shutdown();
		}
		InMemoryFileStorage.clear();
	}

	@Test
	void publicApiMatchesCoreCompatibilityClasses() {
		PublicApiParity.assertParity(io.github.agentic.spring.ai.graph.node.HttpNode.class, HttpNode.class);
		PublicApiParity.assertParity(io.github.agentic.spring.ai.graph.node.DocumentExtractorNode.class,
				DocumentExtractorNode.class);
		assertFalse(Modifier.isPublic(NetworkAccessPolicy.class.getModifiers()));
	}

	@ParameterizedTest(name = "{0} preserves HTTP non-2xx response mapping")
	@MethodSource("nodeFactories")
	void httpMapsNon2xxResponses(NetworkNodeFactory factory) throws Exception {
		MockWebServer server = server();
		server.enqueue(new MockResponse().setResponseCode(404)
			.setBody("{\"error\":\"Not Found\"}")
			.setHeader(HttpHeaders.CONTENT_TYPE, "application/json"));

		Map<String, Object> result = factory.httpBuilder(server.url("/notfound").toString())
			.webClient(WebClient.create(server.url("/").toString()))
			.method(HttpMethod.GET)
			.allowPrivateNetworkAccess(true)
			.retryConfig(0, 0, false)
			.build()
			.apply(new OverAllState());

		Map<String, Object> messages = messages(result);
		assertEquals(HttpStatus.NOT_FOUND.value(), messages.get("status"));
		assertEquals(Map.of("error", "Not Found"), messages.get("body"));
	}

	@ParameterizedTest(name = "{0} preserves HTTP URL/header/query substitution")
	@MethodSource("nodeFactories")
	void httpSubstitutesUrlHeadersAndQueryParams(NetworkNodeFactory factory) throws Exception {
		MockWebServer server = server();
		server.enqueue(new MockResponse().setBody("OK").setHeader(HttpHeaders.CONTENT_TYPE, "text/plain"));
		OverAllState state = new OverAllState(
				Map.of("pathVar", "users", "headerVal", "test-header", "queryVal", "test-query"));

		factory.httpBuilder(server.url("/").toString() + "${pathVar}")
			.webClient(WebClient.create(server.url("/").toString()))
			.method(HttpMethod.GET)
			.header("X-Header", "${headerVal}")
			.queryParam("param", "${queryVal}")
			.allowPrivateNetworkAccess(true)
			.build()
			.apply(state);

		RecordedRequest request = server.takeRequest();
		HttpUrl url = request.getRequestUrl();
		assertNotNull(url);
		assertEquals("/users", url.encodedPath());
		assertEquals("test-query", url.queryParameter("param"));
		assertEquals("test-header", request.getHeader("X-Header"));
	}

	@ParameterizedTest(name = "{0} preserves HTTP JSON/raw/form request bodies")
	@MethodSource("nodeFactories")
	void httpSendsJsonRawAndFormBodies(NetworkNodeFactory factory) throws Exception {
		MockWebServer server = server();
		server.enqueue(new MockResponse().setBody("OK"));
		server.enqueue(new MockResponse().setBody("OK"));
		server.enqueue(new MockResponse().setBody("OK"));
		WebClient webClient = WebClient.create(server.url("/").toString());

		factory.httpBuilder(server.url("/raw").toString())
			.webClient(webClient)
			.method(HttpMethod.POST)
			.bodyFrom("Hello ${name}")
			.allowPrivateNetworkAccess(true)
			.build()
			.apply(new OverAllState(Map.of("name", "Alice")));
		assertEquals("Hello Alice", server.takeRequest().getBody().readUtf8());

		factory.httpBuilder(server.url("/json").toString())
			.webClient(webClient)
			.method(HttpMethod.POST)
			.bodyFromJson(
					"{\"type\":\"JSON\",\"data\":{\"key1out\":\"${key1}\",\"key2out\":\"${key2}\"}}")
			.allowPrivateNetworkAccess(true)
			.build()
			.apply(new OverAllState(Map.of("key1", "value1", "key2", "{\"nested\":true}")));
		assertEquals("{\"key1out\":\"value1\",\"key2out\":{\"nested\":true}}",
				server.takeRequest().getBody().readUtf8().replaceAll("\\s+", ""));

		factory.httpBuilder(server.url("/form").toString())
			.webClient(webClient)
			.method(HttpMethod.POST)
			.bodyFrom(Map.of("type", "X_WWW_FORM_URLENCODED", "data",
					List.of(Map.of("key", "field1", "value", "${val1}"), Map.of("key", "field2", "value", "${val2}"))))
			.allowPrivateNetworkAccess(true)
			.build()
			.apply(new OverAllState(Map.of("val1", "v1", "val2", "v2")));
		assertEquals("field1=v1&field2=v2", server.takeRequest().getBody().readUtf8());
	}

	@ParameterizedTest(name = "{0} preserves HTTP multipart form-data bodies")
	@MethodSource("nodeFactories")
	void httpSendsMultipartFormDataBodies(NetworkNodeFactory factory) throws Exception {
		MockWebServer server = server();
		server.enqueue(new MockResponse().setBody("OK"));
		InMemoryFileStorage.FileRecord file = InMemoryFileStorage.save("binary-payload".getBytes(StandardCharsets.UTF_8),
				MediaType.APPLICATION_OCTET_STREAM_VALUE, "payload.bin");

		factory.httpBuilder(server.url("/multipart").toString())
			.webClient(WebClient.create(server.url("/").toString()))
			.method(HttpMethod.POST)
			.bodyFrom(Map.of("type", "FORM_DATA", "data",
					List.of(Map.of("key", "description", "type", "RAW_TEXT", "value", "report ${name}"),
							Map.of("key", "upload", "type", "file", "value", file.getId()))))
			.allowPrivateNetworkAccess(true)
			.build()
			.apply(new OverAllState(Map.of("name", "Alice")));

		RecordedRequest request = server.takeRequest();
		assertTrue(request.getHeader(HttpHeaders.CONTENT_TYPE).startsWith(MediaType.MULTIPART_FORM_DATA_VALUE));
		String body = request.getBody().readUtf8();
		assertTrue(body.contains("Content-Disposition: form-data; name=\"description\""));
		assertTrue(body.contains("report Alice"));
		assertTrue(body.contains("Content-Disposition: form-data; name=\"upload\"; filename=\"payload.bin\""));
		assertTrue(body.contains("Content-Type: " + MediaType.APPLICATION_OCTET_STREAM_VALUE));
		assertTrue(body.contains("binary-payload"));
	}

	@ParameterizedTest(name = "{0} preserves HTTP standalone binary bodies")
	@MethodSource("nodeFactories")
	void httpSendsStandaloneBinaryBodies(NetworkNodeFactory factory) throws Exception {
		MockWebServer server = server();
		server.enqueue(new MockResponse().setBody("OK"));
		byte[] payload = "standalone-binary".getBytes(StandardCharsets.UTF_8);

		factory.httpBuilder(server.url("/binary").toString())
			.webClient(WebClient.create(server.url("/").toString()))
			.method(HttpMethod.POST)
			.bodyFrom(Map.of("type", "BINARY", "data", List
				.of(Map.of("fileBytes", payload, "filename", "standalone.bin", "mimeType", "application/x-agentic-test"))))
			.allowPrivateNetworkAccess(true)
			.build()
			.apply(new OverAllState());

		RecordedRequest request = server.takeRequest();
		assertEquals("application/x-agentic-test", request.getHeader(HttpHeaders.CONTENT_TYPE));
		assertNull(request.getHeader(HttpHeaders.CONTENT_DISPOSITION));
		assertArrayEquals(payload, request.getBody().readByteArray());
	}

	@ParameterizedTest(name = "{0} preserves HTTP basic auth")
	@MethodSource("nodeFactories")
	void httpSendsBasicAuth(NetworkNodeFactory factory) throws Exception {
		MockWebServer server = server();
		server.enqueue(new MockResponse().setBody("OK"));

		factory.httpBuilder(server.url("/secure").toString())
			.webClient(WebClient.create(server.url("/").toString()))
			.method(HttpMethod.GET)
			.basicAuth("user", "pass")
			.allowPrivateNetworkAccess(true)
			.build()
			.apply(new OverAllState());

		assertEquals("Basic " + Base64.getEncoder().encodeToString("user:pass".getBytes(StandardCharsets.UTF_8)),
				server.takeRequest().getHeader(HttpHeaders.AUTHORIZATION));
	}

	@ParameterizedTest(name = "{0} preserves HTTP bearer auth")
	@MethodSource("nodeFactories")
	void httpSendsBearerAuth(NetworkNodeFactory factory) throws Exception {
		MockWebServer server = server();
		server.enqueue(new MockResponse().setBody("OK"));

		factory.httpBuilder(server.url("/bearer").toString())
			.webClient(WebClient.create(server.url("/").toString()))
			.method(HttpMethod.GET)
			.bearerAuth("token-123")
			.allowPrivateNetworkAccess(true)
			.build()
			.apply(new OverAllState());

		assertEquals("Bearer token-123", server.takeRequest().getHeader(HttpHeaders.AUTHORIZATION));
	}

	@ParameterizedTest(name = "{0} preserves HTTP retry behavior")
	@MethodSource("nodeFactories")
	void httpRetriesNetworkFailures(NetworkNodeFactory factory) throws Exception {
		MockWebServer server = server();
		server.enqueue(new MockResponse().setSocketPolicy(SocketPolicy.DISCONNECT_AT_START));
		server.enqueue(new MockResponse().setSocketPolicy(SocketPolicy.DISCONNECT_AT_START));
		server.enqueue(new MockResponse().setBody("OK").setHeader(HttpHeaders.CONTENT_TYPE, "text/plain"));

		Map<String, Object> result = factory.httpBuilder(server.url("/retry").toString())
			.webClient(WebClient.create(server.url("/").toString()))
			.method(HttpMethod.GET)
			.retryConfig(3, 100, true)
			.allowPrivateNetworkAccess(true)
			.build()
			.apply(new OverAllState());

		assertEquals(HttpStatus.OK.value(), messages(result).get("status"));
		assertEquals(3, server.getRequestCount());
	}

	@ParameterizedTest(name = "{0} preserves HTTP binary file response mapping")
	@MethodSource("nodeFactories")
	void httpStoresBinaryFileResponses(NetworkNodeFactory factory) throws Exception {
		MockWebServer server = server();
		byte[] fileBytes;
		try (InputStream inputStream = getClass().getResourceAsStream("/test.png")) {
			assertNotNull(inputStream);
			fileBytes = inputStream.readAllBytes();
		}
		server.enqueue(new MockResponse().setResponseCode(200)
			.addHeader(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"test.png\"")
			.addHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_OCTET_STREAM_VALUE)
			.setBody(new okio.Buffer().write(fileBytes)));

		Map<String, Object> result = factory.httpBuilder(server.url("/test.png").toString())
			.webClient(WebClient.create(server.url("/").toString()))
			.allowPrivateNetworkAccess(true)
			.build()
			.apply(new OverAllState());

		List<String> files = files(messages(result));
		assertEquals(1, files.size());
		InMemoryFileStorage.FileRecord record = InMemoryFileStorage.get(files.get(0));
		assertNotNull(record);
		assertEquals("test.png", record.getName());
		assertArrayEquals(fileBytes, record.getContent());
	}

	@ParameterizedTest(name = "{0} blocks private HTTP addresses before connect")
	@MethodSource("nodeFactories")
	void httpBlocksPrivateAddressesBeforeConnect(NetworkNodeFactory factory) throws Exception {
		MockWebServer server = server();

		Throwable failure = thrownBy(() -> factory.httpBuilder(server.url("/private").toString())
			.method(HttpMethod.GET)
			.retryConfig(1, 1, false)
			.build()
			.apply(new OverAllState()));

		assertFailure(failure, IOException.class, "Private network access");
		assertEquals(0, server.getRequestCount());
	}

	@ParameterizedTest(name = "{0} keeps custom WebClient private-network preflight")
	@MethodSource("nodeFactories")
	void httpCustomWebClientStillPreflightsPrivateAddresses(NetworkNodeFactory factory) throws Exception {
		MockWebServer server = server();

		Throwable failure = thrownBy(() -> factory.httpBuilder(server.url("/private-custom").toString())
			.webClient(WebClient.create(server.url("/").toString()))
			.method(HttpMethod.GET)
			.retryConfig(1, 1, false)
			.build()
			.apply(new OverAllState()));

		assertFailure(failure, IOException.class, "Private network access");
		assertEquals(0, server.getRequestCount());
	}

	@ParameterizedTest(name = "{0} preserves HTTP request timeout failures")
	@MethodSource("nodeFactories")
	void httpFailsWhenRequestTimeoutExpires(NetworkNodeFactory factory) throws Exception {
		MockWebServer server = server();
		server.enqueue(new MockResponse().setBody("slow")
			.setHeader(HttpHeaders.CONTENT_TYPE, "text/plain")
			.setBodyDelay(1, TimeUnit.SECONDS));

		Throwable failure = thrownBy(() -> factory.httpBuilder(server.url("/slow").toString())
			.webClient(WebClient.create(server.url("/").toString()))
			.method(HttpMethod.GET)
			.retryConfig(1, 1, false)
			.allowPrivateNetworkAccess(true)
			.requestTimeout(Duration.ofMillis(50))
			.build()
			.apply(new OverAllState()));

		assertNotNull(failure);
		assertMessageContains(failure, "timeout");
	}

	@ParameterizedTest(name = "{0} preserves HTTP null and invalid input failures")
	@MethodSource("nodeFactories")
	void httpRejectsNullBodyAndInvalidScheme(NetworkNodeFactory factory) throws Exception {
		MockWebServer server = server();
		server.enqueue(new MockResponse().setBody("OK"));

		Throwable nullBodyFailure = thrownBy(() -> factory.httpBuilder(server.url("/null-body").toString())
			.webClient(WebClient.create(server.url("/").toString()))
			.method(HttpMethod.POST)
			.body(null)
			.allowPrivateNetworkAccess(true)
			.build()
			.apply(new OverAllState()));
		assertFailure(nullBodyFailure, NullPointerException.class, "getType");

		Throwable invalidSchemeFailure = thrownBy(() -> factory.httpBuilder("ftp://example.com/file.txt")
			.method(HttpMethod.GET)
			.build()
			.apply(new OverAllState()));
		assertFailure(invalidSchemeFailure, IOException.class, "Unsupported URI scheme");
	}

	@ParameterizedTest(name = "{0} preserves document local TXT and JSON extraction")
	@MethodSource("nodeFactories")
	void documentReadsLocalTxtAndJson(NetworkNodeFactory factory) throws Exception {
		Path localRoot = Files.createTempDirectory("document-extractor-root");
		Path txt = Files.writeString(localRoot.resolve("sample.txt"), "hello from root");
		Path json = Files.writeString(localRoot.resolve("sample.json"), "{\"text\":\"hello json\"}");

		Map<String, Object> result = factory.documentBuilder(List.of(txt.toString(), json.toString()))
			.localRoot(localRoot)
			.inputIsArray(true)
			.build()
			.apply(new OverAllState());

		assertEquals(List.of("hello from root", "{text=hello json}"), result.get("text"));
	}

	@ParameterizedTest(name = "{0} preserves explicit parser precedence")
	@MethodSource("nodeFactories")
	void documentUsesExplicitParserBeforeExtensionDefaults(NetworkNodeFactory factory) throws Exception {
		Path localRoot = Files.createTempDirectory("document-extractor-root");
		Path document = Files.writeString(localRoot.resolve("sample.json"), "{\"text\":\"original\"}");
		DocumentParser parser = inputStream -> {
			try {
				return List.of(new Document("custom:" + new String(inputStream.readAllBytes(), StandardCharsets.UTF_8)));
			}
			catch (IOException ex) {
				throw new IllegalStateException(ex);
			}
		};

		Map<String, Object> result = factory.documentBuilder(List.of(document.toString()))
			.localRoot(localRoot)
			.documentParser("json", parser)
			.build()
			.apply(new OverAllState());

		assertEquals("custom:{\"text\":\"original\"}", result.get("text"));
	}

	@ParameterizedTest(name = "{0} discovers document parsers from ServiceLoader")
	@MethodSource("nodeFactories")
	void documentDiscoversServiceLoadedParsers(NetworkNodeFactory factory) throws Exception {
		Path localRoot = Files.createTempDirectory("document-extractor-root");
		Path document = Files.writeString(localRoot.resolve("sample.svc"), "loaded");

		Map<String, Object> result = factory.documentBuilder(List.of(document.toString()))
			.localRoot(localRoot)
			.build()
			.apply(new OverAllState());

		assertEquals("service:loaded", result.get("text"));
	}

	@ParameterizedTest(name = "{0} preserves document local-root and regular-file checks")
	@MethodSource("nodeFactories")
	void documentEnforcesLocalRootAndRegularFiles(NetworkNodeFactory factory) throws Exception {
		Path localRoot = Files.createTempDirectory("document-extractor-root");
		Path outside = Files.writeString(Files.createTempDirectory("document-extractor-outside").resolve("secret.txt"),
				"secret");
		Path directory = Files.createDirectory(localRoot.resolve("directory.txt"));

		Throwable outsideFailure = thrownBy(() -> factory.documentBuilder(List.of(outside.toString()))
			.localRoot(localRoot)
			.build()
			.apply(new OverAllState()));
		assertFailure(outsideFailure, RuntimeException.class, "Failed to parse");
		assertMessageContains(outsideFailure.getCause(), "outside the configured root");

		Throwable directoryFailure = thrownBy(() -> factory.documentBuilder(List.of(directory.toString()))
			.localRoot(localRoot)
			.build()
			.apply(new OverAllState()));
		assertFailure(directoryFailure, RuntimeException.class, "Failed to parse");
		assertMessageContains(directoryFailure.getCause(), "regular file");
	}

	@ParameterizedTest(name = "{0} preserves unsupported document extension failures")
	@MethodSource("nodeFactories")
	void documentRejectsUnsupportedExtensions(NetworkNodeFactory factory) throws Exception {
		Path localRoot = Files.createTempDirectory("document-extractor-root");
		Path document = Files.writeString(localRoot.resolve("sample.md"), "# markdown");

		Throwable failure = thrownBy(() -> factory.documentBuilder(List.of(document.toString()))
			.localRoot(localRoot)
			.build()
			.apply(new OverAllState()));

		assertFailure(failure, RuntimeException.class, "Failed to parse");
		assertMessageContains(failure.getCause(), "Unsupported Extension Type: md");
	}

	@ParameterizedTest(name = "{0} preserves remote-disabled document mode")
	@MethodSource("nodeFactories")
	void documentBlocksRemoteAccessWhenDisabled(NetworkNodeFactory factory) throws Exception {
		MockWebServer server = server();

		Throwable failure = thrownBy(() -> factory.documentBuilder(List.of(server.url("/doc.txt").toString()))
			.allowRemoteAccess(false)
			.build()
			.apply(new OverAllState()));

		assertFailure(failure, RuntimeException.class, "Failed to parse");
		assertMessageContains(failure.getCause(), "Remote document access is disabled");
		assertEquals(0, server.getRequestCount());
	}

	@ParameterizedTest(name = "{0} validates document redirects")
	@MethodSource("nodeFactories")
	void documentValidatesRedirectTargets(NetworkNodeFactory factory) throws Exception {
		MockWebServer server = server();
		server.enqueue(new MockResponse().setResponseCode(302).addHeader(HttpHeaders.LOCATION, "ftp://example.com/doc.txt"));

		Throwable failure = thrownBy(() -> factory.documentBuilder(List.of(server.url("/redirect.txt").toString()))
			.allowPrivateNetworkAccess(true)
			.build()
			.apply(new OverAllState()));

		assertFailure(failure, RuntimeException.class, "Failed to parse");
		assertMessageContains(failure.getCause(), "Unsupported URI scheme");
		assertEquals(1, server.getRequestCount());
	}

	@ParameterizedTest(name = "{0} blocks private remote documents before connect")
	@MethodSource("nodeFactories")
	void documentBlocksPrivateRemoteDocuments(NetworkNodeFactory factory) throws Exception {
		MockWebServer server = server();

		Throwable failure = thrownBy(() -> factory.documentBuilder(List.of(server.url("/doc.txt").toString()))
			.build()
			.apply(new OverAllState()));

		assertFailure(failure, RuntimeException.class, "Failed to parse");
		assertMessageContains(failure.getCause(), "Private network access");
		assertEquals(0, server.getRequestCount());
	}

	@ParameterizedTest(name = "{0} preserves document max bytes limit")
	@MethodSource("nodeFactories")
	void documentRejectsDocumentsLargerThanMaxBytes(NetworkNodeFactory factory) throws Exception {
		Path localRoot = Files.createTempDirectory("document-extractor-root");
		Path document = Files.writeString(localRoot.resolve("large.txt"), "0123456789");

		Throwable failure = thrownBy(() -> factory.documentBuilder(List.of(document.toString()))
			.localRoot(localRoot)
			.maxBytes(4)
			.build()
			.apply(new OverAllState()));

		assertFailure(failure, RuntimeException.class, "Failed to parse");
		assertMessageContains(failure.getCause(), "maximum size");
	}

	@ParameterizedTest(name = "{0} preserves document total deadline")
	@MethodSource("nodeFactories")
	void documentAbortsAtTotalDeadline(NetworkNodeFactory factory) throws Exception {
		MockWebServer server = server();
		server.enqueue(new MockResponse().setBody("slow document").throttleBody(1, 100, TimeUnit.MILLISECONDS));

		long startedAt = System.nanoTime();
		Throwable failure = thrownBy(() -> factory.documentBuilder(List.of(server.url("/slow.txt").toString()))
			.allowPrivateNetworkAccess(true)
			.readTimeoutMillis(1000)
			.totalTimeout(Duration.ofMillis(150))
			.build()
			.apply(new OverAllState()));
		long elapsedMillis = Duration.ofNanos(System.nanoTime() - startedAt).toMillis();

		assertFailure(failure, RuntimeException.class, "Failed to parse");
		assertMessageContains(failure.getCause(), "total timeout");
		assertTrue(elapsedMillis < 750, "total timeout should cancel the active network read");
	}

	@ParameterizedTest(name = "{0} preserves document null and invalid input failures")
	@MethodSource("nodeFactories")
	void documentRejectsNullAndInvalidInput(NetworkNodeFactory factory) throws Exception {
		Throwable missingFileFailure = thrownBy(
				() -> factory.documentBuilder(null).build().apply(new OverAllState()));
		assertFailure(missingFileFailure, RuntimeException.class, "File variable not found for selector");

		Throwable invalidArrayFailure = thrownBy(() -> factory.documentBuilder(null)
			.paramsKey("files")
			.inputIsArray(true)
			.build()
			.apply(new OverAllState(Map.of("files", "not-json-array"))));
		assertFailure(invalidArrayFailure, RuntimeException.class, "Variable fileList is not an ArrayFileSegment");
	}

	private MockWebServer server() throws IOException {
		MockWebServer server = new MockWebServer();
		server.start();
		this.servers.add(server);
		return server;
	}

	private static Stream<NetworkNodeFactory> nodeFactories() {
		return Stream.of(
				new NetworkNodeFactory("Core", io.github.agentic.spring.ai.graph.node.HttpNode.class,
						io.github.agentic.spring.ai.graph.node.DocumentExtractorNode.class),
				new NetworkNodeFactory("Extension", HttpNode.class, DocumentExtractorNode.class));
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> messages(Map<String, Object> result) {
		return (Map<String, Object>) result.get("messages");
	}

	@SuppressWarnings("unchecked")
	private static List<String> files(Map<String, Object> messages) {
		return (List<String>) messages.get("files");
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

	private static void assertFailure(Throwable failure, Class<? extends Throwable> type, String messageFragment) {
		assertNotNull(failure);
		assertTrue(type.isInstance(failure), () -> "Expected " + type.getName() + " but got " + failure.getClass());
		assertMessageContains(failure, messageFragment);
	}

	private static void assertMessageContains(Throwable failure, String messageFragment) {
		assertNotNull(failure);
		String message = failure.getMessage();
		if (message == null && failure.getCause() != null) {
			message = failure.getCause().getMessage();
		}
		assertNotNull(message);
		String actualMessage = message;
		assertTrue(actualMessage.toLowerCase(java.util.Locale.ROOT)
			.contains(messageFragment.toLowerCase(java.util.Locale.ROOT)),
				() -> "Expected message to contain [" + messageFragment + "] but was [" + actualMessage + "]");
	}

	@FunctionalInterface
	private interface ThrowingRunnable {

		void run() throws Exception;

	}

	private record NetworkNodeFactory(String label, Class<?> httpType, Class<?> documentType) {

		@Override
		public String toString() {
			return this.label;
		}

		HttpBuilder httpBuilder(String url) throws ReflectiveOperationException {
			return new HttpBuilder(this.httpType, invokeStatic(this.httpType, "builder")).url(url);
		}

		DocumentBuilder documentBuilder(List<String> fileList) throws ReflectiveOperationException {
			return new DocumentBuilder(this.documentType, invokeStatic(this.documentType, "builder")).fileList(fileList);
		}

	}

	private static final class HttpBuilder {

		private final Class<?> httpType;

		private final Object builder;

		private HttpBuilder(Class<?> httpType, Object builder) {
			this.httpType = httpType;
			this.builder = builder;
		}

		HttpBuilder webClient(WebClient webClient) throws ReflectiveOperationException {
			invoke(this.builder, "webClient", WebClient.class, webClient);
			return this;
		}

		HttpBuilder method(HttpMethod method) throws ReflectiveOperationException {
			invoke(this.builder, "method", HttpMethod.class, method);
			return this;
		}

		HttpBuilder url(String url) throws ReflectiveOperationException {
			invoke(this.builder, "url", String.class, url);
			return this;
		}

		HttpBuilder header(String name, String value) throws ReflectiveOperationException {
			invoke(this.builder, "header", String.class, String.class, name, value);
			return this;
		}

		HttpBuilder queryParam(String name, String value) throws ReflectiveOperationException {
			invoke(this.builder, "queryParam", String.class, String.class, name, value);
			return this;
		}

		HttpBuilder body(Object body) throws ReflectiveOperationException {
			invoke(this.builder, "body", nested(this.httpType, "HttpRequestNodeBody"), body);
			return this;
		}

		HttpBuilder bodyFrom(Object raw) throws ReflectiveOperationException {
			return body(invokeStatic(nested(this.httpType, "HttpRequestNodeBody"), "from", Object.class, raw));
		}

		HttpBuilder bodyFromJson(String json) throws ReflectiveOperationException {
			return body(invokeStatic(nested(this.httpType, "HttpRequestNodeBody"), "fromJson", String.class, json));
		}

		HttpBuilder basicAuth(String username, String password) throws ReflectiveOperationException {
			Object auth = invokeStatic(nested(this.httpType, "AuthConfig"), "basic", String.class, String.class, username,
					password);
			invoke(this.builder, "auth", nested(this.httpType, "AuthConfig"), auth);
			return this;
		}

		HttpBuilder bearerAuth(String token) throws ReflectiveOperationException {
			Object auth = invokeStatic(nested(this.httpType, "AuthConfig"), "bearer", String.class, token);
			invoke(this.builder, "auth", nested(this.httpType, "AuthConfig"), auth);
			return this;
		}

		HttpBuilder retryConfig(int maxRetries, long maxRetryInterval, boolean enable) throws ReflectiveOperationException {
			Constructor<?> constructor = nested(this.httpType, "RetryConfig").getConstructor(int.class, long.class,
					boolean.class);
			invoke(this.builder, "retryConfig", nested(this.httpType, "RetryConfig"),
					constructor.newInstance(maxRetries, maxRetryInterval, enable));
			return this;
		}

		HttpBuilder allowPrivateNetworkAccess(boolean allowPrivateNetworkAccess) throws ReflectiveOperationException {
			invoke(this.builder, "allowPrivateNetworkAccess", boolean.class, allowPrivateNetworkAccess);
			return this;
		}

		HttpBuilder requestTimeout(Duration requestTimeout) throws ReflectiveOperationException {
			invoke(this.builder, "requestTimeout", Duration.class, requestTimeout);
			return this;
		}

		NodeAction build() throws ReflectiveOperationException {
			return (NodeAction) invoke(this.builder, "build");
		}

	}

	private static final class DocumentBuilder {

		private final Object builder;

		private DocumentBuilder(Class<?> documentType, Object builder) {
			this.builder = builder;
		}

		DocumentBuilder paramsKey(String paramsKey) throws ReflectiveOperationException {
			invoke(this.builder, "paramsKey", String.class, paramsKey);
			return this;
		}

		DocumentBuilder fileList(List<String> fileList) throws ReflectiveOperationException {
			invoke(this.builder, "fileList", List.class, fileList);
			return this;
		}

		DocumentBuilder inputIsArray(boolean inputIsArray) throws ReflectiveOperationException {
			invoke(this.builder, "inputIsArray", boolean.class, inputIsArray);
			return this;
		}

		DocumentBuilder localRoot(Path localRoot) throws ReflectiveOperationException {
			invoke(this.builder, "localRoot", Path.class, localRoot);
			return this;
		}

		DocumentBuilder allowRemoteAccess(boolean allowRemoteAccess) throws ReflectiveOperationException {
			invoke(this.builder, "allowRemoteAccess", boolean.class, allowRemoteAccess);
			return this;
		}

		DocumentBuilder allowPrivateNetworkAccess(boolean allowPrivateNetworkAccess) throws ReflectiveOperationException {
			invoke(this.builder, "allowPrivateNetworkAccess", boolean.class, allowPrivateNetworkAccess);
			return this;
		}

		DocumentBuilder readTimeoutMillis(int readTimeoutMillis) throws ReflectiveOperationException {
			invoke(this.builder, "readTimeoutMillis", int.class, readTimeoutMillis);
			return this;
		}

		DocumentBuilder maxBytes(long maxBytes) throws ReflectiveOperationException {
			invoke(this.builder, "maxBytes", long.class, maxBytes);
			return this;
		}

		DocumentBuilder totalTimeout(Duration totalTimeout) throws ReflectiveOperationException {
			invoke(this.builder, "totalTimeout", Duration.class, totalTimeout);
			return this;
		}

		DocumentBuilder documentParser(String extension, DocumentParser parser) throws ReflectiveOperationException {
			invoke(this.builder, "documentParser", String.class, DocumentParser.class, extension, parser);
			return this;
		}

		NodeAction build() throws ReflectiveOperationException {
			return (NodeAction) invoke(this.builder, "build");
		}

	}

	private static Class<?> nested(Class<?> type, String nestedName) throws ClassNotFoundException {
		return Class.forName(type.getName() + "$" + nestedName);
	}

	private static Object invokeStatic(Class<?> type, String methodName, Object... signatureAndArgs)
			throws ReflectiveOperationException {
		return invoke(null, type, methodName, signatureAndArgs);
	}

	private static Object invoke(Object target, String methodName, Object... signatureAndArgs)
			throws ReflectiveOperationException {
		return invoke(target, target.getClass(), methodName, signatureAndArgs);
	}

	private static Object invoke(Object target, Class<?> type, String methodName, Object... signatureAndArgs)
			throws ReflectiveOperationException {
		int argumentCount = signatureAndArgs.length / 2;
		Class<?>[] signature = new Class<?>[argumentCount];
		Object[] args = new Object[argumentCount];
		for (int index = 0; index < argumentCount; index++) {
			signature[index] = (Class<?>) signatureAndArgs[index];
			args[index] = signatureAndArgs[index + argumentCount];
		}
		Method method = type.getMethod(methodName, signature);
		return method.invoke(target, args);
	}

	private static Object invoke(Object target, String methodName) throws ReflectiveOperationException {
		return target.getClass().getMethod(methodName).invoke(target);
	}

	private static Object invokeStatic(Class<?> type, String methodName) throws ReflectiveOperationException {
		return type.getMethod(methodName).invoke(null);
	}

}
