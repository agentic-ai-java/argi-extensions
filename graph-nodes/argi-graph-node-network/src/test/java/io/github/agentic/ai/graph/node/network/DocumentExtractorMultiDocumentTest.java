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
package io.github.agentic.ai.graph.node.network;

import io.github.agentic.ai.document.DocumentParser;
import io.github.agentic.ai.graph.OverAllState;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.ai.document.Document;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DocumentExtractorMultiDocumentTest {

	@TempDir
	Path root;

	@Test
	void retainsEveryJsonArrayElementInOrder() throws Exception {
		Path file = Files.writeString(root.resolve("array.json"), "[{\"id\":\"first\"},{\"id\":\"second\"}]");
		assertEquals("{id=first}\n{id=second}", node(file).build().apply(new OverAllState()).get("text"));
	}

	@Test
	void keepsSingleJsonObjectOutputUnchanged() throws Exception {
		Path file = Files.writeString(root.resolve("object.json"), "{\"id\":\"only\"}");
		assertEquals("{id=only}", node(file).build().apply(new OverAllState()).get("text"));
	}

	@Test
	void emptyJsonArrayProducesEmptyText() throws Exception {
		Path file = Files.writeString(root.resolve("empty.json"), "[]");
		assertEquals("", node(file).build().apply(new OverAllState()).get("text"));
	}

	@Test
	void preservesOneOutputPerInputFileAndBothOrders() throws Exception {
		Path first = Files.writeString(root.resolve("first.json"), "[{\"id\":\"a\"},{\"id\":\"b\"}]");
		Path empty = Files.writeString(root.resolve("empty.json"), "[]");
		Path last = Files.writeString(root.resolve("last.json"), "[{\"id\":\"c\"},{\"id\":\"d\"}]");
		var result = DocumentExtractorNode.builder()
			.paramsKey("files")
			.outputKey("extracted")
			.inputIsArray(true)
			.localRoot(root)
			.build()
			.apply(new OverAllState(java.util.Map.of("files", List.of(first.toString(), empty.toString(), last.toString()))));
		assertEquals(List.of("{id=a}\n{id=b}", "", "{id=c}\n{id=d}"), result.get("extracted"));
	}

	@Test
	void customParserRetainsWhitespaceAndEmptyDocumentsAndClosesStream() throws Exception {
		AtomicReference<InputStream> stream = new AtomicReference<>();
		AtomicInteger calls = new AtomicInteger();
		DocumentParser parser = input -> {
			stream.set(input);
			calls.incrementAndGet();
			return List.of(new Document(" first\n"), new Document(""), new Document(" last "));
		};
		assertEquals(" first\n\n\n last ", custom(parser).apply(new OverAllState()).get("text"));
		assertEquals(1, calls.get());
		assertThrows(IOException.class, () -> stream.get().read());
	}

	@Test
	void emptyCustomParserResultProducesEmptyText() throws Exception {
		AtomicReference<InputStream> stream = new AtomicReference<>();
		assertEquals("", custom(input -> {
			stream.set(input);
			return List.of();
		}).apply(new OverAllState()).get("text"));
		assertThrows(IOException.class, () -> stream.get().read());
	}

	@Test
	void nullParserResultIsRejected() throws Exception {
		DocumentExtractorNode extractor = custom(input -> null);
		RuntimeException exception = assertThrows(RuntimeException.class, () -> extractor.apply(new OverAllState()));
		assertEquals("Document parser returned null documents", exception.getCause().getMessage());
	}

	@Test
	void nullDocumentIsRejected() throws Exception {
		DocumentExtractorNode extractor = custom(input -> java.util.Arrays.asList(new Document("first"), null));
		RuntimeException exception = assertThrows(RuntimeException.class, () -> extractor.apply(new OverAllState()));
		assertEquals("Document parser returned a null document", exception.getCause().getMessage());
	}

	@Test
	void parserFailureKeepsCauseAndClosesStream() throws Exception {
		AtomicReference<InputStream> stream = new AtomicReference<>();
		IllegalArgumentException failure = new IllegalArgumentException("invalid document");
		DocumentExtractorNode extractor = custom(input -> {
			stream.set(input);
			throw failure;
		});
		RuntimeException exception = assertThrows(RuntimeException.class, () -> extractor.apply(new OverAllState()));
		assertSame(failure, exception.getCause());
		assertThrows(IOException.class, () -> stream.get().read());
	}

	@Test
	void nullTextIsRejectedRatherThanRenderedAsNull() throws Exception {
		Document noText = new Document("placeholder") {
			@Override
			public String getText() {
				return null;
			}
		};
		DocumentExtractorNode extractor = custom(input -> List.of(new Document("first"), noText));
		RuntimeException exception = assertThrows(RuntimeException.class, () -> extractor.apply(new OverAllState()));
		assertEquals("Document parser returned null text", exception.getCause().getMessage());
	}

	private DocumentExtractorNode.Builder node(Path file) {
		return DocumentExtractorNode.builder().fileList(List.of(file.toString())).localRoot(root);
	}

	private DocumentExtractorNode custom(DocumentParser parser) throws IOException {
		Path file = Files.writeString(root.resolve("sample.custom"), "input");
		return node(file).documentParser("custom", parser).build();
	}

}
