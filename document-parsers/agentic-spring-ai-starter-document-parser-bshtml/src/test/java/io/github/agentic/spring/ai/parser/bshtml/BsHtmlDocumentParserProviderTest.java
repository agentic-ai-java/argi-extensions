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
package io.github.agentic.spring.ai.parser.bshtml;

import io.github.agentic.spring.ai.document.DocumentParserProvider;
import org.junit.jupiter.api.Test;

import java.util.ServiceLoader;

import static org.assertj.core.api.Assertions.assertThat;

class BsHtmlDocumentParserProviderTest {

	@Test
	void should_register_document_parser_provider() {
		DocumentParserProvider provider = ServiceLoader.load(DocumentParserProvider.class)
			.stream()
			.map(ServiceLoader.Provider::get)
			.filter(BsHtmlDocumentParserProvider.class::isInstance)
			.findFirst()
			.orElseThrow();

		assertThat(provider.getSupportedExtensions()).containsExactlyInAnyOrder("html", "htm", "xml");
		assertThat(provider.supports(".HTML")).isTrue();
		assertThat(provider.supports("htm")).isTrue();
		assertThat(provider.supports("txt")).isFalse();
		assertThat(provider.createParser()).isInstanceOf(BsHtmlDocumentParser.class);
	}

}
