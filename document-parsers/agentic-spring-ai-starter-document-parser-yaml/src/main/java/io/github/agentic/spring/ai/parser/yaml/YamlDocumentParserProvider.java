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
package io.github.agentic.spring.ai.parser.yaml;

import io.github.agentic.spring.ai.document.DocumentParser;
import io.github.agentic.spring.ai.document.DocumentParserProvider;

import java.util.Set;

/**
 * Service provider for YAML document parsing.
 */
public class YamlDocumentParserProvider implements DocumentParserProvider {

	private static final Set<String> SUPPORTED_EXTENSIONS = DocumentParserProvider.extensions("yaml", "yml");

	@Override
	public Set<String> getSupportedExtensions() {
		return SUPPORTED_EXTENSIONS;
	}

	@Override
	public DocumentParser createParser() {
		return new YamlDocumentParser();
	}

}
