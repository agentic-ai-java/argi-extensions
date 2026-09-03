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

import java.lang.reflect.Constructor;
import java.lang.reflect.Executable;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class PublicApiParity {

	private static final String CORE_HTTP_NODE = "io.github.agentic.spring.ai.graph.node.HttpNode";

	private static final String EXTENSION_HTTP_NODE = "io.github.agentic.spring.ai.graph.node.network.HttpNode";

	private static final String CORE_DOCUMENT_EXTRACTOR_NODE = "io.github.agentic.spring.ai.graph.node.DocumentExtractorNode";

	private static final String EXTENSION_DOCUMENT_EXTRACTOR_NODE = "io.github.agentic.spring.ai.graph.node.network.DocumentExtractorNode";

	private PublicApiParity() {
	}

	static void assertParity(Class<?> coreType, Class<?> extensionType) {
		assertEquals(signatures(coreType), signatures(extensionType));
	}

	private static List<String> signatures(Class<?> type) {
		List<String> signatures = new ArrayList<>();
		collect(type, signatures);
		signatures.sort(String::compareTo);
		return signatures;
	}

	private static void collect(Class<?> type, List<String> signatures) {
		signatures.add("type " + normalized(type.getName()) + " " + apiModifiers(type.getModifiers()));

		Arrays.stream(type.getDeclaredConstructors())
			.filter(PublicApiParity::isApiMember)
			.map(PublicApiParity::constructorSignature)
			.forEach(signatures::add);

		Arrays.stream(type.getDeclaredMethods())
			.filter(PublicApiParity::isApiMember)
			.filter(method -> !method.isBridge() && !method.isSynthetic())
			.map(PublicApiParity::methodSignature)
			.forEach(signatures::add);

		Arrays.stream(type.getDeclaredFields())
			.filter(PublicApiParity::isApiMember)
			.map(PublicApiParity::fieldSignature)
			.forEach(signatures::add);

		Arrays.stream(type.getDeclaredClasses())
			.filter(PublicApiParity::isApiMember)
			.sorted(Comparator.comparing(Class::getName))
			.forEach(nestedType -> collect(nestedType, signatures));
	}

	private static boolean isApiMember(Class<?> type) {
		return isApi(type.getModifiers());
	}

	private static boolean isApiMember(Constructor<?> constructor) {
		return isApi(constructor.getModifiers());
	}

	private static boolean isApiMember(Method method) {
		return isApi(method.getModifiers());
	}

	private static boolean isApiMember(Field field) {
		return isApi(field.getModifiers());
	}

	private static boolean isApi(int modifiers) {
		return Modifier.isPublic(modifiers) || Modifier.isProtected(modifiers);
	}

	private static String constructorSignature(Constructor<?> constructor) {
		return "constructor " + apiModifiers(constructor.getModifiers()) + " "
				+ normalized(constructor.getDeclaringClass().getName()) + parameters(constructor) + exceptions(constructor);
	}

	private static String methodSignature(Method method) {
		return "method " + apiModifiers(method.getModifiers()) + " " + normalized(method.getGenericReturnType()) + " "
				+ method.getName() + parameters(method) + exceptions(method);
	}

	private static String fieldSignature(Field field) {
		return "field " + apiModifiers(field.getModifiers()) + " " + normalized(field.getGenericType()) + " "
				+ field.getName();
	}

	private static String parameters(Executable executable) {
		return Arrays.stream(executable.getGenericParameterTypes())
			.map(PublicApiParity::normalized)
			.toList()
			.toString();
	}

	private static String exceptions(Executable executable) {
		return Arrays.stream(executable.getGenericExceptionTypes())
			.map(PublicApiParity::normalized)
			.sorted()
			.toList()
			.toString();
	}

	private static String normalized(Type type) {
		return normalized(type.getTypeName());
	}

	private static String normalized(String value) {
		return value.replace(CORE_HTTP_NODE, EXTENSION_HTTP_NODE)
			.replace(CORE_DOCUMENT_EXTRACTOR_NODE, EXTENSION_DOCUMENT_EXTRACTOR_NODE);
	}

	private static String apiModifiers(int modifiers) {
		return Modifier.toString(modifiers & (Modifier.PUBLIC | Modifier.PROTECTED | Modifier.STATIC | Modifier.FINAL
				| Modifier.ABSTRACT | Modifier.TRANSIENT | Modifier.VOLATILE));
	}

}
