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
package io.github.agentic.ai.memory.mem0.core;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.ai.vectorstore.filter.Filter;
import tools.jackson.databind.json.JsonMapper;

import static io.github.agentic.ai.memory.mem0.core.Mem0FilterExpressionConverter.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class Mem0FilterExpressionConverterTest {

	private static final JsonMapper JSON = JsonMapper.shared();

	private final Mem0FilterExpressionConverter converter = new Mem0FilterExpressionConverter();

	@ParameterizedTest
	@MethodSource("logicalExpressions")
	void preservesLogicalObjectsAndOperandOrder(Filter.Expression expression, Map<String, Object> expected) {
		assertThat(JSON.readTree(converter.convertExpression(expression))).isEqualTo(JSON.valueToTree(expected));
	}

	static Stream<Arguments> logicalExpressions() {
		return Stream.of(
				Arguments.of(and(eq("category", "private"), gt("score", 4)), Map.of("AND",
						List.of(Map.of("category", "private"), Map.of("score", Map.of("gt", 4))))),
				Arguments
					.of(or(eq("category", "private"), eq("category",
							"public")), Map.of("OR",
									List.of(Map.of("category", "private"), Map.of("category", "public")))),
				Arguments.of(not(eq("archived",
						true)), Map.of("NOT",
								List.of(Map.of("archived", true)))),
				Arguments.of(
						and(or(in("category", List.of("private",
								"shared")), eq("owner",
										"me")),
								not(nin("score", List.of(1, 2)))),
						Map.of("AND", List.of(
								Map.of("OR",
										List.of(Map.of("category", Map.of("in", List.of("private", "shared"))),
												Map.of("owner", "me"))),
								Map.of("NOT", List.of(Map.of("score", Map.of("nin", List.of(1, 2)))))))),
				Arguments.of(not(and(gte("score", 2), lte("score", 8))), Map.of("NOT", List.of(Map.of("AND",
						List.of(Map.of("score", Map.of("gte", 2)), Map.of("score", Map.of("lte", 8))))))));
	}

	@ParameterizedTest
	@MethodSource("comparisons")
	void preservesComparisonValues(Filter.Expression expression, Map<String, Object> expected) {
		assertThat(JSON.readTree(converter.convertExpression(expression))).isEqualTo(JSON.valueToTree(expected));
	}

	static Stream<Arguments> comparisons() {
		return Stream.of(Arguments.of(eq("flag", false), Map.of("flag", false)),
				Arguments.of(ne("category", "private"), Map.of("category", Map.of("ne", "private"))),
				Arguments.of(gt("score", 2), Map.of("score", Map.of("gt", 2))),
				Arguments.of(gte("score", 2), Map.of("score", Map.of("gte", 2))),
				Arguments.of(lt("score", 2), Map.of("score", Map.of("lt", 2))),
				Arguments.of(lte("score", 2), Map.of("score", Map.of("lte", 2))),
				Arguments.of(in("tags", List.of("one", "two")), Map.of("tags", Map.of("in", List.of("one", "two")))),
				Arguments.of(nin("tags", List.of()), Map.of("tags", Map.of("nin", List.of()))));
	}

	@Test
	void roundTripsSpecialCharactersAndNonAsciiAtEveryDepth() {
		String key = "字段\"\\";
		String value = "引号\"路径\\目录\n\t";
		assertThat(JSON.readTree(converter.convertExpression(and(eq(key, value), not(in("tags", List.of(value)))))))
			.isEqualTo(JSON.valueToTree(Map.of("AND", List.of(Map.of(key, value),
					Map.of("NOT", List.of(Map.of("tags", Map.of("in", List.of(value)))))))));
	}

	@ParameterizedTest
	@MethodSource("invalidExpressions")
	void rejectsInvalidOperandsInsteadOfDroppingConditions(Filter.Expression expression) {
		assertThatThrownBy(() -> converter.convertExpression(expression)).isInstanceOf(IllegalArgumentException.class);
	}

	static Stream<Arguments> invalidExpressions() {
		return Stream.of(Arguments.of(new Filter.Expression(Filter.ExpressionType.AND, eq("a", 1), null)),
				Arguments.of(new Filter.Expression(Filter.ExpressionType.OR, new Filter.Key("a"), eq("b", 2))),
				Arguments.of(new Filter.Expression(Filter.ExpressionType.NOT, new Filter.Key("a"))),
				Arguments.of(new Filter.Expression(Filter.ExpressionType.NOT, eq("a", 1), eq("b", 2))),
				Arguments
					.of(new Filter.Expression(Filter.ExpressionType.AND, new Filter.Group(eq("a", 1)), eq("b", 2))),
				Arguments.of(new Filter.Expression(Filter.ExpressionType.EQ, new Filter.Value(1), new Filter.Value(2))),
				Arguments
					.of(new Filter.Expression(Filter.ExpressionType.EQ, new Filter.Key("a"), new Filter.Value(null))),
				Arguments.of(new Filter.Expression(Filter.ExpressionType.ISNULL, new Filter.Key("a"))),
				Arguments.of(new Filter.Expression(Filter.ExpressionType.ISNOTNULL, new Filter.Key("a"))));
	}

	@Test
	void preservesDirectMem0Helpers() {
		assertThat(icontains("text", "Needle")).isEqualTo(Map.of("text", Map.of("icontains", "Needle")));
		assertThat(wildcard("field")).isEqualTo(Map.of("field", "*"));
		assertThat(JSON.readTree(converter.convertExpression(wildcardExpression("field"))))
			.isEqualTo(JSON.valueToTree(Map.of("field", "*")));
	}

}
