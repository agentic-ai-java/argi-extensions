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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.ai.vectorstore.filter.Filter;
import org.springframework.ai.vectorstore.filter.FilterExpressionBuilder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

class AdVectorFilterExpressionConverterTests {

	private final FilterExpressionBuilder b = new FilterExpressionBuilder();

	private final AdVectorFilterExpressionConverter converter = new AdVectorFilterExpressionConverter();

	private String sql(FilterExpressionBuilder.Op op) {
		return this.converter.convertExpression(op.build());
	}

	@Test
	void stringEqualityReadsTheMetadataColumn() {
		assertThat(sql(this.b.eq("tenantId", "t001")))
			.isEqualTo("(metadata -> E'tenantId') = E'\"t001\"'::jsonb");
	}

	@Test
	void numericComparisonIgnoresNonNumericMetadata() {
		assertThat(sql(this.b.gte("score", 80))).isEqualTo(
				"(jsonb_typeof(metadata -> E'score') = 'number' AND (metadata -> E'score') >= E'80'::jsonb)");
	}

	@ParameterizedTest
	@CsvSource({ "LT,<", "LTE,<=", "GT,>", "GTE,>=" })
	void orderedComparisonsUseNumericNotTextOrdering(Filter.ExpressionType type, String operator) {
		var expression = new Filter.Expression(type, new Filter.Key("price"), new Filter.Value(9.5));
		assertThat(this.converter.convertExpression(expression)).isEqualTo(
				"(jsonb_typeof(metadata -> E'price') = 'number' AND (metadata -> E'price') " + operator
						+ " E'9.5'::jsonb)");
	}

	@Test
	void preservesBooleanGroupingAndListElementTypes() {
		var first = this.b.or(this.b.ne("enabled", false), this.b.in("tag", 1, "1"));
		assertThat(sql(this.b.and(first, this.b.nin("state", "new", "done")))).isEqualTo(
				"(((metadata -> E'enabled') <> E'false'::jsonb OR (metadata -> E'tag') IN (E'1'::jsonb, E'\"1\"'::jsonb))"
						+ " AND (metadata -> E'state') NOT IN (E'\"new\"'::jsonb, E'\"done\"'::jsonb))");
	}

	@Test
	void quotesMetadataKeysAndValuesAsData() {
		assertThat(sql(this.b.eq("owner's.name", "O'Reilly\\UK")))
			.isEqualTo("(metadata -> E'owner''s.name') = E'\"O''Reilly\\\\\\\\UK\"'::jsonb");
	}

	@Test
	void rejectsEmptyListsAndUnrepresentableScalars() {
		assertThatIllegalArgumentException().isThrownBy(() -> sql(this.b.nin("key", List.of())));
		assertThatIllegalArgumentException().isThrownBy(() -> sql(this.b.eq("amount", Double.NaN)));
		assertThatIllegalArgumentException().isThrownBy(() -> sql(this.b.eq("name", "a\0b")));
	}

}
