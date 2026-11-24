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

import org.springframework.ai.vectorstore.filter.Filter.Expression;
import org.springframework.ai.vectorstore.filter.Filter.ExpressionType;
import org.springframework.ai.vectorstore.filter.Filter.Key;
import org.springframework.ai.vectorstore.filter.Filter.Value;
import org.springframework.ai.vectorstore.filter.converter.AbstractFilterExpressionConverter;
import tools.jackson.databind.json.JsonMapper;

/**
 * Renders SQL WHERE predicates for
 * <a href="https://help.aliyun.com/zh/analyticdb/analyticdb-for-postgresql/developer-reference/api-gpdb-2016-05-03-querycollectiondata">QueryCollectionData</a>
 * and
 * <a href="https://help.aliyun.com/zh/analyticdb/analyticdb-for-postgresql/developer-reference/api-gpdb-2016-05-03-deletecollectiondata">DeleteCollectionData</a>.
 * Document attributes live in one JSONB column, not in individual collection columns.
 *
 * @author HeYQ
 */
public class AdVectorFilterExpressionConverter extends AbstractFilterExpressionConverter {

	private static final JsonMapper JSON = JsonMapper.builder().build();

	@Override
	protected void doExpression(Expression expression, StringBuilder context) {
		ExpressionType type = expression.type();
		if (type == ExpressionType.AND || type == ExpressionType.OR) {
			if (expression.right() == null) {
				throw new IllegalArgumentException("Expected two predicates for " + type);
			}
			context.append("(");
			convertOperand(expression.left(), context);
			context.append(type == ExpressionType.AND ? " AND " : " OR ");
			convertOperand(expression.right(), context);
			context.append(")");
			return;
		}

		if (!(expression.left() instanceof Key key) || !(expression.right() instanceof Value value)) {
			throw new IllegalArgumentException("Expected a metadata key and filter value for " + type);
		}
		String field = convertOperand(key);
		if (type == ExpressionType.IN || type == ExpressionType.NIN) {
			if (!(value.value() instanceof List<?> values) || values.isEmpty()) {
				throw new IllegalArgumentException("Expected a non-empty list for " + type);
			}
			context.append("(").append(field).append(type == ExpressionType.IN ? ") IN (" : ") NOT IN (");
			for (int i = 0; i < values.size(); i++) {
				if (i > 0) {
					context.append(", ");
				}
				doSingleValue(values.get(i), context);
			}
			context.append(")");
			return;
		}

		String operator = switch (type) {
			case EQ -> " = ";
			case NE -> " <> ";
			case LT -> " < ";
			case LTE -> " <= ";
			case GT -> " > ";
			case GTE -> " >= ";
			default -> throw new IllegalArgumentException("Unsupported metadata comparison: " + type);
		};
		boolean ordered = type != ExpressionType.EQ && type != ExpressionType.NE;
		if (ordered) {
			// JSONB has a total order across types. Compare only like scalars, without
			// casting arbitrary stored metadata (a row can have "unknown" for a score).
			context.append("(jsonb_typeof(").append(field).append(") = '")
				.append(scalarType(value.value())).append("' AND ");
		}
		context.append("(").append(field).append(")").append(operator);
		doSingleValue(value.value(), context);
		if (ordered) {
			context.append(")");
		}
	}

	@Override
	protected void doKey(Key key, StringBuilder context) {
		context.append("metadata -> ").append(literal(key.key()));
	}

	@Override
	protected void doSingleValue(Object value, StringBuilder context) {
		scalarType(value);
		context.append(literal(JSON.writeValueAsString(value))).append("::jsonb");
	}

	private static String scalarType(Object value) {
		if (value instanceof String text && text.indexOf('\0') < 0) {
			return "string";
		}
		if (value instanceof Boolean) {
			return "boolean";
		}
		if (value instanceof Number && !(value instanceof Double d && !Double.isFinite(d))
				&& !(value instanceof Float f && !Float.isFinite(f))) {
			return "number";
		}
		throw new IllegalArgumentException("Expected a string, finite number or boolean in metadata filter");
	}

	private static String literal(String text) {
		if (text.indexOf('\0') >= 0) {
			throw new IllegalArgumentException("PostgreSQL text cannot contain a NUL character");
		}
		return "E'" + text.replace("\\", "\\\\").replace("'", "''") + "'";
	}

}
