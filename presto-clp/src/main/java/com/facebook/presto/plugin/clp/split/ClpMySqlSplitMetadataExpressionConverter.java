/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.plugin.clp.split;

import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.plugin.clp.ClpErrorCode;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionVisitor;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import io.airlift.slice.Slice;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.common.function.OperatorType.IS_DISTINCT_FROM;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ClpMySqlSplitMetadataExpressionConverter
        implements RowExpressionVisitor<String, Void>
{
    private final FunctionMetadataManager functionManager;
    private final StandardFunctionResolution functionResolution;
    private final Map<String, String> exposedToOriginal;
    private final Map<String, Map<String, String>> dataToMetadataBounds;
    private final Set<String> requiredColumns;
    private final Set<String> seenRequired = new HashSet<>();

    public ClpMySqlSplitMetadataExpressionConverter(
            FunctionMetadataManager functionManager,
            StandardFunctionResolution functionResolution,
            Map<String, String> exposedToOriginal,
            Map<String, Map<String, String>> dataToMetadataBounds,
            Set<String> requiredColumns)
    {
        this.functionManager = requireNonNull(functionManager, "functionManager is null");
        this.functionResolution = requireNonNull(functionResolution, "functionResolution is null");
        this.exposedToOriginal = exposedToOriginal;
        this.dataToMetadataBounds = dataToMetadataBounds;
        this.requiredColumns = requiredColumns;
    }

    public String transform(RowExpression expression)
    {
        String sql = expression.accept(this, null);
        Set<String> missing = new HashSet<>(requiredColumns);
        missing.removeAll(seenRequired);
        if (!missing.isEmpty()) {
            throw new IllegalStateException("Missing required filter columns: " + missing);
        }
        return sql;
    }

    @Override
    public String visitCall(CallExpression node, Void context)
    {
        FunctionHandle functionHandle = node.getFunctionHandle();
        if (functionResolution.isNotFunction(functionHandle)) {
            return format("NOT " + node.getArguments().get(0).accept(this, null));
        }

        FunctionMetadata functionMetadata = functionManager.getFunctionMetadata(node.getFunctionHandle());
        Optional<OperatorType> operatorTypeOptional = functionMetadata.getOperatorType();
        if (operatorTypeOptional.isPresent()) {
            OperatorType operatorType = operatorTypeOptional.get();
            if (operatorType.isComparisonOperator() && operatorType != IS_DISTINCT_FROM) {
                String variableName = node.getArguments().get(0).accept(this, null);
                String literalString = node.getArguments().get(1).accept(this, null);

                String rewritten = rewriteComparisonWithBounds(variableName, operatorType, literalString);
                if (rewritten != null) {
                    return rewritten;
                }

                return format("%s %s %s", variableName, operatorType.getOperator(), literalString);
            }
        }

        throw new PrestoException(ClpErrorCode.CLP_PUSHDOWN_UNSUPPORTED_EXPRESSION, "Unsupported metadata query" + node);
    }

    @Override
    public String visitSpecialForm(SpecialFormExpression node, Void context)
    {
        switch (node.getForm()) {
            case AND:
            case OR:
                String op = node.getForm() == SpecialFormExpression.Form.AND ? "AND" : "OR";
                return node.getArguments().stream()
                        .map(arg -> "(" + arg.accept(this, context) + ")")
                        .collect(Collectors.joining(" " + op + " "));
            case IS_NULL:
                return "(" + node.getArguments().get(0).accept(this, context) + " IS NULL)";
            default:
                throw new PrestoException(ClpErrorCode.CLP_PUSHDOWN_UNSUPPORTED_EXPRESSION, "Unsupported metadata query" + node);
        }
    }

    @Override
    public String visitConstant(ConstantExpression node, Void context)
    {
        Object value = node.getValue();
        if (value instanceof Slice) {
            return "'" + ((Slice) value).toStringUtf8().replace("'", "''") + "'";
        }

        return value.toString();
    }

    @Override
    public String visitVariableReference(VariableReferenceExpression node, Void context)
    {
        String exposed = node.getName();
        seenRequired.add(exposed);
        return exposedToOriginal.getOrDefault(exposed, exposed);
    }

    private String rewriteComparisonWithBounds(String variableName, OperatorType operator, String literal)
    {
        String original = exposedToOriginal.getOrDefault(variableName, variableName);
        Map<String, String> bounds = dataToMetadataBounds.get(original);
        if (bounds == null) {
            return null;
        }

        String lower = bounds.get("lower");
        String upper = bounds.get("upper");
        if (lower == null || upper == null) {
            return null;
        }

        switch (operator) {
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
                // "col >= 5" → "upper_col >= 5"
                return format("%s %s %s", upper, operator.getOperator(), literal);
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
                // "col <= 5" → "lower_col <= 5"
                return format("%s %s %s", lower, operator.getOperator(), literal);
            case EQUAL:
                // "col = 5" → "(lower_col <= 5 AND upper_col >= 5)"
                return format("(%s <= %s AND %s >= %s)", lower, literal, upper, literal);
            default:
                return null;
        }
    }
}