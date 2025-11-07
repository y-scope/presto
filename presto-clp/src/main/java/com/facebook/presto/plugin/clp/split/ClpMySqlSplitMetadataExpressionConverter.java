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
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.Decimals;
import com.facebook.presto.common.type.Type;
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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.common.function.OperatorType.IS_DISTINCT_FROM;
import static com.facebook.presto.plugin.clp.ClpErrorCode.CLP_MANDATORY_COLUMN_NOT_IN_FILTER;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * A converter that converts Presto {@link RowExpression} trees representing metadata predicates
 * into SQL filter strings that can be pushed down to MySQL for split-level metadata filtering.
 * <p></p>
 * The converter:
 * <ul>
 *   <li>Handles standard logical and comparison operators (AND, OR, =, >, >=, <, <=, IS NULL).</li>
 *   <li>Supports range-bound rewriting for data columns that have metadata columns representing
 *       lower and upper bounds.</li>
 *   <li>Tracks required columns and throws an exception if any are missing in the filter
 *       expression.</li>
 * </ul>
 */
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

    /**
     * Converts the given {@link RowExpression} into an equivalent SQL WHERE clause string.
     * <p></p>
     * After conversion, validates that all required columns were referenced in the expression. If
     * any required columns are missing, a {@link PrestoException} is thrown.
     *
     * @param expression the row expression to convert
     * @return a SQL string representing the equivalent predicate
     * @throws PrestoException if required columns are missing from the expression
     */
    public String transform(RowExpression expression)
    {
        String sql = expression.accept(this, null);
        Set<String> missing = new HashSet<>(requiredColumns);
        missing.removeAll(seenRequired);
        if (!missing.isEmpty()) {
            throw new PrestoException(CLP_MANDATORY_COLUMN_NOT_IN_FILTER, "Missing required filter columns: " + missing);
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
            if (operatorType == OperatorType.NEGATION) {
                String value = node.getArguments().get(0).accept(this, null);
                return "-" + value;
            }

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
        Type type = node.getType();
        if (value instanceof Slice) {
            if (type instanceof DecimalType) {
                DecimalType decimalType = (DecimalType) type;
                BigInteger unscaled = Decimals.decodeUnscaledValue((Slice) value);
                BigDecimal decimalValue = new BigDecimal(unscaled, decimalType.getScale());
                return decimalValue.toPlainString();
            }
            return "'" + ((Slice) value).toStringUtf8().replace("'", "''") + "'";
        }

        if (type instanceof DecimalType && value instanceof Long) {
            DecimalType decimalType = (DecimalType) type;
            BigDecimal decimalValue = new BigDecimal(BigInteger.valueOf((Long) value), decimalType.getScale());
            return decimalValue.toPlainString();
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

    /**
     * Rewrites a comparison operator involving a data column into an equivalent expression using
     * its associated range-bound metadata columns (if configured).
     * <p></p>
     * Examples:
     * <ul>
     *   <li><code>col >= 5</code> → <code>upper_col >= 5</code></li>
     *   <li><code>col <= 5</code> → <code>lower_col <= 5</code></li>
     *   <li><code>col = 5</code> → <code>(lower_col <= 5) AND (upper_col >= 5)</code></li>
     * </ul>
     * Returns <code>null</code> if no rewrite is applicable.
     *
     * @param variableName the name of the column being compared
     * @param operator     the comparison operator
     * @param literal      the literal value as a SQL string
     * @return a rewritten SQL expression string, or <code>null</code> if no rewrite applies
     */
    private String rewriteComparisonWithBounds(String variableName, OperatorType operator, String literal)
    {
        String original = exposedToOriginal.getOrDefault(variableName, variableName);
        Map<String, String> bounds = dataToMetadataBounds.get(original);
        if (bounds == null) {
            return null;
        }

        String lower = bounds.get("lower");
        String upper = bounds.get("upper");

        switch (operator) {
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
                if (upper != null) {
                    return format("%s %s %s", upper, operator.getOperator(), literal);
                }
                break;
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
                if (lower != null) {
                    return format("%s %s %s", lower, operator.getOperator(), literal);
                }
                break;
            case EQUAL:
                if (lower != null && upper != null) {
                    return format("(%s <= %s) AND (%s >= %s)", lower, literal, upper, literal);
                } else if (lower != null) {
                    return format("%s <= %s", lower, literal);
                } else if (upper != null) {
                    return format("%s >= %s", upper, literal);
                }
                break;
            default:
                break;
        }

        return null;
    }
}
