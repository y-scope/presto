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
package com.yscope.presto;

import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.ColumnHandle;
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
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;

import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.common.function.OperatorType.EQUAL;
import static com.facebook.presto.common.function.OperatorType.GREATER_THAN;
import static com.facebook.presto.common.function.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.common.function.OperatorType.LESS_THAN;
import static com.facebook.presto.common.function.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.common.function.OperatorType.NOT_EQUAL;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.AND;
import static com.yscope.presto.ClpErrorCode.CLP_PUSHDOWN_UNSUPPORTED_EXPRESSION;
import static java.util.Objects.requireNonNull;

// TODO(Rui): Correctly handle escaping of special characters in LIKE expressions (LIKE 'a%b' ESCAPE 'a')
public class ClpFilterToKqlConverter
        implements RowExpressionVisitor<ClpExpression, Void>
{
    private static final Set<OperatorType> LOGICAL_BINARY_OPS_FILTER =
            ImmutableSet.of(EQUAL, NOT_EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL);

    private final StandardFunctionResolution standardFunctionResolution;
    private final FunctionMetadataManager functionMetadataManager;
    private final Map<VariableReferenceExpression, ColumnHandle> assignments;

    public ClpFilterToKqlConverter(StandardFunctionResolution standardFunctionResolution,
                                   FunctionMetadataManager functionMetadataManager,
                                   Map<VariableReferenceExpression, ColumnHandle> assignments)
    {
        this.standardFunctionResolution =
                requireNonNull(standardFunctionResolution, "standardFunctionResolution is null");
        this.functionMetadataManager = requireNonNull(functionMetadataManager, "function metadata manager is null");
        this.assignments = requireNonNull(assignments, "assignments is null");
    }

    private static String getLiteralString(ConstantExpression literal)
    {
        if (literal.getValue() instanceof Slice) {
            return ((Slice) literal.getValue()).toStringUtf8();
        }
        return literal.toString();
    }

    private String getVariableName(VariableReferenceExpression variable)
    {
        String variableName = ((ClpColumnHandle) assignments.get(variable)).getColumnName();
        if (variableName.endsWith("_bigint") || variableName.endsWith("_double") ||
                variableName.endsWith("_varchar") || variableName.endsWith("_boolean")) {
            return variableName.substring(0, variableName.lastIndexOf('_'));
        }
        return variableName;
    }

    private ClpExpression handleNot(CallExpression node)
    {
        if (node.getArguments().size() != 1) {
            throw new PrestoException(CLP_PUSHDOWN_UNSUPPORTED_EXPRESSION,
                    "NOT operator must have exactly one argument. Received: " + node);
        }

        RowExpression input = node.getArguments().get(0);
        ClpExpression expression = input.accept(this, null);
        if (expression.getRemainingExpression().isPresent() || !expression.getDefinition().isPresent()) {
            return new ClpExpression(node);
        }
        return new ClpExpression("NOT " + expression.getDefinition().get());
    }

    private ClpExpression handleAnd(SpecialFormExpression node)
    {
        StringBuilder queryBuilder = new StringBuilder();
        queryBuilder.append("(");
        ArrayList<RowExpression> remainingExpressions = new ArrayList<>();
        boolean hasDefinition = false;
        for (RowExpression argument : node.getArguments()) {
            ClpExpression expression = argument.accept(this, null);
            if (expression.getDefinition().isPresent()) {
                hasDefinition = true;
                queryBuilder.append(expression.getDefinition().get());
                queryBuilder.append(" AND ");
            }
            if (expression.getRemainingExpression().isPresent()) {
                remainingExpressions.add(expression.getRemainingExpression().get());
            }
        }
        if (!hasDefinition) {
            return new ClpExpression(node);
        }
        else if (!remainingExpressions.isEmpty()) {
            if (remainingExpressions.size() == 1) {
                return new ClpExpression(Optional.of(queryBuilder.substring(0, queryBuilder.length() - 5) + ")"),
                        Optional.of(remainingExpressions.get(0)));
            }
            else {
                return new ClpExpression(Optional.of(queryBuilder.substring(0, queryBuilder.length() - 5) + ")"),
                        Optional.of(new SpecialFormExpression(node.getSourceLocation(),
                                AND,
                                BOOLEAN,
                                remainingExpressions)));
            }
        }
        // Remove the last " AND " from the query
        return new ClpExpression(queryBuilder.substring(0, queryBuilder.length() - 5) + ")");
    }

    private ClpExpression handleOr(SpecialFormExpression node)
    {
        StringBuilder queryBuilder = new StringBuilder();
        queryBuilder.append("(");
        ArrayList<RowExpression> remainingExpressions = new ArrayList<>();
        for (RowExpression argument : node.getArguments()) {
            ClpExpression expression = argument.accept(this, null);
            if (expression.getRemainingExpression().isPresent() || !expression.getDefinition().isPresent()) {
                return new ClpExpression(node);
            }
            queryBuilder.append(expression.getDefinition().get());
            queryBuilder.append(" OR ");
        }
        // Remove the last " OR " from the query
        return new ClpExpression(queryBuilder.substring(0, queryBuilder.length() - 4) + ")");
    }

    private ClpExpression handleIn(SpecialFormExpression node)
    {
        if (!(node.getArguments().get(0) instanceof VariableReferenceExpression)) {
            return new ClpExpression(node);
        }
        String variableName = getVariableName((VariableReferenceExpression) node.getArguments().get(0));
        StringBuilder queryBuilder = new StringBuilder();
        queryBuilder.append("(");
        for (RowExpression argument : node.getArguments().subList(1, node.getArguments().size())) {
            if (!(argument instanceof ConstantExpression)) {
                return new ClpExpression(node);
            }
            ConstantExpression literal = (ConstantExpression) argument;
            String literalString = getLiteralString(literal);
            queryBuilder.append(variableName).append(": ");
            if (literal.getType() instanceof VarcharType) {
                queryBuilder.append("\"").append(literalString).append("\"");
            }
            else {
                queryBuilder.append(literalString);
            }
            queryBuilder.append(" OR ");
        }
        // Remove the last " OR " from the query
        return new ClpExpression(queryBuilder.substring(0, queryBuilder.length() - 4) + ")");
    }

    private ClpExpression handleIsNull(SpecialFormExpression node)
    {
        if (node.getArguments().size() != 1) {
            throw new PrestoException(CLP_PUSHDOWN_UNSUPPORTED_EXPRESSION,
                    "IS NULL operator must have exactly one argument. Received: " + node);
        }

        if (!(node.getArguments().get(0) instanceof VariableReferenceExpression)) {
            return new ClpExpression(node);
        }

        ClpExpression expression = node.getArguments().get(0).accept(this, null);
        if (!expression.getDefinition().isPresent()) {
            return new ClpExpression(node);
        }

        String variableName = expression.getDefinition().get();
        return new ClpExpression(String.format("NOT %s: *", variableName));
    }

    // Only handles the case where there is a SQL wildcard in the middle of the string
    private ClpExpression handleLike(CallExpression node)
    {
        if (node.getArguments().size() != 2) {
            throw new PrestoException(CLP_PUSHDOWN_UNSUPPORTED_EXPRESSION,
                    "LIKE operator must have exactly two arguments. Received: " + node);
        }
        if (!(node.getArguments().get(0) instanceof VariableReferenceExpression)) {
            return new ClpExpression(node);
        }

        String variableName = getVariableName((VariableReferenceExpression) node.getArguments().get(0));
        RowExpression argument = node.getArguments().get(1);

        String pattern;
        if (argument instanceof ConstantExpression) {
            ConstantExpression literal = (ConstantExpression) argument;
            pattern = getLiteralString(literal);
        }
        else if (argument instanceof CallExpression) {
            CallExpression callExpression = (CallExpression) argument;
            if (!standardFunctionResolution.isCastFunction(callExpression.getFunctionHandle())) {
                return new ClpExpression(node);
            }
            if (callExpression.getArguments().size() != 1) {
                throw new PrestoException(CLP_PUSHDOWN_UNSUPPORTED_EXPRESSION,
                        "CAST function must have exactly one argument. Received: " + callExpression);
            }
            if (!(callExpression.getArguments().get(0) instanceof ConstantExpression)) {
                return new ClpExpression(node);
            }
            pattern = getLiteralString((ConstantExpression) callExpression.getArguments().get(0));
        }
        else {
            return new ClpExpression(node);
        }
        pattern = pattern.replace("%", "*").replace("_", "?");
        return new ClpExpression(String.format("%s: \"%s\"", variableName, pattern));
    }

    private static class SubstrInfo
    {
        String variableName;
        RowExpression startExpression;
        RowExpression lengthExpression;
        SubstrInfo(String variableName, RowExpression start, RowExpression length)
        {
            this.variableName = variableName;
            this.startExpression = start;
            this.lengthExpression = length;
        }
    }

    /**
     * Parse SUBSTR(...) calls that appear either as:
     *   SUBSTR(x, start)
     * or
     *   SUBSTR(x, start, length)
     */
    private Optional<SubstrInfo> parseSubstringCall(CallExpression callExpression)
    {
        FunctionMetadata functionMetadata = functionMetadataManager.getFunctionMetadata(callExpression.getFunctionHandle());
        String functionName = functionMetadata.getName().getObjectName();
        if (!functionName.equals("substr")) {
            return Optional.empty();
        }

        int argCount = callExpression.getArguments().size();
        if (argCount < 2 || argCount > 3) {
            return Optional.empty();
        }

        RowExpression arg0 = callExpression.getArguments().get(0);
        if (!(arg0 instanceof VariableReferenceExpression)) {
            return Optional.empty();
        }

        String varName = getVariableName((VariableReferenceExpression) arg0);
        RowExpression startExpression = callExpression.getArguments().get(1);
        RowExpression lengthExpression = null;
        if (argCount == 3) {
            lengthExpression = callExpression.getArguments().get(2);
        }

        return Optional.of(new SubstrInfo(varName, startExpression, lengthExpression));
    }

    /**
     * Attempt to parse "start" or "length" as an integer.
     */
    private Optional<Integer> parseIntValue(RowExpression expression)
    {
        if (expression instanceof ConstantExpression) {
            try {
                return Optional.of(Integer.parseInt(getLiteralString((ConstantExpression) expression)));
            }
            catch (NumberFormatException ignored) { }
        }
        else if (expression instanceof CallExpression) {
            CallExpression call = (CallExpression) expression;
            FunctionMetadata functionMetadata = functionMetadataManager.getFunctionMetadata(call.getFunctionHandle());
            Optional<OperatorType> operatorTypeOptional = functionMetadata.getOperatorType();
            if (operatorTypeOptional.isPresent() && operatorTypeOptional.get().equals(OperatorType.NEGATION)) {
                RowExpression arg0 = call.getArguments().get(0);
                if (arg0 instanceof ConstantExpression) {
                    try {
                        return Optional.of(-Integer.parseInt(getLiteralString((ConstantExpression) arg0)));
                    }
                    catch (NumberFormatException ignored) { }
                }
            }
        }
        return Optional.empty();
    }

    /**
     * If lengthExpression is a constant integer that matches targetString.length(),
     * return that length. Otherwise empty.
     */
    private Optional<Integer> parseLengthLiteralOrFunction(RowExpression lengthExpression, String targetString)
    {
        if (lengthExpression instanceof ConstantExpression) {
            String val = getLiteralString((ConstantExpression) lengthExpression);
            try {
                int parsed = Integer.parseInt(val);
                if (parsed == targetString.length()) {
                    return Optional.of(parsed);
                }
            }
            catch (NumberFormatException ignored) { }
        }
        return Optional.empty();
    }

    /**
     * Translate SUBSTR(x, start) or SUBSTR(x, start, length) = 'someString' to KQL.
     */
    private ClpExpression interpretSubstringEquality(SubstrInfo info, String targetString)
    {
        if (info.lengthExpression != null) {
            Optional<Integer> maybeStart = parseIntValue(info.startExpression);
            Optional<Integer> maybeLen = parseLengthLiteralOrFunction(info.lengthExpression, targetString);

            if (maybeStart.isPresent() && maybeLen.isPresent()) {
                int start = maybeStart.get();
                int len = maybeLen.get();
                if (start > 0 && len == targetString.length()) {
                    StringBuilder result = new StringBuilder();
                    result.append(info.variableName).append(": \"");
                    for (int i = 1; i < start; i++) {
                        result.append("?");
                    }
                    result.append(targetString).append("*\"");
                    return new ClpExpression(result.toString());
                }
            }
        }
        else {
            Optional<Integer> maybeStart = parseIntValue(info.startExpression);
            if (maybeStart.isPresent()) {
                int start = maybeStart.get();
                if (start > 0) {
                    StringBuilder result = new StringBuilder();
                    result.append(info.variableName).append(": \"");
                    for (int i = 1; i < start; i++) {
                        result.append("?");
                    }
                    result.append(targetString).append("\"");
                    return new ClpExpression(result.toString());
                }
                if (start == -targetString.length()) {
                    return new ClpExpression(String.format("%s: \"*%s\"", info.variableName, targetString));
                }
            }
        }

        return new ClpExpression(Optional.empty(), Optional.empty());
    }

    private ClpExpression tryInterpretSubstringEquality(
            OperatorType operator,
            RowExpression possibleSubstring,
            RowExpression possibleLiteral)
    {
        if (!operator.equals(OperatorType.EQUAL)) {
            return new ClpExpression();
        }

        if (!(possibleSubstring instanceof CallExpression) ||
                !(possibleLiteral instanceof ConstantExpression)) {
            return new ClpExpression();
        }

        Optional<SubstrInfo> maybeSubstringCall = parseSubstringCall((CallExpression) possibleSubstring);
        if (!maybeSubstringCall.isPresent()) {
            return new ClpExpression();
        }

        String targetString = getLiteralString((ConstantExpression) possibleLiteral);
        return interpretSubstringEquality(maybeSubstringCall.get(), targetString);
    }

    private ClpExpression buildClpExpression(
            String variableName,
            String literalString,
            OperatorType operator,
            Type literalType,
            RowExpression originalNode)
    {
        if (operator.equals(OperatorType.EQUAL)) {
            if (literalType instanceof VarcharType) {
                return new ClpExpression(String.format("%s: \"%s\"", variableName, literalString));
            }
            else {
                return new ClpExpression(String.format("%s: %s", variableName, literalString));
            }
        }
        else if (operator.equals(OperatorType.NOT_EQUAL)) {
            if (literalType instanceof VarcharType) {
                return new ClpExpression(String.format("NOT %s: \"%s\"", variableName, literalString));
            }
            else {
                return new ClpExpression(String.format("NOT %s: %s", variableName, literalString));
            }
        }
        else if (LOGICAL_BINARY_OPS_FILTER.contains(operator) && !(literalType instanceof VarcharType)) {
            return new ClpExpression(String.format("%s %s %s", variableName, operator.getOperator(), literalString));
        }
        return new ClpExpression(originalNode);
    }

    private ClpExpression handleLogicalBinary(OperatorType operator, CallExpression node)
    {
        if (node.getArguments().size() != 2) {
            throw new PrestoException(CLP_PUSHDOWN_UNSUPPORTED_EXPRESSION,
                    "Logical binary operator must have exactly two arguments. Received: " + node);
        }
        RowExpression left = node.getArguments().get(0);
        RowExpression right = node.getArguments().get(1);

        ClpExpression maybeLeftSubstring = tryInterpretSubstringEquality(operator, left, right);
        if (maybeLeftSubstring.getDefinition().isPresent()) {
            return maybeLeftSubstring;
        }

        ClpExpression maybeRightSubstring = tryInterpretSubstringEquality(operator, right, left);
        if (maybeRightSubstring.getDefinition().isPresent()) {
            return maybeRightSubstring;
        }

        ClpExpression leftExpression = left.accept(this, null);
        ClpExpression rightExpression = right.accept(this, null);
        Optional<String> leftDefinition = leftExpression.getDefinition();
        Optional<String> rightDefinition = rightExpression.getDefinition();
        if (!leftDefinition.isPresent() || !rightDefinition.isPresent()) {
            return new ClpExpression(node);
        }

        boolean leftIsVariable = (left instanceof VariableReferenceExpression);
        boolean rightIsVariable = (right instanceof VariableReferenceExpression);
        boolean leftIsConstant = (left instanceof ConstantExpression);
        boolean rightIsConstant = (right instanceof ConstantExpression);

        Type leftType = left.getType();
        Type rightType = right.getType();

        if (leftIsVariable && rightIsConstant) {
            return buildClpExpression(
                    leftDefinition.get(),    // variable
                    rightDefinition.get(),   // literal
                    operator,
                    rightType,
                    node);
        }
        else if (leftIsConstant && rightIsVariable) {
            OperatorType newOperator = OperatorType.flip(operator);
            return buildClpExpression(
                    rightDefinition.get(),   // variable
                    leftDefinition.get(),    // literal
                    newOperator,
                    leftType,
                    node);
        }
        // fallback
        return new ClpExpression(node);
    }

    @Override
    public ClpExpression visitCall(CallExpression node, Void context)
    {
        FunctionHandle functionHandle = node.getFunctionHandle();
        if (standardFunctionResolution.isNotFunction(functionHandle)) {
            return handleNot(node);
        }

        if (standardFunctionResolution.isLikeFunction(functionHandle)) {
            return handleLike(node);
        }

        FunctionMetadata functionMetadata = functionMetadataManager.getFunctionMetadata(node.getFunctionHandle());
        Optional<OperatorType> operatorTypeOptional = functionMetadata.getOperatorType();
        if (operatorTypeOptional.isPresent()) {
            OperatorType operatorType = operatorTypeOptional.get();
            if (operatorType.isComparisonOperator() || operatorType != OperatorType.IS_DISTINCT_FROM) {
                return handleLogicalBinary(operatorType, node);
            }
        }

        return new ClpExpression(node);
    }

    @Override
    public ClpExpression visitConstant(ConstantExpression node, Void context)
    {
        return new ClpExpression(getLiteralString(node));
    }

    @Override
    public ClpExpression visitVariableReference(VariableReferenceExpression node, Void context)
    {
        return new ClpExpression(getVariableName(node));
    }

    @Override
    public ClpExpression visitSpecialForm(SpecialFormExpression node, Void context)
    {
        switch (node.getForm()) {
            case AND:
                return handleAnd(node);
            case OR:
                return handleOr(node);
            case IN:
                return handleIn(node);
            case IS_NULL:
                return handleIsNull(node);
            default:
                return new ClpExpression(node);
        }
    }

    // For all other expressions, return the original expression
    @Override
    public ClpExpression visitExpression(RowExpression node, Void context)
    {
        return new ClpExpression(node);
    }
}
