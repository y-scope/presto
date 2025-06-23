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
package com.facebook.presto.plugin.clp;

import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.type.RowType;
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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.common.function.OperatorType.EQUAL;
import static com.facebook.presto.common.function.OperatorType.GREATER_THAN;
import static com.facebook.presto.common.function.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.common.function.OperatorType.IS_DISTINCT_FROM;
import static com.facebook.presto.common.function.OperatorType.LESS_THAN;
import static com.facebook.presto.common.function.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.common.function.OperatorType.NEGATION;
import static com.facebook.presto.common.function.OperatorType.NOT_EQUAL;
import static com.facebook.presto.common.function.OperatorType.flip;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.plugin.clp.ClpErrorCode.CLP_PUSHDOWN_UNSUPPORTED_EXPRESSION;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.AND;
import static java.lang.Integer.parseInt;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * A translator to translate Presto RowExpressions into KQL (Kibana Query Language) filters used as
 * CLP queries. This is used primarily for pushing down supported filters to the CLP engine. This
 * class implements the RowExpressionVisitor interface and recursively walks Presto filter
 * expressions, attempting to convert supported expressions into corresponding KQL filter strings.
 * Any part of the expression that cannot be translated is preserved as a "remaining expression" for
 * potential fallback processing.
 * <p></p>
 * Supported translations include:
 * <ul>
 *     <li>Comparisons between variables and constants (e.g., =, !=, <, >, <=, >=).</li>
 *     <li>String pattern matches using LIKE with constant patterns only. <code>"^%[^%_]*%$"</code>
 *         is not supported.</li>
 *     <li>Membership checks using IN with a list of constants only</li>
 *     <li>NULL checks via IS NULL</li>
 *     <li>Substring comparisons (e.g., <code>SUBSTR(x, start, len) = "val"</code>) are supported
 *         only when compared against a constant.</li>
 *     <li>Dereferencing fields from row-typed variables</li>
 *     <li>Logical operators AND, OR, and NOT</li>
 * </ul>
 */
public class ClpFilterToKqlConverter
        implements RowExpressionVisitor<ClpExpression, Void>
{
    private static final Set<OperatorType> LOGICAL_BINARY_OPS_FILTER =
            ImmutableSet.of(EQUAL, NOT_EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL);

    private final StandardFunctionResolution standardFunctionResolution;
    private final FunctionMetadataManager functionMetadataManager;
    private final Map<VariableReferenceExpression, ColumnHandle> assignments;

    public ClpFilterToKqlConverter(
            StandardFunctionResolution standardFunctionResolution,
            FunctionMetadataManager functionMetadataManager,
            Map<VariableReferenceExpression, ColumnHandle> assignments)
    {
        this.standardFunctionResolution = requireNonNull(standardFunctionResolution, "standardFunctionResolution is null");
        this.functionMetadataManager = requireNonNull(functionMetadataManager, "function metadata manager is null");
        this.assignments = requireNonNull(assignments, "assignments is null");
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
            if (operatorType.isComparisonOperator() && operatorType != IS_DISTINCT_FROM) {
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
            case DEREFERENCE:
                return handleDereference(node);
            default:
                return new ClpExpression(node);
        }
    }

    @Override
    public ClpExpression visitExpression(RowExpression node, Void context)
    {
        // For all other expressions, return the original expression
        return new ClpExpression(node);
    }

    /**
     * Extracts the string representation of a constant expression.
     *
     * @param literal the constant expression
     * @return the string representation of the literal
     */
    private String getLiteralString(ConstantExpression literal)
    {
        if (literal.getValue() instanceof Slice) {
            return ((Slice) literal.getValue()).toStringUtf8();
        }
        return literal.toString();
    }

    /**
     * Retrieves the original column name from a variable reference.
     *
     * @param variable the variable reference expression
     * @return the original column name as a string
     */
    private String getVariableName(VariableReferenceExpression variable)
    {
        return ((ClpColumnHandle) assignments.get(variable)).getOriginalColumnName();
    }

    /**
     * Handles the logical NOT expression.
     * <p></p>
     * Example: <code>NOT (col1 = 5)</code> → <code>NOT col1: 5</code>
     *
     * @param node the NOT call expression
     * @return a ClpExpression containing the equivalent KQL query or the original expression if it
     * couldn't be translated
     */
    private ClpExpression handleNot(CallExpression node)
    {
        if (node.getArguments().size() != 1) {
            throw new PrestoException(CLP_PUSHDOWN_UNSUPPORTED_EXPRESSION,
                    "NOT operator must have exactly one argument. Received: " + node);
        }

        RowExpression input = node.getArguments().get(0);
        ClpExpression expression = input.accept(this, null);
        if (expression.getRemainingExpression().isPresent() || !expression.getKqlQuery().isPresent()) {
            return new ClpExpression(node);
        }
        return new ClpExpression("NOT " + expression.getKqlQuery().get());
    }

    /**
     * Handles LIKE expressions.
     * <p></p>
     * Converts SQL LIKE patterns into equivalent KQL queries using <code>*</code> (for <code>%</code>)
     * and <code>?</code> (for <code>_</code>). Only supports constant or casted constant patterns.
     * <p></p>
     * Example: <code>col1 LIKE 'a_bc%'</code> → <code>col1: "a?bc*"</code>
     *
     * @param node the LIKE call expression
     * @return a ClpExpression containing the equivalent KQL query, or the original expression if it
     * couldn't be translated
     */
    private ClpExpression handleLike(CallExpression node)
    {
        if (node.getArguments().size() != 2) {
            throw new PrestoException(CLP_PUSHDOWN_UNSUPPORTED_EXPRESSION, "LIKE operator must have exactly two arguments. Received: " + node);
        }
        ClpExpression variable = node.getArguments().get(0).accept(this, null);
        if (!variable.getKqlQuery().isPresent()) {
            return new ClpExpression(node);
        }

        String variableName = variable.getKqlQuery().get();
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
                throw new PrestoException(CLP_PUSHDOWN_UNSUPPORTED_EXPRESSION, "CAST function must have exactly one argument. Received: " + callExpression);
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
        return new ClpExpression(format("%s: \"%s\"", variableName, pattern));
    }

    /**
     * Handles logical binary operators (e.g., <code>=, !=, <, ></code>) between two expressions.
     * <p></p>
     * Supports constant values on either side and flips the operator if necessary. Also delegates to a
     * substring handler for <code>SUBSTR(x, ...) = 'value'</code> patterns.
     *
     * @param operator the binary operator (e.g., EQUAL, NOT_EQUAL)
     * @param node the call expression representing the binary operation
     * @return a ClpExpression containing the equivalent KQL query or the original expression if it
     * couldn't be translated
     */
    private ClpExpression handleLogicalBinary(OperatorType operator, CallExpression node)
    {
        if (node.getArguments().size() != 2) {
            throw new PrestoException(CLP_PUSHDOWN_UNSUPPORTED_EXPRESSION,
                    "Logical binary operator must have exactly two arguments. Received: " + node);
        }
        RowExpression left = node.getArguments().get(0);
        RowExpression right = node.getArguments().get(1);

        ClpExpression maybeLeftSubstring = tryInterpretSubstringEquality(operator, left, right);
        if (maybeLeftSubstring.getKqlQuery().isPresent()) {
            return maybeLeftSubstring;
        }

        ClpExpression maybeRightSubstring = tryInterpretSubstringEquality(operator, right, left);
        if (maybeRightSubstring.getKqlQuery().isPresent()) {
            return maybeRightSubstring;
        }

        ClpExpression leftExpression = left.accept(this, null);
        ClpExpression rightExpression = right.accept(this, null);
        Optional<String> leftDefinition = leftExpression.getKqlQuery();
        Optional<String> rightDefinition = rightExpression.getKqlQuery();
        if (!leftDefinition.isPresent() || !rightDefinition.isPresent()) {
            return new ClpExpression(node);
        }

        boolean leftIsConstant = (left instanceof ConstantExpression);
        boolean rightIsConstant = (right instanceof ConstantExpression);

        Type leftType = left.getType();
        Type rightType = right.getType();

        if (rightIsConstant) {
            return buildClpExpression(
                    leftDefinition.get(),    // variable
                    rightDefinition.get(),   // literal
                    operator,
                    rightType,
                    node);
        }
        else if (leftIsConstant) {
            OperatorType newOperator = flip(operator);
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

    /**
     * Builds a CLP expression from a basic comparison between a variable and a constant.
     * <p></p>
     * Handles different operator types and formats them appropriately based on whether the literal
     * is a string or a non-string type.
     * <p></p>
     * Examples:
     * <ul>
     *   <li><code>col = 'abc'</code> → <code>col: "abc"</code></li>
     *   <li><code>col != 42</code> → <code>NOT col: 42</code></li>
     *   <li><code>5 < col</code> → <code>col > 5</code></li>
     * </ul>
     *
     * @param variableName name of the variable
     * @param literalString string representation of the literal
     * @param operator the comparison operator
     * @param literalType the type of the literal
     * @param originalNode the original RowExpression node
     * @return a ClpExpression containing the equivalent KQL query or the original expression if it
     * couldn't be translated
     */
    private ClpExpression buildClpExpression(
            String variableName,
            String literalString,
            OperatorType operator,
            Type literalType,
            RowExpression originalNode)
    {
        if (operator.equals(EQUAL)) {
            if (literalType instanceof VarcharType) {
                return new ClpExpression(format("%s: \"%s\"", variableName, literalString));
            }
            else {
                return new ClpExpression(format("%s: %s", variableName, literalString));
            }
        }
        else if (operator.equals(NOT_EQUAL)) {
            if (literalType instanceof VarcharType) {
                return new ClpExpression(format("NOT %s: \"%s\"", variableName, literalString));
            }
            else {
                return new ClpExpression(format("NOT %s: %s", variableName, literalString));
            }
        }
        else if (LOGICAL_BINARY_OPS_FILTER.contains(operator) && !(literalType instanceof VarcharType)) {
            return new ClpExpression(format("%s %s %s", variableName, operator.getOperator(), literalString));
        }
        return new ClpExpression(originalNode);
    }

    /**
     * Checks whether the given expression matches the pattern SUBSTR(x, ...) = 'someString',
     * and if so, attempts to convert it into a KQL query using wildcards and construct a CLP expression.
     *
     * @param operator the comparison operator (should be EQUAL)
     * @param possibleSubstring the left or right expression, possibly a SUBSTR call
     * @param possibleLiteral the opposite expression, possibly a string constant
     * @return a ClpExpression containing the translated KQL filter or an empty one if conversion fails
     */
    private ClpExpression tryInterpretSubstringEquality(
            OperatorType operator,
            RowExpression possibleSubstring,
            RowExpression possibleLiteral)
    {
        if (!operator.equals(EQUAL)) {
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

    /**
     * Parses a <code>SUBSTR(x, start [, length])</code> call into a SubstrInfo object if valid.
     *
     * @param callExpression the call expression to inspect
     * @return an Optional containing SubstrInfo if the expression is a valid SUBSTR call, otherwise empty
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

        ClpExpression variable = callExpression.getArguments().get(0).accept(this, null);
        if (!variable.getKqlQuery().isPresent()) {
            return Optional.empty();
        }

        String varName = variable.getKqlQuery().get();
        RowExpression startExpression = callExpression.getArguments().get(1);
        RowExpression lengthExpression = null;
        if (argCount == 3) {
            lengthExpression = callExpression.getArguments().get(2);
        }

        return Optional.of(new SubstrInfo(varName, startExpression, lengthExpression));
    }

    /**
     * Converts a <code>SUBSTR(x, start [, length]) = 'someString'</code> into a KQL-style wildcard query.
     * <p></p>
     * Examples:
     * <ul>
     *   <li><code>SUBSTR(message, 1, 3) = 'abc'</code> → <code>message: "abc*"</code></li>
     *   <li><code>SUBSTR(message, 4, 3) = 'abc'</code> → <code>message: "???abc*"</code></li>
     *   <li><code>SUBSTR(message, 2) = 'hello'</code> → <code>message: "?hello"</code></li>
     *   <li><code>SUBSTR(message, -5) = 'hello'</code> → <code>message: "*hello"</code></li>
     * </ul>
     *
     * @param info parsed SUBSTR call info
     * @param targetString the literal string being compared to
     * @return a ClpExpression containing the translated KQL query if successful; otherwise, an empty ClpExpression
     */
    private ClpExpression interpretSubstringEquality(SubstrInfo info, String targetString)
    {
        if (info.lengthExpression != null) {
            Optional<Integer> maybeStart = parseIntValue(info.startExpression);
            Optional<Integer> maybeLen = parseLengthLiteral(info.lengthExpression, targetString);

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
                    return new ClpExpression(format("%s: \"*%s\"", info.variableName, targetString));
                }
            }
        }

        return new ClpExpression();
    }

    /**
     * Attempts to parse a RowExpression as an integer constant.
     *
     * @param expression the row expression to parse
     * @return an Optional containing the parsed integer value, if successful
     */
    private Optional<Integer> parseIntValue(RowExpression expression)
    {
        if (expression instanceof ConstantExpression) {
            try {
                return Optional.of(parseInt(getLiteralString((ConstantExpression) expression)));
            }
            catch (NumberFormatException ignored) {
            }
        }
        else if (expression instanceof CallExpression) {
            CallExpression call = (CallExpression) expression;
            FunctionMetadata functionMetadata = functionMetadataManager.getFunctionMetadata(call.getFunctionHandle());
            Optional<OperatorType> operatorTypeOptional = functionMetadata.getOperatorType();
            if (operatorTypeOptional.isPresent() && operatorTypeOptional.get().equals(NEGATION)) {
                RowExpression arg0 = call.getArguments().get(0);
                if (arg0 instanceof ConstantExpression) {
                    try {
                        return Optional.of(-parseInt(getLiteralString((ConstantExpression) arg0)));
                    }
                    catch (NumberFormatException ignored) {
                    }
                }
            }
        }
        return Optional.empty();
    }

    /**
     * Attempts to parse the length expression and match it against the target string's length.
     *
     * @param lengthExpression the expression representing the length parameter
     * @param targetString the target string to compare length against
     * @return an Optional containing the length if it matches targetString.length(), otherwise empty
     */
    private Optional<Integer> parseLengthLiteral(RowExpression lengthExpression, String targetString)
    {
        if (lengthExpression instanceof ConstantExpression) {
            String val = getLiteralString((ConstantExpression) lengthExpression);
            try {
                int parsed = parseInt(val);
                if (parsed == targetString.length()) {
                    return Optional.of(parsed);
                }
            }
            catch (NumberFormatException ignored) {
            }
        }
        return Optional.empty();
    }

    /**
     * Handles the logical AND expression.
     * <p></p>
     * Combines all definable child expressions into a single KQL query joined by AND.
     * Any unsupported children are collected into a remaining expression.
     * <p></p>
     * Example: <code>col1 = 5 AND col2 = 'abc'</code> → <code>(col1: 5 AND col2: "abc")</code>
     *
     * @param node the AND special form expression
     * @return a ClpExpression containing the KQL query and any remaining sub-expressions
     */
    private ClpExpression handleAnd(SpecialFormExpression node)
    {
        StringBuilder queryBuilder = new StringBuilder();
        queryBuilder.append("(");
        List<RowExpression> remainingExpressions = new ArrayList<>();
        boolean hasDefinition = false;
        for (RowExpression argument : node.getArguments()) {
            ClpExpression expression = argument.accept(this, null);
            if (expression.getKqlQuery().isPresent()) {
                hasDefinition = true;
                queryBuilder.append(expression.getKqlQuery().get());
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
                return new ClpExpression(queryBuilder.substring(0, queryBuilder.length() - 5) + ")", remainingExpressions.get(0));
            }
            else {
                return new ClpExpression(
                        queryBuilder.substring(0, queryBuilder.length() - 5) + ")",
                        new SpecialFormExpression(node.getSourceLocation(), AND, BOOLEAN, remainingExpressions));
            }
        }
        // Remove the last " AND " from the query
        return new ClpExpression(queryBuilder.substring(0, queryBuilder.length() - 5) + ")");
    }

    /**
     * Handles the logical OR expression.
     * <p></p>
     * Combines all fully convertible child expressions into a single KQL query joined by OR.
     * Falls back to the original node if any child cannot be converted.
     * <p></p>
     * Example: <code>col1 = 5 OR col1 = 10</code> → <code>(col1: 5 OR col1: 10)</code>
     *
     * @param node the OR special form expression
     * @return a ClpExpression containing the OR-based KQL string, or the original expression if not fully convertible
     */
    private ClpExpression handleOr(SpecialFormExpression node)
    {
        StringBuilder queryBuilder = new StringBuilder();
        queryBuilder.append("(");
        for (RowExpression argument : node.getArguments()) {
            ClpExpression expression = argument.accept(this, null);
            if (expression.getRemainingExpression().isPresent() || !expression.getKqlQuery().isPresent()) {
                return new ClpExpression(node);
            }
            queryBuilder.append(expression.getKqlQuery().get());
            queryBuilder.append(" OR ");
        }
        // Remove the last " OR " from the query
        return new ClpExpression(queryBuilder.substring(0, queryBuilder.length() - 4) + ")");
    }

    /**
     * Handles the IN predicate.
     * <p></p>
     * Example: <code>col1 IN (1, 2, 3)</code> → <code>(col1: 1 OR col1: 2 OR col1: 3)</code>
     *
     * @param node the IN special form expression
     * @return a ClpExpression containing the equivalent KQL query or the original expression if it
     * couldn't be translated
     */
    private ClpExpression handleIn(SpecialFormExpression node)
    {
        ClpExpression variable = node.getArguments().get(0).accept(this, null);
        if (!variable.getKqlQuery().isPresent()) {
            return new ClpExpression(node);
        }
        String variableName = variable.getKqlQuery().get();
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

    /**
     * Handles the IS NULL predicate.
     * <p></p>
     * Example: <code>col1 IS NULL</code> → <code>NOT col1: *</code>
     *
     * @param node the IS_NULL special form expression
     * @return a ClpExpression containing the equivalent KQL query or the original expression if it
     * couldn't be translated
     */
    private ClpExpression handleIsNull(SpecialFormExpression node)
    {
        if (node.getArguments().size() != 1) {
            throw new PrestoException(CLP_PUSHDOWN_UNSUPPORTED_EXPRESSION,
                    "IS NULL operator must have exactly one argument. Received: " + node);
        }

        ClpExpression expression = node.getArguments().get(0).accept(this, null);
        if (!expression.getKqlQuery().isPresent()) {
            return new ClpExpression(node);
        }

        String variableName = expression.getKqlQuery().get();
        return new ClpExpression(format("NOT %s: *", variableName));
    }

    /**
     * Handles dereference expressions on RowTypes (e.g., <code>col.row_field</code>).
     * <p></p>
     * Converts nested row field access into dot-separated KQL-compatible field names.
     * <p></p>
     * Example: <code>address.city</code> (from a RowType 'address') → <code>address.city</code>
     *
     * @param expression the dereference expression (SpecialFormExpression or VariableReferenceExpression)
     * @return a ClpExpression containing the dot-separated field name or the original expression if it
     * couldn't be translated
     */
    private ClpExpression handleDereference(RowExpression expression)
    {
        if (expression instanceof VariableReferenceExpression) {
            return expression.accept(this, null);
        }

        if (!(expression instanceof SpecialFormExpression)) {
            return new ClpExpression(expression);
        }

        SpecialFormExpression specialForm = (SpecialFormExpression) expression;
        List<RowExpression> arguments = specialForm.getArguments();
        if (arguments.size() != 2) {
            throw new PrestoException(CLP_PUSHDOWN_UNSUPPORTED_EXPRESSION, "DEREFERENCE expects 2 arguments");
        }

        RowExpression base = arguments.get(0);
        RowExpression index = arguments.get(1);
        if (!(index instanceof ConstantExpression)) {
            throw new PrestoException(CLP_PUSHDOWN_UNSUPPORTED_EXPRESSION, "DEREFERENCE index must be a constant");
        }

        ConstantExpression constExpr = (ConstantExpression) index;
        Object value = constExpr.getValue();
        if (!(value instanceof Long)) {
            throw new PrestoException(CLP_PUSHDOWN_UNSUPPORTED_EXPRESSION, "DEREFERENCE index constant is not a long");
        }

        int fieldIndex = ((Long) value).intValue();

        Type baseType = base.getType();
        if (!(baseType instanceof RowType)) {
            throw new PrestoException(CLP_PUSHDOWN_UNSUPPORTED_EXPRESSION, "DEREFERENCE base is not a RowType: " + baseType);
        }

        RowType rowType = (RowType) baseType;
        if (fieldIndex < 0 || fieldIndex >= rowType.getFields().size()) {
            throw new PrestoException(CLP_PUSHDOWN_UNSUPPORTED_EXPRESSION, "Invalid field index " + fieldIndex + " for RowType: " + rowType);
        }

        RowType.Field field = rowType.getFields().get(fieldIndex);
        String fieldName = field.getName().orElse("field" + fieldIndex);

        ClpExpression baseString = handleDereference(base);
        if (!baseString.getKqlQuery().isPresent()) {
            return new ClpExpression(expression);
        }
        return new ClpExpression(baseString.getKqlQuery().get() + "." + fieldName);
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
}
