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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import io.airlift.slice.Slice;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.plugin.clp.ClpErrorCode.CLP_PUSHDOWN_UNSUPPORTED_EXPRESSION;
import static com.google.common.collect.ImmutableList.toImmutableList;

/**
 * Utility for rewriting CLP UDFs (e.g., <code>CLP_GET_*</code>) in {@link RowExpression trees.
 * <p>
 * These UDFs are rewritten into {@link VariableReferenceExpression}s with meaningful names. This
 * enables quering fields not present in the original table schema, but available in CLP.
 */
public final class ClpUdfRewriter
{
    private ClpUdfRewriter() {}

    /**
     * Rewrites <code>CLP_GET_*</code> UDFs in a {@link RowExpression}, collecting each resulting
     * variable into the given map along with its associated {@link ColumnHandle}.
     * <p>
     * Each <code>CLP_GET_*</code> UDF must take a single constant string argument, which is used to
     * construct the name of the variable reference (e.g. <code>CLP_GET_STRING('foo')</code> becomes
     * a variable name <code>foo</code>). Invalid usages (e.g., non-constant arguments) will throw a
     * {@link PrestoException}.
     *
     * @param expression the input expression to analyze and possibly rewrite
     * @param context a mapping from variable references to column handles. New entries will be
     * added for any rewritten CLP UDFs
     * @param functionManager function manager used to resolve function metadata
     * @param variableAllocator variable allocator used to create new variable references
     * @return a possibly rewritten {@link RowExpression} with <code>CLP_GET_*</code> calls replaced
     */
    public static RowExpression rewriteClpUdfs(
            RowExpression expression,
            Map<VariableReferenceExpression, ColumnHandle> context,
            FunctionMetadataManager functionManager,
            VariableAllocator variableAllocator)
    {
        if (!(expression instanceof CallExpression)) {
            return expression;
        }

        CallExpression call = (CallExpression) expression;
        String functionName = functionManager.getFunctionMetadata(call.getFunctionHandle()).getName().getObjectName().toUpperCase();

        if (functionName.startsWith("CLP_GET_")) {
            if (call.getArguments().size() != 1 || !(call.getArguments().get(0) instanceof ConstantExpression)) {
                throw new PrestoException(CLP_PUSHDOWN_UNSUPPORTED_EXPRESSION,
                        "CLP_GET_* UDF must have a single constant string argument");
            }

            ConstantExpression constant = (ConstantExpression) call.getArguments().get(0);
            String jsonPath = ((Slice) constant.getValue()).toStringUtf8();

            VariableReferenceExpression variable = variableAllocator.newVariable(
                    expression.getSourceLocation(),
                    encodeJsonPath(jsonPath),
                    call.getType());
            context.put(variable, new ClpColumnHandle(jsonPath, call.getType(), true));
            return variable;
        }

        List<RowExpression> rewrittenArgs = call.getArguments().stream()
                .map(arg -> rewriteClpUdfs(arg, context, functionManager, variableAllocator))
                .collect(toImmutableList());

        return new CallExpression(call.getDisplayName(), call.getFunctionHandle(), call.getType(), rewrittenArgs);
    }

    /**
     * Encodes a JSON path into a valid variable name by replacing uppercase letters with
     * "_u<lowercase letter>", dots with "_dot_", and underscores with "_und_".
     * <p>
     * This is only used internally to ensure that the variable names generated from JSON paths
     * are valid and do not conflict with other variable names in the expression.
     *
     * @param jsonPath the JSON path to encode
     * @return the encoded variable name
     */
    private static String encodeJsonPath(String jsonPath)
    {
        StringBuilder sb = new StringBuilder();
        for (char c : jsonPath.toCharArray()) {
            if (Character.isUpperCase(c)) {
                sb.append("_ux").append(Character.toLowerCase(c));
            }
            else if (c == '.') {
                sb.append("_dot_");
            }
            else if (c == '_') {
                sb.append("_und_");
            }
            else {
                sb.append(c);
            }
        }
        return sb.toString();
    }
}
