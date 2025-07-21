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
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import io.airlift.slice.Slice;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.plugin.clp.ClpErrorCode.CLP_PUSHDOWN_UNSUPPORTED_EXPRESSION;
import static com.google.common.collect.ImmutableList.toImmutableList;

/**
 * Utility methods for rewriting CLP UDFs (e.g., <code>CLP_GET_*</code>) in {@link RowExpression}
 * trees.
 * <p>
 * These UDFs are rewritten into {@link VariableReferenceExpression}s with meaningful names. This
 * enables quering fields not present in the original table schema, but available in CLP.
 * <p>
 * Each <code>CLP_GET_*</code> UDF must take a single constant string argument, which is used to
 * construct the name of the variable reference (e.g. <code>CLP_GET_STRING('foo')</code> becomes a
 * variable name <code>foo</code>). Invalid usages (e.g., non-constant arguments) will throw a
 * {@link PrestoException}.
 * <p>
 * Depending on usage context:
 * <ul>
 *   <li><code>rewriteClpUdfsWithMap(...)</code> collects rewritten variables and associates them
 *       with {@link ColumnHandle}s — used for <code>TableScanNode</code> predicate pushdown.</li>
 *   <li><code>rewriteClpUdfsWithSet(...)</code> collects rewritten variables into a set —
 *       used for tracking projections in <code>ProjectNode</code>.</li>
 * </ul>
 */
public final class ClpUdfRewriter
{
    private ClpUdfRewriter() {}

    /**
     * Rewrites <code>CLP_GET_*</code> UDFs in a {@link RowExpression}, collecting each resulting
     * variable into the given set.
     * <p>
     * Used for tracking projected variables in <code>ProjectNode</code>.
     *
     * @param expression the input expression to analyze and possibly rewrite
     * @param clpUdfVariables a set to collect variable references corresponding to CLP UDFs
     * @param functionManager function manager used to resolve function metadata
     * @return a possibly rewritten {@link RowExpression} with <code>CLP_GET_*</code> calls replaced
     */
    public static RowExpression rewriteClpUdfsWithSet(
            RowExpression expression,
            Set<VariableReferenceExpression> clpUdfVariables,
            FunctionMetadataManager functionManager)
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

            VariableReferenceExpression variable = new VariableReferenceExpression(
                    expression.getSourceLocation(),
                    jsonPath,
                    call.getType());

            clpUdfVariables.add(variable);
            return variable;
        }

        List<RowExpression> rewrittenArgs = call.getArguments().stream()
                .map(arg -> rewriteClpUdfsWithSet(arg, clpUdfVariables, functionManager))
                .collect(toImmutableList());

        return new CallExpression(call.getDisplayName(), call.getFunctionHandle(), call.getType(), rewrittenArgs);
    }

    /**
     * Rewrites <code>CLP_GET_*</code> UDFs in a {@link RowExpression}, collecting each resulting
     * variable into the given map along with its associated {@link ColumnHandle}.
     * <p>
     * Used for processing filter predicates in <code>TableScanNode</code>.
     *
     * @param expression the input expression to analyze and possibly rewrite
     * @param context a mapping from variable references to column handles. New entries will be
     * added for any rewritten CLP UDFs
     * @param functionManager function manager used to resolve function metadata
     * @return a possibly rewritten {@link RowExpression} with <code>CLP_GET_*</code> calls replaced
     */
    public static RowExpression rewriteClpUdfsWithMap(
            RowExpression expression,
            Map<VariableReferenceExpression, ColumnHandle> context,
            FunctionMetadataManager functionManager)
    {
        Set<VariableReferenceExpression> collected = new HashSet<>();
        RowExpression rewritten = rewriteClpUdfsWithSet(expression, collected, functionManager);

        for (VariableReferenceExpression var : collected) {
            context.putIfAbsent(var, new ClpColumnHandle(var.getName(), var.getType(), true));
        }

        return rewritten;
    }
}
