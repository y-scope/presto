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
package com.facebook.presto.plugin.clp.optimization;

import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionVisitor;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ClpSplitMetadataExpressionTransformer
        implements RowExpressionVisitor<RowExpression, Void>
{
    private final Map<String, String> exposedToOriginal;
    private final Map<String, Map<String, String>> dataToMetadataBounds;
    private final Set<String> requiredColumns;
    private final Set<String> presentColumns = new HashSet<>();

    public ClpSplitMetadataExpressionTransformer(
            Map<String, String> exposedToOriginal,
            Map<String, Map<String, String>> dataToMetadataBounds,
            Set<String> requiredColumns)
    {
        this.exposedToOriginal = exposedToOriginal;
        this.dataToMetadataBounds = dataToMetadataBounds;
        this.requiredColumns = requiredColumns;
    }

    public RowExpression rewrite(RowExpression expr)
    {
        return expr.accept(this, null);
    }

    public boolean containsAllRequiredColumns()
    {
        return presentColumns.containsAll(requiredColumns);
    }

    @Override
    public RowExpression visitCall(CallExpression call, Void context)
    {
        List<RowExpression> rewrittenArgs = call.getArguments().stream()
                .map(arg -> arg.accept(this, context))
                .collect(Collectors.toList());
        return new CallExpression(call.getSourceLocation(), call.getDisplayName(), call.getFunctionHandle(), call.getType(), rewrittenArgs, call.getFunctionMetadata());
    }

    @Override
    public RowExpression visitConstant(ConstantExpression constant, Void context)
    {
        return constant;
    }

    @Override
    public RowExpression visitInputReference(InputReferenceExpression reference, Void context)
    {
        return reference;
    }

    @Override
    public RowExpression visitFieldReference(FieldReferenceExpression reference, Void context)
    {
        // Replace column names if applicable
        String originalName = reference.getField().toString();
        presentColumns.add(originalName);

        // Step 1: replace exposed name -> original
        String mappedName = exposedToOriginal.getOrDefault(originalName, originalName);

        // Step 2: remap data column -> metadata column (range-based)
        Map<String, String> bounds = dataToMetadataBounds.get(mappedName);
        if (bounds != null) {
            // by convention: replace with begin_timestamp or end_timestamp depending on predicate direction
            // this part might require operator context; here we can only do simple rename
            // we could wrap it in metadata-specific placeholder for later replacement
        }

        return new FieldReferenceExpression(mappedName, reference.getType());
    }

    @Override
    public RowExpression visitSpecialForm(SpecialFormExpression specialForm, Void context)
    {
        List<RowExpression> args = specialForm.getArguments().stream()
                .map(arg -> arg.accept(this, context))
                .collect(Collectors.toList());
        return new SpecialFormExpression(
                specialForm.getForm(),
                specialForm.getType(),
                args,
                specialForm.getSourceLocation());
    }

    @Override
    public RowExpression visitLambda(LambdaDefinitionExpression lambda, Void context)
    {
        return lambda;
    }

    @Override
    public RowExpression visitVariableReference(VariableReferenceExpression reference, Void context)
    {
        String name = reference.getName();
        presentColumns.add(name);

        // Step 1: replace exposed name → original
        String mappedName = exposedToOriginal.getOrDefault(name, name);

        // Step 2: remap data column → metadata columns (range mapping)
        Map<String, String> bounds = dataToMetadataBounds.get(mappedName);
        if (bounds != null && bounds.size() == 1) {
            // Simple direct remap (not range)
            mappedName = bounds.values().iterator().next();
        }

        return new VariableReferenceExpression(
                reference.getSourceLocation(),
                mappedName,
                reference.getType());
    }
}