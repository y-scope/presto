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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.expressions.DefaultRowExpressionTraversalVisitor;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPlanOptimizer;
import com.facebook.presto.spi.ConnectorPlanRewriter;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionVisitor;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import io.airlift.slice.Slice;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.plugin.clp.ClpConnectorFactory.CONNECTOR_NAME;
import static com.facebook.presto.plugin.clp.ClpErrorCode.CLP_PUSHDOWN_UNSUPPORTED_EXPRESSION;
import static com.facebook.presto.spi.ConnectorPlanRewriter.rewriteWith;
import static java.util.Objects.requireNonNull;

public class ClpPlanOptimizer
        implements ConnectorPlanOptimizer
{
    private static final Logger log = Logger.get(ClpPlanOptimizer.class);
    private final FunctionMetadataManager functionManager;
    private final StandardFunctionResolution functionResolution;
    private final ClpMetadataFilterProvider metadataFilterProvider;

    public ClpPlanOptimizer(FunctionMetadataManager functionManager, StandardFunctionResolution functionResolution, ClpMetadataFilterProvider metadataFilterProvider)
    {
        this.functionManager = requireNonNull(functionManager, "functionManager is null");
        this.functionResolution = requireNonNull(functionResolution, "functionResolution is null");
        this.metadataFilterProvider = requireNonNull(metadataFilterProvider, "metadataFilterProvider is null");
    }

    @Override
    public PlanNode optimize(PlanNode maxSubplan, ConnectorSession session, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator)
    {
        return rewriteWith(new Rewriter(idAllocator), maxSubplan);
    }

    private class Rewriter
            extends ConnectorPlanRewriter<Void>
    {
        private final PlanNodeIdAllocator idAllocator;

        public Rewriter(PlanNodeIdAllocator idAllocator)
        {
            this.idAllocator = idAllocator;
        }

        @Override
        public PlanNode visitProject(ProjectNode node, RewriteContext<Void> context)
        {
            Assignments.Builder assignmentsBuilder = new Assignments.Builder();
            Set<VariableReferenceExpression> clpUdfVariablesInProjectNode = new HashSet<>();
            for (Map.Entry<VariableReferenceExpression, RowExpression> entry : node.getAssignments().getMap().entrySet()) {
                VariableReferenceExpression oldKey = entry.getKey();
                RowExpression oldValue = entry.getValue();

                if (!(oldValue instanceof CallExpression)) {
                    assignmentsBuilder.put(oldKey, oldValue);
                    continue;
                }

                CallExpression callExpression = (CallExpression) oldValue;
                String functionName = functionManager.getFunctionMetadata((callExpression).getFunctionHandle())
                        .getName().getObjectName().toUpperCase();

                if (!functionName.startsWith("CLP_GET")) {
                    assignmentsBuilder.put(oldKey, oldValue);
                    continue;
                }

                if (!callExpression.getArguments().isEmpty()) {
                    RowExpression argument = callExpression.getArguments().get(0);
                    if (!(argument instanceof ConstantExpression)) {
                        throw new PrestoException(
                                CLP_PUSHDOWN_UNSUPPORTED_EXPRESSION,
                                "The argument of " + functionName + " must be a ConstantExpression");
                    }
                    String jsonPath = ((Slice) ((ConstantExpression) argument).getValue()).toStringUtf8();

                    VariableReferenceExpression newValue = new VariableReferenceExpression(
                            oldValue.getSourceLocation(), jsonPath, oldValue.getType());
                    assignmentsBuilder.put(oldKey, newValue);
                    clpUdfVariablesInProjectNode.add(newValue);
                }
            }

            PlanNode childNode = node.getSource();

            // Handle Project -> TableScan
            if (childNode instanceof TableScanNode) {
                TableScanNode newTableScanNode = buildNewTableScanNode((TableScanNode) childNode, clpUdfVariablesInProjectNode);
                return new ProjectNode(
                        newTableScanNode.getSourceLocation(),
                        idAllocator.getNextId(),
                        newTableScanNode,
                        assignmentsBuilder.build(),
                        node.getLocality());
            }

            // Handle Project -> Filter -> TableScan
            if (childNode instanceof FilterNode && ((FilterNode) childNode).getSource() instanceof TableScanNode) {
                FilterNode filterNode = (FilterNode) childNode;
                TableScanNode tableScanNode = (TableScanNode) filterNode.getSource();

                // Build new TableScanNode with CLP_GET projection pushes (even if empty)
                TableScanNode newTableScanNode = buildNewTableScanNode(tableScanNode, clpUdfVariablesInProjectNode);

                // Apply KQL pushdown for the FilterNode
                PlanNode newSourceNode = processFilter(filterNode, newTableScanNode);
                return new ProjectNode(
                        newSourceNode.getSourceLocation(),
                        idAllocator.getNextId(),
                        newSourceNode,
                        assignmentsBuilder.build(),
                        node.getLocality());
            }
            if (clpUdfVariablesInProjectNode.isEmpty()) {
                return node;
            }
            throw new PrestoException(CLP_PUSHDOWN_UNSUPPORTED_EXPRESSION,
                    "Unsupported plan shape for CLP pushdown: " + childNode.getClass().getSimpleName());
        }

        @Override
        public PlanNode visitFilter(FilterNode node, RewriteContext<Void> context)
        {
            if (!(node.getSource() instanceof TableScanNode)) {
                return node;
            }

            return processFilter(node, (TableScanNode) node.getSource());
        }

        private TableScanNode buildNewTableScanNode(
                TableScanNode tableScanNode,
                Set<VariableReferenceExpression> clpUdfVariables)
        {
            List<VariableReferenceExpression> newOutputVariables = new ArrayList<>(tableScanNode.getOutputVariables());
            Map<VariableReferenceExpression, ColumnHandle> newAssignments = new HashMap<>(tableScanNode.getAssignments());
            for (VariableReferenceExpression var : clpUdfVariables) {
                newOutputVariables.add(var);
                newAssignments.put(var, new ClpColumnHandle(var.getName(), var.getType(), true));
            }

            return new TableScanNode(
                    tableScanNode.getSourceLocation(),
                    idAllocator.getNextId(),
                    tableScanNode.getTable(),
                    newOutputVariables,
                    newAssignments,
                    tableScanNode.getTableConstraints(),
                    tableScanNode.getCurrentConstraint(),
                    tableScanNode.getEnforcedConstraint(),
                    tableScanNode.getCteMaterializationInfo());
        }

        private PlanNode processFilter(FilterNode filterNode, TableScanNode tableScanNode)
        {
            Map<VariableReferenceExpression, ColumnHandle> assignments = tableScanNode.getAssignments();
            TableHandle tableHandle = tableScanNode.getTable();
            ClpTableHandle clpTableHandle = (ClpTableHandle) tableHandle.getConnectorHandle();

            Set<VariableReferenceExpression> clpUdfVariablesInFilterNode = new HashSet<>();
            String scope = CONNECTOR_NAME + "." + clpTableHandle.getSchemaTableName().toString();
            ClpExpression clpExpression = filterNode.getPredicate().accept(
                    new ClpFilterToKqlConverter(
                            functionResolution,
                            functionManager,
                            assignments,
                            metadataFilterProvider.getColumnNames(scope)),
                    clpUdfVariablesInFilterNode);

            Optional<String> kqlQuery = clpExpression.getPushDownExpression();
            Optional<String> metadataSqlQuery = clpExpression.getMetadataSqlQuery();
            Optional<RowExpression> remainingPredicate = clpExpression.getRemainingExpression();

            if (remainingPredicate.isPresent()) {
                // Collect all variables actually present in the remainingPredicate
                Set<VariableReferenceExpression> variablesInPredicate = new HashSet<>();

                RowExpressionVisitor<Void, Void> visitor = new DefaultRowExpressionTraversalVisitor<Void>() {
                    @Override
                    public Void visitVariableReference(VariableReferenceExpression variable, Void context)
                    {
                        variablesInPredicate.add(variable);
                        return null;
                    }
                };

                remainingPredicate.get().accept(visitor, null);
                // Retain only the variables that also exist in the remainingPredicate
                clpUdfVariablesInFilterNode.retainAll(variablesInPredicate);
                if (!clpUdfVariablesInFilterNode.isEmpty()) {
                    tableScanNode = buildNewTableScanNode(tableScanNode, clpUdfVariablesInFilterNode);
                }
            }

            // Perform required metadata filter checks
            metadataFilterProvider.checkContainsRequiredFilters(clpTableHandle.getSchemaTableName(), metadataSqlQuery.orElse(""));
            boolean hasMetadataFilter = metadataSqlQuery.isPresent() && !metadataSqlQuery.get().isEmpty();
            if (hasMetadataFilter) {
                metadataSqlQuery = Optional.of(metadataFilterProvider.remapFilterSql(scope, metadataSqlQuery.get()));
                log.debug("Metadata SQL query: %s", metadataSqlQuery);
            }

            if (kqlQuery.isPresent() || hasMetadataFilter) {
                if (kqlQuery.isPresent()) {
                    log.debug("KQL query: %s", kqlQuery);
                }

                ClpTableLayoutHandle layoutHandle = new ClpTableLayoutHandle(clpTableHandle, kqlQuery, metadataSqlQuery);
                TableHandle newTableHandle = new TableHandle(
                        tableHandle.getConnectorId(),
                        clpTableHandle,
                        tableHandle.getTransaction(),
                        Optional.of(layoutHandle));

                tableScanNode = new TableScanNode(
                        tableScanNode.getSourceLocation(),
                        idAllocator.getNextId(),
                        newTableHandle,
                        tableScanNode.getOutputVariables(),
                        tableScanNode.getAssignments(),
                        tableScanNode.getTableConstraints(),
                        tableScanNode.getCurrentConstraint(),
                        tableScanNode.getEnforcedConstraint(),
                        tableScanNode.getCteMaterializationInfo());
            }

            if (remainingPredicate.isPresent()) {
                // Not all predicate pushed down, need new FilterNode
                return new FilterNode(
                        filterNode.getSourceLocation(),
                        idAllocator.getNextId(),
                        tableScanNode,
                        remainingPredicate.get());
            }
            else {
                return tableScanNode;
            }
        }
    }
}
