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
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.plugin.clp.ClpConnectorFactory.CONNECTOR_NAME;
import static com.facebook.presto.plugin.clp.ClpUdfRewriter.rewriteClpUdfs;
import static com.facebook.presto.spi.ConnectorPlanRewriter.rewriteWith;
import static com.google.common.collect.ImmutableList.toImmutableList;
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
        return rewriteWith(new Rewriter(idAllocator, variableAllocator), maxSubplan);
    }

    private class Rewriter
            extends ConnectorPlanRewriter<Void>
    {
        private final PlanNodeIdAllocator idAllocator;
        private final VariableAllocator variableAllocator;

        public Rewriter(PlanNodeIdAllocator idAllocator, VariableAllocator variableAllocator)
        {
            this.idAllocator = idAllocator;
            this.variableAllocator = variableAllocator;
        }

        @Override
        public PlanNode visitProject(ProjectNode node, RewriteContext<Void> context)
        {
            Assignments.Builder assignmentsBuilder = new Assignments.Builder();
            Map<VariableReferenceExpression, ColumnHandle> clpUdfAssignments = new HashMap<>();
            for (Map.Entry<VariableReferenceExpression, RowExpression> entry : node.getAssignments().getMap().entrySet()) {
                VariableReferenceExpression oldKey = entry.getKey();
                RowExpression oldValue = entry.getValue();

                RowExpression rewrittenExpr = rewriteClpUdfs(
                        oldValue,
                        clpUdfAssignments,
                        functionManager,
                        variableAllocator);
                assignmentsBuilder.put(oldKey, rewrittenExpr);
            }

            if (clpUdfAssignments.isEmpty()) {
                // No CLP UDFs in projection: return as-is
                return context.defaultRewrite(node);
            }

            PlanNode newSource = rewriteClpSubtree(node.getSource(), clpUdfAssignments);
            return new ProjectNode(
                    node.getSourceLocation(),
                    idAllocator.getNextId(),
                    newSource,
                    assignmentsBuilder.build(),
                    node.getLocality());
        }

        @Override
        public PlanNode visitFilter(FilterNode node, RewriteContext<Void> context)
        {
            if (!(node.getSource() instanceof TableScanNode)) {
                return node;
            }

            return processFilter(node, (TableScanNode) node.getSource());
        }

        private PlanNode rewriteClpSubtree(PlanNode node, Map<VariableReferenceExpression, ColumnHandle> clpUdfAssignments)
        {
            // Base case: TableScanNode
            if (node instanceof TableScanNode) {
                return buildNewTableScanNode((TableScanNode) node, clpUdfAssignments);
            }

            // Base case: FilterNode -> TableScanNode
            if (node instanceof FilterNode && ((FilterNode) node).getSource() instanceof TableScanNode) {
                FilterNode filterNode = (FilterNode) node;
                TableScanNode tableScanNode = (TableScanNode) filterNode.getSource();
                TableScanNode newTableScanNode = buildNewTableScanNode(tableScanNode, clpUdfAssignments);
                return processFilter(filterNode, newTableScanNode);
            }

            // Recursively rewrite all child sources
            List<PlanNode> rewrittenSources = node.getSources().stream()
                    .map(source -> rewriteClpSubtree(source, clpUdfAssignments))
                    .collect(toImmutableList());

            for (int i = 0; i < node.getSources().size(); i++) {
                if (rewrittenSources.get(i) != node.getSources().get(i)) {
                    return node.replaceChildren(rewrittenSources);
                }
            }
            return node;
        }

        private TableScanNode buildNewTableScanNode(
                TableScanNode tableScanNode,
                Map<VariableReferenceExpression, ColumnHandle> assignments)
        {
            List<VariableReferenceExpression> newOutputVariables = new ArrayList<>(tableScanNode.getOutputVariables());
            Map<VariableReferenceExpression, ColumnHandle> newAssignments = new HashMap<>(tableScanNode.getAssignments());
            for (Map.Entry<VariableReferenceExpression, ColumnHandle> entry : assignments.entrySet()) {
                newOutputVariables.add(entry.getKey());
                newAssignments.put(entry.getKey(), entry.getValue());
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
            TableHandle tableHandle = tableScanNode.getTable();
            ClpTableHandle clpTableHandle = (ClpTableHandle) tableHandle.getConnectorHandle();

            String scope = CONNECTOR_NAME + "." + clpTableHandle.getSchemaTableName().toString();
            Map<VariableReferenceExpression, ColumnHandle> originalAssignments = tableScanNode.getAssignments();
            Map<VariableReferenceExpression, ColumnHandle> assignments = new HashMap<>(originalAssignments);

            ClpExpression clpExpression = filterNode.getPredicate().accept(
                    new ClpFilterToKqlConverter(
                            functionResolution,
                            functionManager,
                            metadataFilterProvider.getColumnNames(scope),
                            variableAllocator),
                    assignments);

            Optional<String> kqlQuery = clpExpression.getPushDownExpression();
            Optional<String> metadataSqlQuery = clpExpression.getMetadataSqlQuery();
            Optional<RowExpression> remainingPredicate = clpExpression.getRemainingExpression();

            if (remainingPredicate.isPresent()) {
                // Collect all variables actually present in the remainingPredicate
                Set<VariableReferenceExpression> variablesInPredicate = new HashSet<>();

                remainingPredicate.get().accept(new DefaultRowExpressionTraversalVisitor<Void>() {
                    @Override
                    public Void visitVariableReference(VariableReferenceExpression variable, Void context)
                    {
                        variablesInPredicate.add(variable);
                        return null;
                    }
                }, null);

                // Select only new entries added to assignments and used in the remaining predicate
                Map<VariableReferenceExpression, ColumnHandle> filteredNewAssignments = assignments.entrySet().stream()
                        .filter(entry -> !originalAssignments.containsKey(entry.getKey())
                                && variablesInPredicate.contains(entry.getKey()))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

                if (!filteredNewAssignments.isEmpty()) {
                    tableScanNode = buildNewTableScanNode(tableScanNode, filteredNewAssignments);
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
