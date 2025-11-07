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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.plugin.clp.ClpTableHandle;
import com.facebook.presto.plugin.clp.ClpTableLayoutHandle;
import com.facebook.presto.plugin.clp.split.ClpSplitMetadataConfig;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPlanOptimizer;
import com.facebook.presto.spi.ConnectorPlanRewriter;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.collect.ImmutableSet;

import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.plugin.clp.ClpErrorCode.CLP_MANDATORY_COLUMN_NOT_IN_FILTER;
import static com.facebook.presto.spi.ConnectorPlanRewriter.rewriteWith;
import static java.util.Objects.requireNonNull;

public class ClpComputePushDown
        implements ConnectorPlanOptimizer
{
    private static final Logger log = Logger.get(ClpComputePushDown.class);
    private final FunctionMetadataManager functionManager;
    private final StandardFunctionResolution functionResolution;
    private final ClpSplitMetadataConfig metadataConfig;

    public ClpComputePushDown(
            FunctionMetadataManager functionManager,
            StandardFunctionResolution functionResolution,
            ClpSplitMetadataConfig metadataConfig)
    {
        this.functionManager = requireNonNull(functionManager, "functionManager is null");
        this.functionResolution = requireNonNull(functionResolution, "functionResolution is null");
        this.metadataConfig = requireNonNull(metadataConfig, "metadataConfig is null");
    }

    @Override
    public PlanNode optimize(PlanNode maxSubplan, ConnectorSession session, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator)
    {
        Rewriter rewriter = new Rewriter(idAllocator);
        return rewriteWith(rewriter, maxSubplan);
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
        public PlanNode visitTableScan(TableScanNode node, RewriteContext<Void> context)
        {
            TableHandle tableHandle = node.getTable();
            ClpTableHandle clpTableHandle = (ClpTableHandle) tableHandle.getConnectorHandle();
            if (!metadataConfig.getRequiredColumns(clpTableHandle.getSchemaTableName()).isEmpty()) {
                throw new PrestoException(CLP_MANDATORY_COLUMN_NOT_IN_FILTER, "required filters must be specified");
            }

            return super.visitTableScan(node, context);
        }

        @Override
        public PlanNode visitFilter(FilterNode node, RewriteContext<Void> context)
        {
            if (!(node.getSource() instanceof TableScanNode)) {
                return node;
            }

            return processFilter(node, (TableScanNode) node.getSource());
        }

        private PlanNode processFilter(FilterNode filterNode, TableScanNode tableScanNode)
        {
            TableHandle tableHandle = tableScanNode.getTable();
            ClpTableHandle clpTableHandle = (ClpTableHandle) tableHandle.getConnectorHandle();

            Map<VariableReferenceExpression, ColumnHandle> assignments = tableScanNode.getAssignments();
            SchemaTableName schemaTableName = clpTableHandle.getSchemaTableName();
            ImmutableSet.Builder<String> metadataFilterColumns = ImmutableSet.builder();
            metadataFilterColumns.addAll(metadataConfig.getMetadataColumns(schemaTableName).keySet());
            metadataFilterColumns.addAll(metadataConfig.getDataColumnsWithRangeBounds(schemaTableName));
            ClpExpression clpExpression = filterNode.getPredicate().accept(
                    new ClpFilterToKqlConverter(
                            functionResolution,
                            functionManager,
                            assignments,
                            metadataFilterColumns.build()),
                    null);
            Optional<String> kqlQuery = clpExpression.getPushDownExpression();
            Optional<RowExpression> metadataExpression = clpExpression.getMetadataExpression();
            Optional<RowExpression> remainingPredicate = clpExpression.getRemainingExpression();

            if (kqlQuery.isPresent() || metadataExpression.isPresent()) {
                if (kqlQuery.isPresent()) {
                    log.debug("KQL query: %s", kqlQuery.get());
                }

                ClpTableLayoutHandle layoutHandle = new ClpTableLayoutHandle(clpTableHandle, kqlQuery, metadataExpression);
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
