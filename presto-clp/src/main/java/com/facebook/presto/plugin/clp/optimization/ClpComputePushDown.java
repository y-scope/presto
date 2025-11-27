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
import com.facebook.presto.common.block.SortOrder;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.plugin.clp.ClpColumnHandle;
import com.facebook.presto.plugin.clp.ClpMetadata;
import com.facebook.presto.plugin.clp.ClpTableHandle;
import com.facebook.presto.plugin.clp.ClpTableLayoutHandle;
import com.facebook.presto.plugin.clp.split.ClpSplitMetadataConfig;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPlanOptimizer;
import com.facebook.presto.spi.ConnectorPlanRewriter;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.Ordering;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.collect.ImmutableList;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.plugin.clp.ClpErrorCode.CLP_UNSUPPORTED_METADATA_PROJECTION;
import static com.facebook.presto.spi.ConnectorPlanRewriter.rewriteWith;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
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
        public PlanNode visitFilter(FilterNode node, RewriteContext<Void> context)
        {
            if (!(node.getSource() instanceof TableScanNode)) {
                return node;
            }

            PlanNode processedNode = processFilter(node, (TableScanNode) node.getSource());

            if (processedNode instanceof TableScanNode) {
                return context.rewrite(processedNode, null);
            }

            if (processedNode instanceof FilterNode) {
                FilterNode filterNode = (FilterNode) processedNode;
                if (filterNode.getSource() instanceof TableScanNode) {
                    PlanNode rewrittenScan = context.rewrite(filterNode.getSource(), null);
                    return new FilterNode(
                            filterNode.getSourceLocation(),
                            idAllocator.getNextId(),
                            rewrittenScan,
                            filterNode.getPredicate());
                }
            }

            return processedNode;
        }

        /**
         * Rewrites a TableScanNode to attach metadata projection information to the table layout.
         * <p>
         * Identifies metadata columns in the projection, resolves their exposed names to original
         * database column names, and embeds this information in a {@link ClpTableLayoutHandle}.
         *
         * @param node the original TableScanNode to rewrite
         * @param context
         * @return a new TableScanNode with metadata projection in the layout handle
         * @throw PrestoException with {@link ClpErrorCode#CLP_UNSUPPORTED_METADATA_PROJECTION}
         *        if a metadata column maps to an unsupported range-bound column
         */
        @Override
        public PlanNode visitTableScan(TableScanNode node, RewriteContext<Void> context)
        {
            // Retrieve projection column names
            Set<String> projectionColumns = new HashSet<>();
            for (VariableReferenceExpression variable : node.getOutputVariables()) {
                projectionColumns.add(variable.getName());
            }

            // Retrieve metadata column
            TableHandle tableHandle = node.getTable();
            ClpTableHandle clpTableHandle = (ClpTableHandle) tableHandle.getConnectorHandle();
            SchemaTableName schemaTableName = clpTableHandle.getSchemaTableName();
            Set<String> metadataColumns = metadataConfig.getMetadataColumns(schemaTableName).keySet();

            // Metadata Projection: intersection between the projection column and metadata column
            Set<String> metadataProjection = new HashSet<>();
            for (String columnName : projectionColumns) {
                if (metadataColumns.contains(columnName)) {
                    Set<String> metadataColumnsWithRangeBound =
                            metadataConfig.getMetadataColumnsWithRangeBounds(schemaTableName);
                    Map<String, String> exposedToOriginalMap =
                            metadataConfig.getExposedToOriginalMapping(schemaTableName);

                    // resolve the exposed name to the original name in the metadata database
                    // note, if the original name is a range bound mapping, we currently dont support
                    String originalColumnName = exposedToOriginalMap.get(columnName);
                    if (metadataColumnsWithRangeBound.contains(originalColumnName)) {
                        throw new PrestoException(CLP_UNSUPPORTED_METADATA_PROJECTION,
                                format("Unsupported metadata projection column: %s", columnName));
                    }
                    metadataProjection.add(originalColumnName);
                }
            }

            ClpTableLayoutHandle layoutHandle = new ClpTableLayoutHandle(
                    clpTableHandle, Optional.of(metadataProjection));
            return new TableScanNode(
                    node.getSourceLocation(),
                    idAllocator.getNextId(),
                    new TableHandle(
                            tableHandle.getConnectorId(),
                            clpTableHandle,
                            tableHandle.getTransaction(),
                            Optional.of(layoutHandle)),
                    node.getOutputVariables(),
                    node.getAssignments(),
                    node.getTableConstraints(),
                    node.getCurrentConstraint(),
                    node.getEnforcedConstraint(),
                    node.getCteMaterializationInfo());
        }

        @Override
        public PlanNode visitTopN(TopNNode node, RewriteContext<Void> context)
        {
            PlanNode rewrittenSource = context.rewrite(node.getSource(), null);

            ProjectNode project = null;
            FilterNode filter = null;
            PlanNode cursor = rewrittenSource;

            if (cursor instanceof ProjectNode) {
                project = (ProjectNode) cursor;
                cursor = project.getSource();
            }
            if (cursor instanceof FilterNode) {
                filter = (FilterNode) cursor;
                cursor = filter.getSource();
            }
            if (!(cursor instanceof TableScanNode)) {
                return node.replaceChildren(ImmutableList.of(rewrittenSource));
            }

            TableScanNode scan = (TableScanNode) cursor;
            TableHandle tableHandle = scan.getTable();
            if (!(tableHandle.getConnectorHandle() instanceof ClpTableHandle)) {
                return node.replaceChildren(ImmutableList.of(rewrittenSource));
            }

            // only allow TopN pushdown when metadata-only is true
            boolean metadataOnly = false;
            Optional<ConnectorTableLayoutHandle> layout = tableHandle.getLayout();
            Optional<String> kql = Optional.empty();
            Optional<RowExpression> metadataSql = Optional.empty();
            Optional<ClpTopNSpec> existingTopN = Optional.empty();
            ClpTableHandle clpTableHandle = null;

            if (layout.isPresent() && layout.get() instanceof ClpTableLayoutHandle) {
                ClpTableLayoutHandle cl = (ClpTableLayoutHandle) layout.get();
                metadataOnly = cl.isMetadataQueryOnly();
                kql = cl.getKqlQuery();
                metadataSql = cl.getMetadataExpression();
                existingTopN = cl.getTopN();
                clpTableHandle = cl.getTable();
            }

            if (!metadataOnly) {
                // Rule: skip TopN pushdown unless metadataQueryOnly is true
                return node.replaceChildren(ImmutableList.of(rewrittenSource));
            }

            // Ensure ORDER BY items are plain variables (allow identity through Project)
            List<Ordering> ords = node.getOrderingScheme().getOrderBy();
            if (project != null && !areIdents(project, ords)) {
                return node.replaceChildren(ImmutableList.of(rewrittenSource));
            }

            Map<VariableReferenceExpression, ColumnHandle> assignments = scan.getAssignments();
            List<ClpTopNSpec.Ordering> newOrderings = new ArrayList<>(ords.size());
            for (Ordering ord : ords) {
                VariableReferenceExpression outVar = ord.getVariable();
                Optional<String> columnNameOpt = buildOrderColumnName(project, outVar, assignments);
                if (!columnNameOpt.isPresent()) {
                    return node.replaceChildren(ImmutableList.of(rewrittenSource));
                }

                String tableScope = ClpMetadata.DEFAULT_SCHEMA_NAME;
                if (clpTableHandle != null) {
                    SchemaTableName schemaTableName = clpTableHandle.getSchemaTableName();
                    tableScope = format("%s.%s", schemaTableName.getSchemaName(), schemaTableName.getTableName());
                }

                newOrderings.add(new ClpTopNSpec.Ordering(columnNameOpt.get(), toClpOrder(ord.getSortOrder())));
            }

            if (existingTopN.isPresent()) {
                ClpTopNSpec ex = existingTopN.get();
                if (!sameOrdering(ex.getOrderings(), newOrderings)) {
                    return node.replaceChildren(ImmutableList.of(rewrittenSource)); // leave existing as-is
                }
                long mergedLimit = Math.min(ex.getLimit(), node.getCount());
                if (mergedLimit == ex.getLimit()) {
                    // No change needed; keep current layout/spec
                    return node.replaceChildren(ImmutableList.of(rewrittenSource));
                }

                // Tighten the limit on the layout
                ClpTopNSpec tightened = new ClpTopNSpec(mergedLimit, ex.getOrderings());
                ClpTableHandle clpHandle = (ClpTableHandle) tableHandle.getConnectorHandle();
                ClpTableLayoutHandle newLayout =
                        new ClpTableLayoutHandle(clpHandle, kql, metadataSql, true, Optional.empty(), Optional.of(tightened));

                TableScanNode newScan = new TableScanNode(
                        scan.getSourceLocation(),
                        idAllocator.getNextId(),
                        new TableHandle(
                                tableHandle.getConnectorId(),
                                clpHandle,
                                tableHandle.getTransaction(),
                                Optional.of(newLayout)),
                        scan.getOutputVariables(),
                        scan.getAssignments(),
                        scan.getTableConstraints(),
                        scan.getCurrentConstraint(),
                        scan.getEnforcedConstraint(),
                        scan.getCteMaterializationInfo());

                PlanNode newSource = newScan;
                if (filter != null) {
                    newSource = new FilterNode(filter.getSourceLocation(), idAllocator.getNextId(), newSource, filter.getPredicate());
                }
                if (project != null) {
                    newSource = new ProjectNode(
                            project.getSourceLocation(),
                            idAllocator.getNextId(),
                            newSource,
                            project.getAssignments(),
                            project.getLocality());
                }

                return new TopNNode(node.getSourceLocation(), idAllocator.getNextId(), newSource, node.getCount(), node.getOrderingScheme(), node.getStep());
            }

            ClpTopNSpec spec = new ClpTopNSpec(node.getCount(), newOrderings);
            ClpTableHandle clpHandle = (ClpTableHandle) tableHandle.getConnectorHandle();
            ClpTableLayoutHandle newLayout =
                    new ClpTableLayoutHandle(clpHandle, kql, metadataSql, true, Optional.empty(), Optional.of(spec));

            TableScanNode newScanNode = new TableScanNode(
                    scan.getSourceLocation(),
                    idAllocator.getNextId(),
                    new TableHandle(
                            tableHandle.getConnectorId(),
                            clpHandle,
                            tableHandle.getTransaction(),
                            Optional.of(newLayout)),
                    scan.getOutputVariables(),
                    scan.getAssignments(),
                    scan.getTableConstraints(),
                    scan.getCurrentConstraint(),
                    scan.getEnforcedConstraint(),
                    scan.getCteMaterializationInfo());

            PlanNode newSource = newScanNode;
            if (filter != null) {
                newSource = new FilterNode(filter.getSourceLocation(), idAllocator.getNextId(), newSource, filter.getPredicate());
            }
            if (project != null) {
                newSource = new ProjectNode(project.getSourceLocation(), idAllocator.getNextId(), newSource, project.getAssignments(), project.getLocality());
            }

            return new TopNNode(node.getSourceLocation(), idAllocator.getNextId(), newSource, node.getCount(), node.getOrderingScheme(), node.getStep());
        }

        private PlanNode processFilter(FilterNode filterNode, TableScanNode tableScanNode)
        {
            TableHandle tableHandle = tableScanNode.getTable();
            ClpTableHandle clpTableHandle = (ClpTableHandle) tableHandle.getConnectorHandle();

            Map<VariableReferenceExpression, ColumnHandle> assignments = tableScanNode.getAssignments();
            SchemaTableName schemaTableName = clpTableHandle.getSchemaTableName();
            Set<String> metadataColumns = metadataConfig.getMetadataColumns(schemaTableName).keySet();
            Set<String> dataColumnsWithRangeBounds = metadataConfig.getDataColumnsWithRangeBounds(schemaTableName);
            ClpExpression clpExpression = filterNode.getPredicate().accept(
                    new ClpFilterToKqlConverter(
                            functionResolution,
                            functionManager,
                            assignments,
                            metadataColumns,
                            dataColumnsWithRangeBounds),
                    null);
            Optional<String> kqlQuery = clpExpression.getPushDownExpression();
            Optional<RowExpression> metadataExpression = clpExpression.getMetadataExpression();
            Optional<RowExpression> remainingPredicate = clpExpression.getRemainingExpression();
            Set<String> pushDownVariables = clpExpression.getPushDownVariables();
            boolean allInMetadata = pushDownVariables.stream().allMatch(
                    v -> metadataColumns.contains(v) || dataColumnsWithRangeBounds.contains(v));

            if (kqlQuery.isPresent() || metadataExpression.isPresent()) {
                kqlQuery.ifPresent(s -> log.debug("KQL query: %s", s));

                ClpTableLayoutHandle layoutHandle = new ClpTableLayoutHandle(
                        clpTableHandle, kqlQuery, metadataExpression, allInMetadata, Optional.empty(), Optional.empty());
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

        private boolean sameOrdering(List<ClpTopNSpec.Ordering> a, List<ClpTopNSpec.Ordering> b)
        {
            if (a.size() != b.size()) {
                return false;
            }
            for (int i = 0; i < a.size(); i++) {
                ClpTopNSpec.Ordering x = a.get(i);
                ClpTopNSpec.Ordering y = b.get(i);
                if (!Objects.equals(x.getColumn(), y.getColumn())) {
                    return false;
                }
                if (x.getOrder() != y.getOrder()) {
                    return false;
                }
            }
            return true;
        }

        /** Accept plain var or dereference-of-var passthroughs. */
        private boolean areIdents(ProjectNode project, List<Ordering> vars)
        {
            for (Ordering ord : vars) {
                VariableReferenceExpression out = ord.getVariable();
                RowExpression expr = project.getAssignments().get(out);

                if (expr instanceof VariableReferenceExpression) {
                    continue;
                }
                if (isDereferenceChainOverVariable(expr)) {
                    continue;
                }
                return false;
            }
            return true;
        }

        /** Build final column name string for CLP (e.g., "msg.timestamp"), or empty if not pushdownable. */
        private Optional<String> buildOrderColumnName(
                ProjectNode project,
                VariableReferenceExpression outVar,
                Map<VariableReferenceExpression, ColumnHandle> assignments)
        {
            if (project == null) {
                // ORDER BY directly on scan var
                ColumnHandle ch = assignments.get(outVar);
                if (!(ch instanceof ClpColumnHandle)) {
                    return Optional.empty();
                }
                return Optional.of(((ClpColumnHandle) ch).getOriginalColumnName());
            }

            RowExpression expr = project.getAssignments().get(outVar);
            if (expr instanceof VariableReferenceExpression) {
                ColumnHandle ch = assignments.get((VariableReferenceExpression) expr);
                if (!(ch instanceof ClpColumnHandle)) {
                    return Optional.empty();
                }
                return Optional.of(((ClpColumnHandle) ch).getOriginalColumnName());
            }

            // Handle DEREFERENCE chain: baseVar.field1.field2...
            Deque<String> path = new ArrayDeque<>();
            RowExpression cur = expr;

            while (cur instanceof SpecialFormExpression
                    && ((SpecialFormExpression) cur).getForm() == SpecialFormExpression.Form.DEREFERENCE) {
                SpecialFormExpression s = (SpecialFormExpression) cur;
                RowExpression base = s.getArguments().get(0);
                RowExpression indexExpr = s.getArguments().get(1);

                if (!(indexExpr instanceof ConstantExpression) || !(base.getType() instanceof RowType)) {
                    return Optional.empty();
                }
                int idx;
                Object v = ((ConstantExpression) indexExpr).getValue();
                if (v instanceof Long) {
                    idx = toIntExact((Long) v);
                }
                else if (v instanceof Integer) {
                    idx = (Integer) v;
                }
                else {
                    return Optional.empty();
                }

                RowType rowType = (RowType) base.getType();
                if (idx < 0 || idx >= rowType.getFields().size()) {
                    return Optional.empty();
                }
                String fname = rowType.getFields().get(idx).getName().orElse(String.valueOf(idx));
                // We traverse outer->inner; collect in deque and join later
                path.addLast(fname);

                cur = base; // move up the chain
            }

            if (!(cur instanceof VariableReferenceExpression)) {
                return Optional.empty();
            }

            ColumnHandle baseCh = assignments.get((VariableReferenceExpression) cur);
            if (!(baseCh instanceof ClpColumnHandle)) {
                return Optional.empty();
            }

            String baseName = ((ClpColumnHandle) baseCh).getOriginalColumnName();
            if (path.isEmpty()) {
                return Optional.of(baseName);
            }
            return Optional.of(baseName + "." + String.join(".", path));
        }

        /** True if expr is DEREFERENCE(... DEREFERENCE(baseVar, i) ..., j) with baseVar a VariableReferenceExpression. */
        private boolean isDereferenceChainOverVariable(RowExpression expr)
        {
            RowExpression cur = expr;
            while (cur instanceof SpecialFormExpression
                    && ((SpecialFormExpression) cur).getForm() == SpecialFormExpression.Form.DEREFERENCE) {
                cur = ((SpecialFormExpression) cur).getArguments().get(0);
            }
            return (cur instanceof VariableReferenceExpression);
        }

        private ClpTopNSpec.Order toClpOrder(SortOrder so)
        {
            switch (so) {
                case ASC_NULLS_FIRST:
                case ASC_NULLS_LAST:
                    return ClpTopNSpec.Order.ASC;
                case DESC_NULLS_FIRST:
                case DESC_NULLS_LAST:
                    return ClpTopNSpec.Order.DESC;
                default: throw new IllegalArgumentException("Unknown sort order: " + so);
            }
        }
    }
}
