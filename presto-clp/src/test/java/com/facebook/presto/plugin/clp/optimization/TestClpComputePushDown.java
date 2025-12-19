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

import com.facebook.presto.common.block.SortOrder;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.plugin.clp.ClpColumnHandle;
import com.facebook.presto.plugin.clp.ClpConfig;
import com.facebook.presto.plugin.clp.ClpMetadata;
import com.facebook.presto.plugin.clp.ClpTableHandle;
import com.facebook.presto.plugin.clp.ClpTableLayoutHandle;
import com.facebook.presto.plugin.clp.ClpTransactionHandle;
import com.facebook.presto.plugin.clp.TestClpQueryBase;
import com.facebook.presto.plugin.clp.split.ClpSplitMetadataConfig;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.Ordering;
import com.facebook.presto.spi.plan.OrderingScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestClpComputePushDown
        extends TestClpQueryBase
{
    private ClpSplitMetadataConfig metadataConfig;
    private ClpComputePushDown optimizer;
    private PlanNodeIdAllocator idAllocator;
    private VariableAllocator variableAllocator;
    private FunctionAndTypeManager functionAndTypeManager;
    private StandardFunctionResolution functionResolution;
    private SchemaTableName schemaTableName;
    private ClpTableHandle clpTableHandle;
    private ConnectorId connectorId;
    private ClpMetadata mockClpMetadata;

    @BeforeMethod
    public void setUp()
            throws URISyntaxException
    {
        URL resource = getClass().getClassLoader().getResource("test-pinot-split-metadata.json");
        if (resource == null) {
            throw new IllegalStateException("test-pinot-split-metadata.json not found");
        }

        ClpConfig config = new ClpConfig()
                .setSplitMetadataConfigPath(Paths.get(resource.toURI()).toAbsolutePath().toString());

        // Use the shared function/type manager from the base class.
        functionAndTypeManager = TestClpQueryBase.functionAndTypeManager;
        functionResolution = new FunctionResolution(functionAndTypeManager.getFunctionAndTypeResolver());

        metadataConfig = new ClpSplitMetadataConfig(config, functionAndTypeManager);

        mockClpMetadata = mock(ClpMetadata.class);
        when(mockClpMetadata.getColumnHandles(any(), any()))
                .thenReturn(ImmutableMap.of(
                        TestClpQueryBase.city.getColumnName(), TestClpQueryBase.city,
                        TestClpQueryBase.fare.getColumnName(), TestClpQueryBase.fare,
                        TestClpQueryBase.isHoliday.getColumnName(), TestClpQueryBase.isHoliday));

        optimizer = new ClpComputePushDown(functionAndTypeManager, functionResolution, metadataConfig, mockClpMetadata);
        idAllocator = new PlanNodeIdAllocator();
        variableAllocator = new VariableAllocator();
        schemaTableName = new SchemaTableName("default", "table_1");
        clpTableHandle = new ClpTableHandle(schemaTableName, "test");
        connectorId = new ConnectorId("clp");
    }

    /**
     * Validates that the optimizer resolves metadata projections
     */
    @Test
    public void testMetadataProjectionWithDataProjection()
    {
        Map<String, Type> metadataColumns = metadataConfig.getMetadataColumns(schemaTableName);
        Type hostnameString = metadataColumns.get("hostname");
        Type scoreDouble = metadataColumns.get("score");
        Type statusCodeInt = metadataColumns.get("status_code");

        VariableReferenceExpression hostname = new VariableReferenceExpression(
                Optional.empty(), "hostname", hostnameString);
        VariableReferenceExpression score = new VariableReferenceExpression(
                Optional.empty(), "score", scoreDouble);
        VariableReferenceExpression statusCode = new VariableReferenceExpression(
                Optional.empty(), "status_code", statusCodeInt);
        VariableReferenceExpression fare = new VariableReferenceExpression(
                Optional.empty(), "fare", DOUBLE);

        // building projection columns that are pushed down to TableScanNode
        // metadata projections:
        ClpColumnHandle fileNameHandle =
                new ClpColumnHandle("hostname", "hostname", hostnameString);
        ClpColumnHandle scoreHandle =
                new ClpColumnHandle("score", "score", scoreDouble);
        ClpColumnHandle statusCodeHandle =
                new ClpColumnHandle("status_code", "status_code", statusCodeInt);
        // data projection:
        ClpColumnHandle fareHandle = new ClpColumnHandle("fare", DOUBLE);

        TableHandle tableHandle = new TableHandle(
                connectorId,
                clpTableHandle,
                ClpTransactionHandle.INSTANCE,
                Optional.empty());

        TableScanNode originalScan = new TableScanNode(
                Optional.empty(),
                idAllocator.getNextId(),
                tableHandle,
                ImmutableList.of(hostname, score, statusCode, fare),
                ImmutableMap.<VariableReferenceExpression, ColumnHandle>builder()
                        .put(hostname, fileNameHandle)
                        .put(score, scoreHandle)
                        .put(statusCode, statusCodeHandle)
                        .put(fare, fareHandle)
                        .build(),
                ImmutableList.of(),
                TupleDomain.all(),
                TupleDomain.all(),
                Optional.empty());

        PlanNode optimized = optimizer.optimize(
                originalScan, new SessionHolder().getConnectorSession(), variableAllocator, idAllocator);
        TableScanNode rewrittenScan = (TableScanNode) optimized;

        assertNotEquals(rewrittenScan.getId(), originalScan.getId(), "TableScan id should change after rewrite");
        assertTrue(rewrittenScan.getTable().getLayout().isPresent(), "Layout should be set on rewritten scan");

        ClpTableLayoutHandle layout = (ClpTableLayoutHandle) rewrittenScan.getTable().getLayout().get();
        Map<String, String> exposedToOriginalMap = metadataConfig.getExposedToOriginalMapping(schemaTableName);
        Set<String> expectedMetadataProjection = ImmutableSet.of(
                exposedToOriginalMap.get("hostname"),
                exposedToOriginalMap.get("score"),
                exposedToOriginalMap.get("status_code"));
        assertEquals(layout.getOrInitializeSplitMetadataColumnNames(), expectedMetadataProjection);
    }

    /**
     * Validates that projecting a metadata column backed by a range-bound field is handled gracefully
     * by skipping it in the metadata projection (returning NULL for that column).
     */
    @Test
    public void testMetadataProjectionWithRangeBoundColumn()
    {
        // Get a metadata column that has range bounds (should be skipped in metadata projection)
        Set<String> metadataColumnsWithRangeBound =
                metadataConfig.getMetadataColumnsWithRangeBounds(schemaTableName);
        Map<String, String> exposedToOriginalMap =
                metadataConfig.getExposedToOriginalMapping(schemaTableName);

        // Find an exposed column name that maps to a range-bound original column
        String rangeBoundExposedColumn = null;
        Type rangeBoundColumnType = null;
        for (Map.Entry<String, String> entry : exposedToOriginalMap.entrySet()) {
            if (metadataColumnsWithRangeBound.contains(entry.getValue())) {
                rangeBoundExposedColumn = entry.getKey();
                rangeBoundColumnType = metadataConfig.getMetadataColumns(schemaTableName)
                        .get(rangeBoundExposedColumn);
                break;
            }
        }

        VariableReferenceExpression rangeBoundVar = new VariableReferenceExpression(
                Optional.empty(), rangeBoundExposedColumn, rangeBoundColumnType);

        ClpColumnHandle rangeBoundHandle = new ClpColumnHandle(
                rangeBoundExposedColumn, rangeBoundExposedColumn, rangeBoundColumnType);

        TableHandle tableHandle = new TableHandle(
                connectorId,
                clpTableHandle,
                ClpTransactionHandle.INSTANCE,
                Optional.empty());

        TableScanNode originalScan = new TableScanNode(
                Optional.empty(),
                idAllocator.getNextId(),
                tableHandle,
                ImmutableList.of(rangeBoundVar),
                ImmutableMap.<VariableReferenceExpression, ColumnHandle>builder()
                        .put(rangeBoundVar, rangeBoundHandle)
                        .build(),
                ImmutableList.of(),
                TupleDomain.all(),
                TupleDomain.all(),
                Optional.empty());

        PlanNode optimized = optimizer.optimize(
                originalScan,
                new SessionHolder().getConnectorSession(),
                variableAllocator,
                idAllocator);

        TableScanNode rewrittenScan = (TableScanNode) optimized;

        // Verify that the layout is NOT present (since the only metadata column was range-bound and skipped)
        assertFalse(rewrittenScan.getTable().getLayout().isPresent(),
                "Layout should not be present when only range-bound columns are projected");
    }

    /**
     * Validates that metadata projection (splitMetadataColumnNames) is preserved
     * after the plan passes through visitFilter and visitTopN.
     *
     * Plan structure: TopN -> Filter -> TableScan
     * Expected: The optimizer should:
     * 1. Visit TableScan first (DFS), adding splitMetadataColumnNames to the layout
     * 2. Visit Filter, preserving splitMetadataColumnNames when creating new layout
     * 3. Visit TopN, preserving splitMetadataColumnNames when creating new layout
     */
    @Test
    public void testMetadataProjectionPreservedThroughFilterAndTopN()
    {
        // Setup: Use metadata columns that will trigger metadataQueryOnly=true
        Map<String, Type> metadataColumns = metadataConfig.getMetadataColumns(schemaTableName);
        Type hostnameType = metadataColumns.get("hostname");
        Type scoreType = metadataColumns.get("score");

        VariableReferenceExpression hostname = new VariableReferenceExpression(
                Optional.empty(), "hostname", hostnameType);
        VariableReferenceExpression score = new VariableReferenceExpression(
                Optional.empty(), "score", scoreType);

        ClpColumnHandle hostnameHandle = new ClpColumnHandle("hostname", "hostname", hostnameType);
        ClpColumnHandle scoreHandle = new ClpColumnHandle("score", "score", scoreType);

        // Create TableScanNode with metadata columns
        TableHandle tableHandle = new TableHandle(
                connectorId,
                clpTableHandle,
                ClpTransactionHandle.INSTANCE,
                Optional.empty());

        TableScanNode tableScan = new TableScanNode(
                Optional.empty(),
                idAllocator.getNextId(),
                tableHandle,
                ImmutableList.of(hostname, score),
                ImmutableMap.<VariableReferenceExpression, ColumnHandle>builder()
                        .put(hostname, hostnameHandle)
                        .put(score, scoreHandle)
                        .build(),
                ImmutableList.of(),
                TupleDomain.all(),
                TupleDomain.all(),
                Optional.empty());

        // Create FilterNode on top of TableScan
        // Use a filter on a metadata column: score > 0.5 -> trigger split pruning, append metadata sql in
        // ClpTableLayoutHandle
        SessionHolder sessionHolder = new SessionHolder();
        TypeProvider localTypeProvider = TypeProvider.fromVariables(ImmutableList.of(hostname, score));
        RowExpression filterPredicate = getRowExpression("score > 0.5", localTypeProvider, sessionHolder);

        FilterNode filterNode = new FilterNode(
                Optional.empty(),
                idAllocator.getNextId(),
                tableScan,
                filterPredicate);

        // Create TopNNode on top of FilterNode
        // ORDER BY hostname LIMIT 10
        OrderingScheme orderingScheme = new OrderingScheme(
                ImmutableList.of(new Ordering(hostname, SortOrder.ASC_NULLS_LAST)));

        TopNNode topNNode = new TopNNode(
                Optional.empty(),
                idAllocator.getNextId(),
                filterNode,
                10,
                orderingScheme,
                TopNNode.Step.SINGLE);

        // Run the optimizer
        PlanNode optimized = optimizer.optimize(
                topNNode,
                sessionHolder.getConnectorSession(),
                variableAllocator,
                idAllocator);

        // Navigate to the TableScanNode in the optimized plan
        // Structure should be: TopN -> Filter -> TableScan (or TopN -> TableScan if filter was fully pushed)
        assertTrue(optimized instanceof TopNNode, "Root should be TopNNode");
        TopNNode optimizedTopN = (TopNNode) optimized;

        PlanNode source = optimizedTopN.getSource();
        TableScanNode optimizedScan;
        if (source instanceof FilterNode) {
            FilterNode optimizedFilter = (FilterNode) source;
            assertTrue(optimizedFilter.getSource() instanceof TableScanNode,
                    "Filter source should be TableScanNode");
            optimizedScan = (TableScanNode) optimizedFilter.getSource();
        }
        else {
            assertTrue(source instanceof TableScanNode,
                    "TopN source should be either FilterNode or TableScanNode");
            optimizedScan = (TableScanNode) source;
        }

        // Verify that the layout is present and contains splitMetadataColumnNames
        assertTrue(optimizedScan.getTable().getLayout().isPresent(),
                "Layout should be present on optimized TableScan");

        ClpTableLayoutHandle layout = (ClpTableLayoutHandle) optimizedScan.getTable().getLayout().get();

        // Verify splitMetadataColumnNames is preserved
        assertTrue(layout.getSplitMetadataColumnNames().isPresent(),
                "splitMetadataColumnNames should be present in the layout");

        Map<String, String> exposedToOriginalMap = metadataConfig.getExposedToOriginalMapping(schemaTableName);
        Set<String> expectedMetadataProjection = ImmutableSet.of(
                exposedToOriginalMap.get("hostname"),
                exposedToOriginalMap.get("score"));

        assertEquals(layout.getSplitMetadataColumnNames().get(), expectedMetadataProjection,
                "splitMetadataColumnNames should contain the projected metadata columns");

        // Verify that filter was pushed down (metadataExpression should be present)
        assertTrue(layout.getMetadataExpression() != null || layout.getKqlQuery().isPresent(),
                "Filter should be pushed down to the layout");

        // Verify metadataQueryOnly is true (all columns are metadata columns)
        assertTrue(layout.isMetadataQueryOnly(),
                "metadataQueryOnly should be true since all projected columns are metadata columns");
    }
}
