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

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.plugin.clp.ClpColumnHandle;
import com.facebook.presto.plugin.clp.ClpConfig;
import com.facebook.presto.plugin.clp.ClpTableHandle;
import com.facebook.presto.plugin.clp.ClpTableLayoutHandle;
import com.facebook.presto.plugin.clp.ClpTransactionHandle;
import com.facebook.presto.plugin.clp.TestClpQueryBase;
import com.facebook.presto.plugin.clp.split.ClpSplitMetadataConfig;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
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
import static com.facebook.presto.plugin.clp.ClpErrorCode.CLP_UNSUPPORTED_METADATA_PROJECTION;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

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
        optimizer = new ClpComputePushDown(functionAndTypeManager, functionResolution, metadataConfig);
        idAllocator = new PlanNodeIdAllocator();
        variableAllocator = new VariableAllocator();
        schemaTableName = new SchemaTableName("default", "table_1");
        clpTableHandle = new ClpTableHandle(schemaTableName, "test");
        connectorId = new ConnectorId("clp");
    }

    /**
     * Validates that the optimizer resolves metadata projections given a new layout
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
        assertEquals(layout.getSplitMetadataColumnNames(), expectedMetadataProjection);
    }

    /**
     * Validates that the optimizer resolves metadata projections given an existing layout and preserves pre-existing
     * layout fields such as a kqlQuery.
     */
    @Test
    public void testMetadataProjectionWithExistingLayout()
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

        ClpColumnHandle fileNameHandle =
                new ClpColumnHandle("hostname", "hostname", hostnameString);
        ClpColumnHandle scoreHandle =
                new ClpColumnHandle("score", "score", scoreDouble);
        ClpColumnHandle statusCodeHandle =
                new ClpColumnHandle("status_code", "status_code", statusCodeInt);
        ClpColumnHandle fareHandle = new ClpColumnHandle("fare", DOUBLE);

        ClpTableLayoutHandle existingLayout = new ClpTableLayoutHandle(
                clpTableHandle, Optional.of("existing-kql"), Optional.empty());

        TableHandle tableHandle = new TableHandle(
                connectorId,
                clpTableHandle,
                ClpTransactionHandle.INSTANCE,
                Optional.of(existingLayout));

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

        assertTrue(rewrittenScan.getTable().getLayout().isPresent(), "Layout should remain present after rewrite");
        ClpTableLayoutHandle layout = (ClpTableLayoutHandle) rewrittenScan.getTable().getLayout().get();
        assertEquals(layout.getKqlQuery(), Optional.of("existing-kql"), "Existing layout fields should be preserved");
        assertTrue(layout == existingLayout, "Optimizer should reuse and mutate the existing layout");

        Map<String, String> exposedToOriginalMap = metadataConfig.getExposedToOriginalMapping(schemaTableName);
        Set<String> expectedMetadataProjection = ImmutableSet.of(
                exposedToOriginalMap.get("hostname"),
                exposedToOriginalMap.get("score"),
                exposedToOriginalMap.get("status_code"));
        assertEquals(layout.getSplitMetadataColumnNames(), expectedMetadataProjection);
    }

    /**
     * Validates that attempting to project a metadata column backed by a range-bound field raises a
     * CLP_UNSUPPORTED_METADATA_PROJECTION PrestoException with the offending column name in the message.
     */
    @Test
    public void testMetadataProjectionWithErrorHandling()
    {
        // Get a metadata column that has range bounds (should trigger the exception)
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

        // Verify that PrestoException is thrown with the correct error code and message
        final String columnName = rangeBoundExposedColumn;
        try {
            optimizer.optimize(
                    originalScan,
                    new SessionHolder().getConnectorSession(),
                    variableAllocator,
                    idAllocator);
            fail("Expected PrestoException to be thrown");
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), CLP_UNSUPPORTED_METADATA_PROJECTION.toErrorCode());
            assertTrue(e.getMessage().contains(columnName),
                    "Exception message should contain the column name: " + columnName);
        }
    }
}
