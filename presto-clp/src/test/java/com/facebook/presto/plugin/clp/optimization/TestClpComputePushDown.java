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
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.common.type.DoubleType.DOUBLE;
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

    @Test
    public void testMetadataProjectionWithDataProjection()
    {
        Map<String, Type> metadataColumns = metadataConfig.getMetadataColumns(schemaTableName);
        Type fileNameString = metadataColumns.get("file_name");
        Type scoreDouble = metadataColumns.get("score");
        Type statusCodeInt = metadataColumns.get("status_code");

        VariableReferenceExpression fileName = new VariableReferenceExpression(
                Optional.empty(), "file_name", fileNameString);
        VariableReferenceExpression score = new VariableReferenceExpression(
                Optional.empty(), "score", scoreDouble);
        VariableReferenceExpression statusCode = new VariableReferenceExpression(
                Optional.empty(), "status_code", statusCodeInt);
        VariableReferenceExpression fare = new VariableReferenceExpression(
                Optional.empty(), "fare", DOUBLE);

        // building projection columns that are pushed down to TableScanNode
        // metadata projection
        ClpColumnHandle fileNameHandle =
                new ClpColumnHandle("file_name", "file_name", fileNameString);
        ClpColumnHandle scoreHandle =
                new ClpColumnHandle("score", "score", scoreDouble);
        ClpColumnHandle statusCodeHandle =
                new ClpColumnHandle("status_code", "status_code", statusCodeInt);
        // data projection
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
                ImmutableList.of(fileName, score, statusCode, fare),
                ImmutableMap.<VariableReferenceExpression, ColumnHandle>builder()
                        .put(fileName, fileNameHandle)
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
        assertTrue(layout.getSplitMetaColumnNames().isPresent(), "Metadata projection should be present");
        assertEquals(
                layout.getSplitMetaColumnNames().get(),
                ImmutableMap.of(
                        "file_name", fileNameString,
                        "score", scoreDouble,
                        "status_code", statusCodeInt));

        // The original scan had no layout
        assertFalse(originalScan.getTable().getLayout().isPresent());
    }
}
