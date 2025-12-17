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
package com.facebook.presto.plugin.clp.split;

import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.plugin.clp.ClpConfig;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestClpPinotSplitProvider
{
    private static final JsonNodeFactory JSON_NODE_FACTORY = JsonNodeFactory.instance;

    @Mock
    private ClpConfig config;

    @Mock
    private FunctionMetadataManager functionManager;

    @Mock
    private StandardFunctionResolution functionResolution;

    @Mock
    private ClpSplitMetadataConfig metadataConfig;

    private SchemaTableName schemaTableName;
    private ClpPinotSplitProvider splitProvider;

    private AutoCloseable mocks;

    @BeforeMethod
    public void setUp()
    {
        mocks = MockitoAnnotations.openMocks(this);
        schemaTableName = new SchemaTableName("test_schema", "test_table");

        when(config.getMetadataDbUrl()).thenReturn("http://localhost:8099");

        splitProvider = new ClpPinotSplitProvider(config, functionManager, functionResolution, metadataConfig);
    }

    @AfterMethod
    public void tearDown() throws Exception
    {
        if (mocks != null) {
            mocks.close();
        }
    }

    /**
     * Verifies extractMetadataColumns converts each JSON value to the expected Presto type and maps it back to the
     * exposed column name, ignoring nulls but retaining numeric and string values with exact typing.
     */
    @Test
    public void testExtractMetadataColumnsWithAllValidTypes()
            throws Exception
    {
        Map<String, String> exposedToOriginal = new HashMap<>();
        exposedToOriginal.put("null_col", "null_col");
        exposedToOriginal.put("string_col", "orig_string");
        exposedToOriginal.put("bigint_col", "orig_bigint");
        exposedToOriginal.put("double_col1", "orig_double1");
        exposedToOriginal.put("double_col2", "orig_double2");
        exposedToOriginal.put("double_col3", "orig_double3");
        when(metadataConfig.getExposedToOriginalMapping(schemaTableName)).thenReturn(exposedToOriginal);

        Map<String, Type> metadataColumnTypes = new HashMap<>();
        metadataColumnTypes.put("null_col", VarcharType.VARCHAR);
        metadataColumnTypes.put("string_col", VarcharType.VARCHAR);
        metadataColumnTypes.put("bigint_col", BigintType.BIGINT);
        metadataColumnTypes.put("double_col1", DoubleType.DOUBLE);
        metadataColumnTypes.put("double_col2", DoubleType.DOUBLE);
        metadataColumnTypes.put("double_col3", DoubleType.DOUBLE);
        when(metadataConfig.getMetadataColumns(schemaTableName)).thenReturn(metadataColumnTypes);

        Map<String, JsonNode> row = new HashMap<>();
        row.put("null_col", JSON_NODE_FACTORY.nullNode());
        row.put("orig_string", JSON_NODE_FACTORY.textNode("test_string"));
        row.put("orig_bigint", JSON_NODE_FACTORY.numberNode(9223372036854775807L));
        row.put("orig_double1", JSON_NODE_FACTORY.numberNode(0.123456));
        row.put("orig_double2", JSON_NODE_FACTORY.numberNode(9.7e+2));
        row.put("orig_double3", JSON_NODE_FACTORY.numberNode(9.7E+2));

        List<String> metadataColumnNames = Arrays.asList(
                "null_col", "orig_string", "orig_bigint", "orig_double1", "orig_double2", "orig_double3");

        Map<String, Object> result = invokeExtractMetadataColumns(row, metadataColumnNames, schemaTableName);

        assertEquals(result.size(), 5);
        assertFalse(result.containsKey("null_col"));
        assertEquals(result.get("string_col"), "test_string");
        assertEquals(result.get("bigint_col"), 9223372036854775807L);
        assertEquals(result.get("double_col1"), 0.123456);
        assertEquals(result.get("double_col2"), 9.7e2);
        assertEquals(result.get("double_col3"), 9.7E+2);
    }

    /**
     * Ensures extractMetadataColumns throws a PrestoException when a metadata value's JSON type conflicts with the
     * expected Presto type (here, text provided for a BIGINT column).
     */
    @Test(expectedExceptions = PrestoException.class)
    public void testExtractMetadataColumnsWithInvalidType()
            throws Exception
    {
        Map<String, String> exposedToOriginal = new HashMap<>();
        exposedToOriginal.put("valid_col", "orig_valid");
        exposedToOriginal.put("invalid_col", "orig_invalid");
        when(metadataConfig.getExposedToOriginalMapping(schemaTableName)).thenReturn(exposedToOriginal);

        Map<String, Type> metadataColumnTypes = new HashMap<>();
        metadataColumnTypes.put("valid_col", VarcharType.VARCHAR);
        metadataColumnTypes.put("invalid_col", BigintType.BIGINT);
        when(metadataConfig.getMetadataColumns(schemaTableName)).thenReturn(metadataColumnTypes);

        Map<String, JsonNode> row = new HashMap<>();
        row.put("orig_valid", JSON_NODE_FACTORY.textNode("valid_string"));
        row.put("orig_invalid", JSON_NODE_FACTORY.textNode("not_a_number"));

        List<String> metadataColumnNames = Arrays.asList("orig_valid", "orig_invalid");

        invokeExtractMetadataColumns(row, metadataColumnNames, schemaTableName);
    }

    /**
     * Reflectively invokes {@code ClpPinotSplitProvider.extractMetadataColumns} on the test instance,
     * propagating any underlying {@link PrestoException} instead of wrapping it so tests can assert
     * on the original failure type.
     */
    private Map<String, Object> invokeExtractMetadataColumns(
            Map<String, JsonNode> row,
            List<String> metadataColumnNames,
            SchemaTableName schemaTableName)
            throws Exception
    {
        Method method = ClpPinotSplitProvider.class.getDeclaredMethod(
                "extractMetadataColumns", Map.class, List.class, SchemaTableName.class);
        method.setAccessible(true);
        try {
            return (Map<String, Object>) method.invoke(splitProvider, row, metadataColumnNames, schemaTableName);
        }
        catch (java.lang.reflect.InvocationTargetException e) {
            if (e.getCause() instanceof PrestoException) {
                throw (PrestoException) e.getCause();
            }
            throw e;
        }
    }

    /**
     * Test that buildSplitSelectionQuery correctly wraps metadata columns with LASTWITHTIME.
     * Each metadata column should be aggregated using LASTWITHTIME to get the latest value.
     */
    @Test
    public void testBuildSplitSelectionQueryWithMetadataColumns()
    {
        List<String> metadataColumns = new ArrayList<>();
        metadataColumns.add("hostname");
        metadataColumns.add("creationtime");

        String query = splitProvider.buildSplitSelectionQuery(
                "test_table", metadataColumns, "creationtime > 1000");

        assertTrue(query.contains("tpath"));
        assertTrue(query.contains("LASTWITHTIME(hostname, lastmodifiedtime, 'long') AS hostname"));
        assertTrue(query.contains("LASTWITHTIME(creationtime, lastmodifiedtime, 'long') AS creationtime"));
        assertTrue(query.contains("GROUP BY tpath"));
        assertTrue(query.contains("test_table"));
        assertTrue(query.contains("creationtime > 1000"));
    }

    /**
     * Test that buildSplitSelectionQueryWithTopN correctly wraps TopN columns with LASTWITHTIME.
     * The TopN query should include creationtime, lastmodifiedtime, and num_messages columns
     * aggregated using LASTWITHTIME for deduplication.
     */
    @Test
    public void testBuildSplitSelectionQueryWithTopN()
    {
        String query = splitProvider.buildSplitSelectionQueryWithTopN(
                "test_table", "creationtime > 1000");

        assertTrue(query.contains("tpath"));
        assertTrue(query.contains("LASTWITHTIME(creationtime, lastmodifiedtime, 'long') AS creationtime"));
        assertTrue(query.contains("LASTWITHTIME(lastmodifiedtime, lastmodifiedtime, 'long') AS lastmodifiedtime"));
        assertTrue(query.contains("LASTWITHTIME(num_messages, lastmodifiedtime, 'long') AS num_messages"));
        assertTrue(query.contains("GROUP BY tpath"));
        assertTrue(query.contains("test_table"));
        assertTrue(query.contains("creationtime > 1000"));
    }
}
