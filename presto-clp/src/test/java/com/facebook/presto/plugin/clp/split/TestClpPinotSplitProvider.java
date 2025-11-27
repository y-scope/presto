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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

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

    @BeforeMethod
    public void setUp()
    {
        MockitoAnnotations.openMocks(this);
        schemaTableName = new SchemaTableName("test_schema", "test_table");

        when(config.getMetadataDbUrl()).thenReturn("http://localhost:8099");

        splitProvider = new ClpPinotSplitProvider(config, functionManager, functionResolution, metadataConfig);
    }

    @Test
    public void testExtractMetadataColumnsWithAllValidTypes()
            throws Exception
    {
        Map<String, String> exposedToOriginal = new HashMap<>();
        exposedToOriginal.put("null_col", "null_col");
        exposedToOriginal.put("string_col", "orig_string");
        exposedToOriginal.put("bigint_col", "orig_bigint");
        exposedToOriginal.put("double_col", "orig_double");
        when(metadataConfig.getExposedToOriginalMapping(schemaTableName)).thenReturn(exposedToOriginal);

        Map<String, Type> metadataColumnTypes = new HashMap<>();
        metadataColumnTypes.put("null_col", VarcharType.VARCHAR);
        metadataColumnTypes.put("string_col", VarcharType.VARCHAR);
        metadataColumnTypes.put("bigint_col", BigintType.BIGINT);
        metadataColumnTypes.put("double_col", DoubleType.DOUBLE);
        when(metadataConfig.getMetadataColumns(schemaTableName)).thenReturn(metadataColumnTypes);

        ArrayNode row = JSON_NODE_FACTORY.arrayNode();
        row.add("/path/to/split.clp");
        row.addNull();
        row.add("test_string");
        row.add(9223372036854775807L);
        row.add(3.14159);

        List<String> metadataColumnNames = Arrays.asList("orig_null", "orig_string", "orig_bigint", "orig_double");

        Map<String, Object> result = invokeExtractMetadataColumns(row, metadataColumnNames, schemaTableName);

        assertEquals(result.size(), 3);
        assertFalse(result.containsKey("null_col"));
        assertEquals(result.get("string_col"), "test_string");
        assertEquals(result.get("bigint_col"), 9223372036854775807L);
        assertEquals(result.get("double_col"), 3.14159);
    }

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

        ArrayNode row = JSON_NODE_FACTORY.arrayNode();
        row.add("/path/to/split.clp");
        row.add("valid_string");
        row.add("not_a_number");

        List<String> metadataColumnNames = Arrays.asList("orig_valid", "orig_invalid");

        invokeExtractMetadataColumns(row, metadataColumnNames, schemaTableName);
    }

    private Map<String, Object> invokeExtractMetadataColumns(
            JsonNode row,
            List<String> metadataColumnNames,
            SchemaTableName schemaTableName)
            throws Exception
    {
        Method method = ClpPinotSplitProvider.class.getDeclaredMethod(
                "extractMetadataColumns", JsonNode.class, List.class, SchemaTableName.class);
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
}
