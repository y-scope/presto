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

import com.facebook.presto.plugin.clp.ClpConfig;
import com.facebook.presto.plugin.clp.ClpTableHandle;
import com.facebook.presto.plugin.clp.TestClpQueryBase;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

/**
 * Unit tests for ClpUberPinotSplitProvider.
 * Tests Uber-specific customizations including Neutrino endpoint URL construction
 * and RTA table name prefixing.
 */
@Test(singleThreaded = true)
public class TestClpUberPinotSplitProvider
        extends TestClpQueryBase
{
    private ClpUberPinotSplitProvider splitProvider;
    private ClpConfig config;

    @BeforeMethod
    public void setUp()
    {
        config = new ClpConfig();
        config.setMetadataDbUrl("https://neutrino.uber.com");
        config.setSplitProviderType(ClpConfig.SplitProviderType.PINOT_UBER);
        splitProvider = new ClpUberPinotSplitProvider(
                config,
                functionAndTypeManager,
                standardFunctionResolution,
                new ClpSplitMetadataConfig(config, functionAndTypeManager));
    }

    /**
     * Test that the Neutrino endpoint URL is correctly constructed.
     */
    @Test
    public void testBuildPinotSqlQueryEndpointUrl() throws Exception
    {
        // Use reflection to access the protected method
        Method method = ClpUberPinotSplitProvider.class.getDeclaredMethod("buildPinotSqlQueryEndpointUrl", ClpConfig.class);
        method.setAccessible(true);

        URL result = (URL) method.invoke(splitProvider, config);

        assertNotNull(result);
        assertEquals(result.toString(), "https://neutrino.uber.com/v1/globalStatement");
        assertEquals(result.getProtocol(), "https");
        assertEquals(result.getHost(), "neutrino.uber.com");
        assertEquals(result.getPath(), "/v1/globalStatement");
    }

    /**
     * Test URL construction with different base URLs.
     */
    @Test
    public void testBuildPinotSqlQueryEndpointUrlVariations() throws Exception
    {
        Method method = ClpUberPinotSplitProvider.class.getDeclaredMethod("buildPinotSqlQueryEndpointUrl", ClpConfig.class);
        method.setAccessible(true);

        // Test with trailing slash
        config.setMetadataDbUrl("https://neutrino.uber.com/");
        URL result = (URL) method.invoke(splitProvider, config);
        assertEquals(result.toString(), "https://neutrino.uber.com//v1/globalStatement");

        // Test without protocol (should work as URL constructor handles it)
        config.setMetadataDbUrl("http://neutrino-dev.uber.com");
        result = (URL) method.invoke(splitProvider, config);
        assertEquals(result.toString(), "http://neutrino-dev.uber.com/v1/globalStatement");

        // Test with port
        config.setMetadataDbUrl("https://neutrino.uber.com:8080");
        result = (URL) method.invoke(splitProvider, config);
        assertEquals(result.toString(), "https://neutrino.uber.com:8080/v1/globalStatement");
    }

    /**
     * Test that invalid URLs throw MalformedURLException.
     */
    @Test
    public void testBuildPinotSqlQueryEndpointUrlInvalid() throws Exception
    {
        Method method = ClpUberPinotSplitProvider.class.getDeclaredMethod("buildPinotSqlQueryEndpointUrl", ClpConfig.class);
        method.setAccessible(true);

        config.setMetadataDbUrl("not a valid url");
        try {
            method.invoke(splitProvider, config);
            fail("Expected MalformedURLException");
        }
        catch (Exception e) {
            assertTrue(e.getCause() instanceof MalformedURLException);
        }
    }

    /**
     * Test that table names are correctly prefixed with "rta.logging."
     */
    @Test
    public void testInferMetadataTableName()
    {
        SchemaTableName schemaTableName = new SchemaTableName("default", "logs");
        ClpTableHandle tableHandle = new ClpTableHandle(schemaTableName, "test");

        String result = splitProvider.inferMetadataTableName(tableHandle);

        assertEquals(result, "rta.logging.logs");
    }

    /**
     * Test table name inference with different schemas.
     * Verifies that schema name doesn't affect the output (flat namespace).
     */
    @Test
    public void testInferMetadataTableNameDifferentSchemas()
    {
        // Test with default schema
        SchemaTableName schemaTableName1 = new SchemaTableName("default", "events");
        ClpTableHandle tableHandle1 = new ClpTableHandle(schemaTableName1, "test");
        assertEquals(splitProvider.inferMetadataTableName(tableHandle1), "rta.logging.events");

        // Test with production schema - should produce same result
        SchemaTableName schemaTableName2 = new SchemaTableName("production", "events");
        ClpTableHandle tableHandle2 = new ClpTableHandle(schemaTableName2, "test");
        assertEquals(splitProvider.inferMetadataTableName(tableHandle2), "rta.logging.events");

        // Test with staging schema
        SchemaTableName schemaTableName3 = new SchemaTableName("staging", "metrics");
        ClpTableHandle tableHandle3 = new ClpTableHandle(schemaTableName3, "test");
        assertEquals(splitProvider.inferMetadataTableName(tableHandle3), "rta.logging.metrics");
    }

    /**
     * Test table name inference with special characters.
     */
    @Test
    public void testInferMetadataTableNameSpecialCharacters()
    {
        // Test with underscore
        SchemaTableName schemaTableName1 = new SchemaTableName("default", "user_logs");
        ClpTableHandle tableHandle1 = new ClpTableHandle(schemaTableName1, "test");
        assertEquals(splitProvider.inferMetadataTableName(tableHandle1), "rta.logging.user_logs");

        // Test with hyphen
        SchemaTableName schemaTableName2 = new SchemaTableName("default", "app-logs");
        ClpTableHandle tableHandle2 = new ClpTableHandle(schemaTableName2, "test");
        assertEquals(splitProvider.inferMetadataTableName(tableHandle2), "rta.logging.app-logs");

        // Test with numbers
        SchemaTableName schemaTableName3 = new SchemaTableName("default", "logs2024");
        ClpTableHandle tableHandle3 = new ClpTableHandle(schemaTableName3, "test");
        assertEquals(splitProvider.inferMetadataTableName(tableHandle3), "rta.logging.logs2024");
    }

    /**
     * Test that null table handle throws NullPointerException.
     */
    @Test(expectedExceptions = NullPointerException.class,
            expectedExceptionsMessageRegExp = "tableHandle is null")
    public void testInferMetadataTableNameNull()
    {
        splitProvider.inferMetadataTableName(null);
    }

    /**
     * Test the factory method for building Uber table names.
     */
    @Test
    public void testBuildUberTableName()
    {
        assertEquals(splitProvider.buildUberTableName("logs"), "rta.logging.logs");
        assertEquals(splitProvider.buildUberTableName("events"), "rta.logging.events");
        assertEquals(splitProvider.buildUberTableName("metrics"), "rta.logging.metrics");
        assertEquals(splitProvider.buildUberTableName("user_activity"), "rta.logging.user_activity");
        assertEquals(splitProvider.buildUberTableName("app-logs"), "rta.logging.app-logs");
    }

    /**
     * Test that the split provider is correctly instantiated with configuration.
     */
    @Test
    public void testConstructor()
    {
        assertNotNull(splitProvider);

        // Verify it's an instance of the parent class
        assertTrue(splitProvider instanceof ClpPinotSplitProvider);
        assertTrue(splitProvider instanceof ClpSplitProvider);
    }

    /**
     * Test SQL query building methods inherited from parent.
     */
    @Test
    public void testInheritedSqlQueryMethods()
    {
        // Test buildSplitSelectionQuery (inherited from parent)
        String query = splitProvider.buildSplitSelectionQuery(
                "rta.logging.logs", new ArrayList<>(), "status = 200");
        assertTrue(query.contains("rta.logging.logs"));
        assertTrue(query.contains("status = 200"));
        assertTrue(query.contains("SELECT"));
        assertTrue(query.contains("tpath"));

        // Test buildSplitSelectionQueryWithTopN (inherited from parent)
        String metaQuery = splitProvider.buildSplitSelectionQueryWithTopN("rta.logging.events", "timestamp > 1000");
        assertTrue(metaQuery.contains("rta.logging.events"));
        assertTrue(metaQuery.contains("timestamp > 1000"));
        assertTrue(metaQuery.contains("tpath"));
        assertTrue(metaQuery.contains("creationtime"));
        assertTrue(metaQuery.contains("lastmodifiedtime"));
        assertTrue(metaQuery.contains("num_messages"));
    }

    /**
     * Test configuration with different split provider types.
     */
    @Test
    public void testConfigurationTypes()
    {
        // Test that the configuration is set correctly
        assertEquals(config.getSplitProviderType(), ClpConfig.SplitProviderType.PINOT_UBER);

        // Create a new instance with different config to ensure isolation
        ClpConfig newConfig = new ClpConfig();
        newConfig.setMetadataDbUrl("https://other-neutrino.uber.com");
        newConfig.setSplitProviderType(ClpConfig.SplitProviderType.PINOT_UBER);

        ClpUberPinotSplitProvider newProvider = new ClpUberPinotSplitProvider(
                newConfig,
                functionAndTypeManager,
                standardFunctionResolution,
                new ClpSplitMetadataConfig(newConfig, functionAndTypeManager));
        assertNotNull(newProvider);
    }

    /**
     * Test parsing of Uber Neutrino query response format.
     * Verifies that the JSON response with "columns" and "data" fields is correctly
     * parsed into a list of row maps.
     */
    @Test
    public void testParseQueryResponse() throws Exception
    {
        // Sample JSON in Uber Neutrino response format
        String jsonResponse = String.join("\n",
                "{",
                "  'id': '20251211_200011_71046_z3qq5',",
                "  'infoUri': '//localhost:5436/ui/query.html?20251211_200011_71046_z3qq5',",
                "  'columns': [",
                "    {",
                "      'name': 'tpath',",
                "      'type': 'varchar',",
                "      'typeSignature': {",
                "        'rawType': 'varchar',",
                "        'typeArguments': [],",
                "        'literalArguments': [],",
                "        'arguments': [{'kind': 'LONG_LITERAL', 'value': 2147483647}]",
                "      }",
                "    },",
                "    {",
                "      'name': '_timestampMillis',",
                "      'type': 'bigint',",
                "      'typeSignature': {",
                "        'rawType': 'bigint',",
                "        'typeArguments': [],",
                "        'literalArguments': [],",
                "        'arguments': []",
                "      }",
                "    }",
                "  ],",
                "  'data': [",
                "    ['/prod/logging/athena/table_a/dca/archive1.clp.zst', 1765483211084],",
                "    ['/prod/logging/athena/table_b/dca/archive2.clp.zst', 1765483211043],",
                "    ['/prod/logging/athena/table_c/dca/archive3.clp.zst', 1765483211043]",
                "  ]",
                "}").replace('\'', '"');

        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(jsonResponse);

        List<Map<String, JsonNode>> results = splitProvider.parseQueryResponse(root);

        // Verify the correct number of rows
        assertEquals(results.size(), 3);

        // Verify the first row
        Map<String, JsonNode> row0 = results.get(0);
        assertEquals(row0.size(), 2);
        assertEquals(row0.get("tpath").asText(), "/prod/logging/athena/table_a/dca/archive1.clp.zst");
        assertEquals(row0.get("_timestampMillis").asLong(), 1765483211084L);

        // Verify the second row
        Map<String, JsonNode> row1 = results.get(1);
        assertEquals(row1.get("tpath").asText(), "/prod/logging/athena/table_b/dca/archive2.clp.zst");
        assertEquals(row1.get("_timestampMillis").asLong(), 1765483211043L);

        // Verify the third row
        Map<String, JsonNode> row2 = results.get(2);
        assertEquals(row2.get("tpath").asText(), "/prod/logging/athena/table_c/dca/archive3.clp.zst");
        assertEquals(row2.get("_timestampMillis").asLong(), 1765483211043L);

        // Verify that non-existent columns return null
        for (Map<String, JsonNode> row : results) {
            assertEquals(row.get("nonExistentColumn"), null);
            assertEquals(row.get("id"), null);
            assertEquals(row.get("infoUri"), null);
        }
    }
}
