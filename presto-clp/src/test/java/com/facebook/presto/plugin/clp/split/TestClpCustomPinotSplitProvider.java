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
import com.facebook.presto.plugin.clp.mockdb.ClpMockPinotDatabase;
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
 * Unit tests for ClpCustomPinotSplitProvider.
 * Tests custom endpoint URL construction and RTA table name prefixing.
 */
@Test(singleThreaded = true)
public class TestClpCustomPinotSplitProvider
        extends TestClpQueryBase
{
    private ClpCustomPinotSplitProvider splitProvider;
    private ClpConfig config;

    @BeforeMethod
    public void setUp()
    {
        config = new ClpConfig();
        config.setMetadataDbUrl("https://pinot-service.example.com");
        config.setSplitProviderType(ClpConfig.SplitProviderType.PINOT_CUSTOM);
        config.setCustomTableNamePrefix("rta.logging.");
        config.setCustomApiEndpointPath("/v1/globalStatement");
        splitProvider = new ClpCustomPinotSplitProvider(
                config,
                functionAndTypeManager,
                standardFunctionResolution,
                new ClpSplitMetadataConfig(config, functionAndTypeManager));
    }

    /**
     * Test that the custom endpoint URL is correctly constructed.
     */
    @Test
    public void testBuildPinotSqlQueryEndpointUrl() throws Exception
    {
        // Use reflection to access the protected method
        Method method = ClpCustomPinotSplitProvider.class.getDeclaredMethod("buildPinotSqlQueryEndpointUrl", ClpConfig.class);
        method.setAccessible(true);

        URL result = (URL) method.invoke(splitProvider, config);

        assertNotNull(result);
        assertEquals(result.toString(), "https://pinot-service.example.com/v1/globalStatement");
        assertEquals(result.getProtocol(), "https");
        assertEquals(result.getHost(), "pinot-service.example.com");
        assertEquals(result.getPath(), "/v1/globalStatement");
    }

    /**
     * Test URL construction with different base URLs.
     */
    @Test
    public void testBuildPinotSqlQueryEndpointUrlVariations() throws Exception
    {
        Method method = ClpCustomPinotSplitProvider.class.getDeclaredMethod("buildPinotSqlQueryEndpointUrl", ClpConfig.class);
        method.setAccessible(true);

        // Test with trailing slash
        config.setMetadataDbUrl("https://pinot-service.example.com/");
        URL result = (URL) method.invoke(splitProvider, config);
        assertEquals(result.toString(), "https://pinot-service.example.com//v1/globalStatement");

        // Test without protocol (should work as URL constructor handles it)
        config.setMetadataDbUrl("http://pinot-dev.example.com");
        result = (URL) method.invoke(splitProvider, config);
        assertEquals(result.toString(), "http://pinot-dev.example.com/v1/globalStatement");

        // Test with port
        config.setMetadataDbUrl("https://pinot-service.example.com:8080");
        result = (URL) method.invoke(splitProvider, config);
        assertEquals(result.toString(), "https://pinot-service.example.com:8080/v1/globalStatement");
    }

    /**
     * Test URL construction with custom API endpoint path.
     */
    @Test
    public void testBuildPinotSqlQueryEndpointUrlCustomPath() throws Exception
    {
        Method method = ClpCustomPinotSplitProvider.class.getDeclaredMethod("buildPinotSqlQueryEndpointUrl", ClpConfig.class);
        method.setAccessible(true);

        // Test with custom endpoint path
        config.setMetadataDbUrl("https://pinot-service.example.com");
        config.setCustomApiEndpointPath("/api/v2/query");
        URL result = (URL) method.invoke(splitProvider, config);
        assertEquals(result.toString(), "https://pinot-service.example.com/api/v2/query");
    }

    /**
     * Test that invalid URLs throw MalformedURLException.
     */
    @Test
    public void testBuildPinotSqlQueryEndpointUrlInvalid() throws Exception
    {
        Method method = ClpCustomPinotSplitProvider.class.getDeclaredMethod("buildPinotSqlQueryEndpointUrl", ClpConfig.class);
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
     * Test that table names are correctly prefixed when prefix is configured.
     */
    @Test
    public void testInferMetadataTableNameWithPrefix()
    {
        SchemaTableName schemaTableName = new SchemaTableName("default", "logs");
        ClpTableHandle tableHandle = new ClpTableHandle(schemaTableName, "test");

        String result = splitProvider.inferMetadataTableName(tableHandle);

        assertEquals(result, "rta.logging.logs");
    }

    /**
     * Test table name inference without prefix configured.
     */
    @Test
    public void testInferMetadataTableNameWithoutPrefix()
    {
        ClpConfig noPrefixConfig = new ClpConfig();
        noPrefixConfig.setMetadataDbUrl("https://pinot-service.example.com");
        noPrefixConfig.setSplitProviderType(ClpConfig.SplitProviderType.PINOT_CUSTOM);
        noPrefixConfig.setCustomApiEndpointPath("/v1/globalStatement");
        // No table name prefix configured

        ClpCustomPinotSplitProvider noPrefixProvider = new ClpCustomPinotSplitProvider(
                noPrefixConfig,
                functionAndTypeManager,
                standardFunctionResolution,
                new ClpSplitMetadataConfig(noPrefixConfig, functionAndTypeManager));

        SchemaTableName schemaTableName = new SchemaTableName("default", "logs");
        ClpTableHandle tableHandle = new ClpTableHandle(schemaTableName, "test");

        // Without prefix, table name should be returned as-is
        assertEquals(noPrefixProvider.inferMetadataTableName(tableHandle), "logs");
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
     * Test that buildSplitSelectionQuery correctly wraps metadata columns with LASTWITHTIME.
     * Each metadata column should be aggregated using LASTWITHTIME to get the latest value.
     */
    @Test
    public void testBuildSplitSelectionQueryWithMetadataColumns()
    {
        // Test case 1: No metadata projection (empty list)
        List<String> emptyColumns = new ArrayList<>();
        String queryNoMetadata = splitProvider.buildSplitSelectionQuery(
                "rta.logging.logs", emptyColumns, "1 = 1");
        assertEquals(queryNoMetadata,
                "SELECT tpath  FROM rta.logging.logs WHERE 1 = 1 AND (1 = 1) GROUP BY tpath LIMIT 999999");

        // Test case 2: Metadata projection with tpath (tpath should be skipped)
        List<String> columnsWithTpath = new ArrayList<>();
        columnsWithTpath.add("tpath");
        columnsWithTpath.add("hostname");
        String queryWithTpath = splitProvider.buildSplitSelectionQuery(
                "rta.logging.logs", columnsWithTpath, "creationtime > 1000");
        assertEquals(queryWithTpath,
                "SELECT tpath , LASTWITHTIME(hostname, \"_timestampMillis\", 'string') AS hostname FROM rta.logging.logs WHERE 1 = 1 AND (creationtime > 1000) GROUP BY tpath LIMIT 999999");

        // Test case 3: Projection with repeated column (duplicates should be removed)
        List<String> columnsWithDuplicate = new ArrayList<>();
        columnsWithDuplicate.add("hostname");
        columnsWithDuplicate.add("hostname");
        String queryWithDuplicate = splitProvider.buildSplitSelectionQuery(
                "rta.logging.logs", columnsWithDuplicate, "1 = 1");
        assertEquals(queryWithDuplicate,
                "SELECT tpath , LASTWITHTIME(hostname, \"_timestampMillis\", 'string') AS hostname FROM rta.logging.logs WHERE 1 = 1 AND (1 = 1) GROUP BY tpath LIMIT 999999");
    }

    /**
     * Test configuration with different split provider types.
     */
    @Test
    public void testConfigurationTypes()
    {
        // Test that the configuration is set correctly
        assertEquals(config.getSplitProviderType(), ClpConfig.SplitProviderType.PINOT_CUSTOM);

        // Create a new instance with different config to ensure isolation
        ClpConfig newConfig = new ClpConfig();
        newConfig.setMetadataDbUrl("https://other-service.example.com");
        newConfig.setSplitProviderType(ClpConfig.SplitProviderType.PINOT_CUSTOM);
        newConfig.setCustomApiEndpointPath("/v1/globalStatement");

        ClpCustomPinotSplitProvider newProvider = new ClpCustomPinotSplitProvider(
                newConfig,
                functionAndTypeManager,
                standardFunctionResolution,
                new ClpSplitMetadataConfig(newConfig, functionAndTypeManager));
        assertNotNull(newProvider);
    }

    /**
     * Test that buildFullSplitPath correctly constructs the full path with URL prefix.
     */
    @Test
    public void testBuildFullSplitPath() throws Exception
    {
        // Create a config with custom storage base URL configured
        ClpConfig testConfig = new ClpConfig();
        testConfig.setMetadataDbUrl("https://pinot-service.example.com");
        testConfig.setSplitProviderType(ClpConfig.SplitProviderType.PINOT_CUSTOM);
        testConfig.setCustomStorageBaseUrl("http://localhost:19617");
        testConfig.setCustomApiEndpointPath("/v1/globalStatement");

        ClpCustomPinotSplitProvider testProvider = new ClpCustomPinotSplitProvider(
                testConfig,
                functionAndTypeManager,
                standardFunctionResolution,
                new ClpSplitMetadataConfig(testConfig, functionAndTypeManager));

        Method method = ClpCustomPinotSplitProvider.class.getDeclaredMethod("buildFullSplitPath", String.class);
        method.setAccessible(true);

        // Test with a typical relative path
        String result = (String) method.invoke(testProvider, "/data/archives/table1/ir001.clp.zst");
        assertEquals(result, "http://localhost:19617/data/archives/table1/ir001.clp.zst");

        // Test with base URL without explicit port (should omit port in result)
        testConfig.setCustomStorageBaseUrl("http://storage.example.com");
        result = (String) method.invoke(testProvider, "/ir.clp.zst");
        assertEquals(result, "http://storage.example.com/ir.clp.zst");
    }

    /**
     * Test that buildFullSplitPath throws the exception when base URL is not configured.
     */
    @Test
    public void testBuildFullSplitPathMissingConfig() throws Exception
    {
        Method method = ClpCustomPinotSplitProvider.class.getDeclaredMethod("buildFullSplitPath", String.class);
        method.setAccessible(true);

        // config from setUp() does not have custom storage URL configured
        try {
            method.invoke(splitProvider, "/some/path");
            fail("Expected IllegalArgumentException for missing custom storage base URL");
        }
        catch (Exception e) {
            assertTrue(e.getCause() instanceof IllegalArgumentException);
            assertTrue(e.getCause().getMessage().contains("clp.custom-storage-base-url"));
        }
    }

    /**
     * Test parsing of custom query response format.
     * Verifies that the JSON response with "columns" and "data" fields is correctly
     * parsed into a list of row maps.
     */
    @Test
    public void testParseQueryResponse() throws Exception
    {
        // Sample JSON in custom response format
        String jsonResponse = String.join("\n",
                "{",
                "  'id': '12345',",
                "  'uri': '//localhost:5436/query.html?12345',",
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
                "    ['/path/to/table_a/archive1.clp.zst', 1765483211084],",
                "    ['/path/to/table_b/archive2.clp.zst', 1765483211043],",
                "    ['/path/to/table_c/archive3.clp.zst', 1765483211043]",
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
        assertEquals(row0.get("tpath").asText(), "/path/to/table_a/archive1.clp.zst");
        assertEquals(row0.get("_timestampMillis").asLong(), 1765483211084L);

        // Verify the second row
        Map<String, JsonNode> row1 = results.get(1);
        assertEquals(row1.get("tpath").asText(), "/path/to/table_b/archive2.clp.zst");
        assertEquals(row1.get("_timestampMillis").asLong(), 1765483211043L);

        // Verify the third row
        Map<String, JsonNode> row2 = results.get(2);
        assertEquals(row2.get("tpath").asText(), "/path/to/table_c/archive3.clp.zst");
        assertEquals(row2.get("_timestampMillis").asLong(), 1765483211043L);

        // Verify that non-existent columns return null
        for (Map<String, JsonNode> row : results) {
            assertEquals(row.get("nonExistentColumn"), null);
            assertEquals(row.get("id"), null);
            assertEquals(row.get("uri"), null);
        }
    }

    /**
     * Test deduplication query against a mock Pinot database with duplicate rows.
     * Inserts multiple versions of the same tpath with different timestamps,
     * then verifies that the query returns only the latest version.
     */
    @Test
    public void testDeduplicationQueryWithMockDatabase() throws Exception
    {
        ClpMockPinotDatabase mockDb = ClpMockPinotDatabase.builder()
                .setTableName("test_table")
                .build();

        // Insert duplicate rows for archive1 (simulating Pinot's append-only model)
        // archive1 v1: old data with timestamp 1000
        mockDb.insertRow("/path/to/archive1.clp.zst", "host-old", "1000", "100", "1000");
        // archive1 v2: updated data with timestamp 2000 (latest)
        mockDb.insertRow("/path/to/archive1.clp.zst", "host-new", "1500", "150", "2000");

        // Insert single row for archive2
        mockDb.insertRow("/path/to/archive2.clp.zst", "host-b", "1200", "200", "1500");

        mockDb.start();

        try {
            ClpConfig mockConfig = new ClpConfig();
            mockConfig.setMetadataDbUrl(mockDb.getUrl());
            mockConfig.setSplitProviderType(ClpConfig.SplitProviderType.PINOT_CUSTOM);
            mockConfig.setCustomApiEndpointPath("/v1/globalStatement");

            ClpCustomPinotSplitProvider mockSplitProvider = new ClpCustomPinotSplitProvider(
                    mockConfig,
                    functionAndTypeManager,
                    standardFunctionResolution,
                    new ClpSplitMetadataConfig(mockConfig, functionAndTypeManager));

            List<String> metadataColumns = new ArrayList<>();
            metadataColumns.add("hostname");

            String query = mockSplitProvider.buildSplitSelectionQuery(
                    "test_table",
                    metadataColumns,
                    "1 = 1");

            // Verify query format includes LASTWITHTIME and GROUP BY
            assertTrue(query.contains("LASTWITHTIME(hostname"));
            assertTrue(query.contains("GROUP BY tpath"));

            // Execute the query against mock database
            List<Map<String, JsonNode>> results = mockSplitProvider.getQueryResult(query);

            // Should get 2 rows (deduplicated), not 3
            assertEquals(results.size(), 2);

            // Find archive1 row and verify it has the LATEST hostname value
            Map<String, JsonNode> archive1Row = null;
            Map<String, JsonNode> archive2Row = null;
            for (Map<String, JsonNode> row : results) {
                String tpath = row.get("tpath").asText();
                if (tpath.contains("archive1")) {
                    archive1Row = row;
                }
                else if (tpath.contains("archive2")) {
                    archive2Row = row;
                }
            }

            assertNotNull(archive1Row, "archive1 row should exist");
            assertNotNull(archive2Row, "archive2 row should exist");

            // archive1 should have hostname from the LATEST version (timestamp 2000)
            assertEquals(archive1Row.get("hostname").asText(), "host-new");

            // archive2 should have its only hostname value
            assertEquals(archive2Row.get("hostname").asText(), "host-b");
        }
        finally {
            mockDb.stop();
        }
    }
}
