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
     * Test that buildSplitSelectionQuery generates deduplication queries using LASTWITHTIME.
     * Pinot's append-only model requires GROUP BY tpath with LASTWITHTIME aggregation
     * to retrieve only the latest version of each record.
     */
    @Test
    public void testBuildSplitSelectionQueryWithDeduplication()
    {
        String query = splitProvider.buildSplitSelectionQuery(
                "rta.logging.logs", new ArrayList<>(), "status = 200");

        assertTrue(query.contains("SELECT tpath"));
        assertTrue(query.contains("rta.logging.logs"));
        assertTrue(query.contains("status = 200"));
        assertTrue(query.contains("GROUP BY tpath"));
        assertTrue(query.contains("LIMIT 999999"));
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
                "rta.logging.logs", metadataColumns, "creationtime > 1000");

        assertTrue(query.contains("SELECT tpath"));
        assertTrue(query.contains("LASTWITHTIME(hostname, \"_timestampMillis\", 'long') AS hostname"));
        assertTrue(query.contains("LASTWITHTIME(creationtime, \"_timestampMillis\", 'long') AS creationtime"));
        assertTrue(query.contains("GROUP BY tpath"));
        assertTrue(query.contains("rta.logging.logs"));
        assertTrue(query.contains("creationtime > 1000"));
    }

    /**
     * Test that TopN queries throw UnsupportedOperationException.
     * TopN optimization is currently disabled for Uber Pinot queries.
     */
    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testBuildSplitSelectionQueryWithTopNThrowsException()
    {
        splitProvider.buildSplitSelectionQueryWithTopN("rta.logging.events", "timestamp > 1000");
    }

    /**
     * Test parsing of deduplicated query response from Pinot.
     * Simulates a response where LASTWITHTIME has already deduplicated the rows,
     * returning only the latest version of each tpath.
     */
    @Test
    public void testParseDeduplicatedQueryResponse() throws Exception
    {
        // Simulates Pinot response after LASTWITHTIME deduplication:
        // - archive1 has latest hostname="host-v2" (from timestamp 2000)
        // - archive2 has latest hostname="host-b" (from timestamp 1500)
        // Note: In real Pinot, duplicate rows would be aggregated by GROUP BY tpath
        String jsonResponse = String.join("\n",
                "{",
                "  'columns': [",
                "    {'name': 'tpath', 'type': 'varchar'},",
                "    {'name': 'hostname', 'type': 'varchar'},",
                "    {'name': 'creationtime', 'type': 'bigint'}",
                "  ],",
                "  'data': [",
                "    ['/path/to/archive1.clp.zst', 'host-v2', 2000],",
                "    ['/path/to/archive2.clp.zst', 'host-b', 1500]",
                "  ]",
                "}").replace('\'', '"');

        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(jsonResponse);

        List<Map<String, JsonNode>> results = splitProvider.parseQueryResponse(root);

        // Verify deduplicated results: each tpath appears exactly once with latest values
        assertEquals(results.size(), 2);

        Map<String, JsonNode> row0 = results.get(0);
        assertEquals(row0.get("tpath").asText(), "/path/to/archive1.clp.zst");
        assertEquals(row0.get("hostname").asText(), "host-v2");
        assertEquals(row0.get("creationtime").asLong(), 2000L);

        Map<String, JsonNode> row1 = results.get(1);
        assertEquals(row1.get("tpath").asText(), "/path/to/archive2.clp.zst");
        assertEquals(row1.get("hostname").asText(), "host-b");
        assertEquals(row1.get("creationtime").asLong(), 1500L);
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
        mockDb.insertRow("/path/to/archive1.clp.zst", "host-old", 1000, 100, 1000);
        // archive1 v2: updated data with timestamp 2000 (latest)
        mockDb.insertRow("/path/to/archive1.clp.zst", "host-new", 1500, 150, 2000);

        // Insert single row for archive2
        mockDb.insertRow("/path/to/archive2.clp.zst", "host-b", 1200, 200, 1500);

        mockDb.start();

        try {
            ClpConfig mockConfig = new ClpConfig();
            mockConfig.setMetadataDbUrl(mockDb.getUrl());
            mockConfig.setSplitProviderType(ClpConfig.SplitProviderType.PINOT_UBER);

            ClpUberPinotSplitProvider mockSplitProvider = new ClpUberPinotSplitProvider(
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

    /**
     * Test deduplication with multiple metadata columns against mock database.
     * Verifies that all columns are correctly deduplicated to their latest values.
     */
    @Test
    public void testDeduplicationQueryWithMultipleColumnsAndMockDatabase() throws Exception
    {
        ClpMockPinotDatabase mockDb = ClpMockPinotDatabase.builder()
                .setTableName("test_table")
                .build();

        // Insert multiple versions for archive1
        mockDb.insertRow("/path/to/archive1.clp.zst", "host-v1", 1000, 100, 1000);
        mockDb.insertRow("/path/to/archive1.clp.zst", "host-v2", 1100, 110, 1500);
        mockDb.insertRow("/path/to/archive1.clp.zst", "host-v3", 1200, 120, 2000);  // Latest

        // Insert multiple versions for archive2
        mockDb.insertRow("/path/to/archive2.clp.zst", "host-a", 2000, 200, 1000);
        mockDb.insertRow("/path/to/archive2.clp.zst", "host-b", 2500, 250, 3000);  // Latest

        // Insert single version for archive3
        mockDb.insertRow("/path/to/archive3.clp.zst", "host-c", 3000, 300, 2500);

        mockDb.start();

        try {
            ClpConfig mockConfig = new ClpConfig();
            mockConfig.setMetadataDbUrl(mockDb.getUrl());
            mockConfig.setSplitProviderType(ClpConfig.SplitProviderType.PINOT_UBER);

            ClpUberPinotSplitProvider mockSplitProvider = new ClpUberPinotSplitProvider(
                    mockConfig,
                    functionAndTypeManager,
                    standardFunctionResolution,
                    new ClpSplitMetadataConfig(mockConfig, functionAndTypeManager));

            List<String> metadataColumns = new ArrayList<>();
            metadataColumns.add("hostname");
            metadataColumns.add("creationtime");
            metadataColumns.add("num_messages");

            String query = mockSplitProvider.buildSplitSelectionQuery(
                    "test_table",
                    metadataColumns,
                    "1 = 1");

            // Verify all columns are wrapped with LASTWITHTIME
            assertTrue(query.contains("LASTWITHTIME(hostname, \"_timestampMillis\", 'long') AS hostname"));
            assertTrue(query.contains("LASTWITHTIME(creationtime, \"_timestampMillis\", 'long') AS creationtime"));
            assertTrue(query.contains("LASTWITHTIME(num_messages, \"_timestampMillis\", 'long') AS num_messages"));
            assertTrue(query.contains("GROUP BY tpath"));

            // Execute query against mock database
            List<Map<String, JsonNode>> results = mockSplitProvider.getQueryResult(query);

            // Should get 3 rows (one per unique tpath), not 6
            assertEquals(results.size(), 3);

            // Verify each archive has the LATEST values
            for (Map<String, JsonNode> row : results) {
                String tpath = row.get("tpath").asText();
                if (tpath.contains("archive1")) {
                    // archive1 latest: host-v3, creationtime=1200, num_messages=120
                    assertEquals(row.get("hostname").asText(), "host-v3");
                    assertEquals(row.get("creationtime").asLong(), 1200L);
                    assertEquals(row.get("num_messages").asLong(), 120L);
                }
                else if (tpath.contains("archive2")) {
                    // archive2 latest: host-b, creationtime=2500, num_messages=250
                    assertEquals(row.get("hostname").asText(), "host-b");
                    assertEquals(row.get("creationtime").asLong(), 2500L);
                    assertEquals(row.get("num_messages").asLong(), 250L);
                }
                else if (tpath.contains("archive3")) {
                    // archive3 only version: host-c, creationtime=3000, num_messages=300
                    assertEquals(row.get("hostname").asText(), "host-c");
                    assertEquals(row.get("creationtime").asLong(), 3000L);
                    assertEquals(row.get("num_messages").asLong(), 300L);
                }
            }
        }
        finally {
            mockDb.stop();
        }
    }
}
