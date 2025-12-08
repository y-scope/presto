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
package com.facebook.presto.plugin.clp.metadata;

import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.plugin.clp.ClpColumnHandle;
import com.facebook.presto.plugin.clp.ClpConfig;
import com.facebook.presto.plugin.clp.ClpTableHandle;
import com.facebook.presto.plugin.clp.split.ClpSplitMetadataConfig;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static com.facebook.presto.plugin.clp.ClpConfig.MetadataProviderType.YAML;
import static com.facebook.presto.plugin.clp.ClpMetadata.DEFAULT_SCHEMA_NAME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestClpYamlMetadataProvider
{
    private final List<File> tempFiles = new ArrayList<>();
    private TypeManager typeManager;

    @BeforeClass
    public void setup()
    {
        FunctionAndTypeManager functionAndTypeManager = createTestFunctionAndTypeManager();
        typeManager = functionAndTypeManager;
    }

    @AfterClass
    public void cleanup()
    {
        // Clean up temporary files
        for (File file : tempFiles) {
            if (file.exists()) {
                file.delete();
            }
        }
    }

    /**
     * Helper method to create a ClpSplitMetadataConfig for testing
     */
    private ClpSplitMetadataConfig createSplitMetadataConfig(ClpConfig config)
    {
        return new ClpSplitMetadataConfig(config, typeManager);
    }

    /**
     * Test that listSchemaNames returns only the default schema when YAML has single schema
     */
    @Test
    public void testListSchemaNamesSingleSchema() throws IOException
    {
        File metadataFile = createTempYamlFile(
                "clp:\n" +
                "  default:\n" +
                "    table1: /path/to/table1.yaml\n" +
                "    table2: /path/to/table2.yaml\n");

        ClpConfig config = new ClpConfig()
                .setMetadataProviderType(YAML)
                .setMetadataYamlPath(metadataFile.getAbsolutePath());

        ClpYamlMetadataProvider provider = new ClpYamlMetadataProvider(config, createSplitMetadataConfig(config));
        List<String> schemas = provider.listSchemaNames();

        assertEquals(schemas.size(), 1);
        assertEquals(schemas.get(0), DEFAULT_SCHEMA_NAME);
    }

    /**
     * Test that listSchemaNames discovers multiple schemas from YAML
     */
    @Test
    public void testListSchemaNamesMultipleSchemas() throws IOException
    {
        File metadataFile = createTempYamlFile(
                "clp:\n" +
                "  default:\n" +
                "    logs: /path/to/default/logs.yaml\n" +
                "  dev:\n" +
                "    test_logs: /path/to/dev/logs.yaml\n" +
                "  staging:\n" +
                "    staging_logs: /path/to/staging/logs.yaml\n" +
                "  prod:\n" +
                "    production_logs: /path/to/prod/logs.yaml\n");

        ClpConfig config = new ClpConfig()
                .setMetadataProviderType(YAML)
                .setMetadataYamlPath(metadataFile.getAbsolutePath());

        ClpYamlMetadataProvider provider = new ClpYamlMetadataProvider(config, createSplitMetadataConfig(config));
        List<String> schemas = provider.listSchemaNames();

        assertEquals(schemas.size(), 4);
        Set<String> schemaSet = ImmutableSet.copyOf(schemas);
        assertTrue(schemaSet.contains("default"));
        assertTrue(schemaSet.contains("dev"));
        assertTrue(schemaSet.contains("staging"));
        assertTrue(schemaSet.contains("prod"));
    }

    /**
     * Test that listSchemaNames handles missing YAML path gracefully
     */
    @Test
    public void testListSchemaNamesNullPath()
    {
        ClpConfig config = new ClpConfig()
                .setMetadataProviderType(YAML);
        // Note: not setting metadataYamlPath

        ClpYamlMetadataProvider provider = new ClpYamlMetadataProvider(config, createSplitMetadataConfig(config));
        List<String> schemas = provider.listSchemaNames();

        assertEquals(schemas.size(), 1);
        assertEquals(schemas.get(0), DEFAULT_SCHEMA_NAME);
    }

    /**
     * Test that listSchemaNames handles nonexistent file gracefully
     */
    @Test
    public void testListSchemaNamesNonexistentFile()
    {
        ClpConfig config = new ClpConfig()
                .setMetadataProviderType(YAML)
                .setMetadataYamlPath("/nonexistent/path/metadata.yaml");

        ClpYamlMetadataProvider provider = new ClpYamlMetadataProvider(config, createSplitMetadataConfig(config));
        List<String> schemas = provider.listSchemaNames();

        assertEquals(schemas.size(), 1);
        assertEquals(schemas.get(0), DEFAULT_SCHEMA_NAME);
    }

    /**
     * Test that listSchemaNames handles malformed YAML gracefully
     */
    @Test
    public void testListSchemaNamesMalformedYaml() throws IOException
    {
        File metadataFile = createTempYamlFile(
                "this is not\n" +
                "  valid: yaml: content\n" +
                "    - with random structure\n");

        ClpConfig config = new ClpConfig()
                .setMetadataProviderType(YAML)
                .setMetadataYamlPath(metadataFile.getAbsolutePath());

        ClpYamlMetadataProvider provider = new ClpYamlMetadataProvider(config, createSplitMetadataConfig(config));
        List<String> schemas = provider.listSchemaNames();

        assertEquals(schemas.size(), 1);
        assertEquals(schemas.get(0), DEFAULT_SCHEMA_NAME);
    }

    /**
     * Test that listSchemaNames handles YAML without catalog field
     */
    @Test
    public void testListSchemaNamesNoCatalogField() throws IOException
    {
        File metadataFile = createTempYamlFile(
                "some_other_catalog:\n" +
                "  default:\n" +
                "    table1: /path/to/table1.yaml\n");

        ClpConfig config = new ClpConfig()
                .setMetadataProviderType(YAML)
                .setMetadataYamlPath(metadataFile.getAbsolutePath());

        ClpYamlMetadataProvider provider = new ClpYamlMetadataProvider(config, createSplitMetadataConfig(config));
        List<String> schemas = provider.listSchemaNames();

        // Should fall back to default schema on error
        assertEquals(schemas.size(), 1);
        assertEquals(schemas.get(0), DEFAULT_SCHEMA_NAME);
    }

    /**
     * Test that listTableHandles returns correct tables for a schema
     */
    @Test
    public void testListTableHandles() throws IOException
    {
        // Create schema YAML files
        File table1Schema = createTempYamlFile("column1: 1\ncolumn2: 2\n");
        File table2Schema = createTempYamlFile("field1: 3\nfield2: 4\n");

        File metadataFile = createTempYamlFile(
                "clp:\n" +
                "  default:\n" +
                "    table1: " + table1Schema.getAbsolutePath() + "\n" +
                "    table2: " + table2Schema.getAbsolutePath() + "\n");

        ClpConfig config = new ClpConfig()
                .setMetadataProviderType(YAML)
                .setMetadataYamlPath(metadataFile.getAbsolutePath());

        ClpYamlMetadataProvider provider = new ClpYamlMetadataProvider(config, createSplitMetadataConfig(config));
        List<ClpTableHandle> tables = provider.listTableHandles(DEFAULT_SCHEMA_NAME);

        assertEquals(tables.size(), 2);
        Set<String> tableNames = ImmutableSet.of(
                tables.get(0).getSchemaTableName().getTableName(),
                tables.get(1).getSchemaTableName().getTableName());
        assertTrue(tableNames.contains("table1"));
        assertTrue(tableNames.contains("table2"));
    }

    /**
     * Test that listTableHandles returns correct tables for multiple schemas
     */
    @Test
    public void testListTableHandlesMultipleSchemas() throws IOException
    {
        File devTable = createTempYamlFile("col: 1\n");
        File prodTable = createTempYamlFile("col: 2\n");

        File metadataFile = createTempYamlFile(
                "clp:\n" +
                "  dev:\n" +
                "    dev_logs: " + devTable.getAbsolutePath() + "\n" +
                "  prod:\n" +
                "    prod_logs: " + prodTable.getAbsolutePath() + "\n");

        ClpConfig config = new ClpConfig()
                .setMetadataProviderType(YAML)
                .setMetadataYamlPath(metadataFile.getAbsolutePath());

        ClpYamlMetadataProvider provider = new ClpYamlMetadataProvider(config, createSplitMetadataConfig(config));

        // Test dev schema
        List<ClpTableHandle> devTables = provider.listTableHandles("dev");
        assertEquals(devTables.size(), 1);
        assertEquals(devTables.get(0).getSchemaTableName().getTableName(), "dev_logs");
        assertEquals(devTables.get(0).getSchemaTableName().getSchemaName(), "dev");

        // Test prod schema
        List<ClpTableHandle> prodTables = provider.listTableHandles("prod");
        assertEquals(prodTables.size(), 1);
        assertEquals(prodTables.get(0).getSchemaTableName().getTableName(), "prod_logs");
        assertEquals(prodTables.get(0).getSchemaTableName().getSchemaName(), "prod");
    }

    /**
     * Test that schema names are returned in consistent order
     */
    @Test
    public void testSchemaNameConsistency() throws IOException
    {
        File metadataFile = createTempYamlFile(
                "clp:\n" +
                "  schema_a:\n" +
                "    table: /path/a.yaml\n" +
                "  schema_b:\n" +
                "    table: /path/b.yaml\n" +
                "  schema_c:\n" +
                "    table: /path/c.yaml\n");

        ClpConfig config = new ClpConfig()
                .setMetadataProviderType(YAML)
                .setMetadataYamlPath(metadataFile.getAbsolutePath());

        ClpYamlMetadataProvider provider = new ClpYamlMetadataProvider(config, createSplitMetadataConfig(config));

        // Call multiple times to verify consistency
        List<String> schemas1 = provider.listSchemaNames();
        List<String> schemas2 = provider.listSchemaNames();
        List<String> schemas3 = provider.listSchemaNames();

        assertEquals(schemas1, schemas2);
        assertEquals(schemas2, schemas3);
    }

    /**
     * Test empty schema (no tables)
     */
    @Test
    public void testEmptySchema() throws IOException
    {
        File metadataFile = createTempYamlFile(
                "clp:\n" +
                "  empty_schema:\n");

        ClpConfig config = new ClpConfig()
                .setMetadataProviderType(YAML)
                .setMetadataYamlPath(metadataFile.getAbsolutePath());

        ClpYamlMetadataProvider provider = new ClpYamlMetadataProvider(config, createSplitMetadataConfig(config));
        List<String> schemas = provider.listSchemaNames();

        assertTrue(schemas.contains("empty_schema"));

        List<ClpTableHandle> tables = provider.listTableHandles("empty_schema");
        assertTrue(tables.isEmpty());
    }

    /**
     * Test that schemas with special characters in names are handled
     */
    @Test
    public void testSchemaWithSpecialCharacters() throws IOException
    {
        File metadataFile = createTempYamlFile(
                "clp:\n" +
                "  schema_with_underscores:\n" +
                "    table: /path/table.yaml\n" +
                "  schema-with-dashes:\n" +
                "    table: /path/table2.yaml\n");

        ClpConfig config = new ClpConfig()
                .setMetadataProviderType(YAML)
                .setMetadataYamlPath(metadataFile.getAbsolutePath());

        ClpYamlMetadataProvider provider = new ClpYamlMetadataProvider(config, createSplitMetadataConfig(config));
        List<String> schemas = provider.listSchemaNames();

        assertEquals(schemas.size(), 2);
        Set<String> schemaSet = ImmutableSet.copyOf(schemas);
        assertTrue(schemaSet.contains("schema_with_underscores"));
        assertTrue(schemaSet.contains("schema-with-dashes"));
    }

    /**
     * Test that metadata columns from split-filter config are automatically merged with YAML columns
     */
    @Test
    public void testListColumnHandlesWithMetadataColumns() throws IOException
    {
        // Create a split metadata config file (JSON format)
        File splitMetadataFile = createTempJsonFile(
                "{\n" +
                "  \"default.test_table\": {\n" +
                "    \"metaColumns\": {\n" +
                "      \"file_name\": {\n" +
                "        \"type\": \"VARCHAR\"\n" +
                "      },\n" +
                "      \"begin_timestamp\": {\n" +
                "        \"type\": \"BIGINT\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}");

        // Create a table schema YAML file with regular columns
        File tableSchemaFile = createTempYamlFile(
                "order_id: 0\n" +
                "customer_name: 3\n" +
                "price: 1\n");

        // Create the main metadata YAML file
        File metadataFile = createTempYamlFile(
                "clp:\n" +
                "  default:\n" +
                "    test_table: " + tableSchemaFile.getAbsolutePath() + "\n");

        ClpConfig config = new ClpConfig()
                .setMetadataProviderType(YAML)
                .setMetadataYamlPath(metadataFile.getAbsolutePath())
                .setSplitMetadataConfigPath(splitMetadataFile.getAbsolutePath());

        ClpYamlMetadataProvider provider = new ClpYamlMetadataProvider(config, createSplitMetadataConfig(config));

        // Load tables first (required for listColumnHandles to work)
        provider.listTableHandles(DEFAULT_SCHEMA_NAME);

        // Get column handles for the test table
        SchemaTableName schemaTableName = new SchemaTableName(DEFAULT_SCHEMA_NAME, "test_table");
        List<ClpColumnHandle> columns = provider.listColumnHandles(schemaTableName);

        // Verify we have both YAML columns and metadata columns
        // 3 from YAML (order_id, customer_name, price) + 2 from metadata (file_name, begin_timestamp) = 5 total
        assertEquals(columns.size(), 5);

        // Build a set of column names for easy verification
        Set<String> columnNames = ImmutableSet.copyOf(
                columns.stream()
                        .map(ClpColumnHandle::getColumnName)
                        .collect(java.util.stream.Collectors.toList()));

        // Verify YAML columns are present
        assertTrue(columnNames.contains("order_id"));
        assertTrue(columnNames.contains("customer_name"));
        assertTrue(columnNames.contains("price"));

        // Verify metadata columns are present
        assertTrue(columnNames.contains("file_name"));
        assertTrue(columnNames.contains("begin_timestamp"));
    }

    /**
     * Test that duplicate columns (defined in both YAML and metadata config) are not duplicated
     */
    @Test
    public void testListColumnHandlesWithDuplicateColumns() throws IOException
    {
        // Create a split metadata config file (JSON format) with a column that's also in the YAML
        File splitMetadataFile = createTempJsonFile(
                "{\n" +
                "  \"default.dup_table\": {\n" +
                "    \"metaColumns\": {\n" +
                "      \"order_id\": {\n" +
                "        \"type\": \"BIGINT\"\n" +
                "      },\n" +
                "      \"file_name\": {\n" +
                "        \"type\": \"VARCHAR\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}");

        // Create a table schema YAML file that also has order_id
        File tableSchemaFile = createTempYamlFile(
                "order_id: 0\n" +
                "customer_name: 3\n");

        // Create the main metadata YAML file
        File metadataFile = createTempYamlFile(
                "clp:\n" +
                "  default:\n" +
                "    dup_table: " + tableSchemaFile.getAbsolutePath() + "\n");

        ClpConfig config = new ClpConfig()
                .setMetadataProviderType(YAML)
                .setMetadataYamlPath(metadataFile.getAbsolutePath())
                .setSplitMetadataConfigPath(splitMetadataFile.getAbsolutePath());

        ClpYamlMetadataProvider provider = new ClpYamlMetadataProvider(config, createSplitMetadataConfig(config));

        // Load tables first
        provider.listTableHandles(DEFAULT_SCHEMA_NAME);

        // Get column handles
        SchemaTableName schemaTableName = new SchemaTableName(DEFAULT_SCHEMA_NAME, "dup_table");
        List<ClpColumnHandle> columns = provider.listColumnHandles(schemaTableName);

        // Should have 3 columns total: order_id (from YAML, not duplicated), customer_name (from YAML), file_name (from metadata)
        assertEquals(columns.size(), 3);

        // Build a set of column names
        Set<String> columnNames = ImmutableSet.copyOf(
                columns.stream()
                        .map(ClpColumnHandle::getColumnName)
                        .collect(java.util.stream.Collectors.toList()));

        assertTrue(columnNames.contains("order_id"));
        assertTrue(columnNames.contains("customer_name"));
        assertTrue(columnNames.contains("file_name"));

        // Verify no duplicates by checking list size equals set size
        assertEquals(columns.size(), columnNames.size());
    }

    /**
     * Test that metadata columns are added when YAML has no columns (metadata-only table)
     */
    @Test
    public void testListColumnHandlesMetadataOnly() throws IOException
    {
        File splitMetadataFile = createTempJsonFile(
                "{\n" +
                "  \"default.metadata_table\": {\n" +
                "    \"metaColumns\": {\n" +
                "      \"file_name\": {\n" +
                "        \"type\": \"VARCHAR\"\n" +
                "      },\n" +
                "      \"record_count\": {\n" +
                "        \"type\": \"BIGINT\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}");

        // Empty YAML file (no columns defined)
        File tableSchemaFile = createTempYamlFile("");

        File metadataFile = createTempYamlFile(
                "clp:\n" +
                "  default:\n" +
                "    metadata_table: " + tableSchemaFile.getAbsolutePath() + "\n");

        ClpConfig config = new ClpConfig()
                .setMetadataProviderType(YAML)
                .setMetadataYamlPath(metadataFile.getAbsolutePath())
                .setSplitMetadataConfigPath(splitMetadataFile.getAbsolutePath());

        ClpYamlMetadataProvider provider = new ClpYamlMetadataProvider(config, createSplitMetadataConfig(config));
        provider.listTableHandles(DEFAULT_SCHEMA_NAME);

        SchemaTableName schemaTableName = new SchemaTableName(DEFAULT_SCHEMA_NAME, "metadata_table");
        List<ClpColumnHandle> columns = provider.listColumnHandles(schemaTableName);

        // Should have only the 2 metadata columns
        assertEquals(columns.size(), 2);

        Set<String> columnNames = ImmutableSet.copyOf(
                columns.stream()
                        .map(ClpColumnHandle::getColumnName)
                        .collect(java.util.stream.Collectors.toList()));

        assertTrue(columnNames.contains("file_name"));
        assertTrue(columnNames.contains("record_count"));
    }

    /**
     * Test that when no split metadata config is provided, only YAML columns are returned
     */
    @Test
    public void testListColumnHandlesYamlOnly() throws IOException
    {
        // Create a table schema YAML file with columns
        File tableSchemaFile = createTempYamlFile(
                "order_id: 0\n" +
                "customer_name: 3\n" +
                "price: 1\n");

        File metadataFile = createTempYamlFile(
                "clp:\n" +
                "  default:\n" +
                "    yaml_table: " + tableSchemaFile.getAbsolutePath() + "\n");

        // No split metadata config path set
        ClpConfig config = new ClpConfig()
                .setMetadataProviderType(YAML)
                .setMetadataYamlPath(metadataFile.getAbsolutePath());

        ClpYamlMetadataProvider provider = new ClpYamlMetadataProvider(config, createSplitMetadataConfig(config));
        provider.listTableHandles(DEFAULT_SCHEMA_NAME);

        SchemaTableName schemaTableName = new SchemaTableName(DEFAULT_SCHEMA_NAME, "yaml_table");
        List<ClpColumnHandle> columns = provider.listColumnHandles(schemaTableName);

        // Should have only the 3 YAML columns
        assertEquals(columns.size(), 3);

        Set<String> columnNames = ImmutableSet.copyOf(
                columns.stream()
                        .map(ClpColumnHandle::getColumnName)
                        .collect(java.util.stream.Collectors.toList()));

        assertTrue(columnNames.contains("order_id"));
        assertTrue(columnNames.contains("customer_name"));
        assertTrue(columnNames.contains("price"));
    }

    /**
     * Test that exposed-to-original name mapping works correctly
     */
    @Test
    public void testListColumnHandlesWithExposedNames() throws IOException
    {
        File splitMetadataFile = createTempJsonFile(
                "{\n" +
                "  \"default.exposed_table\": {\n" +
                "    \"metaColumns\": {\n" +
                "      \"internal_id\": {\n" +
                "        \"type\": \"BIGINT\",\n" +
                "        \"exposedAs\": \"record_id\"\n" +
                "      },\n" +
                "      \"internal_timestamp\": {\n" +
                "        \"type\": \"BIGINT\",\n" +
                "        \"exposedAs\": \"created_at\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}");

        File tableSchemaFile = createTempYamlFile("data_column: 3\n");

        File metadataFile = createTempYamlFile(
                "clp:\n" +
                "  default:\n" +
                "    exposed_table: " + tableSchemaFile.getAbsolutePath() + "\n");

        ClpConfig config = new ClpConfig()
                .setMetadataProviderType(YAML)
                .setMetadataYamlPath(metadataFile.getAbsolutePath())
                .setSplitMetadataConfigPath(splitMetadataFile.getAbsolutePath());

        ClpYamlMetadataProvider provider = new ClpYamlMetadataProvider(config, createSplitMetadataConfig(config));
        provider.listTableHandles(DEFAULT_SCHEMA_NAME);

        SchemaTableName schemaTableName = new SchemaTableName(DEFAULT_SCHEMA_NAME, "exposed_table");
        List<ClpColumnHandle> columns = provider.listColumnHandles(schemaTableName);

        assertEquals(columns.size(), 3);

        // Find the metadata columns and verify exposed vs original names
        ClpColumnHandle recordIdColumn = columns.stream()
                .filter(c -> c.getColumnName().equals("record_id"))
                .findFirst()
                .orElse(null);

        ClpColumnHandle createdAtColumn = columns.stream()
                .filter(c -> c.getColumnName().equals("created_at"))
                .findFirst()
                .orElse(null);

        // Verify exposed names are used for column name
        assertEquals(recordIdColumn != null, true);
        assertEquals(createdAtColumn != null, true);

        // Verify original names are stored correctly
        assertEquals(recordIdColumn.getOriginalColumnName(), "internal_id");
        assertEquals(createdAtColumn.getOriginalColumnName(), "internal_timestamp");
    }

    /**
     * Test behavior with multiple tables having different metadata columns
     */
    @Test
    public void testListColumnHandlesMultipleTables() throws IOException
    {
        File splitMetadataFile = createTempJsonFile(
                "{\n" +
                "  \"default.table_a\": {\n" +
                "    \"metaColumns\": {\n" +
                "      \"file_name\": {\n" +
                "        \"type\": \"VARCHAR\"\n" +
                "      }\n" +
                "    }\n" +
                "  },\n" +
                "  \"default.table_b\": {\n" +
                "    \"metaColumns\": {\n" +
                "      \"partition_id\": {\n" +
                "        \"type\": \"BIGINT\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}");

        File tableASchemaFile = createTempYamlFile("col_a: 0\n");
        File tableBSchemaFile = createTempYamlFile("col_b: 3\n");

        File metadataFile = createTempYamlFile(
                "clp:\n" +
                "  default:\n" +
                "    table_a: " + tableASchemaFile.getAbsolutePath() + "\n" +
                "    table_b: " + tableBSchemaFile.getAbsolutePath() + "\n");

        ClpConfig config = new ClpConfig()
                .setMetadataProviderType(YAML)
                .setMetadataYamlPath(metadataFile.getAbsolutePath())
                .setSplitMetadataConfigPath(splitMetadataFile.getAbsolutePath());

        ClpYamlMetadataProvider provider = new ClpYamlMetadataProvider(config, createSplitMetadataConfig(config));
        provider.listTableHandles(DEFAULT_SCHEMA_NAME);

        // Test table_a
        SchemaTableName tableA = new SchemaTableName(DEFAULT_SCHEMA_NAME, "table_a");
        List<ClpColumnHandle> columnsA = provider.listColumnHandles(tableA);
        assertEquals(columnsA.size(), 2); // col_a + file_name

        Set<String> columnNamesA = columnsA.stream()
                .map(ClpColumnHandle::getColumnName)
                .collect(java.util.stream.Collectors.toSet());
        assertTrue(columnNamesA.contains("col_a"));
        assertTrue(columnNamesA.contains("file_name"));

        // Test table_b
        SchemaTableName tableB = new SchemaTableName(DEFAULT_SCHEMA_NAME, "table_b");
        List<ClpColumnHandle> columnsB = provider.listColumnHandles(tableB);
        assertEquals(columnsB.size(), 2); // col_b + partition_id

        Set<String> columnNamesB = columnsB.stream()
                .map(ClpColumnHandle::getColumnName)
                .collect(java.util.stream.Collectors.toSet());
        assertTrue(columnNamesB.contains("col_b"));
        assertTrue(columnNamesB.contains("partition_id"));

        // Verify table_a doesn't have table_b's metadata column
        assertEquals(columnNamesA.contains("partition_id"), false);
        assertEquals(columnNamesB.contains("file_name"), false);
    }

    /**
     * Test that conflicting column types between YAML and metadata config throw an exception
     */
    @Test
    public void testListColumnHandlesWithTypeConflict() throws IOException
    {
        // Create a split metadata config file where order_id is VARCHAR
        File splitMetadataFile = createTempJsonFile(
                "{\n" +
                "  \"default.conflict_table\": {\n" +
                "    \"metaColumns\": {\n" +
                "      \"order_id\": {\n" +
                "        \"type\": \"VARCHAR\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}");

        // Create a table schema YAML file where order_id is BIGINT (type 0)
        File tableSchemaFile = createTempYamlFile(
                "order_id: 0\n" +
                "customer_name: 3\n");

        File metadataFile = createTempYamlFile(
                "clp:\n" +
                "  default:\n" +
                "    conflict_table: " + tableSchemaFile.getAbsolutePath() + "\n");

        ClpConfig config = new ClpConfig()
                .setMetadataProviderType(YAML)
                .setMetadataYamlPath(metadataFile.getAbsolutePath())
                .setSplitMetadataConfigPath(splitMetadataFile.getAbsolutePath());

        ClpYamlMetadataProvider provider = new ClpYamlMetadataProvider(config, createSplitMetadataConfig(config));
        provider.listTableHandles(DEFAULT_SCHEMA_NAME);

        SchemaTableName schemaTableName = new SchemaTableName(DEFAULT_SCHEMA_NAME, "conflict_table");

        try {
            provider.listColumnHandles(schemaTableName);
            fail("Expected PrestoException for type conflict");
        }
        catch (Exception e) {
            assertTrue(e.getMessage().contains("has conflicting type definitions"),
                    "Exception should mention type conflict: " + e.getMessage());
            assertTrue(e.getMessage().contains("order_id"),
                    "Exception should mention the conflicting column name: " + e.getMessage());
        }
    }

    /**
     * Helper method to create temporary YAML files for testing
     */
    private File createTempYamlFile(String content) throws IOException
    {
        File tempFile = Files.createTempFile("clp-test-", ".yaml").toFile();
        tempFile.deleteOnExit();
        tempFiles.add(tempFile);

        try (FileWriter writer = new FileWriter(tempFile)) {
            writer.write(content);
        }

        return tempFile;
    }

    /**
     * Helper method to create temporary JSON files for testing
     */
    private File createTempJsonFile(String content) throws IOException
    {
        File tempFile = Files.createTempFile("clp-test-", ".json").toFile();
        tempFile.deleteOnExit();
        tempFiles.add(tempFile);

        try (FileWriter writer = new FileWriter(tempFile)) {
            writer.write(content);
        }

        return tempFile;
    }
}
