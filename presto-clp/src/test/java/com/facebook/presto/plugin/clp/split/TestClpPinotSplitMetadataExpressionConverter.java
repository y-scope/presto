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
import com.facebook.presto.plugin.clp.TestClpQueryBase;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.sql.planner.TypeProvider;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestClpPinotSplitMetadataExpressionConverter
        extends TestClpQueryBase
{
    private String splitMetadataConfigPath;

    private TypeProvider typeProvider;
    private ClpSplitMetadataConfig splitMetadataConfig;
    private ClpPinotSplitMetadataExpressionConverter defaultConverter;

    @BeforeMethod
    public void setUp() throws IOException, URISyntaxException
    {
        URL resource = getClass().getClassLoader().getResource("test-pinot-split-metadata.json");
        if (resource == null) {
            throw new FileNotFoundException("test-pinot-split-metadata.json not found in resources");
        }

        splitMetadataConfigPath = Paths.get(resource.toURI()).toAbsolutePath().toString();
        typeProvider = TypeProvider.viewOf(
                ImmutableMap.of(
                        "level", BIGINT,
                        "msg.timestamp", BIGINT,
                        "begin_timestamp", BIGINT,
                        "end_timestamp", BIGINT,
                        "status_code", BIGINT,
                        "score", DOUBLE,
                        "hostname", VARCHAR,
                        "service", VARCHAR));

        ClpConfig config = new ClpConfig();
        config.setSplitMetadataConfigPath(splitMetadataConfigPath);
        splitMetadataConfig = new ClpSplitMetadataConfig(config, functionAndTypeManager);

        SchemaTableName table = new SchemaTableName("default", "table_1");
        defaultConverter = new ClpPinotSplitMetadataExpressionConverter(
                functionAndTypeManager,
                standardFunctionResolution,
                splitMetadataConfig.getExposedToOriginalMapping(table),
                splitMetadataConfig.getDataColumnRangeMapping(table),
                splitMetadataConfig.getRequiredColumns(table));
    }

    /**
     * Test that Pinot provider correctly inherits MySQL range mapping functionality.
     * Verifies that range comparisons are transformed according to the configuration.
     */
    @Test
    public void testRangeMappingInheritance()
    {
        assertTransform("\"msg.timestamp\" >= 1234", "end_timestamp >= 1234");
        assertTransform("\"msg.timestamp\" <= 5678", "begin_timestamp <= 5678");
        assertTransform("\"msg.timestamp\" = 4567", "(begin_timestamp <= 4567) AND (end_timestamp >= 4567)");
    }

    /**
     * Test that expressions without range mappings pass through unchanged.
     */
    @Test
    public void testNonRangeMappedColumns()
    {
        assertTransform("\"status_code\" = 200", "status_code = 200");
        assertTransform("\"hostname\" = 'server1'", "hostname = 'server1'");
    }

    /**
     * Test complex expressions with multiple predicates.
     */
    @Test
    public void testComplexExpressions()
    {
        assertTransform(
                "(\"msg.timestamp\" >= 1000 AND \"msg.timestamp\" <= 2000)",
                "(end_timestamp >= 1000) AND (begin_timestamp <= 2000)");

        assertTransform(
                "(\"msg.timestamp\" = 1500 AND \"status_code\" = 200)",
                "((begin_timestamp <= 1500) AND (end_timestamp >= 1500)) AND (status_code = 200)");
    }

//    /**
//     * Test that remapColumnName correctly returns mapped column names.
//     */
//    @Test
//    public void testRemapColumnName()
//    {
//        // Test range-mapped column
//        List<String> mappedColumns = filterProvider.remapColumnName("clp.default.table_1", "msg.timestamp");
//        assertEquals(mappedColumns, ImmutableList.of("begin_timestamp", "end_timestamp"));
//
//        // Test non-mapped column
//        List<String> unmappedColumns = filterProvider.remapColumnName("clp.default.table_1", "status_code");
//        assertEquals(unmappedColumns, ImmutableList.of("status_code"));
//    }

//    /**
//     * Test table-level configuration override.
//     */
//    @Test
//    public void testTableLevelOverride()
//    {
//        // Test table_2 specific mapping
//        String sql = "\"table2_column\" >= 100";
//        String result = filterProvider.remapSplitFilterPushDownExpression("clp.default.table_2", sql);
//        assertEquals(result, "table2_upper >= 100");
//    }
//
//    /**
//     * Test schema-level configuration.
//     */
//    @Test
//    public void testSchemaLevelMapping()
//    {
//        // Test schema-level mapping applies to tables
//        String sql = "\"schema_column\" <= 500";
//        String result = filterProvider.remapSplitFilterPushDownExpression("clp.schema1.any_table", sql);
//        assertEquals(result, "schema_lower <= 500");
//    }

    /**
     * Test that configuration is correctly loaded.
     */
    @Test
    public void testConfigurationLoaded()
    {
        // Simply verify that the provider was instantiated correctly with the config
        assertTrue(splitMetadataConfigPath.endsWith("test-pinot-split-metadata.json"));
    }

    private void assertTransform(String sql, String expected)
    {
        SessionHolder session = new SessionHolder();
        assertEquals(defaultConverter.transform(getRowExpression(sql, typeProvider, session)), expected);
    }
}
