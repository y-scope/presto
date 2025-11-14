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

/**
 * Unit tests for ClpUberPinotSplitFilterProvider.
 * Tests Uber-specific TEXT_MATCH transformations in addition to inherited
 * range mapping functionality.
 */
@Test(singleThreaded = true)
public class TestClpUberPinotSplitMetadataExpressionConverter
        extends TestClpQueryBase
{
    private TypeProvider typeProvider;
    private ClpSplitMetadataConfig splitMetadataConfig;
    private ClpUberPinotSplitMetadataExpressionConverter defaultConverter;

    @BeforeMethod
    public void setUp() throws IOException, URISyntaxException
    {
        URL resource = getClass().getClassLoader().getResource("test-pinot-split-metadata.json");
        if (resource == null) {
            throw new FileNotFoundException("test-pinot-split-metadata.json not found in resources");
        }

        // Types for exposed schema columns
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
        config.setSplitMetadataConfigPath(Paths.get(resource.toURI()).toAbsolutePath().toString());
        splitMetadataConfig = new ClpSplitMetadataConfig(config, functionAndTypeManager);

        SchemaTableName table = new SchemaTableName("default", "table_1");
        defaultConverter = new ClpUberPinotSplitMetadataExpressionConverter(
                functionAndTypeManager,
                standardFunctionResolution,
                splitMetadataConfig.getExposedToOriginalMapping(table),
                splitMetadataConfig.getDataColumnRangeMapping(table),
                splitMetadataConfig.getRequiredColumns(table));
    }

    /**
     * Test TEXT_MATCH transformation for simple equality predicates.
     * Verifies that Uber-specific TEXT_MATCH transformations are applied.
     */
    @Test
    public void testTextMatchTransformationSimpleEquality()
    {
        assertMatch("\"status_code\" = 200",
                "TEXT_MATCH(\"__mergedTextIndex\", '/200:status_code/')");

        assertMatch("\"level\" = -1",
                "TEXT_MATCH(\"__mergedTextIndex\", '/-1:level/')");

        assertMatch("\"score\" = 3.14",
                "TEXT_MATCH(\"__mergedTextIndex\", '/3.14:score/')");
    }

    /**
     * Test TEXT_MATCH transformation for string literal equality predicates.
     */
    @Test
    public void testTextMatchTransformationStringLiterals()
    {
        assertMatch("\"hostname\" = 'uber-server1'",
                "TEXT_MATCH(\"__mergedTextIndex\", '/uber-server1:hostname/')");

        assertMatch("\"service\" = 'uber.logging.service'",
                "TEXT_MATCH(\"__mergedTextIndex\", '/uber.logging.service:service/')");

        assertMatch("\"service\" = ''",
                "TEXT_MATCH(\"__mergedTextIndex\", '/:service/')");

        assertMatch("\"service\" = 'Hello Uber World'",
                "TEXT_MATCH(\"__mergedTextIndex\", '/Hello Uber World:service/')");
    }

    /**
     * Test that range mappings are inherited and work correctly.
     * Columns with range mappings should NOT be transformed to TEXT_MATCH.
     */
    @Test
    public void testRangeMappingInheritance()
    {
        assertMatch("\"msg.timestamp\" = 1234", "(begin_timestamp <= 1234) AND (end_timestamp >= 1234)");
        assertMatch("\"msg.timestamp\" >= 5000", "end_timestamp >= 5000");
        assertMatch("\"msg.timestamp\" <= 10000", "begin_timestamp <= 10000");
    }

    /**
     * Test complex expressions with both TEXT_MATCH and range mappings.
     */
    @Test
    public void testMixedTransformations()
    {
        assertMatch(
                "(\"msg.timestamp\" >= 1000 AND \"status_code\" = 200)",
                "(end_timestamp >= 1000) AND (TEXT_MATCH(\"__mergedTextIndex\", '/200:status_code/'))");

        assertMatch(
                "(\"hostname\" = 'uber1' AND \"service\" = 'logging')",
                "(TEXT_MATCH(\"__mergedTextIndex\", '/uber1:hostname/')) AND (TEXT_MATCH(\"__mergedTextIndex\", '/logging:service/'))");

        assertMatch(
                "((\"msg.timestamp\" <= 2000 AND \"hostname\" = 'uber2') OR \"status_code\" = 404)",
                "((begin_timestamp <= 2000) AND (TEXT_MATCH(\"__mergedTextIndex\", '/uber2:hostname/'))) OR (TEXT_MATCH(\"__mergedTextIndex\", '/404:status_code/'))");
    }

//    /**
//     * Test transformations at different scope levels.
//     */
//    @Test
//    public void testDifferentScopes()
//    {
//        // Table-level scope
//        String sql1 = "\"status_code\" = 200";
//        String result1 = filterProvider.remapSplitFilterPushDownExpression("clp.default.table_1", sql1);
//        assertEquals(result1, "TEXT_MATCH(\"__mergedTextIndex\", '/200:status_code/')");
//
//        // Schema-level scope
//        String result2 = filterProvider.remapSplitFilterPushDownExpression("clp.default", sql1);
//        assertEquals(result2, "TEXT_MATCH(\"__mergedTextIndex\", '/200:status_code/')");
//
//        // Catalog-level scope
//        String result3 = filterProvider.remapSplitFilterPushDownExpression("clp", sql1);
//        assertEquals(result3, "TEXT_MATCH(\"__mergedTextIndex\", '/200:status_code/')");
//    }

//    /**
//     * Test configuration is loaded correctly.
//     */
//    @Test
//    public void testConfigurationLoaded()
//    {
//        // Simply verify that the provider was instantiated correctly with the config
//        assertTrue(filterConfigPath.endsWith("test-pinot-split-metadata.json"));
//        assertNotNull(filterProvider);
//    }

    /**
     * Test that non-equality expressions are not transformed to TEXT_MATCH.
     */
    @Test
    public void testNonEqualityNotTransformed()
    {
        assertMatch("\"status_code\" > 200", "status_code > 200");
        assertMatch("\"level\" < 5", "level < 5");
        assertMatch("\"hostname\" != 'server1'", "hostname <> 'server1'");
    }

//    /**
//     * Test edge cases and special patterns.
//     */
//    @Test
//    public void testEdgeCases()
//    {
//        // Test expression with no transformable parts
//        String sql1 = "1 = 1";
//        String result1 = filterProvider.remapSplitFilterPushDownExpression("clp.default.table_1", sql1);
//        assertEquals(result1, "1 = 1");
//
//        // Test column names with special characters (should still work if quoted properly)
//        String sql2 = "\"column.with.dots\" = 'value'";
//        String result2 = filterProvider.remapSplitFilterPushDownExpression("clp.default.table_1", sql2);
//        assertEquals(result2, "TEXT_MATCH(\"__mergedTextIndex\", '/value:column.with.dots/')");
//
//        // Test multiple spaces in expression
//        String sql3 = "\"status_code\"    =    200";
//        String result3 = filterProvider.remapSplitFilterPushDownExpression("clp.default.table_1", sql3);
//        assertEquals(result3, "TEXT_MATCH(\"__mergedTextIndex\", '/200:status_code/')");
//    }

    private void assertMatch(String original, String expected)
    {
        SessionHolder session = new SessionHolder();
        assertEquals(defaultConverter.transform(getRowExpression(original, typeProvider, session)), expected);
    }

    private String transform(String sql, SessionHolder session)
    {
        return defaultConverter.transform(getRowExpression(sql, typeProvider, session));
    }

    private String transform(String sql, TypeProvider types, SessionHolder session)
    {
        return defaultConverter.transform(getRowExpression(sql, types, session));
    }
}
