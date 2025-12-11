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

import com.facebook.presto.common.type.Type;
import com.facebook.presto.plugin.clp.ClpColumnHandle;
import com.facebook.presto.plugin.clp.TestClpQueryBase;
import com.facebook.presto.plugin.clp.split.ClpSplitMetadataConfig;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.TypeProvider;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test
public class TestClpFilterToKql
        extends TestClpQueryBase
{
    private ClpSplitMetadataConfig mockMetadataConfig;
    private SchemaTableName testTableName;
    private Map<String, ColumnHandle> columnHandles;

    @BeforeMethod
    public void setUp()
    {
        testTableName = new SchemaTableName("test", "table");

        // Build table schema
        columnHandles = ImmutableMap.copyOf(
                variableToColumnHandleMap.entrySet().stream()
                        .collect(java.util.stream.Collectors.toMap(
                                e -> ((ClpColumnHandle) e.getValue()).getOriginalColumnName(),
                                Map.Entry::getValue)));

        mockMetadataConfig = mock(ClpSplitMetadataConfig.class);
    }

    @Test
    public void testStringMatchPushDown()
    {
        SessionHolder sessionHolder = new SessionHolder();

        // Exact match
        testPushDown(sessionHolder, "city.Name = 'hello world'", "city.Name: \"hello world\"", null);
        testPushDown(sessionHolder, "'hello world' = city.Name", "city.Name: \"hello world\"", null);

        // Like predicates that are transformed into substring match
        testPushDown(sessionHolder, "city.Name like 'hello%'", "city.Name: \"hello*\"", null);
        testPushDown(sessionHolder, "city.Name like '%hello'", "city.Name: \"*hello\"", null);

        // Like predicate not pushed down
        testPushDown(sessionHolder, "city.Name like '%hello%'", null, "city.Name like '%hello%'");

        // Like predicates that are kept in the original forms
        testPushDown(sessionHolder, "city.Name like 'hello_'", "city.Name: \"hello?\"", null);
        testPushDown(sessionHolder, "city.Name like '_hello'", "city.Name: \"?hello\"", null);
        testPushDown(sessionHolder, "city.Name like 'hello_w%'", "city.Name: \"hello?w*\"", null);
        testPushDown(sessionHolder, "city.Name like '%hello_w'", "city.Name: \"*hello?w\"", null);
        testPushDown(sessionHolder, "city.Name like 'hello%world'", "city.Name: \"hello*world\"", null);
        testPushDown(sessionHolder, "city.Name like 'hello%wor%ld'", "city.Name: \"hello*wor*ld\"", null);
    }

    @Test
    public void testSubStringPushDown()
    {
        SessionHolder sessionHolder = new SessionHolder();

        testPushDown(sessionHolder, "substr(city.Name, 1, 2) = 'he'", "city.Name: \"he*\"", null);
        testPushDown(sessionHolder, "substr(city.Name, 5, 2) = 'he'", "city.Name: \"????he*\"", null);
        testPushDown(sessionHolder, "substr(city.Name, 5) = 'he'", "city.Name: \"????he\"", null);
        testPushDown(sessionHolder, "substr(city.Name, -2) = 'he'", "city.Name: \"*he\"", null);

        // Invalid substring index — not pushed down
        testPushDown(sessionHolder, "substr(city.Name, 1, 5) = 'he'", null, "substr(city.Name, 1, 5) = 'he'");
        testPushDown(sessionHolder, "substr(city.Name, -5) = 'he'", null, "substr(city.Name, -5) = 'he'");
    }

    @Test
    public void testNumericComparisonPushDown()
    {
        SessionHolder sessionHolder = new SessionHolder();

        // Numeric comparisons
        testPushDown(sessionHolder, "fare > 0", "fare > 0", null);
        testPushDown(sessionHolder, "fare >= 0", "fare >= 0", null);
        testPushDown(sessionHolder, "fare < 0", "fare < 0", null);
        testPushDown(sessionHolder, "fare <= 0", "fare <= 0", null);
        testPushDown(sessionHolder, "fare = 0", "fare: 0", null);
        testPushDown(sessionHolder, "fare != 0", "NOT fare: 0", null);
        testPushDown(sessionHolder, "fare <> 0", "NOT fare: 0", null);
        testPushDown(sessionHolder, "0 < fare", "fare > 0", null);
        testPushDown(sessionHolder, "0 <= fare", "fare >= 0", null);
        testPushDown(sessionHolder, "0 > fare", "fare < 0", null);
        testPushDown(sessionHolder, "0 >= fare", "fare <= 0", null);
        testPushDown(sessionHolder, "0 = fare", "fare: 0", null);
        testPushDown(sessionHolder, "0 != fare", "NOT fare: 0", null);
        testPushDown(sessionHolder, "0 <> fare", "NOT fare: 0", null);
    }

    @Test
    public void testBetweenPushDown()
    {
        SessionHolder sessionHolder = new SessionHolder();

        // Normal cases
        testPushDown(sessionHolder, "fare BETWEEN 0 AND 5", "fare >= 0 AND fare <= 5", null);
        testPushDown(sessionHolder, "fare BETWEEN 5 AND 0", "fare >= 5 AND fare <= 0", null);

        // No push down for non-constant expressions
        testPushDown(
                sessionHolder,
                "fare BETWEEN (city.Region.Id - 5) AND (city.Region.Id + 5)",
                null,
                "fare BETWEEN (city.Region.Id - 5) AND (city.Region.Id + 5)");

        // If the last two arguments of BETWEEN are not numeric constants, then the CLP connector
        // won't push them down.
        testPushDown(
                sessionHolder,
                "city.Name BETWEEN 'a' AND 'b'",
                "city.Name >= \"a\" AND city.Name <= \"b\"",
                null);
    }

    @Test
    public void testOrPushDown()
    {
        SessionHolder sessionHolder = new SessionHolder();

        // OR conditions with partial push down support
        testPushDown(sessionHolder, "fare > 0 OR city.Name like 'b%'", "(fare > 0 OR city.Name: \"b*\")", null);
        testPushDown(
                sessionHolder,
                "lower(city.Region.Name) = 'hello world' OR city.Region.Id != 1",
                null,
                "(lower(city.Region.Name) = 'hello world' OR city.Region.Id != 1)");

        // Multiple ORs
        testPushDown(
                sessionHolder,
                "fare > 0 OR city.Name like 'b%' OR lower(city.Region.Name) = 'hello world' OR city.Region.Id != 1",
                null,
                "fare > 0 OR city.Name like 'b%' OR lower(city.Region.Name) = 'hello world' OR city.Region.Id != 1");
        testPushDown(
                sessionHolder,
                "fare > 0 OR city.Name like 'b%' OR city.Region.Id != 1",
                "((fare > 0 OR city.Name: \"b*\") OR NOT city.Region.Id: 1)",
                null);
    }

    @Test
    public void testAndPushDown()
    {
        SessionHolder sessionHolder = new SessionHolder();

        // AND conditions with partial/full push down
        testPushDown(sessionHolder, "fare > 0 AND city.Name like 'b%'", "(fare > 0 AND city.Name: \"b*\")", null);

        testPushDown(sessionHolder, "lower(city.Region.Name) = 'hello world' AND city.Region.Id != 1", "(NOT city.Region.Id: 1)", "lower(city.Region.Name) = 'hello world'");

        // Multiple ANDs
        testPushDown(
                sessionHolder,
                "fare > 0 AND city.Name like 'b%' AND lower(city.Region.Name) = 'hello world' AND city.Region.Id != 1",
                "(((fare > 0 AND city.Name: \"b*\")) AND NOT city.Region.Id: 1)",
                "(lower(city.Region.Name) = 'hello world')");

        testPushDown(
                sessionHolder,
                "fare > 0 AND city.Name like '%b%' AND lower(city.Region.Name) = 'hello world' AND city.Region.Id != 1",
                "(((fare > 0)) AND NOT city.Region.Id: 1)",
                "city.Name like '%b%' AND lower(city.Region.Name) = 'hello world'");
    }

    @Test
    public void testNotPushDown()
    {
        SessionHolder sessionHolder = new SessionHolder();

        // NOT and inequality predicates
        testPushDown(sessionHolder, "city.Region.Name NOT LIKE 'hello%'", "NOT city.Region.Name: \"hello*\"", null);
        testPushDown(sessionHolder, "NOT (city.Region.Name LIKE 'hello%')", "NOT city.Region.Name: \"hello*\"", null);
        testPushDown(sessionHolder, "city.Name != 'hello world'", "NOT city.Name: \"hello world\"", null);
        testPushDown(sessionHolder, "city.Name <> 'hello world'", "NOT city.Name: \"hello world\"", null);
        testPushDown(sessionHolder, "NOT (city.Name = 'hello world')", "NOT city.Name: \"hello world\"", null);
        testPushDown(sessionHolder, "fare != 0", "NOT fare: 0", null);
        testPushDown(sessionHolder, "fare <> 0", "NOT fare: 0", null);
        testPushDown(sessionHolder, "NOT (fare = 0)", "NOT fare: 0", null);

        // Multiple NOTs
        testPushDown(sessionHolder, "NOT (NOT fare = 0)", "NOT NOT fare: 0", null);
        testPushDown(sessionHolder, "NOT (fare = 0 AND city.Name = 'hello world')", "NOT (fare: 0 AND city.Name: \"hello world\")", null);
        testPushDown(sessionHolder, "NOT (fare = 0 OR city.Name = 'hello world')", "NOT (fare: 0 OR city.Name: \"hello world\")", null);
    }

    @Test
    public void testInPushDown()
    {
        SessionHolder sessionHolder = new SessionHolder();

        // IN predicate
        testPushDown(sessionHolder, "city.Name IN ('hello world', 'hello world 2')", "(city.Name: \"hello world\" OR city.Name: \"hello world 2\")", null);
    }

    @Test
    public void testIsNullPushDown()
    {
        SessionHolder sessionHolder = new SessionHolder();

        // IS NULL / IS NOT NULL predicates
        testPushDown(sessionHolder, "city.Name IS NULL", "NOT city.Name: *", null);
        testPushDown(sessionHolder, "city.Name IS NOT NULL", "NOT NOT city.Name: *", null);
        testPushDown(sessionHolder, "NOT (city.Name IS NULL)", "NOT NOT city.Name: *", null);
    }

    @Test
    public void testComplexPushDown()
    {
        SessionHolder sessionHolder = new SessionHolder();

        // Complex AND/OR with partial pushdown
        testPushDown(
                sessionHolder,
                "(fare > 0 OR city.Name like 'b%') AND (lower(city.Region.Name) = 'hello world' OR city.Name IS NULL)",
                "((fare > 0 OR city.Name: \"b*\"))",
                "(lower(city.Region.Name) = 'hello world' OR city.Name IS NULL)");

        testPushDown(
                sessionHolder,
                "city.Region.Id = 1 AND (fare > 0 OR city.Name NOT like 'b%') AND (lower(city.Region.Name) = 'hello world' OR city.Name IS NULL)",
                "((city.Region.Id: 1 AND (fare > 0 OR NOT city.Name: \"b*\")))",
                "lower(city.Region.Name) = 'hello world' OR city.Name IS NULL");
    }

    @Ignore
    @Test
    public void testMetadataExpressionGeneration()
    {
        SessionHolder sessionHolder = new SessionHolder();
        Set<String> testMetadataFilterColumns = ImmutableSet.of("fare");
        // Assuming no exposed name, directly reference the bounded data column
        Set<String> testDataColumnsWithRangeBounds = ImmutableSet.of("city.Region.Id");

        // Normal case
        testPushDown(
                sessionHolder,
                "(fare > 0 AND city.Name like 'b%')",
                "(city.Name: \"b*\")",
                "fare > 0",
                TypeProvider.viewOf(ImmutableMap.of("fare", BIGINT)),
                testMetadataFilterColumns,
                testDataColumnsWithRangeBounds);

        // With BETWEEN
        testPushDown(
                sessionHolder,
                "((fare BETWEEN 0 AND 5) AND city.Name like 'b%')",
                "(city.Name: \"b*\")",
                "fare >= 0 AND fare <= 5",
                TypeProvider.viewOf(ImmutableMap.of("fare", BIGINT)),
                testMetadataFilterColumns,
                testDataColumnsWithRangeBounds);

        // The cases of that the metadata filter column exist but cannot be push down
        testPushDown(
                sessionHolder,
                "(fare > 0 OR city.Name like 'b%')",
                "(city.Name: \"b*\")",
                null,
                testMetadataFilterColumns,
                testDataColumnsWithRangeBounds);
        testPushDown(
                sessionHolder,
                "(fare > 0 AND city.Name like 'b%') OR city.Region.Id = 1",
                "((city.Name: \"b*\") OR city.Region.Id: 1)",
                null,
                testMetadataFilterColumns,
                testDataColumnsWithRangeBounds);

        // Complicated case
        testPushDown(
                sessionHolder,
                "fare = 0 AND (city.Name like 'b%' OR city.Region.Id = 1)",
                "((city.Name: \"b*\" OR city.Region.Id: 1))",
                "fare = 0",
                TypeProvider.viewOf(ImmutableMap.of("fare", BIGINT)),
                testMetadataFilterColumns,
                testDataColumnsWithRangeBounds);
        testPushDown(
                sessionHolder,
                "city.Region.Id = 1 AND (city.Name like 'b%' OR fare = 0)",
                "(city.Region.Id: 1 AND (city.Name: \"b*\"))",
                "\"city.Region.Id\" = 1",
                TypeProvider.viewOf(ImmutableMap.of("city.Region.Id", BIGINT)),
                testMetadataFilterColumns,
                testDataColumnsWithRangeBounds);
    }

    @Test
    public void testExposedRangeBoundColumnResolutionToDataColumn()
    {
        SessionHolder sessionHolder = new SessionHolder();

        // Define explicit mapping: exposed column name -> data column name
        Map<String, String> exposedToRangeMapping = ImmutableMap.of(
                "timestampExposed", "timestampData",
                "valueExposed", "valueData",
                "varCharExposed", "bigIntData");  // Different types: exposed is VARCHAR, data is BIGINT

        // The data columns that have range bounds in metadata
        Set<String> dataColumnsWithRangeBounds = ImmutableSet.of("timestampData", "valueData", "bigIntData");

        // Test BETWEEN with VARCHAR exposed columns
        testPushDownWithExposedRangeBound(
                sessionHolder,
                "timestampExposed BETWEEN '100' AND '200'",
                "timestampData >= \"100\" AND timestampData <= \"200\"",
                null,  // no remaining expression
                dataColumnsWithRangeBounds,
                exposedToRangeMapping);

        // Test comparison with the BIGINT exposed column
        testPushDownWithExposedRangeBound(
                sessionHolder,
                "valueExposed >= 50",
                "valueData >= 50",
                null,
                dataColumnsWithRangeBounds,
                exposedToRangeMapping);

        // Test multiple exposed columns in the logical binary expression
        testPushDownWithExposedRangeBound(
                sessionHolder,
                "timestampExposed >= '100'",
                "timestampData >= \"100\"",
                null,
                dataColumnsWithRangeBounds,
                exposedToRangeMapping);

        testPushDownWithExposedRangeBound(
                sessionHolder,
                "timestampExposed = '100'",
                "timestampData: \"100\"",
                null,
                dataColumnsWithRangeBounds,
                exposedToRangeMapping);

        testPushDownWithExposedRangeBound(
                sessionHolder,
                "timestampExposed <= '200'",
                "timestampData <= \"200\"",
                null,
                dataColumnsWithRangeBounds,
                exposedToRangeMapping);

        // Test multiple exposed columns in compound expression
        testPushDownWithExposedRangeBound(
                sessionHolder,
                "timestampExposed >= '100' AND valueExposed < 200",
                "(timestampData >= \"100\" AND valueData < 200)",
                null,
                dataColumnsWithRangeBounds,
                exposedToRangeMapping);

        // Test exposed column with different type from data column
        // varCharExposed is VARCHAR (metadata type), but bigIntData is BIGINT (actual data type)
        // The generated KQL should use bigIntData with BIGINT type
        testPushDownWithExposedRangeBound(
                sessionHolder,
                "varCharExposed >= '10'",
                "bigIntData >= 10",
                null,
                dataColumnsWithRangeBounds,
                exposedToRangeMapping);

        // Test compound expression with type-mismatched exposed column
        testPushDownWithExposedRangeBound(
                sessionHolder,
                "varCharExposed > '50' AND varCharExposed < '1000'",
                "(bigIntData > 50 AND bigIntData < 1000)",
                null,
                dataColumnsWithRangeBounds,
                exposedToRangeMapping);
    }

    @Test
    public void testClpWildcardUdf()
    {
        SessionHolder sessionHolder = new SessionHolder();

        testPushDown(sessionHolder, "CLP_WILDCARD_STRING_COLUMN() = 'Beijing'", "*: \"Beijing\"", null);
        testPushDown(sessionHolder, "CLP_WILDCARD_INT_COLUMN() = 1", "*: 1", null);
        testPushDown(sessionHolder, "CLP_WILDCARD_FLOAT_COLUMN() > 0", "* > 0", null);
        testPushDown(sessionHolder, "CLP_WILDCARD_BOOL_COLUMN() = true", "*: true", null);

        testPushDown(sessionHolder, "CLP_WILDCARD_STRING_COLUMN() like 'hello%'", "*: \"hello*\"", null);
        testPushDown(sessionHolder, "substr(CLP_WILDCARD_STRING_COLUMN(), 1, 2) = 'he'", "*: \"he*\"", null);
        testPushDown(sessionHolder, "CLP_WILDCARD_INT_COLUMN() BETWEEN 0 AND 5", "* >= 0 AND * <= 5", null);
        testPushDown(sessionHolder, "CLP_WILDCARD_STRING_COLUMN() IN ('hello world', 'hello world 2')", "(*: \"hello world\" OR *: \"hello world 2\")", null);

        testPushDown(sessionHolder, "NOT CLP_WILDCARD_FLOAT_COLUMN() > 0", "NOT * > 0", null);
        testPushDown(
                sessionHolder,
                "CLP_WILDCARD_STRING_COLUMN() = 'Beijing' AND CLP_WILDCARD_INT_COLUMN() = 1 AND city.Region.Id = 1",
                "((*: \"Beijing\" AND *: 1) AND city.Region.Id: 1)",
                null);
        testPushDown(
                sessionHolder,
                "CLP_WILDCARD_STRING_COLUMN() = 'Toronto' OR CLP_WILDCARD_INT_COLUMN() = 2",
                "(*: \"Toronto\" OR *: 2)",
                null);
        testPushDown(
                sessionHolder,
                "CLP_WILDCARD_STRING_COLUMN() = 'Shanghai' AND (CLP_WILDCARD_INT_COLUMN() = 3 OR city.Region.Id = 5)",
                "(*: \"Shanghai\" AND (*: 3 OR city.Region.Id: 5))",
                null);
    }

    private void testPushDown(SessionHolder sessionHolder, String sql, String expectedKql, String expectedRemaining)
    {
        ClpExpression clpExpression = tryPushDown(sql, sessionHolder, ImmutableSet.of(), ImmutableSet.of());
        testFilter(clpExpression, expectedKql, expectedRemaining, sessionHolder);
    }

    private void testPushDown(
            SessionHolder sessionHolder,
            String sql,
            String expectedKql,
            String expectedMetadataSqlQuery,
            Set<String> metadataFilterColumns,
            Set<String> dataColumnsWithRangeBounds)
    {
        testPushDown(
                sessionHolder,
                sql,
                expectedKql,
                expectedMetadataSqlQuery,
                typeProvider,
                metadataFilterColumns,
                dataColumnsWithRangeBounds);
    }

    private void testPushDownWithExposedRangeBound(
            SessionHolder sessionHolder,
            String sql,
            String expectedKql,
            String expectedRemaining,
            Set<String> dataColumnsWithRangeBounds,
            Map<String, String> exposedToRangeMapping)
    {
        Map<String, Type> metadataColumns = ImmutableMap.of(
                "timestampExposed", VARCHAR,
                "valueExposed", BIGINT,
                "varCharExposed", VARCHAR);

        ClpExpression clpExpression = tryPushDown(sql, sessionHolder, metadataColumns, dataColumnsWithRangeBounds, exposedToRangeMapping);
        testFilter(clpExpression, expectedKql, expectedRemaining, sessionHolder);
    }

    private void testPushDown(
            SessionHolder sessionHolder,
            String sql,
            String expectedKql,
            String expectedMetadataSqlQuery,
            TypeProvider typeProviderForMetadataExpression,
            Set<String> metadataFilterColumns,
            Set<String> dataColumnsWithRangeBounds)
    {
        ClpExpression clpExpression = tryPushDown(sql, sessionHolder, metadataFilterColumns, dataColumnsWithRangeBounds);
        testFilter(clpExpression, expectedKql, null, sessionHolder);
        // verify metadata split pruning
        if (expectedMetadataSqlQuery != null) {
            assertTrue(clpExpression.getMetadataExpression().isPresent());
            assertEquals(
                    clpExpression.getMetadataExpression().get(),
                    getRowExpression(expectedMetadataSqlQuery, typeProviderForMetadataExpression, sessionHolder));
        }
        else {
            assertFalse(clpExpression.getMetadataExpression().isPresent());
        }
    }

    /**
     * Test helper for filter pushdown with default identity mapping for exposed columns.
     */
    private ClpExpression tryPushDown(
            String sqlExpression,
            SessionHolder sessionHolder,
            Set<String> metadataFilterColumns,
            Set<String> dataColumnsWithRangeBounds)
    {
        // Build identity mapping: exposed name -> data column name (same name)
        Map<String, String> exposedToRangeMapping = new HashMap<>();
        for (String dataColumn : dataColumnsWithRangeBounds) {
            exposedToRangeMapping.put(dataColumn, dataColumn);
        }
        Map<String, Type> metadataColumnsMap = new HashMap<>();
        for (String col : metadataFilterColumns) {
            metadataColumnsMap.put(col, BIGINT);
        }
        return tryPushDown(sqlExpression, sessionHolder, metadataColumnsMap, dataColumnsWithRangeBounds, exposedToRangeMapping);
    }

    /**
     * Test helper for filter pushdown with explicit exposed-to-data column mapping.
     */
    private ClpExpression tryPushDown(
            String sqlExpression,
            SessionHolder sessionHolder,
            Map<String, Type> metadataColumnsMap,
            Set<String> dataColumnsWithRangeBounds,
            Map<String, String> exposedToRangeMapping)
    {
        RowExpression pushDownExpression = getRowExpression(sqlExpression, sessionHolder);
        Map<VariableReferenceExpression, ColumnHandle> assignments = new HashMap<>(variableToColumnHandleMap);

        when(mockMetadataConfig.getMetadataColumns(any(SchemaTableName.class)))
                .thenReturn(metadataColumnsMap);
        when(mockMetadataConfig.getDataColumnsWithRangeBounds(any(SchemaTableName.class)))
                .thenReturn(dataColumnsWithRangeBounds);
        when(mockMetadataConfig.getExposedToRangeMapping(any(SchemaTableName.class)))
                .thenReturn(exposedToRangeMapping);

        return pushDownExpression.accept(
                new ClpFilterToKqlConverter(
                        standardFunctionResolution,
                        functionAndTypeManager,
                        assignments,
                        mockMetadataConfig,
                        testTableName,
                        columnHandles),
                null);
    }

    private void testFilter(
            ClpExpression clpExpression,
            String expectedKqlExpression,
            String expectedRemainingExpression,
            SessionHolder sessionHolder)
    {
        Optional<String> kqlExpression = clpExpression.getPushDownExpression();
        Optional<RowExpression> remainingExpression = clpExpression.getRemainingExpression();
        if (expectedKqlExpression != null) {
            assertTrue(kqlExpression.isPresent());
            assertEquals(kqlExpression.get(), expectedKqlExpression);
        }
        else {
            assertFalse(kqlExpression.isPresent());
        }

        if (expectedRemainingExpression != null) {
            assertTrue(remainingExpression.isPresent());
            assertEquals(remainingExpression.get(), getRowExpression(expectedRemainingExpression, sessionHolder));
        }
        else {
            assertFalse(remainingExpression.isPresent());
        }
    }
}
