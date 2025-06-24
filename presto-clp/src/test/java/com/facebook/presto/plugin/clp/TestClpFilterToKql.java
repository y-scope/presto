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
package com.facebook.presto.plugin.clp;

import com.facebook.presto.spi.relation.RowExpression;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.Optional;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

@Test
public class TestClpFilterToKql
        extends TestClpQueryBase
{
    @Test
    public void testStringMatchPushDown()
    {
        SessionHolder sessionHolder = new SessionHolder();

        // Exact match
        testFilter(tryPushDown("city.Name = 'hello world'", sessionHolder), "city.Name: \"hello world\"", null, sessionHolder);
        testFilter(tryPushDown("'hello world' = city.Name", sessionHolder), "city.Name: \"hello world\"", null, sessionHolder);

        // Like predicates that are transformed into substring match
        testFilter(tryPushDown("city.Name like 'hello%'", sessionHolder), "city.Name: \"hello*\"", null, sessionHolder);
        testFilter(tryPushDown("city.Name like '%hello'", sessionHolder), "city.Name: \"*hello\"", null, sessionHolder);

        // Like predicates that are transformed into CARDINALITY(SPLIT(x, 'some string', 2)) = 2 form, and they are not pushed down for now
        testFilter(tryPushDown("city.Name like '%hello%'", sessionHolder), null, "city.Name like '%hello%'", sessionHolder);

        // Like predicates that are kept in the original forms
        testFilter(tryPushDown("city.Name like 'hello_'", sessionHolder), "city.Name: \"hello?\"", null, sessionHolder);
        testFilter(tryPushDown("city.Name like '_hello'", sessionHolder), "city.Name: \"?hello\"", null, sessionHolder);
        testFilter(tryPushDown("city.Name like 'hello_w%'", sessionHolder), "city.Name: \"hello?w*\"", null, sessionHolder);
        testFilter(tryPushDown("city.Name like '%hello_w'", sessionHolder), "city.Name: \"*hello?w\"", null, sessionHolder);
        testFilter(tryPushDown("city.Name like 'hello%world'", sessionHolder), "city.Name: \"hello*world\"", null, sessionHolder);
        testFilter(tryPushDown("city.Name like 'hello%wor%ld'", sessionHolder), "city.Name: \"hello*wor*ld\"", null, sessionHolder);
    }

    @Test
    public void testSubStringPushDown()
    {
        SessionHolder sessionHolder = new SessionHolder();

        testFilter(tryPushDown("substr(city.Name, 1, 2) = 'he'", sessionHolder), "city.Name: \"he*\"", null, sessionHolder);
        testFilter(tryPushDown("substr(city.Name, 5, 2) = 'he'", sessionHolder), "city.Name: \"????he*\"", null, sessionHolder);
        testFilter(tryPushDown("substr(city.Name, 5) = 'he'", sessionHolder), "city.Name: \"????he\"", null, sessionHolder);
        testFilter(tryPushDown("substr(city.Name, -2) = 'he'", sessionHolder), "city.Name: \"*he\"", null, sessionHolder);

        // Invalid substring index is not pushed down
        testFilter(tryPushDown("substr(city.Name, 1, 5) = 'he'", sessionHolder), null, "substr(city.Name, 1, 5) = 'he'", sessionHolder);
        testFilter(tryPushDown("substr(city.Name, -5) = 'he'", sessionHolder), null, "substr(city.Name, -5) = 'he'", sessionHolder);
    }

    @Test
    public void testNumericComparisonPushDown()
    {
        SessionHolder sessionHolder = new SessionHolder();

        testFilter(tryPushDown("fare > 0", sessionHolder), "fare > 0", null, sessionHolder);
        testFilter(tryPushDown("fare >= 0", sessionHolder), "fare >= 0", null, sessionHolder);
        testFilter(tryPushDown("fare < 0", sessionHolder), "fare < 0", null, sessionHolder);
        testFilter(tryPushDown("fare <= 0", sessionHolder), "fare <= 0", null, sessionHolder);
        testFilter(tryPushDown("fare = 0", sessionHolder), "fare: 0", null, sessionHolder);
        testFilter(tryPushDown("fare != 0", sessionHolder), "NOT fare: 0", null, sessionHolder);
        testFilter(tryPushDown("fare <> 0", sessionHolder), "NOT fare: 0", null, sessionHolder);
        testFilter(tryPushDown("0 < fare", sessionHolder), "fare > 0", null, sessionHolder);
        testFilter(tryPushDown("0 <= fare", sessionHolder), "fare >= 0", null, sessionHolder);
        testFilter(tryPushDown("0 > fare", sessionHolder), "fare < 0", null, sessionHolder);
        testFilter(tryPushDown("0 >= fare", sessionHolder), "fare <= 0", null, sessionHolder);
        testFilter(tryPushDown("0 = fare", sessionHolder), "fare: 0", null, sessionHolder);
        testFilter(tryPushDown("0 != fare", sessionHolder), "NOT fare: 0", null, sessionHolder);
        testFilter(tryPushDown("0 <> fare", sessionHolder), "NOT fare: 0", null, sessionHolder);
    }

    @Test
    public void testOrPushDown()
    {
        SessionHolder sessionHolder = new SessionHolder();

        testFilter(tryPushDown("fare > 0 OR city.Name like 'b%'", sessionHolder), "(fare > 0 OR city.Name: \"b*\")", null, sessionHolder);
        testFilter(tryPushDown("lower(city.Region.Name) = 'hello world' OR city.Region.Id != 1", sessionHolder), null, "(lower(city.Region.Name) = 'hello world' OR city.Region.Id != 1)", sessionHolder);

        // Multiple ORs
        testFilter(tryPushDown("fare > 0 OR city.Name like 'b%' OR lower(city.Region.Name) = 'hello world' OR city.Region.Id != 1", sessionHolder), null, "fare > 0 OR city.Name like 'b%' OR lower(city.Region.Name) = 'hello world' OR city.Region.Id != 1", sessionHolder);
        testFilter(tryPushDown("fare > 0 OR city.Name like 'b%' OR city.Region.Id != 1", sessionHolder), "((fare > 0 OR city.Name: \"b*\") OR NOT city.Region.Id: 1)", null, sessionHolder);
    }

    @Test
    public void testAndPushDown()
    {
        SessionHolder sessionHolder = new SessionHolder();

        testFilter(tryPushDown("fare > 0 AND city.Name like 'b%'", sessionHolder), "(fare > 0 AND city.Name: \"b*\")", null, sessionHolder);
        testFilter(tryPushDown("lower(city.Region.Name) = 'hello world' AND city.Region.Id != 1", sessionHolder), "(NOT city.Region.Id: 1)", "lower(city.Region.Name) = 'hello world'", sessionHolder);

        // Multiple ANDs
        testFilter(tryPushDown("fare > 0 AND city.Name like 'b%' AND lower(city.Region.Name) = 'hello world' AND city.Region.Id != 1", sessionHolder), "(((fare > 0 AND city.Name: \"b*\")) AND NOT city.Region.Id: 1)", "(lower(city.Region.Name) = 'hello world')", sessionHolder);
        testFilter(tryPushDown("fare > 0 AND city.Name like '%b%' AND lower(city.Region.Name) = 'hello world' AND city.Region.Id != 1", sessionHolder), "(((fare > 0)) AND NOT city.Region.Id: 1)", "city.Name like '%b%' AND lower(city.Region.Name) = 'hello world'", sessionHolder);
    }

    @Test
    public void testNotPushDown()
    {
        SessionHolder sessionHolder = new SessionHolder();

        testFilter(tryPushDown("city.Region.Name NOT LIKE 'hello%'", sessionHolder), "NOT city.Region.Name: \"hello*\"", null, sessionHolder);
        testFilter(tryPushDown("NOT (city.Region.Name LIKE 'hello%')", sessionHolder), "NOT city.Region.Name: \"hello*\"", null, sessionHolder);
        testFilter(tryPushDown("city.Name != 'hello world'", sessionHolder), "NOT city.Name: \"hello world\"", null, sessionHolder);
        testFilter(tryPushDown("city.Name <> 'hello world'", sessionHolder), "NOT city.Name: \"hello world\"", null, sessionHolder);
        testFilter(tryPushDown("NOT (city.Name = 'hello world')", sessionHolder), "NOT city.Name: \"hello world\"", null, sessionHolder);
        testFilter(tryPushDown("fare != 0", sessionHolder), "NOT fare: 0", null, sessionHolder);
        testFilter(tryPushDown("fare <> 0", sessionHolder), "NOT fare: 0", null, sessionHolder);
        testFilter(tryPushDown("NOT (fare = 0)", sessionHolder), "NOT fare: 0", null, sessionHolder);

        // Multiple NOTs
        testFilter(tryPushDown("NOT (NOT fare = 0)", sessionHolder), "NOT NOT fare: 0", null, sessionHolder);
        testFilter(tryPushDown("NOT (fare = 0 AND city.Name = 'hello world')", sessionHolder), "NOT (fare: 0 AND city.Name: \"hello world\")", null, sessionHolder);
        testFilter(tryPushDown("NOT (fare = 0 OR city.Name = 'hello world')", sessionHolder), "NOT (fare: 0 OR city.Name: \"hello world\")", null, sessionHolder);
    }

    @Test
    public void testInPushDown()
    {
        SessionHolder sessionHolder = new SessionHolder();

        testFilter(tryPushDown("city.Name IN ('hello world', 'hello world 2')", sessionHolder), "(city.Name: \"hello world\" OR city.Name: \"hello world 2\")", null, sessionHolder);
    }

    @Test
    public void testIsNullPushDown()
    {
        SessionHolder sessionHolder = new SessionHolder();

        testFilter(tryPushDown("city.Name IS NULL", sessionHolder), "NOT city.Name: *", null, sessionHolder);
        testFilter(tryPushDown("city.Name IS NOT NULL", sessionHolder), "NOT NOT city.Name: *", null, sessionHolder);
        testFilter(tryPushDown("NOT (city.Name IS NULL)", sessionHolder), "NOT NOT city.Name: *", null, sessionHolder);
    }

    @Test
    public void testComplexPushDown()
    {
        SessionHolder sessionHolder = new SessionHolder();

        testFilter(tryPushDown("(fare > 0 OR city.Name like 'b%') AND (lower(city.Region.Name) = 'hello world' OR city.Name IS NULL)", sessionHolder), "((fare > 0 OR city.Name: \"b*\"))", "(lower(city.Region.Name) = 'hello world' OR city.Name IS NULL)", sessionHolder);
        testFilter(tryPushDown("city.Region.Id = 1 AND (fare > 0 OR city.Name NOT like 'b%') AND (lower(city.Region.Name) = 'hello world' OR city.Name IS NULL)", sessionHolder), "((city.Region.Id: 1 AND (fare > 0 OR NOT city.Name: \"b*\")))", "lower(city.Region.Name) = 'hello world' OR city.Name IS NULL", sessionHolder);
    }

    public void testMetadataSqlGeneration()
    {
        SessionHolder sessionHolder = new SessionHolder();
        testFilterWithMetadataSql(
                tryPushDown("(fare > 0 AND city.Name like 'b%')",
                        sessionHolder),
                sessionHolder,
                "(fare > 0 AND city.Name: \"b*\")",
                "(\"fare\" > 0)");
        testFilterWithMetadataSql(
                tryPushDown("(fare > 0 OR city.Name like 'b%')",
                        sessionHolder),
                sessionHolder,
                "(fare > 0 OR city.Name: \"b*\")",
                null);
        testFilterWithMetadataSql(
                tryPushDown("(fare > 0 AND city.Name like 'b%') OR city.Region.Id = 1",
                        sessionHolder),
                sessionHolder,
                "((fare > 0 AND city.Name: \"b*\") OR city.Region.Id: 1)",
                null);
        testFilterWithMetadataSql(
                tryPushDown("fare = 0 AND (city.Name like 'b%' OR city.Region.Id = 1)",
                        sessionHolder),
                sessionHolder,
                "(fare: 0 AND (city.Name: \"b*\" OR city.Region.Id: 1))",
                "(\"fare\" = 0)");
    }

    private ClpExpression tryPushDown(String sqlExpression, SessionHolder sessionHolder)
    {
        RowExpression pushDownExpression = getRowExpression(sqlExpression, sessionHolder);
        ClpExpression clpExpression = pushDownExpression.accept(new ClpFilterToKqlConverter(
                        standardFunctionResolution,
                        functionAndTypeManager,
                        variableToColumnHandleMap,
                        ImmutableSet.of("fare")),
                null);
        return clpExpression;
    }

    private void testFilter(ClpExpression clpExpression, String expectedKqlExpression, String expectedRemainingExpression, SessionHolder sessionHolder)
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

    private void testFilterWithMetadataSql(
            ClpExpression clpExpression,
            SessionHolder sessionHolder,
            String expectedKqlExpression,
            String expectedMetadataSqlExpression)
    {
        testFilter(clpExpression, expectedKqlExpression, null, sessionHolder);
        if (clpExpression.getMetadataSql().isPresent()) {
            assertEquals(clpExpression.getMetadataSql().get(), expectedMetadataSqlExpression);
        }
        else {
            assertNull(expectedMetadataSqlExpression);
        }
    }
}
