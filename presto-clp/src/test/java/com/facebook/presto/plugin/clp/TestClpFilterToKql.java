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
import static org.testng.Assert.assertTrue;

@Test
public class TestClpFilterToKql
        extends TestClpQueryBase
{
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

    private void testFilter(ClpExpression clpExpression,
                            SessionHolder sessionHolder,
                            Optional<String> expectedKqlExpression,
                            Optional<String> expectedRemainingExpression)
    {
        Optional<String> kqlExpression = clpExpression.getDefinition();
        Optional<RowExpression> remainingExpression = clpExpression.getRemainingExpression();
        if (expectedKqlExpression.isPresent()) {
            assertTrue(kqlExpression.isPresent());
            assertEquals(kqlExpression.get(), expectedKqlExpression.get());
        }
        else {
            assertFalse(kqlExpression.isPresent());
        }

        if (expectedRemainingExpression.isPresent()) {
            assertTrue(remainingExpression.isPresent());
            assertEquals(remainingExpression.get(), getRowExpression(expectedRemainingExpression.get(), sessionHolder));
        }
        else {
            assertFalse(remainingExpression.isPresent());
        }
    }

    private void testFilterWithMetadataSql(
            ClpExpression clpExpression,
            SessionHolder sessionHolder,
            Optional<String> expectedKqlExpression,
            Optional<String> expectedMetadataSqlExpression,
            Optional<String> expectedRemainingExpression)
    {
        testFilter(clpExpression, sessionHolder, expectedKqlExpression, expectedRemainingExpression);
        if (clpExpression.getMetadataSql().isPresent()) {
            assertEquals(clpExpression.getMetadataSql().get(), expectedMetadataSqlExpression.get());
        }
        else {
            assertFalse(expectedMetadataSqlExpression.isPresent());
        }
    }

    @Test
    public void testStringMatchPushDown()
    {
        SessionHolder sessionHolder = new SessionHolder();

        // Exact match
        testFilter(tryPushDown("city.Name = 'hello world'", sessionHolder), sessionHolder, Optional.of("city.Name: \"hello world\""), Optional.empty());
        testFilter(tryPushDown("'hello world' = city.Name", sessionHolder), sessionHolder, Optional.of("city.Name: \"hello world\""), Optional.empty());

        // Like predicates that are transformed into substring match
        testFilter(tryPushDown("city.Name like 'hello%'", sessionHolder), sessionHolder, Optional.of("city.Name: \"hello*\""), Optional.empty());
        testFilter(tryPushDown("city.Name like '%hello'", sessionHolder), sessionHolder, Optional.of("city.Name: \"*hello\""), Optional.empty());

        // Like predicates that are transformed into CARDINALITY(SPLIT(x, 'some string', 2)) = 2 form, and they are not pushed down for now
        testFilter(tryPushDown("city.Name like '%hello%'", sessionHolder), sessionHolder, Optional.empty(), Optional.of("city.Name like '%hello%'"));

        // Like predicates that are kept in the original forms
        testFilter(tryPushDown("city.Name like 'hello_'", sessionHolder), sessionHolder, Optional.of("city.Name: \"hello?\""), Optional.empty());
        testFilter(tryPushDown("city.Name like '_hello'", sessionHolder), sessionHolder, Optional.of("city.Name: \"?hello\""), Optional.empty());
        testFilter(tryPushDown("city.Name like 'hello_w%'", sessionHolder), sessionHolder, Optional.of("city.Name: \"hello?w*\""), Optional.empty());
        testFilter(tryPushDown("city.Name like '%hello_w'", sessionHolder), sessionHolder, Optional.of("city.Name: \"*hello?w\""), Optional.empty());
        testFilter(tryPushDown("city.Name like 'hello%world'", sessionHolder), sessionHolder, Optional.of("city.Name: \"hello*world\""), Optional.empty());
        testFilter(tryPushDown("city.Name like 'hello%wor%ld'", sessionHolder), sessionHolder, Optional.of("city.Name: \"hello*wor*ld\""), Optional.empty());
    }

    @Test
    public void testSubStringPushdown()
    {
        SessionHolder sessionHolder = new SessionHolder();

        testFilter(tryPushDown("substr(city.Name, 1, 2) = 'he'", sessionHolder), sessionHolder, Optional.of("city.Name: \"he*\""), Optional.empty());
        testFilter(tryPushDown("substr(city.Name, 5, 2) = 'he'", sessionHolder), sessionHolder, Optional.of("city.Name: \"????he*\""), Optional.empty());
        testFilter(tryPushDown("substr(city.Name, 5) = 'he'", sessionHolder), sessionHolder, Optional.of("city.Name: \"????he\""), Optional.empty());
        testFilter(tryPushDown("substr(city.Name, -2) = 'he'", sessionHolder), sessionHolder, Optional.of("city.Name: \"*he\""), Optional.empty());

        // Invalid substring index is not pushed down
        testFilter(tryPushDown("substr(city.Name, 1, 5) = 'he'", sessionHolder), sessionHolder, Optional.empty(), Optional.of("substr(city.Name, 1, 5) = 'he'"));
        testFilter(tryPushDown("substr(city.Name, -5) = 'he'", sessionHolder), sessionHolder, Optional.empty(), Optional.of("substr(city.Name, -5) = 'he'"));
    }

    @Test
    public void testNumericComparisonPushdown()
    {
        SessionHolder sessionHolder = new SessionHolder();

        testFilter(tryPushDown("fare > 0", sessionHolder), sessionHolder, Optional.of("fare > 0"), Optional.empty());
        testFilter(tryPushDown("fare >= 0", sessionHolder), sessionHolder, Optional.of("fare >= 0"), Optional.empty());
        testFilter(tryPushDown("fare < 0", sessionHolder), sessionHolder, Optional.of("fare < 0"), Optional.empty());
        testFilter(tryPushDown("fare <= 0", sessionHolder), sessionHolder, Optional.of("fare <= 0"), Optional.empty());
        testFilter(tryPushDown("fare = 0", sessionHolder), sessionHolder, Optional.of("fare: 0"), Optional.empty());
        testFilter(tryPushDown("fare != 0", sessionHolder), sessionHolder, Optional.of("NOT fare: 0"), Optional.empty());
        testFilter(tryPushDown("fare <> 0", sessionHolder), sessionHolder, Optional.of("NOT fare: 0"), Optional.empty());
        testFilter(tryPushDown("0 < fare", sessionHolder), sessionHolder, Optional.of("fare > 0"), Optional.empty());
        testFilter(tryPushDown("0 <= fare", sessionHolder), sessionHolder, Optional.of("fare >= 0"), Optional.empty());
        testFilter(tryPushDown("0 > fare", sessionHolder), sessionHolder, Optional.of("fare < 0"), Optional.empty());
        testFilter(tryPushDown("0 >= fare", sessionHolder), sessionHolder, Optional.of("fare <= 0"), Optional.empty());
        testFilter(tryPushDown("0 = fare", sessionHolder), sessionHolder, Optional.of("fare: 0"), Optional.empty());
        testFilter(tryPushDown("0 != fare", sessionHolder), sessionHolder, Optional.of("NOT fare: 0"), Optional.empty());
        testFilter(tryPushDown("0 <> fare", sessionHolder), sessionHolder, Optional.of("NOT fare: 0"), Optional.empty());
    }

    @Test
    public void testOrPushdown()
    {
        SessionHolder sessionHolder = new SessionHolder();

        testFilter(tryPushDown("fare > 0 OR city.Name like 'b%'", sessionHolder), sessionHolder, Optional.of("(fare > 0 OR city.Name: \"b*\")"), Optional.empty());
        testFilter(tryPushDown("lower(city.Region.Name) = 'hello world' OR city.Region.Id != 1", sessionHolder), sessionHolder, Optional.empty(), Optional.of("(lower(city.Region.Name) = 'hello world' OR city.Region.Id != 1)"));

        // Multiple ORs
        testFilter(tryPushDown("fare > 0 OR city.Name like 'b%' OR lower(city.Region.Name) = 'hello world' OR city.Region.Id != 1",
                        sessionHolder), sessionHolder, Optional.empty(),
                Optional.of("fare > 0 OR city.Name like 'b%' OR lower(city.Region.Name) = 'hello world' OR city.Region.Id != 1"));
        testFilter(tryPushDown("fare > 0 OR city.Name like 'b%' OR city.Region.Id != 1",
                        sessionHolder), sessionHolder, Optional.of("((fare > 0 OR city.Name: \"b*\") OR NOT city.Region.Id: 1)"),
                Optional.empty());
    }

    @Test
    public void testAndPushdown()
    {
        SessionHolder sessionHolder = new SessionHolder();

        testFilter(tryPushDown("fare > 0 AND city.Name like 'b%'", sessionHolder), sessionHolder, Optional.of("(fare > 0 AND city.Name: \"b*\")"), Optional.empty());
        testFilter(tryPushDown("lower(city.Region.Name) = 'hello world' AND city.Region.Id != 1", sessionHolder), sessionHolder, Optional.of("(NOT city.Region.Id: 1)"), Optional.of("lower(city.Region.Name) = 'hello world'"));

        // Multiple ANDs
        testFilter(tryPushDown("fare > 0 AND city.Name like 'b%' AND lower(city.Region.Name) = 'hello world' AND city.Region.Id != 1",
                        sessionHolder), sessionHolder, Optional.of("(((fare > 0 AND city.Name: \"b*\")) AND NOT city.Region.Id: 1)"),
                Optional.of("(lower(city.Region.Name) = 'hello world')"));
        testFilter(tryPushDown("fare > 0 AND city.Name like '%b%' AND lower(city.Region.Name) = 'hello world' AND city.Region.Id != 1",
                        sessionHolder), sessionHolder, Optional.of("(((fare > 0)) AND NOT city.Region.Id: 1)"),
                Optional.of("city.Name like '%b%' AND lower(city.Region.Name) = 'hello world'"));
    }

    @Test
    public void testNotPushdown()
    {
        SessionHolder sessionHolder = new SessionHolder();

        testFilter(tryPushDown("city.Region.Name NOT LIKE 'hello%'", sessionHolder), sessionHolder, Optional.of("NOT city.Region.Name: \"hello*\""), Optional.empty());
        testFilter(tryPushDown("NOT (city.Region.Name LIKE 'hello%')", sessionHolder), sessionHolder, Optional.of("NOT city.Region.Name: \"hello*\""), Optional.empty());
        testFilter(tryPushDown("city.Name != 'hello world'", sessionHolder), sessionHolder, Optional.of("NOT city.Name: \"hello world\""), Optional.empty());
        testFilter(tryPushDown("city.Name <> 'hello world'", sessionHolder), sessionHolder, Optional.of("NOT city.Name: \"hello world\""), Optional.empty());
        testFilter(tryPushDown("NOT (city.Name = 'hello world')", sessionHolder), sessionHolder, Optional.of("NOT city.Name: \"hello world\""), Optional.empty());
        testFilter(tryPushDown("fare != 0", sessionHolder), sessionHolder, Optional.of("NOT fare: 0"), Optional.empty());
        testFilter(tryPushDown("fare <> 0", sessionHolder), sessionHolder, Optional.of("NOT fare: 0"), Optional.empty());
        testFilter(tryPushDown("NOT (fare = 0)", sessionHolder), sessionHolder, Optional.of("NOT fare: 0"), Optional.empty());

        // Multiple NOTs
        testFilter(tryPushDown("NOT (NOT fare = 0)", sessionHolder), sessionHolder, Optional.of("NOT NOT fare: 0"), Optional.empty());
        testFilter(tryPushDown("NOT (fare = 0 AND city.Name = 'hello world')", sessionHolder), sessionHolder, Optional.of("NOT (fare: 0 AND city.Name: \"hello world\")"), Optional.empty());
        testFilter(tryPushDown("NOT (fare = 0 OR city.Name = 'hello world')", sessionHolder), sessionHolder, Optional.of("NOT (fare: 0 OR city.Name: \"hello world\")"), Optional.empty());
    }

    @Test
    public void testInPushdown()
    {
        SessionHolder sessionHolder = new SessionHolder();

        testFilter(tryPushDown("city.Name IN ('hello world', 'hello world 2')", sessionHolder), sessionHolder, Optional.of("(city.Name: \"hello world\" OR city.Name: \"hello world 2\")"), Optional.empty());
    }

    @Test
    public void testIsNullPushdown()
    {
        SessionHolder sessionHolder = new SessionHolder();

        testFilter(tryPushDown("city.Name IS NULL", sessionHolder), sessionHolder, Optional.of("NOT city.Name: *"), Optional.empty());
        testFilter(tryPushDown("city.Name IS NOT NULL", sessionHolder), sessionHolder, Optional.of("NOT NOT city.Name: *"), Optional.empty());
        testFilter(tryPushDown("NOT (city.Name IS NULL)", sessionHolder), sessionHolder, Optional.of("NOT NOT city.Name: *"), Optional.empty());
    }

    @Test
    public void testComplexPushdown()
    {
        SessionHolder sessionHolder = new SessionHolder();

        testFilter(tryPushDown("(fare > 0 OR city.Name like 'b%') AND (lower(city.Region.Name) = 'hello world' OR city.Name IS NULL)",
                        sessionHolder), sessionHolder, Optional.of("((fare > 0 OR city.Name: \"b*\"))"),
                Optional.of("(lower(city.Region.Name) = 'hello world' OR city.Name IS NULL)"));
        testFilter(tryPushDown("city.Region.Id = 1 AND (fare > 0 OR city.Name NOT like 'b%') AND (lower(city.Region.Name) = 'hello world' OR city.Name IS NULL)",
                        sessionHolder), sessionHolder, Optional.of("((city.Region.Id: 1 AND (fare > 0 OR NOT city.Name: \"b*\")))"),
                Optional.of("lower(city.Region.Name) = 'hello world' OR city.Name IS NULL"));
    }

    public void testMetadataSqlGeneration()
    {
        SessionHolder sessionHolder = new SessionHolder();
        testFilterWithMetadataSql(
                tryPushDown("(fare > 0 AND city.Name like 'b%')",
                        sessionHolder),
                sessionHolder,
                Optional.of("(fare > 0 AND city.Name: \"b*\")"),
                Optional.of("(\"fare\" > 0)"),
                Optional.empty());
        testFilterWithMetadataSql(
                tryPushDown("(fare > 0 OR city.Name like 'b%')",
                        sessionHolder),
                sessionHolder,
                Optional.of("(fare > 0 OR city.Name: \"b*\")"),
                Optional.empty(),
                Optional.empty());
        testFilterWithMetadataSql(
                tryPushDown("(fare > 0 AND city.Name like 'b%') OR city.Region.Id = 1",
                        sessionHolder),
                sessionHolder,
                Optional.of("((fare > 0 AND city.Name: \"b*\") OR city.Region.Id: 1)"),
                Optional.empty(),
                Optional.empty());
        testFilterWithMetadataSql(
                tryPushDown("fare = 0 AND (city.Name like 'b%' OR city.Region.Id = 1)",
                        sessionHolder),
                sessionHolder,
                Optional.of("(fare: 0 AND (city.Name: \"b*\" OR city.Region.Id: 1))"),
                Optional.of("(\"fare\" = 0)"),
                Optional.empty());
    }
}
