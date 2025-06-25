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
import com.facebook.presto.sql.analyzer.SemanticException;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.Set;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
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
        testFilter(tryPushDown("city.Name = 'hello world'", sessionHolder, ImmutableSet.of()), "city.Name: \"hello world\"", null, sessionHolder, true);
        testFilter(tryPushDown("'hello world' = city.Name", sessionHolder, ImmutableSet.of()), "city.Name: \"hello world\"", null, sessionHolder, true);

        // Like predicates that are transformed into substring match
        testFilter(tryPushDown("city.Name like 'hello%'", sessionHolder, ImmutableSet.of()), "city.Name: \"hello*\"", null, sessionHolder, true);
        testFilter(tryPushDown("city.Name like '%hello'", sessionHolder, ImmutableSet.of()), "city.Name: \"*hello\"", null, sessionHolder, true);

        // Like predicates that are transformed into CARDINALITY(SPLIT(x, 'some string', 2)) = 2 form, and they are not pushed down for now
        testFilter(tryPushDown("city.Name like '%hello%'", sessionHolder, ImmutableSet.of()), null, "city.Name like '%hello%'", sessionHolder, true);

        // Like predicates that are kept in the original forms
        testFilter(tryPushDown("city.Name like 'hello_'", sessionHolder, ImmutableSet.of()), "city.Name: \"hello?\"", null, sessionHolder, true);
        testFilter(tryPushDown("city.Name like '_hello'", sessionHolder, ImmutableSet.of()), "city.Name: \"?hello\"", null, sessionHolder, true);
        testFilter(tryPushDown("city.Name like 'hello_w%'", sessionHolder, ImmutableSet.of()), "city.Name: \"hello?w*\"", null, sessionHolder, true);
        testFilter(tryPushDown("city.Name like '%hello_w'", sessionHolder, ImmutableSet.of()), "city.Name: \"*hello?w\"", null, sessionHolder, true);
        testFilter(tryPushDown("city.Name like 'hello%world'", sessionHolder, ImmutableSet.of()), "city.Name: \"hello*world\"", null, sessionHolder, true);
        testFilter(tryPushDown("city.Name like 'hello%wor%ld'", sessionHolder, ImmutableSet.of()), "city.Name: \"hello*wor*ld\"", null, sessionHolder, true);
    }

    @Test
    public void testSubStringPushDown()
    {
        SessionHolder sessionHolder = new SessionHolder();

        testFilter(tryPushDown("substr(city.Name, 1, 2) = 'he'", sessionHolder, ImmutableSet.of()), "city.Name: \"he*\"", null, sessionHolder, true);
        testFilter(tryPushDown("substr(city.Name, 5, 2) = 'he'", sessionHolder, ImmutableSet.of()), "city.Name: \"????he*\"", null, sessionHolder, true);
        testFilter(tryPushDown("substr(city.Name, 5) = 'he'", sessionHolder, ImmutableSet.of()), "city.Name: \"????he\"", null, sessionHolder, true);
        testFilter(tryPushDown("substr(city.Name, -2) = 'he'", sessionHolder, ImmutableSet.of()), "city.Name: \"*he\"", null, sessionHolder, true);

        // Invalid substring index is not pushed down
        testFilter(tryPushDown("substr(city.Name, 1, 5) = 'he'", sessionHolder, ImmutableSet.of()), null, "substr(city.Name, 1, 5) = 'he'", sessionHolder, true);
        testFilter(tryPushDown("substr(city.Name, -5) = 'he'", sessionHolder, ImmutableSet.of()), null, "substr(city.Name, -5) = 'he'", sessionHolder, true);
    }

    @Test
    public void testNumericComparisonPushDown()
    {
        SessionHolder sessionHolder = new SessionHolder();

        testFilter(tryPushDown("fare > 0", sessionHolder, ImmutableSet.of()), "fare > 0", null, sessionHolder, true);
        testFilter(tryPushDown("fare >= 0", sessionHolder, ImmutableSet.of()), "fare >= 0", null, sessionHolder, true);
        testFilter(tryPushDown("fare < 0", sessionHolder, ImmutableSet.of()), "fare < 0", null, sessionHolder, true);
        testFilter(tryPushDown("fare <= 0", sessionHolder, ImmutableSet.of()), "fare <= 0", null, sessionHolder, true);
        testFilter(tryPushDown("fare = 0", sessionHolder, ImmutableSet.of()), "fare: 0", null, sessionHolder, true);
        testFilter(tryPushDown("fare != 0", sessionHolder, ImmutableSet.of()), "NOT fare: 0", null, sessionHolder, true);
        testFilter(tryPushDown("fare <> 0", sessionHolder, ImmutableSet.of()), "NOT fare: 0", null, sessionHolder, true);
        testFilter(tryPushDown("0 < fare", sessionHolder, ImmutableSet.of()), "fare > 0", null, sessionHolder, true);
        testFilter(tryPushDown("0 <= fare", sessionHolder, ImmutableSet.of()), "fare >= 0", null, sessionHolder, true);
        testFilter(tryPushDown("0 > fare", sessionHolder, ImmutableSet.of()), "fare < 0", null, sessionHolder, true);
        testFilter(tryPushDown("0 >= fare", sessionHolder, ImmutableSet.of()), "fare <= 0", null, sessionHolder, true);
        testFilter(tryPushDown("0 = fare", sessionHolder, ImmutableSet.of()), "fare: 0", null, sessionHolder, true);
        testFilter(tryPushDown("0 != fare", sessionHolder, ImmutableSet.of()), "NOT fare: 0", null, sessionHolder, true);
        testFilter(tryPushDown("0 <> fare", sessionHolder, ImmutableSet.of()), "NOT fare: 0", null, sessionHolder, true);
    }

    @Test
    public void testBetweenPushDown()
    {
        SessionHolder sessionHolder = new SessionHolder();

        // Normal cases
        testFilter(tryPushDown("fare BETWEEN 0 AND 5", sessionHolder, ImmutableSet.of()), "fare >= 0 AND fare <= 5", null, sessionHolder, true);
        testFilter(tryPushDown("fare BETWEEN 5 AND 0", sessionHolder, ImmutableSet.of()), "fare >= 5 AND fare <= 0", null, sessionHolder, true);

        // No push down cases
        testFilter(tryPushDown("NULL BETWEEN 0 AND 5", sessionHolder, ImmutableSet.of()), null, "BETWEEN(null, 0, 5)", sessionHolder, false);
        testFilter(tryPushDown("0 BETWEEN NULL AND 5", sessionHolder, ImmutableSet.of()), null, "BETWEEN(0, null, 5)", sessionHolder, false);

        // Illegal cases
        assertThrows(SemanticException.class, () -> {
            testFilter(
                    tryPushDown("fare BETWEEN 'Apple' AND 'Pear'", sessionHolder, ImmutableSet.of()),
                    "fare >= 0 AND fare <= 5",
                    null,
                    sessionHolder,
                    true);
        });
    }

    @Test
    public void testOrPushDown()
    {
        SessionHolder sessionHolder = new SessionHolder();

        testFilter(tryPushDown("fare > 0 OR city.Name like 'b%'",
                sessionHolder,
                ImmutableSet.of()),
                "(fare > 0 OR city.Name: \"b*\")",
                null,
                sessionHolder,
                true);
        testFilter(tryPushDown(
                "lower(city.Region.Name) = 'hello world' OR city.Region.Id != 1",
                sessionHolder,
                ImmutableSet.of()),
                null,
                "(lower(city.Region.Name) = 'hello world' OR city.Region.Id != 1)",
                sessionHolder,
                true);

        // Multiple ORs
        testFilter(tryPushDown(
                "fare > 0 OR city.Name like 'b%' OR lower(city.Region.Name) = 'hello world' OR city.Region.Id != 1",
                sessionHolder,
                ImmutableSet.of()),
                null,
                "fare > 0 OR city.Name like 'b%' OR lower(city.Region.Name) = 'hello world' OR city.Region.Id != 1",
                sessionHolder,
                true);
        testFilter(tryPushDown(
                "fare > 0 OR city.Name like 'b%' OR city.Region.Id != 1",
                sessionHolder,
                ImmutableSet.of()),
                "((fare > 0 OR city.Name: \"b*\") OR NOT city.Region.Id: 1)",
                null,
                sessionHolder,
                true);
    }

    @Test
    public void testAndPushDown()
    {
        SessionHolder sessionHolder = new SessionHolder();

        testFilter(tryPushDown(
                "fare > 0 AND city.Name like 'b%'",
                sessionHolder,
                ImmutableSet.of()),
                "(fare > 0 AND city.Name: \"b*\")",
                null,
                sessionHolder,
                true);
        testFilter(tryPushDown(
                "lower(city.Region.Name) = 'hello world' AND city.Region.Id != 1",
                sessionHolder,
                ImmutableSet.of()),
                "(NOT city.Region.Id: 1)",
                "lower(city.Region.Name) = 'hello world'",
                sessionHolder,
                true);

        // Multiple ANDs
        testFilter(tryPushDown(
                "fare > 0 AND city.Name like 'b%' AND lower(city.Region.Name) = 'hello world' AND city.Region.Id != 1",
                sessionHolder,
                ImmutableSet.of()),
                "(((fare > 0 AND city.Name: \"b*\")) AND NOT city.Region.Id: 1)",
                "(lower(city.Region.Name) = 'hello world')",
                sessionHolder,
                true);
        testFilter(tryPushDown(
                "fare > 0 AND city.Name like '%b%' AND lower(city.Region.Name) = 'hello world' AND city.Region.Id != 1",
                sessionHolder,
                ImmutableSet.of()),
                "(((fare > 0)) AND NOT city.Region.Id: 1)",
                "city.Name like '%b%' AND lower(city.Region.Name) = 'hello world'",
                sessionHolder,
                true);
    }

    @Test
    public void testNotPushDown()
    {
        SessionHolder sessionHolder = new SessionHolder();

        testFilter(tryPushDown("city.Region.Name NOT LIKE 'hello%'", sessionHolder, ImmutableSet.of()), "NOT city.Region.Name: \"hello*\"", null, sessionHolder, true);
        testFilter(tryPushDown("NOT (city.Region.Name LIKE 'hello%')", sessionHolder, ImmutableSet.of()), "NOT city.Region.Name: \"hello*\"", null, sessionHolder, true);
        testFilter(tryPushDown("city.Name != 'hello world'", sessionHolder, ImmutableSet.of()), "NOT city.Name: \"hello world\"", null, sessionHolder, true);
        testFilter(tryPushDown("city.Name <> 'hello world'", sessionHolder, ImmutableSet.of()), "NOT city.Name: \"hello world\"", null, sessionHolder, true);
        testFilter(tryPushDown("NOT (city.Name = 'hello world')", sessionHolder, ImmutableSet.of()), "NOT city.Name: \"hello world\"", null, sessionHolder, true);
        testFilter(tryPushDown("fare != 0", sessionHolder, ImmutableSet.of()), "NOT fare: 0", null, sessionHolder, true);
        testFilter(tryPushDown("fare <> 0", sessionHolder, ImmutableSet.of()), "NOT fare: 0", null, sessionHolder, true);
        testFilter(tryPushDown("NOT (fare = 0)", sessionHolder, ImmutableSet.of()), "NOT fare: 0", null, sessionHolder, true);

        // Multiple NOTs
        testFilter(tryPushDown("NOT (NOT fare = 0)", sessionHolder, ImmutableSet.of()), "NOT NOT fare: 0", null, sessionHolder, true);
        testFilter(tryPushDown(
                "NOT (fare = 0 AND city.Name = 'hello world')",
                sessionHolder,
                ImmutableSet.of()),
                "NOT (fare: 0 AND city.Name: \"hello world\")",
                null,
                sessionHolder,
                true);
        testFilter(tryPushDown(
                "NOT (fare = 0 OR city.Name = 'hello world')",
                sessionHolder,
                ImmutableSet.of()),
                "NOT (fare: 0 OR city.Name: \"hello world\")",
                null,
                sessionHolder,
                true);
    }

    @Test
    public void testInPushDown()
    {
        SessionHolder sessionHolder = new SessionHolder();

        testFilter(tryPushDown(
                "city.Name IN ('hello world', 'hello world 2')",
                sessionHolder,
                ImmutableSet.of()),
                "(city.Name: \"hello world\" OR city.Name: \"hello world 2\")",
                null,
                sessionHolder,
                true);
    }

    @Test
    public void testIsNullPushDown()
    {
        SessionHolder sessionHolder = new SessionHolder();

        testFilter(tryPushDown("city.Name IS NULL", sessionHolder, ImmutableSet.of()), "NOT city.Name: *", null, sessionHolder, true);
        testFilter(tryPushDown("city.Name IS NOT NULL", sessionHolder, ImmutableSet.of()), "NOT NOT city.Name: *", null, sessionHolder, true);
        testFilter(tryPushDown("NOT (city.Name IS NULL)", sessionHolder, ImmutableSet.of()), "NOT NOT city.Name: *", null, sessionHolder, true);
    }

    @Test
    public void testComplexPushDown()
    {
        SessionHolder sessionHolder = new SessionHolder();

        testFilter(tryPushDown(
                "(fare > 0 OR city.Name like 'b%') AND (lower(city.Region.Name) = 'hello world' OR city.Name IS NULL)",
                sessionHolder,
                ImmutableSet.of()),
                "((fare > 0 OR city.Name: \"b*\"))", "(lower(city.Region.Name) = 'hello world' OR city.Name IS NULL)",
                sessionHolder,
                true);
        testFilter(tryPushDown(
                "city.Region.Id = 1 AND (fare > 0 OR city.Name NOT like 'b%') AND (lower(city.Region.Name) = 'hello world' OR city.Name IS NULL)",
                sessionHolder,
                ImmutableSet.of()),
                "((city.Region.Id: 1 AND (fare > 0 OR NOT city.Name: \"b*\")))",
                "lower(city.Region.Name) = 'hello world' OR city.Name IS NULL",
                sessionHolder,
                true);
    }

    @Test
    public void testMetadataSqlGeneration()
    {
        SessionHolder sessionHolder = new SessionHolder();
        testFilterWithMetadataSql(
                tryPushDown("(fare > 0 AND city.Name like 'b%')",
                        sessionHolder, ImmutableSet.of("fare")),
                sessionHolder,
                "(fare > 0 AND city.Name: \"b*\")",
                "(\"fare\" > 0)");
        testFilterWithMetadataSql(
                tryPushDown("(fare > 0 OR city.Name like 'b%')",
                        sessionHolder, ImmutableSet.of("fare")),
                sessionHolder,
                "(fare > 0 OR city.Name: \"b*\")",
                null);
        testFilterWithMetadataSql(
                tryPushDown("(fare > 0 AND city.Name like 'b%') OR city.Region.Id = 1",
                        sessionHolder, ImmutableSet.of("fare")),
                sessionHolder,
                "((fare > 0 AND city.Name: \"b*\") OR city.Region.Id: 1)",
                null);
        testFilterWithMetadataSql(
                tryPushDown("fare = 0 AND (city.Name like 'b%' OR city.Region.Id = 1)",
                        sessionHolder, ImmutableSet.of("fare")),
                sessionHolder,
                "(fare: 0 AND (city.Name: \"b*\" OR city.Region.Id: 1))",
                "(\"fare\" = 0)");
    }

    private ClpExpression tryPushDown(String sqlExpression, SessionHolder sessionHolder, Set<String> metadataFilterColumns)
    {
        RowExpression pushDownExpression = getRowExpression(sqlExpression, sessionHolder);
        return pushDownExpression.accept(new ClpFilterToKqlConverter(
                        standardFunctionResolution,
                        functionAndTypeManager,
                        variableToColumnHandleMap,
                        metadataFilterColumns),
                null);
    }

    private void testFilter(
            ClpExpression clpExpression,
            String expectedKqlExpression,
            String expectedRemainingExpression,
            SessionHolder sessionHolder,
            boolean remainingToRowExpression)
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
            if (remainingToRowExpression) {
                assertEquals(remainingExpression.get(), getRowExpression(expectedRemainingExpression, sessionHolder));
            }
            else {
                assertEquals(remainingExpression.get().toString(), expectedRemainingExpression);
            }
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
        testFilter(clpExpression, expectedKqlExpression, null, sessionHolder, true);
        if (clpExpression.getMetadataSql().isPresent()) {
            assertEquals(clpExpression.getMetadataSql().get(), expectedMetadataSqlExpression);
        }
        else {
            assertNull(expectedMetadataSqlExpression);
        }
    }
}
