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
package com.yscope.presto;

import com.facebook.presto.spi.relation.RowExpression;
import org.testng.annotations.Test;

import java.util.Optional;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test
public class TestClpPlanOptimizer
        extends TestClpQueryBase
{
    private void testFilter(String sqlExpression, Optional<String> expectedKqlExpression,
                            Optional<String> expectedRemainingExpression, SessionHolder sessionHolder)
    {
        RowExpression pushDownExpression = getRowExpression(sqlExpression, sessionHolder);
        ClpExpression clpExpression = pushDownExpression.accept(new ClpFilterToKqlConverter(
                        standardFunctionResolution,
                        functionAndTypeManager,
                        variableToColumnHandleMap),
                null);
        Optional<String> kqlExpression = clpExpression.getDefinition();
        Optional<RowExpression> remainingExpression = clpExpression.getRemainingExpression();
        if (expectedKqlExpression.isPresent()) {
            assertTrue(kqlExpression.isPresent());
            assertEquals(kqlExpression.get(), expectedKqlExpression.get());
        }

        if (expectedRemainingExpression.isPresent()) {
            assertTrue(remainingExpression.isPresent());
            assertEquals(remainingExpression.get(), getRowExpression(expectedRemainingExpression.get(), sessionHolder));
        }
        else {
            assertFalse(remainingExpression.isPresent());
        }
    }

    @Test
    public void testStringMatchPushdown()
    {
        SessionHolder sessionHolder = new SessionHolder();

        // Exact match
        testFilter("city = 'hello world'", Optional.of("city: \"hello world\""), Optional.empty(), sessionHolder);
        testFilter("'hello world' = city", Optional.of("city: \"hello world\""), Optional.empty(), sessionHolder);

        // Like predicates that are transformed into substring match
        testFilter("city like 'hello%'", Optional.of("city: \"hello*\""), Optional.empty(), sessionHolder);
        testFilter("city like '%hello'", Optional.of("city: \"*hello\""), Optional.empty(), sessionHolder);

        // Like predicates that are transformed into CARDINALITY(SPLIT(x, 'some string', 2)) = 2 form, and they are not pushed down for now
        testFilter("city like '%hello%'", Optional.empty(), Optional.of("city like '%hello%'"), sessionHolder);

        // Like predicates that are kept in the original forms
        testFilter("city like 'hello_'", Optional.of("city: \"hello?\""), Optional.empty(), sessionHolder);
        testFilter("city like '_hello'", Optional.of("city: \"?hello\""), Optional.empty(), sessionHolder);
        testFilter("city like 'hello_w%'", Optional.of("city: \"hello?w*\""), Optional.empty(), sessionHolder);
        testFilter("city like '%hello_w'", Optional.of("city: \"*hello?w\""), Optional.empty(), sessionHolder);
        testFilter("city like 'hello%world'", Optional.of("city: \"hello*world\""), Optional.empty(), sessionHolder);
        testFilter("city like 'hello%wor%ld'", Optional.of("city: \"hello*wor*ld\""), Optional.empty(), sessionHolder);
    }

    @Test
    public void testSubStringPushdown()
    {
        SessionHolder sessionHolder = new SessionHolder();

        testFilter("substr(city, 1, 2) = 'he'", Optional.of("city: \"he*\""), Optional.empty(), sessionHolder);
        testFilter("substr(city, 5, 2) = 'he'", Optional.of("city: \"????he*\""), Optional.empty(), sessionHolder);
        testFilter("substr(city, 5) = 'he'", Optional.of("city: \"????he\""), Optional.empty(), sessionHolder);
        testFilter("substr(city, -2) = 'he'", Optional.of("city: \"*he\""), Optional.empty(), sessionHolder);

        // Invalid substring index is not pushed down
        testFilter("substr(city, 1, 5) = 'he'", Optional.empty(), Optional.of("substr(city, 1, 5) = 'he'"), sessionHolder);
        testFilter("substr(city, -5) = 'he'", Optional.empty(), Optional.of("substr(city, -5) = 'he'"), sessionHolder);
    }

    @Test
    public void testNumericComparisonPushdown()
    {
        SessionHolder sessionHolder = new SessionHolder();

        testFilter("fare > 0", Optional.of("fare > 0"), Optional.empty(), sessionHolder);
        testFilter("fare >= 0", Optional.of("fare >= 0"), Optional.empty(), sessionHolder);
        testFilter("fare < 0", Optional.of("fare < 0"), Optional.empty(), sessionHolder);
        testFilter("fare <= 0", Optional.of("fare <= 0"), Optional.empty(), sessionHolder);
        testFilter("fare = 0", Optional.of("fare: 0"), Optional.empty(), sessionHolder);
        testFilter("fare != 0", Optional.of("NOT fare: 0"), Optional.empty(), sessionHolder);
        testFilter("fare <> 0", Optional.of("NOT fare: 0"), Optional.empty(), sessionHolder);
        testFilter("0 < fare", Optional.of("fare > 0"), Optional.empty(), sessionHolder);
        testFilter("0 <= fare", Optional.of("fare >= 0"), Optional.empty(), sessionHolder);
        testFilter("0 > fare", Optional.of("fare < 0"), Optional.empty(), sessionHolder);
        testFilter("0 >= fare", Optional.of("fare <= 0"), Optional.empty(), sessionHolder);
        testFilter("0 = fare", Optional.of("fare: 0"), Optional.empty(), sessionHolder);
        testFilter("0 != fare", Optional.of("NOT fare: 0"), Optional.empty(), sessionHolder);
        testFilter("0 <> fare", Optional.of("NOT fare: 0"), Optional.empty(), sessionHolder);
    }

    @Test
    public void testOrPushdown()
    {
        SessionHolder sessionHolder = new SessionHolder();

        testFilter("fare > 0 OR city like 'b%'", Optional.of("(fare > 0 OR city: \"b*\")"), Optional.empty(),
                sessionHolder);
        testFilter("lower(\"region.Name\") = 'hello world' OR \"region.Id\" != 1", Optional.empty(), Optional.of("(lower(\"region.Name\") = 'hello world' OR \"region.Id\" != 1)"),
                sessionHolder);

        // Multiple ORs
        testFilter("fare > 0 OR city like 'b%' OR lower(\"region.Name\") = 'hello world' OR \"region.Id\" != 1",
                Optional.empty(),
                Optional.of("fare > 0 OR city like 'b%' OR lower(\"region.Name\") = 'hello world' OR \"region.Id\" != 1"),
                sessionHolder);
        testFilter("fare > 0 OR city like 'b%' OR \"region.Id\" != 1",
                Optional.of("((fare > 0 OR city: \"b*\") OR NOT region.Id: 1)"),
                Optional.empty(),
                sessionHolder);
    }

    @Test
    public void testAndPushdown()
    {
        SessionHolder sessionHolder = new SessionHolder();

        testFilter("fare > 0 AND city like 'b%'", Optional.of("(fare > 0 AND city: \"b*\")"), Optional.empty(), sessionHolder);
        testFilter("lower(\"region.Name\") = 'hello world' AND \"region.Id\" != 1", Optional.of("(NOT region.Id: 1)"), Optional.of("lower(\"region.Name\") = 'hello world'"),
                sessionHolder);

        // Multiple ANDs
        testFilter("fare > 0 AND city like 'b%' AND lower(\"region.Name\") = 'hello world' AND \"region.Id\" != 1",
                Optional.of("(((fare > 0 AND city: \"b*\")) AND NOT region.Id: 1)"),
                Optional.of("(lower(\"region.Name\") = 'hello world')"),
                sessionHolder);
        testFilter("fare > 0 AND city like '%b%' AND lower(\"region.Name\") = 'hello world' AND \"region.Id\" != 1",
                Optional.of("(((fare > 0)) AND NOT region.Id: 1)"),
                Optional.of("city like '%b%' AND lower(\"region.Name\") = 'hello world'"),
                sessionHolder);
    }

    @Test
    public void testNotPushdown()
    {
        SessionHolder sessionHolder = new SessionHolder();

        testFilter("\"region.Name\" NOT LIKE 'hello%'", Optional.of("NOT region.Name: \"hello*\""), Optional.empty(), sessionHolder);
        testFilter("NOT (\"region.Name\" LIKE 'hello%')", Optional.of("NOT region.Name: \"hello*\""), Optional.empty(), sessionHolder);
        testFilter("city != 'hello world'", Optional.of("NOT city: \"hello world\""), Optional.empty(), sessionHolder);
        testFilter("city <> 'hello world'", Optional.of("NOT city: \"hello world\""), Optional.empty(), sessionHolder);
        testFilter("NOT (city = 'hello world')", Optional.of("NOT city: \"hello world\""), Optional.empty(), sessionHolder);
        testFilter("fare != 0", Optional.of("NOT fare: 0"), Optional.empty(), sessionHolder);
        testFilter("fare <> 0", Optional.of("NOT fare: 0"), Optional.empty(), sessionHolder);
        testFilter("NOT (fare = 0)", Optional.of("NOT fare: 0"), Optional.empty(), sessionHolder);

        // Multiple NOTs
        testFilter("NOT (NOT fare = 0)", Optional.of("NOT NOT fare: 0"), Optional.empty(), sessionHolder);
        testFilter("NOT (fare = 0 AND city = 'hello world')", Optional.of("NOT (fare: 0 AND city: \"hello world\")"), Optional.empty(), sessionHolder);
    }

    @Test
    public void testInPushdown()
    {
        SessionHolder sessionHolder = new SessionHolder();

        testFilter("city IN ('hello world', 'hello world 2')", Optional.of("(city: \"hello world\" OR city: \"hello world 2\")"), Optional.empty(), sessionHolder);
    }

    @Test
    public void testIsNullPushdown()
    {
        SessionHolder sessionHolder = new SessionHolder();

        testFilter("city IS NULL", Optional.of("NOT city: *"), Optional.empty(), sessionHolder);
        testFilter("city IS NOT NULL", Optional.of("NOT NOT city: *"), Optional.empty(), sessionHolder);
    }

    @Test
    public void testComplexPushdown()
    {
        testFilter("(fare > 0 OR city like 'b%') AND (lower(\"region.Name\") = 'hello world' OR city IS NULL)",
                Optional.of("((fare > 0 OR city: \"b*\"))"),
                Optional.of("(lower(\"region.Name\") = 'hello world' OR city IS NULL)"),
                new SessionHolder());
        // complex cases with and, or and not
        testFilter("\"region.Id\" = 1 AND (fare > 0 OR city not like 'b%') AND (lower(\"region.Name\") = 'hello world' OR city IS NULL)",
                Optional.of("((region.Id: 1 AND (fare > 0 OR NOT city: \"b*\")))"),
                Optional.of("lower(\"region.Name\") = 'hello world' OR city IS NULL"),
                new SessionHolder());
    }
}
