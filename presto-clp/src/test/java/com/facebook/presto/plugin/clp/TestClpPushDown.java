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

import com.facebook.presto.dispatcher.DispatchManager;
import com.facebook.presto.execution.SqlQueryManager;
import com.facebook.presto.plugin.clp.mockdb.ClpMockMetadataDatabase;
import com.facebook.presto.plugin.clp.mockdb.table.ColumnMetadataTableRows;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

import static com.facebook.presto.execution.QueryState.RUNNING;
import static com.facebook.presto.plugin.clp.ClpQueryRunner.createQueryRunner;
import static com.facebook.presto.plugin.clp.metadata.ClpSchemaTreeNodeType.Boolean;
import static com.facebook.presto.plugin.clp.metadata.ClpSchemaTreeNodeType.DateString;
import static com.facebook.presto.plugin.clp.metadata.ClpSchemaTreeNodeType.Float;
import static com.facebook.presto.plugin.clp.metadata.ClpSchemaTreeNodeType.Integer;
import static com.facebook.presto.plugin.clp.metadata.ClpSchemaTreeNodeType.VarString;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestClpPushDown
{
    private static final String TABLE_NAME = "test_pushdown";
    private static final Pattern BASE_PTR =
            Pattern.compile("(?<=base=)\\[B@[0-9a-fA-F]+");

    private ClpMockMetadataDatabase mockMetadataDatabase;
    private DistributedQueryRunner queryRunner;
    private SqlQueryManager queryManager;
    private DispatchManager dispatchManager;

    @BeforeClass
    public void setUp()
                throws Exception
    {
        mockMetadataDatabase = ClpMockMetadataDatabase
                .builder()
                .build();
        mockMetadataDatabase.addTableToDatasetsTableIfNotExist(ImmutableList.of(TABLE_NAME));
        mockMetadataDatabase.addColumnMetadata(ImmutableMap.of(TABLE_NAME, new ColumnMetadataTableRows(
                ImmutableList.of(
                        "city.Name",
                        "city.Region.id",
                        "city.Region.population_lowerbound",
                        "city.Region.population_upperbound",
                        "city.Region.Name",
                        "fare",
                        "isHoliday",
                        "ts"),
                ImmutableList.of(
                        VarString,
                        Integer,
                        Float,
                        Float,
                        VarString,
                        Float,
                        Boolean,
                        DateString))));
        queryRunner = createQueryRunner(
                mockMetadataDatabase.getUrl(),
                mockMetadataDatabase.getUsername(),
                mockMetadataDatabase.getPassword(),
                mockMetadataDatabase.getTablePrefix(),
                Optional.of(0),
                Optional.empty());
        queryManager = (SqlQueryManager) queryRunner.getCoordinator().getQueryManager();
        requireNonNull(queryManager, "queryManager is null");
        dispatchManager = queryRunner.getCoordinator().getDispatchManager();
    }

    @AfterClass
    public void tearDown()
                throws InterruptedException
    {
        long maxCleanUpTime = 5 * 1000L;    // 5 seconds
        long currentCleanUpTime = 0L;
        while (!queryManager.getQueries().isEmpty() && currentCleanUpTime < maxCleanUpTime) {
            Thread.sleep(1000L);
            currentCleanUpTime += 1000L;
        }
        if (null != mockMetadataDatabase) {
            mockMetadataDatabase.teardown();
        }
    }

    @Test
    public void testStringMatchPushDown()
    {
        // Exact match
        testPushDown("city.Name = 'hello world'", "city.Name: \"hello world\"", null);
        testPushDown("'hello world' = city.Name", "city.Name: \"hello world\"", null);

        // Like predicates that are transformed into substring match
        testPushDown("city.Name like 'hello%'", "city.Name: \"hello*\"", null);
        testPushDown("city.Name like '%hello'", "city.Name: \"*hello\"", null);

        // Like predicate not pushed down
        testPushDown("city.Name like '%hello%'", null, "city.Name like '%hello%'");

        // Like predicates that are kept in the original forms
        testPushDown("city.Name like 'hello_'", "city.Name: \"hello?\"", null);
        testPushDown("city.Name like '_hello'", "city.Name: \"?hello\"", null);
        testPushDown("city.Name like 'hello_w%'", "city.Name: \"hello?w*\"", null);
        testPushDown("city.Name like '%hello_w'", "city.Name: \"*hello?w\"", null);
        testPushDown("city.Name like 'hello%world'", "city.Name: \"hello*world\"", null);
        testPushDown("city.Name like 'hello%wor%ld'", "city.Name: \"hello*wor*ld\"", null);
    }

    @Test
    public void testSubStringPushDown()
    {
        testPushDown("substr(city.Name, 1, 2) = 'he'", "city.Name: \"he*\"", null);
        testPushDown("substr(city.Name, 5, 2) = 'he'", "city.Name: \"????he*\"", null);
        testPushDown("substr(city.Name, 5) = 'he'", "city.Name: \"????he\"", null);
        testPushDown("substr(city.Name, -2) = 'he'", "city.Name: \"*he\"", null);

        // Invalid substring index — not pushed down
        testPushDown("substr(city.Name, 1, 5) = 'he'", null, "substr(city.Name, 1, 5) = 'he'");
        testPushDown("substr(city.Name, -5) = 'he'", null, "substr(city.Name, -5) = 'he'");
    }

    @Test
    public void testNumericComparisonPushDown()
    {
        // Numeric comparisons
        testPushDown("fare > 0", "fare > 0.0", null);
        testPushDown("fare >= 0", "fare >= 0.0", null);
        testPushDown("fare < 0", "fare < 0.0", null);
        testPushDown("fare <= 0", "fare <= 0.0", null);
        testPushDown("fare = 0", "fare: 0.0", null);
        testPushDown("fare != 0", "NOT fare: 0.0", null);
        testPushDown("fare <> 0", "NOT fare: 0.0", null);
        testPushDown("0 < fare", "fare > 0.0", null);
        testPushDown("0 <= fare", "fare >= 0.0", null);
        testPushDown("0 > fare", "fare < 0.0", null);
        testPushDown("0 >= fare", "fare <= 0.0", null);
        testPushDown("0 = fare", "fare: 0.0", null);
        testPushDown("0 != fare", "NOT fare: 0.0", null);
        testPushDown("0 <> fare", "NOT fare: 0.0", null);
    }

    @Test
    public void testBetweenPushDown()
    {
        // Normal cases
        testPushDown("fare BETWEEN 0 AND 5", "fare >= 0.0 AND fare <= 5.0", null);
        testPushDown("fare BETWEEN 5 AND 0", null, null);

        // No push down for non-constant expressions
        testPushDown(
                "fare BETWEEN city.Region.population_lowerbound AND city.Region.population_upperbound",
                null,
                "fare >= city.Region.population_lowerbound AND fare <= city.Region.population_upperbound");

        // If the last two arguments of BETWEEN are not numeric constants, then the CLP connector
        // won't push them down.
        testPushDown("city.Name BETWEEN 'a' AND 'b'", null, "city.Name >= 'a' AND city.Name <= 'b'");
    }

    @Test
    public void testOrPushDown()
    {
        // OR conditions with partial push down support
        testPushDown("fare > 0 OR city.Name like 'b%'", "(fare > 0.0 OR city.Name: \"b*\")", null);
        testPushDown(
                "lower(city.Region.Name) = 'hello world' OR city.Region.id != 1",
                null,
                "(lower(city.Region.Name) = 'hello world' OR city.Region.id != 1)");

        // Multiple ORs
        testPushDown(
                "fare > 0 OR city.Name like 'b%' OR lower(city.Region.Name) = 'hello world' OR city.Region.id != 1",
                null,
                "(fare > 0 OR city.Name like 'b%') OR (lower(city.Region.Name) = 'hello world' OR city.Region.id != 1)");
        testPushDown(
                "fare > 0 OR city.Name like 'b%' OR city.Region.id != 1",
                "((fare > 0.0 OR city.Name: \"b*\") OR NOT city.Region.id: 1)",
                null);
    }

    @Test
    public void testAndPushDown()
    {
        // AND conditions with partial/full push down
        testPushDown("fare > 0 AND city.Name like 'b%'", "(fare > 0.0 AND city.Name: \"b*\")", null);

        testPushDown("lower(city.Region.Name) = 'hello world' AND city.Region.id != 1", "(NOT city.Region.id: 1)", "lower(city.Region.Name) = 'hello world'");

        // Multiple ANDs
        testPushDown(
                "fare > 0 AND city.Name like 'b%' AND lower(city.Region.Name) = 'hello world' AND city.Region.id != 1",
                "((fare > 0.0 AND city.Name: \"b*\") AND (NOT city.Region.id: 1))",
                "(lower(city.Region.Name) = 'hello world')");

        testPushDown(
                "fare > 0 AND city.Name like '%b%' AND lower(city.Region.Name) = 'hello world' AND city.Region.id != 1",
                "((fare > 0.0) AND (NOT city.Region.id: 1))",
                "city.Name like '%b%' AND lower(city.Region.Name) = 'hello world'");
    }

    @Test
    public void testNotPushDown()
    {
        // NOT and inequality predicates
        testPushDown("city.Region.Name NOT LIKE 'hello%'", "NOT city.Region.Name: \"hello*\"", null);
        testPushDown("NOT (city.Region.Name LIKE 'hello%')", "NOT city.Region.Name: \"hello*\"", null);
        testPushDown("city.Name != 'hello world'", "NOT city.Name: \"hello world\"", null);
        testPushDown("city.Name <> 'hello world'", "NOT city.Name: \"hello world\"", null);
        testPushDown("NOT (city.Name = 'hello world')", "NOT city.Name: \"hello world\"", null);
        testPushDown("fare != 0", "NOT fare: 0.0", null);
        testPushDown("fare <> 0", "NOT fare: 0.0", null);
        testPushDown("NOT (fare = 0)", "NOT fare: 0.0", null);

        // Multiple NOTs
        testPushDown("NOT (NOT fare = 0)", "fare: 0.0", null);
        testPushDown("NOT (fare = 0 AND city.Name = 'hello world')", "(NOT fare: 0.0 OR NOT city.Name: \"hello world\")", null);
        testPushDown("NOT (fare = 0 OR city.Name = 'hello world')", "(NOT fare: 0.0 AND NOT city.Name: \"hello world\")", null);
    }

    @Test
    public void testInPushDown()
    {
        // IN predicate
        testPushDown("city.Name IN ('hello world', 'hello world 2')", "(city.Name: \"hello world\" OR city.Name: \"hello world 2\")", null);
    }

    @Test
    public void testIsNullPushDown()
    {
        // IS NULL / IS NOT NULL predicates
        testPushDown("city.Name IS NULL", "NOT city.Name: *", null);
        testPushDown("city.Name IS NOT NULL", "NOT NOT city.Name: *", null);
        testPushDown("NOT (city.Name IS NULL)", "NOT NOT city.Name: *", null);
    }

    @Test
    public void testComplexPushDown()
    {
        // Complex AND/OR with partial pushdown
        testPushDown(
                "(fare > 0 OR city.Name like 'b%') AND (lower(city.Region.Name) = 'hello world' OR city.Name IS NULL)",
                "((fare > 0.0 OR city.Name: \"b*\"))",
                "(lower(city.Region.Name) = 'hello world' OR city.Name IS NULL)");

        testPushDown(
                "city.Region.id = 1 AND (fare > 0 OR city.Name NOT like 'b%') AND (lower(city.Region.Name) = 'hello world' OR city.Name IS NULL)",
                "((city.Region.id: 1 AND (fare > 0.0 OR NOT city.Name: \"b*\")))",
                "lower(city.Region.Name) = 'hello world' OR city.Name IS NULL");
    }

    @Test
    public void testClpWildcardUdf()
    {
        testPushDown("CLP_WILDCARD_STRING_COLUMN() = 'Beijing'", "*: \"Beijing\"", null);
        testPushDown("CLP_WILDCARD_INT_COLUMN() = 1", "*: 1", null);
        testPushDown("CLP_WILDCARD_FLOAT_COLUMN() > 0", "* > 0.0", null);
        testPushDown("CLP_WILDCARD_BOOL_COLUMN() = true", "*: true", null);

        testPushDown("CLP_WILDCARD_STRING_COLUMN() like 'hello%'", "*: \"hello*\"", null);
        testPushDown("substr(CLP_WILDCARD_STRING_COLUMN(), 1, 2) = 'he'", "*: \"he*\"", null);
        testPushDown("CLP_WILDCARD_INT_COLUMN() BETWEEN 0 AND 5", "* >= 0 AND * <= 5", null);
        testPushDown("CLP_WILDCARD_STRING_COLUMN() IN ('hello world', 'hello world 2')", "(*: \"hello world\" OR *: \"hello world 2\")", null);

        testPushDown("NOT CLP_WILDCARD_FLOAT_COLUMN() > 0", "* <= 0.0", null);
        testPushDown(
                "CLP_WILDCARD_STRING_COLUMN() = 'Beijing' AND CLP_WILDCARD_INT_COLUMN() = 1 AND city.Region.id = 1",
                "((*: \"Beijing\" AND *: 1) AND city.Region.id: 1)",
                null);
        testPushDown(
                "CLP_WILDCARD_STRING_COLUMN() = 'Toronto' OR CLP_WILDCARD_INT_COLUMN() = 2",
                "(*: \"Toronto\" OR *: 2)",
                null);
        testPushDown(
                "CLP_WILDCARD_STRING_COLUMN() = 'Shanghai' AND (CLP_WILDCARD_INT_COLUMN() = 3 OR city.Region.id = 5)",
                "(*: \"Shanghai\" AND (*: 3 OR city.Region.id: 5))",
                null);
    }

    private void testPushDown(String filter, String expectedPushDown, String expectedRemaining)
    {
        try {
            // We first execute a query using the original filter and look for the FilterNode (for remaining expression)
            // and TableScanNode (for KQL pushdown and split filter pushdown)
            QueryId originalQueryId = createAndPlanQuery(filter);
            String actualPushDown = null;
            RowExpression actualRemainingExpression = null;
            Plan originalQueryPlan = queryManager.getQueryPlan(originalQueryId);
            for (Map.Entry<PlanNodeId, PlanNode> entry : originalQueryPlan.getPlanIdNodeMap().entrySet()) {
                ClpTableLayoutHandle clpTableLayoutHandle = tryGetClpTableLayoutHandleFromFilterNode(entry.getValue());
                if (clpTableLayoutHandle != null && actualRemainingExpression == null) {
                    actualRemainingExpression = ((FilterNode) entry.getValue()).getPredicate();
                    continue;
                }
                clpTableLayoutHandle = tryGetClpTableLayoutHandleFromTableScanNode(entry.getValue());
                if (clpTableLayoutHandle != null && actualPushDown == null) {
                    actualPushDown = clpTableLayoutHandle.getKqlQuery().orElse(null);
                }
            }
            assertEquals(actualPushDown, expectedPushDown);
            if (expectedRemaining != null) {
                assertNotNull(actualRemainingExpression);
                // Since the remaining expression cannot be simply compared by given String, we have to first convert
                // the expectedRemaining to a RowExpression so that we can compare with what we just fetched from the
                // plan of the original query. To ensure the translation process is the same, we create another query
                // which only contain the remaining expression as the filter, then we look for the FilterNode and get
                // the predict as the translated RowExpression of the expectedRemaining.
                QueryId remainingFilterQueryId = createAndPlanQuery(expectedRemaining);
                Plan remainingFilterPlan = queryManager.getQueryPlan(remainingFilterQueryId);
                RowExpression expectedRemainingExpression = null;
                for (Map.Entry<PlanNodeId, PlanNode> entry : remainingFilterPlan.getPlanIdNodeMap().entrySet()) {
                    if (tryGetClpTableLayoutHandleFromFilterNode(entry.getValue()) != null) {
                        expectedRemainingExpression = ((FilterNode) entry.getValue()).getPredicate();
                        break;
                    }
                }
                equalsIgnoreBase(actualRemainingExpression, expectedRemainingExpression);
                queryManager.cancelQuery(remainingFilterQueryId);
            }
            queryManager.cancelQuery(originalQueryId);
        }
        catch (Exception e) {
            fail(e.getMessage());
        }
    }

    private QueryId createAndPlanQuery(String filter)
                throws ExecutionException, InterruptedException
    {
        QueryId id = queryRunner.getCoordinator().getDispatchManager().createQueryId();
        @Language("SQL") String sql = format("SELECT * FROM clp.default.test_pushdown WHERE %s LIMIT 1", filter);
        dispatchManager.createQuery(
                id,
                "slug",
                0,
                new TestingClpSessionContext(queryRunner.getDefaultSession()),
                sql).get();
        long maxDispatchingAndPlanningTime = 60 * 1000L;    // 1 minute
        long currentWaitingTime = 0L;
        while (dispatchManager.getQueryInfo(id).getState().ordinal() != RUNNING.ordinal() && currentWaitingTime < maxDispatchingAndPlanningTime) {
            Thread.sleep(1000L);
            currentWaitingTime += 1000L;
        }
        assertTrue(currentWaitingTime < maxDispatchingAndPlanningTime);
        return id;
    }

    private void equalsIgnoreBase(RowExpression actualExpression, RowExpression expectedExpression)
    {
        if (actualExpression == null) {
            assertNull(expectedExpression);
            return;
        }
        String normalizedActualExpressionText = BASE_PTR.matcher(actualExpression.toString()).replaceAll("[B@IGNORED");
        String normalizedExpectedExpressionText = BASE_PTR.matcher(expectedExpression.toString()).replaceAll("[B@IGNORED");
        assertEquals(normalizedActualExpressionText, normalizedExpectedExpressionText);
    }

    private ClpTableLayoutHandle tryGetClpTableLayoutHandleFromFilterNode(PlanNode node)
    {
        if (!(node instanceof FilterNode)) {
            return null;
        }
        return (tryGetClpTableLayoutHandleFromTableScanNode(((FilterNode) node).getSource()));
    }

    private ClpTableLayoutHandle tryGetClpTableLayoutHandleFromTableScanNode(PlanNode node)
    {
        if (!(node instanceof TableScanNode)) {
            return null;
        }
        ConnectorTableLayoutHandle tableLayoutHandle = ((TableScanNode) node).getTable().getLayout().orElse(null);
        if (!(tableLayoutHandle instanceof ClpTableLayoutHandle)) {
            return null;
        }
        return (ClpTableLayoutHandle) tableLayoutHandle;
    }
}
