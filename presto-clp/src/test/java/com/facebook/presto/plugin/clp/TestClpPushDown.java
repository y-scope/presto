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
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.plugin.clp.metadata.ClpMetadataProvider;
import com.facebook.presto.plugin.clp.metadata.ClpMySqlMetadataProvider;
import com.facebook.presto.plugin.clp.mockdb.ClpMockMetadataDatabase;
import com.facebook.presto.plugin.clp.mockdb.table.ColumnMetadataTableRows;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.execution.QueryState.RUNNING;
import static com.facebook.presto.plugin.clp.ClpQueryRunner.createQueryRunner;
import static com.facebook.presto.plugin.clp.metadata.ClpSchemaTreeNodeType.Boolean;
import static com.facebook.presto.plugin.clp.metadata.ClpSchemaTreeNodeType.DateString;
import static com.facebook.presto.plugin.clp.metadata.ClpSchemaTreeNodeType.Float;
import static com.facebook.presto.plugin.clp.metadata.ClpSchemaTreeNodeType.Integer;
import static com.facebook.presto.plugin.clp.metadata.ClpSchemaTreeNodeType.VarString;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestClpPushDown
{
    private static final String TABLE_NAME = "test_pushdown";
    private static final Long TEST_TS_SECONDS = 1746003005L;
    private static final Long TEST_TS_NANOSECONDS = 1746003005000000000L;

    private ClpMockMetadataDatabase mockMetadataDatabase;
    private DistributedQueryRunner queryRunner;
    private QueryManager queryManager;
    private DispatchManager dispatchManager;

    @BeforeMethod
    public void setUp() throws Exception {
        mockMetadataDatabase = ClpMockMetadataDatabase
                .builder()
                .build();
        mockMetadataDatabase.addTableToDatasetsTableIfNotExist(ImmutableList.of(TABLE_NAME));
        mockMetadataDatabase.addColumnMetadata(ImmutableMap.of(TABLE_NAME, new ColumnMetadataTableRows(
                ImmutableList.of(
                        "city.Name",
                        "city.Region.id",
                        "city.Region.Name",
                        "fare",
                        "isHoliday",
                        "ts"),
                ImmutableList.of(
                        VarString,
                        Integer,
                        VarString,
                        Float,
                        Boolean,
                        DateString))));
        ClpConfig config = new ClpConfig()
                .setPolymorphicTypeEnabled(true)
                .setMetadataDbUrl(mockMetadataDatabase.getUrl())
                .setMetadataDbUser(mockMetadataDatabase.getUsername())
                .setMetadataDbPassword(mockMetadataDatabase.getPassword())
                .setMetadataTablePrefix(mockMetadataDatabase.getTablePrefix());
        ClpMetadataProvider metadataProvider = new ClpMySqlMetadataProvider(config);
        queryRunner = createQueryRunner(
                mockMetadataDatabase.getUrl(),
                mockMetadataDatabase.getUsername(),
                mockMetadataDatabase.getPassword(),
                mockMetadataDatabase.getTablePrefix(),
                Optional.of(0),
                Optional.empty());
        queryManager = queryRunner.getCoordinator().getQueryManager();
        dispatchManager = queryRunner.getCoordinator().getDispatchManager();
    }

    @AfterMethod
    public void tearDown() throws InterruptedException {
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

    public void testTimestampComparisons()
    {
        testPushDown(format("ts > from_unixtime(%s)", TEST_TS_SECONDS), format("ts > %s", TEST_TS_NANOSECONDS));
        testPushDown(format("ts >= from_unixtime(%s)", TEST_TS_SECONDS), format("ts >= %s", TEST_TS_NANOSECONDS));
        testPushDown(format("ts < from_unixtime(%s)", TEST_TS_SECONDS), format("ts < %s", TEST_TS_NANOSECONDS));
        testPushDown(format("ts <= from_unixtime(%s)", TEST_TS_SECONDS), format("ts <= %s", TEST_TS_NANOSECONDS));
        testPushDown(format("ts = from_unixtime(%s)", TEST_TS_SECONDS), format("ts: %s", TEST_TS_NANOSECONDS));
        testPushDown(format("ts != from_unixtime(%s)", TEST_TS_SECONDS), format("NOT ts: %s", TEST_TS_NANOSECONDS));
        testPushDown(format("ts <> from_unixtime(%s)", TEST_TS_SECONDS), format("NOT ts: %s", TEST_TS_NANOSECONDS));
        testPushDown(format("from_unixtime(%s) < ts", TEST_TS_SECONDS), format("ts > %s", TEST_TS_NANOSECONDS));
        testPushDown(format("from_unixtime(%s) <= ts", TEST_TS_SECONDS), format("ts >= %s", TEST_TS_NANOSECONDS));
        testPushDown(format("from_unixtime(%s) > ts", TEST_TS_SECONDS), format("ts < %s", TEST_TS_NANOSECONDS));
        testPushDown(format("from_unixtime(%s) >= ts", TEST_TS_SECONDS), format("ts <= %s", TEST_TS_NANOSECONDS));
        testPushDown(format("from_unixtime(%s) = ts", TEST_TS_SECONDS), format("ts: %s", TEST_TS_NANOSECONDS));
        testPushDown(format("from_unixtime(%s) != ts", TEST_TS_SECONDS), format("NOT ts: %s", TEST_TS_NANOSECONDS));
        testPushDown(format("from_unixtime(%s) <> ts", TEST_TS_SECONDS), format("NOT ts: %s", TEST_TS_NANOSECONDS));
    }

    private void testPushDown(String filter, String expectedPushDown) {
        try {
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
            boolean isPushDownGenerated = false;
            for (Map.Entry<PlanNodeId, PlanNode> entry : queryManager.getFullQueryInfo(id).getPlanIdNodeMap().entrySet()) {
                if (!(entry.getValue() instanceof TableScanNode)
                        || (!(((TableScanNode) entry.getValue()).getTable().getLayout().orElse(null) instanceof ClpTableLayoutHandle))) {
                    continue;
                }
                ClpTableLayoutHandle clpTableLayoutHandle = (ClpTableLayoutHandle) ((TableScanNode) entry.getValue()).getTable().getLayout().orElseThrow(AssertionError::new);
                String actualPushDown = clpTableLayoutHandle.getKqlQuery().orElse(null);
                assertEquals(actualPushDown, expectedPushDown);
                isPushDownGenerated = true;
                break;
            }
            assertTrue(isPushDownGenerated);
            queryManager.cancelQuery(id);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }
}
