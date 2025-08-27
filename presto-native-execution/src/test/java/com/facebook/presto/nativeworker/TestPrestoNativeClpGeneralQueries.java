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
package com.facebook.presto.nativeworker;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.Session;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.plugin.clp.mockdb.ClpMockMetadataDatabase;
import com.facebook.presto.plugin.clp.mockdb.table.ArchivesTableRows;
import com.facebook.presto.plugin.clp.mockdb.table.ColumnMetadataTableRows;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterTest;
import org.testng.annotations.Test;

import java.net.URL;
import java.nio.file.Paths;
import java.util.List;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.getNativeQueryRunnerParameters;
import static com.facebook.presto.plugin.clp.ClpQueryRunner.DEFAULT_NUM_OF_WORKERS;
import static com.facebook.presto.plugin.clp.metadata.ClpSchemaTreeNodeType.Boolean;
import static com.facebook.presto.plugin.clp.metadata.ClpSchemaTreeNodeType.ClpString;
import static com.facebook.presto.plugin.clp.metadata.ClpSchemaTreeNodeType.DateString;
import static com.facebook.presto.plugin.clp.metadata.ClpSchemaTreeNodeType.Integer;
import static com.facebook.presto.plugin.clp.metadata.ClpSchemaTreeNodeType.NullValue;
import static com.facebook.presto.plugin.clp.metadata.ClpSchemaTreeNodeType.UnstructuredArray;
import static com.facebook.presto.plugin.clp.metadata.ClpSchemaTreeNodeType.VarString;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestPrestoNativeClpGeneralQueries
        extends AbstractTestQueryFramework
{
    private static final Logger log = Logger.get(TestPrestoNativeClpGeneralQueries.class);
    private static final String DEFAULT_TABLE_NAME = "test_e2e";
    private ClpMockMetadataDatabase mockMetadataDatabase;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        URL resource = requireNonNull(
                getClass().getClassLoader().getResource("clp-archives"),
                "Test resource 'clp-archives' not found on classpath");
        String archiveStorageDirectory = format("%s/", Paths.get(resource.toURI()));
        mockMetadataDatabase = ClpMockMetadataDatabase.builder().setArchiveStorageDirectory(archiveStorageDirectory).build();
        return PrestoNativeQueryRunnerUtils.createNativeClpQueryRunner(
                mockMetadataDatabase.getUrl(),
                mockMetadataDatabase.getUsername(),
                mockMetadataDatabase.getPassword(),
                mockMetadataDatabase.getTablePrefix());
    }

    @Override
    protected void createTables()
    {
        mockMetadataDatabase.addTableToDatasetsTableIfNotExist(ImmutableList.of(DEFAULT_TABLE_NAME));
        mockMetadataDatabase.addColumnMetadata(ImmutableMap.of(DEFAULT_TABLE_NAME, new ColumnMetadataTableRows(
                ImmutableList.of(
                        "id",
                        "msg",
                        "msg",
                        "attr.command.q._id.uid.dollar_sign_binary.sub_type",
                        "attr.existing",
                        "tags",
                        "attr.obj.md.indexes",
                        "attr.build_u_u_i_d",
                        "t.dollar_sign_date"),
                ImmutableList.of(
                        Integer,
                        ClpString,
                        VarString,
                        VarString,
                        Boolean,
                        UnstructuredArray,
                        UnstructuredArray,
                        NullValue,
                        DateString))));
        mockMetadataDatabase.addSplits(ImmutableMap.of(DEFAULT_TABLE_NAME, new ArchivesTableRows(
                ImmutableList.of("mongodb-processed-single-file-archive"),
                ImmutableList.of(1679441694576L),
                ImmutableList.of(1679442346492L))));
    }

    @Test
    public void test()
    {
        QueryRunner queryRunner = getQueryRunner();
        assertEquals(queryRunner.getNodeCount(), getNativeQueryRunnerParameters().workerCount.orElse(DEFAULT_NUM_OF_WORKERS) + 1);
        assertTrue(queryRunner.tableExists(getSession(), DEFAULT_TABLE_NAME));

        // Test select star
        assertClpQuery(
                queryRunner,
                getSession(),
                format("SELECT * FROM %s LIMIT 1", DEFAULT_TABLE_NAME),
                ImmutableList.of(
                        VARCHAR,
                        RowType.from(ImmutableList.of(RowType.field("dollar_sign_date", TIMESTAMP))),
                        BIGINT,
                        RowType.from(ImmutableList.of(
                                RowType.field("build_u_u_i_d", VARCHAR),
                                RowType.field("command", RowType.from(ImmutableList.of(
                                        RowType.field("q", RowType.from(ImmutableList.of(
                                                RowType.field("_id", RowType.from(ImmutableList.of(
                                                        RowType.field("uid", RowType.from(ImmutableList.of(
                                                                RowType.field("dollar_sign_binary", RowType.from(ImmutableList.of(
                                                                        RowType.field("sub_type", VARCHAR)))))))))))))))),
                                RowType.field("existing", BOOLEAN),
                                RowType.field("obj", RowType.from(ImmutableList.of(
                                        RowType.field("md", RowType.from(ImmutableList.of(
                                                RowType.field("indexes", new ArrayType(VARCHAR)))))))))),
                        new ArrayType(VARCHAR)),
                1);
    }

    @AfterTest
    public void teardown()
    {
        if (null != mockMetadataDatabase) {
            mockMetadataDatabase.teardown();
        }
    }

    /**
     * Executes the given query with the given query runner and session, then checks the type and
     * the number of returning rows.
     *
     * @param queryRunner
     * @param session
     * @param query
     * @param expectedTypes
     * @param expectedNumOfRows
     */
    private static void assertClpQuery(
            QueryRunner queryRunner,
            Session session,
            String query,
            List<Type> expectedTypes,
            int expectedNumOfRows)
    {
        try {
            long start = System.nanoTime();
            QueryRunner.MaterializedResultWithPlan resultWithPlan = queryRunner.executeWithPlan(session, query, WarningCollector.NOOP);
            MaterializedResult actualResults = resultWithPlan.getMaterializedResult().toTestTypes();
            assertEquals(actualResults.getRowCount(), expectedNumOfRows);
            List<Type> actualTypes = actualResults.getTypes();
            assertEquals(actualTypes.size(), expectedTypes.size());
            for (int i = 0; i < actualTypes.size(); ++i) {
                assertEquals(actualTypes.get(i), expectedTypes.get(i));
            }
            long end = System.nanoTime();
            log.info("Query [%s] end-to-end latency: %s s", query, (end - start) / 1e9);
        }
        catch (RuntimeException ex) {
            fail("Execution of 'actual' query failed: ", ex);
        }
    }
}
