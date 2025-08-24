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

import com.facebook.presto.Session;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.plugin.clp.ClpPlugin;
import com.facebook.presto.plugin.clp.mockdb.ClpMockMetadataDatabase;
import com.facebook.presto.plugin.clp.mockdb.table.ArchivesTableRows;
import com.facebook.presto.plugin.clp.mockdb.table.ColumnMetadataTableRows;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.security.SelectedRole;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterTest;
import org.testng.annotations.Test;

import java.net.URI;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.NativeQueryRunnerParameters;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.getExternalClpWorkerLauncher;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.getNativeQueryRunnerParameters;
import static com.facebook.presto.plugin.clp.metadata.ClpSchemaTreeNodeType.Boolean;
import static com.facebook.presto.plugin.clp.metadata.ClpSchemaTreeNodeType.ClpString;
import static com.facebook.presto.plugin.clp.metadata.ClpSchemaTreeNodeType.DateString;
import static com.facebook.presto.plugin.clp.metadata.ClpSchemaTreeNodeType.Integer;
import static com.facebook.presto.plugin.clp.metadata.ClpSchemaTreeNodeType.NullValue;
import static com.facebook.presto.plugin.clp.metadata.ClpSchemaTreeNodeType.UnstructuredArray;
import static com.facebook.presto.plugin.clp.metadata.ClpSchemaTreeNodeType.VarString;
import static com.facebook.presto.spi.security.SelectedRole.Type.ROLE;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestPrestoNativeClpGeneralQueries
        extends AbstractTestQueryFramework
{
    private static final String TABLE_NAME = "test_e2e";
    private static final String CLP_CATALOG = "clp";
    private static final String CLP_CONNECTOR = CLP_CATALOG;
    private static final String DEFAULT_SCHEMA = "default";
    private ClpMockMetadataDatabase mockMetadataDatabase;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        URL resource = getClass().getClassLoader().getResource("clp-archives");
        String archiveStorageDirectory = format("%s/", resource.getPath());
        mockMetadataDatabase = ClpMockMetadataDatabase
                .builder()
                .setArchiveStorageDirectory(archiveStorageDirectory)
                .createDatasetsTableIfNotExist()
                .addTables(ImmutableList.of(TABLE_NAME))
                .addColumnMetadata(ImmutableMap.of(TABLE_NAME, new ColumnMetadataTableRows(
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
                                DateString))))
                .addSplits(ImmutableMap.of(TABLE_NAME, new ArchivesTableRows(
                        ImmutableList.of("mongodb-processed-single-file-archive"),
                        ImmutableList.of(1679441694576L),
                        ImmutableList.of(1679442346492L))))
                .build();

        NativeQueryRunnerParameters nativeQueryRunnerParameters = getNativeQueryRunnerParameters();
        // Make query runner with external workers for tests
        return createQueryRunner(
                mockMetadataDatabase.getUrl(),
                mockMetadataDatabase.getUsername(),
                mockMetadataDatabase.getPassword(),
                mockMetadataDatabase.getTablePrefix(),
                nativeQueryRunnerParameters.workerCount,
                getExternalClpWorkerLauncher(
                        CLP_CATALOG,
                        nativeQueryRunnerParameters.serverBinary.toString()));
    }

    @Test
    public void test()
    {
        QueryRunner queryRunner = getQueryRunner();
        assertEquals(queryRunner.getNodeCount(), 2);
        assertTrue(queryRunner.tableExists(getSession(), TABLE_NAME));

        // Test select star
        assertClpQuery(
                queryRunner,
                getSession(),
                format("SELECT * FROM %s LIMIT 1", TABLE_NAME),
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
        mockMetadataDatabase.teardown();
    }

    private static DistributedQueryRunner createQueryRunner(
            String metadataDatabaseUrl,
            String metadataDatabaseUser,
            String metadataDatabasePassword,
            String metadataTablePrefix,
            Optional<Integer> workerCount,
            Optional<BiFunction<Integer, URI, Process>> externalWorkerLauncher) throws Exception
    {
        requireNonNull(metadataDatabaseUrl, "metadataDatabaseUrl is null");
        requireNonNull(metadataDatabaseUser, "metadataDatabaseUser is null");
        requireNonNull(metadataDatabasePassword, "metadataDatabasePassword is null");
        requireNonNull(metadataTablePrefix, "metadataTablePrefix is null");
        DistributedQueryRunner queryRunner =
                DistributedQueryRunner.builder(createSession(Optional.of(new SelectedRole(ROLE, Optional.of("admin")))))
                        .setNodeCount(workerCount.orElse(1))
                        .setExternalWorkerLauncher(externalWorkerLauncher)
                        .build();

        Map<String, String> clpProperties = ImmutableMap.<String, String>builder()
                .put("clp.metadata-provider-type", "mysql")
                .put("clp.metadata-db-url", metadataDatabaseUrl)
                .put("clp.metadata-db-user", metadataDatabaseUser)
                .put("clp.metadata-db-password", metadataDatabasePassword)
                .put("clp.metadata-table-prefix", metadataTablePrefix)
                .put("clp.split-provider-type", "mysql")
                .build();
        queryRunner.installPlugin(new ClpPlugin());
        queryRunner.createCatalog(CLP_CATALOG, CLP_CONNECTOR, clpProperties);
        return queryRunner;
    }

    private static Session createSession(Optional<SelectedRole> role)
    {
        return testSessionBuilder()
                .setIdentity(new Identity(
                        "clp",
                        Optional.empty(),
                        role.map(selectedRole -> ImmutableMap.of(CLP_CATALOG, selectedRole))
                                .orElse(ImmutableMap.of()),
                        ImmutableMap.of(),
                        ImmutableMap.of(),
                        Optional.empty(),
                        Optional.empty()))
                .setCatalog(CLP_CATALOG)
                .setSchema(DEFAULT_SCHEMA)
                .build();
    }

    private static void assertClpQuery(
            QueryRunner queryRunner,
            Session session,
            String query,
            List<Type> expectedTypes,
            int expectedNumOfRows)
    {
        try {
            QueryRunner.MaterializedResultWithPlan resultWithPlan = queryRunner.executeWithPlan(session, query, WarningCollector.NOOP);
            MaterializedResult actualResults = resultWithPlan.getMaterializedResult().toTestTypes();
            assertEquals(actualResults.getRowCount(), expectedNumOfRows);
            List<Type> actualTypes = actualResults.getTypes();
            assertEquals(actualTypes.size(), expectedTypes.size());
            for (int i = 0; i < actualTypes.size(); ++i) {
                assertEquals(actualTypes.get(i), expectedTypes.get(i));
            }
            Plan queryPlan = resultWithPlan.getQueryPlan();
            // This ensures the query has been finished to avoid "Server is shut down" exception when tearing down
            queryPlan.getStatsAndCosts();
        }
        catch (RuntimeException ex) {
            fail("Execution of 'actual' query failed: ", ex);
        }
    }
}
