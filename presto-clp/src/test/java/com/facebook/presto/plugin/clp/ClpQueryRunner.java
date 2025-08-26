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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.Session;
import com.facebook.presto.plugin.clp.mockdb.ClpMockMetadataDatabase;
import com.facebook.presto.plugin.clp.mockdb.table.ArchivesTableRows;
import com.facebook.presto.plugin.clp.mockdb.table.ColumnMetadataTableRows;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.security.SelectedRole;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;

import static com.facebook.presto.plugin.clp.metadata.ClpSchemaTreeNodeType.Boolean;
import static com.facebook.presto.plugin.clp.metadata.ClpSchemaTreeNodeType.ClpString;
import static com.facebook.presto.plugin.clp.metadata.ClpSchemaTreeNodeType.DateString;
import static com.facebook.presto.plugin.clp.metadata.ClpSchemaTreeNodeType.Integer;
import static com.facebook.presto.plugin.clp.metadata.ClpSchemaTreeNodeType.NullValue;
import static com.facebook.presto.plugin.clp.metadata.ClpSchemaTreeNodeType.UnstructuredArray;
import static com.facebook.presto.plugin.clp.metadata.ClpSchemaTreeNodeType.VarString;
import static com.facebook.presto.spi.security.SelectedRole.Type.ROLE;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

public class ClpQueryRunner
{
    private static final Logger log = Logger.get(ClpQueryRunner.class);

    public static final String CLP_CATALOG = "clp";
    public static final String CLP_CONNECTOR = CLP_CATALOG;
    public static final int DEFAULT_NUM_OF_WORKERS = 1;
    public static final String DEFAULT_SCHEMA = "default";
    public static final String DEFAULT_TABLE_NAME = "test_e2e";

    private DistributedQueryRunner actualQueryRunner;
    private ClpMockMetadataDatabase mockMetadataDatabase;

    public static ClpQueryRunner createDefaultQueryRunnerWithMockDatabase(
            String archiveStorageDirectory,
            Optional<Integer> workerCount,
            Optional<BiFunction<Integer, URI, Process>> externalWorkerLauncher)
            throws Exception
    {
        log.info("Creating default ClpQueryRunner");
        return createQueryRunnerWithMockDatabase(
                createDefaultSession(),
                archiveStorageDirectory,
                ImmutableList.of(DEFAULT_TABLE_NAME),
                ImmutableMap.of(DEFAULT_TABLE_NAME, new ColumnMetadataTableRows(
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
                                DateString))),
                ImmutableMap.of(DEFAULT_TABLE_NAME, new ArchivesTableRows(
                        ImmutableList.of("mongodb-processed-single-file-archive"),
                        ImmutableList.of(1679441694576L),
                        ImmutableList.of(1679442346492L))),
                workerCount,
                externalWorkerLauncher);
    }

    public static ClpQueryRunner createQueryRunnerWithMockDatabase(
            Session session,
            String archiveStorageDirectory,
            List<String> tableNames,
            Map<String, ColumnMetadataTableRows> clpFields,
            Map<String, ArchivesTableRows> splits,
            Optional<Integer> workerCount,
            Optional<BiFunction<Integer, URI, Process>> externalWorkerLauncher)
                throws Exception
    {
        ClpQueryRunner clpQueryRunner = new ClpQueryRunner();
        clpQueryRunner.actualQueryRunner = DistributedQueryRunner.builder(session)
                        .setNodeCount(workerCount.orElse(DEFAULT_NUM_OF_WORKERS))
                        .setExternalWorkerLauncher(externalWorkerLauncher)
                        .build();

        ClpMockMetadataDatabase mockMetadataDatabase = ClpMockMetadataDatabase
                .builder()
                .setArchiveStorageDirectory(archiveStorageDirectory)
                .addTables(tableNames)
                .addColumnMetadata(clpFields)
                .addSplits(splits)
                .build();
        Map<String, String> clpProperties = ImmutableMap.<String, String>builder()
                .put("clp.metadata-provider-type", "mysql")
                .put("clp.metadata-db-url", mockMetadataDatabase.getUrl())
                .put("clp.metadata-db-user", mockMetadataDatabase.getUsername())
                .put("clp.metadata-db-password", mockMetadataDatabase.getPassword())
                .put("clp.metadata-table-prefix", mockMetadataDatabase.getTablePrefix())
                .put("clp.split-provider-type", "mysql")
                .build();

        clpQueryRunner.mockMetadataDatabase = mockMetadataDatabase;
        clpQueryRunner.actualQueryRunner.installPlugin(new ClpPlugin());
        clpQueryRunner.actualQueryRunner.createCatalog(CLP_CATALOG, CLP_CONNECTOR, clpProperties);
        return clpQueryRunner;
    }

    /**
     * Creates a default mock session for query use.
     *
     * @return a default session
     */
    private static Session createDefaultSession()
    {
        Optional<SelectedRole> role = Optional.of(new SelectedRole(ROLE, Optional.of("admin")));
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

    public DistributedQueryRunner getActualQueryRunner()
    {
        return actualQueryRunner;
    }

    public ClpMockMetadataDatabase getMockMetadataDatabase()
    {
        return mockMetadataDatabase;
    }

    private ClpQueryRunner()
    {
    }
}
