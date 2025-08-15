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

import com.facebook.presto.Session;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.security.SelectedRole;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;

import java.net.URI;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;

import static com.facebook.presto.spi.security.SelectedRole.Type.ROLE;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;

public class ClpQueryRunner {

    public static final String CLP_CATALOG = "clp";
    public static final String DEFAULT_SCHEMA = "default";

    public static DistributedQueryRunner createQueryRunner(
            String metadataDatabaseUrl,
            String metadataDatabaseName,
            String metadataDatabaseUser,
            String metadataDatabasePassword,
            String metadataTablePrefix,
            Optional<Integer> workerCount,
            Optional<BiFunction<Integer, URI, Process>> externalWorkerLauncher) throws Exception
    {
        requireNonNull(metadataDatabaseUrl, "metadataDatabaseUrl is null");
        requireNonNull(metadataDatabaseName, "metadataDatabaseName is null");
        requireNonNull(metadataDatabaseUser, "metadataDatabaseUser is null");
        requireNonNull(metadataDatabasePassword, "metadataDatabasePassword is null");
        requireNonNull(metadataTablePrefix, "metadataTablePrefix is null");
        DistributedQueryRunner queryRunner =
                DistributedQueryRunner.builder(createSession(Optional.of(new SelectedRole(ROLE, Optional.of("admin")))))
                        .setNodeCount(workerCount.orElse(1))
                        .setExternalWorkerLauncher(externalWorkerLauncher)
                        .build();

        Map<String, String> clpProperties = ImmutableMap.<String, String>builder()
                .put("clp.metadata-provider", "mysql")
                .put("clp.metadata-db-url", metadataDatabaseUrl)
                .put("clp.metadata-db-name", metadataDatabaseName)
                .put("clp.metadata-db-user", metadataDatabaseUser)
                .put("clp.metadata-db-password", metadataDatabasePassword)
                .put("clp.metadata-table-prefix", metadataTablePrefix)
                .put("clp.split-provider", "mysql")
                .build();
        queryRunner.installPlugin(new ClpPlugin());
        queryRunner.createCatalog(CLP_CATALOG, CLP_CATALOG, clpProperties);
        return queryRunner;
    }

    public static Session createSession(Optional<SelectedRole> role)
    {
        return testSessionBuilder()
                .setIdentity(new Identity(
                        "hive",
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

}
