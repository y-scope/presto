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

import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import com.facebook.presto.plugin.clp.ClpQueryRunner;
import java.nio.file.Paths;
import java.util.Optional;

import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.*;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.getNativeWorkerTpcdsProperties;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.getExternalWorkerLauncher;

public class TestPrestoNativeClpGeneralQueries
    extends AbstractTestQueryFramework
{

    @Override
    protected QueryRunner createQueryRunner() throws Exception {
        ImmutableMap<String, String> clpProperties = ImmutableMap.of("clp.storage-type", "fs");

        ImmutableMap.Builder<String, String> coordinatorProperties = ImmutableMap.builder();
        coordinatorProperties.put("native-execution-enabled", "true");

//        // Make query runner with external workers for tests
//        return ClpQueryRunner.createQueryRunner(
//                ImmutableList.of(),
//                ImmutableList.of(),
//                ImmutableMap.<String, String>builder()
//                        .put("http-server.http.port", "8081")
//                        .put("experimental.internal-communication.thrift-transport-enabled", String.valueOf(useThrift))
//                        .putAll(getNativeWorkerSystemProperties())
//                        .putAll(isCoordinatorSidecarEnabled ? getNativeSidecarProperties() : ImmutableMap.of())
//                        .putAll(extraProperties)
//                        .build(),
//                coordinatorProperties.build(),
//                "legacy",
//                hiveProperties,
//                workerCount,
//                Optional.of(Paths.get(addStorageFormatToPath ? dataDirectory + "/" + storageFormat : dataDirectory)),
//                getExternalWorkerLauncher("hive", prestoServerPath, cacheMaxSize, remoteFunctionServerUds, failOnNestedLoopJoin,
//                        isCoordinatorSidecarEnabled, enableRuntimeMetricsCollection, enableSsdCache),
//                getNativeWorkerTpcdsProperties());
    }
}
