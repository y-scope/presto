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
package com.facebook.presto.plugin.clp.metadata.filter;

import com.facebook.presto.plugin.clp.ClpConfig;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;

import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestClpMySqlMetadataFilterConfig
{
    private String filterConfigPath;

    @BeforeMethod
    public void setUp() throws IOException, URISyntaxException
    {
        URL resource = getClass().getClassLoader().getResource("test-mysql-metadata-filter.json");
        if (resource == null) {
            throw new FileNotFoundException("test-mysql-metadata-filter.json not found in resources");
        }

        filterConfigPath = Paths.get(resource.toURI()).toAbsolutePath().toString();
    }

    @Test
    public void remapMetadataFilterPushDown()
    {
        ClpConfig config = new ClpConfig();
        config.setMetadataFilterConfig(filterConfigPath);
        ClpMySqlMetadataFilterProvider filterProvider = new ClpMySqlMetadataFilterProvider(config);

        String metadataFilterSql1 = "(\"msg.timestamp\" > 1234 AND \"msg.timestamp\" < 5678)";
        String remappedSql1 = filterProvider.remapMetadataFilterPushDown("clp.default.table_1", metadataFilterSql1);
        assertEquals(remappedSql1, "(end_timestamp > 1234 AND begin_timestamp < 5678)");

        String metadataFilterSql2 = "(\"msg.timestamp\" >= 1234 AND \"msg.timestamp\" <= 5678)";
        String remappedSql2 = filterProvider.remapMetadataFilterPushDown("clp.default.table_1", metadataFilterSql2);
        assertEquals(remappedSql2, "(end_timestamp >= 1234 AND begin_timestamp <= 5678)");

        String metadataFilterSql3 = "(\"msg.timestamp\" > 1234 AND \"msg.timestamp\" <= 5678)";
        String remappedSql3 = filterProvider.remapMetadataFilterPushDown("clp.default.table_1", metadataFilterSql3);
        assertEquals(remappedSql3, "(end_timestamp > 1234 AND begin_timestamp <= 5678)");

        String metadataFilterSql4 = "(\"msg.timestamp\" >= 1234 AND \"msg.timestamp\" < 5678)";
        String remappedSql4 = filterProvider.remapMetadataFilterPushDown("clp.default.table_1", metadataFilterSql4);
        assertEquals(remappedSql4, "(end_timestamp >= 1234 AND begin_timestamp < 5678)");

        String metadataFilterSql5 = "(\"msg.timestamp\" = 1234)";
        String remappedSql5 = filterProvider.remapMetadataFilterPushDown("clp.default.table_1", metadataFilterSql5);
        assertEquals(remappedSql5, "((begin_timestamp <= 1234 AND end_timestamp >= 1234))");
    }
}
