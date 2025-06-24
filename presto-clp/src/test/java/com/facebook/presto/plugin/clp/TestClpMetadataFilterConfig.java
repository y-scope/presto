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

import com.google.common.collect.ImmutableSet;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Set;

import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestClpMetadataFilterConfig
{
    private String filterConfigPath;
    private File tempFile;

    @BeforeMethod
    public void setUp() throws IOException
    {
        tempFile = File.createTempFile("metadata-filter", ".json");
        tempFile.deleteOnExit();
        filterConfigPath = tempFile.getAbsolutePath();

        String json =
                "{\n" +
                        "  \"clp\": {\n" +
                        "    \"filters\": [\n" +
                        "      {\n" +
                        "        \"filterName\": \"level\"\n" +
                        "      }\n" +
                        "    ]\n" +
                        "  },\n" +
                        "  \"clp.default\": {\n" +
                        "    \"filters\": [\n" +
                        "      {\n" +
                        "        \"filterName\": \"author\"\n" +
                        "      }\n" +
                        "    ]\n" +
                        "  },\n" +
                        "  \"clp.default.table_1\": {\n" +
                        "    \"filters\": [\n" +
                        "      {\n" +
                        "        \"filterName\": \"msg.timestamp\",\n" +
                        "        \"rangeMapping\": {\n" +
                        "          \"lowerBound\": \"begin_timestamp\",\n" +
                        "          \"upperBound\": \"end_timestamp\"\n" +
                        "        }\n" +
                        "      },\n" +
                        "      {\n" +
                        "        \"filterName\": \"file_name\"\n" +
                        "      }\n" +
                        "    ]\n" +
                        "  }\n" +
                        "}";

        try (FileWriter writer = new FileWriter(tempFile)) {
            writer.write(json);
        }
    }

    @Test
    public void getFilterNames()
    {
        ClpConfig config = new ClpConfig();
        config.setMetadataFilterConfig(filterConfigPath);
        ClpMetadataFilterProvider filterProvider = new ClpMetadataFilterProvider(config);
        Set<String> catalogFilterNames = filterProvider.getFilterNames("clp");
        assertEquals(ImmutableSet.of("level"), catalogFilterNames);
        Set<String> schemaFilterNames = filterProvider.getFilterNames("clp.default");
        assertEquals(ImmutableSet.of("level", "author"), schemaFilterNames);
        Set<String> tableFilterNames = filterProvider.getFilterNames("clp.default.table_1");
        assertEquals(ImmutableSet.of("level", "author", "msg.timestamp", "file_name"), tableFilterNames);
    }

    @Test
    public void remapSql()
    {
        ClpConfig config = new ClpConfig();
        config.setMetadataFilterConfig(filterConfigPath);
        ClpMetadataFilterProvider filterProvider = new ClpMetadataFilterProvider(config);

        String metadataFilterSql1 = "(\"msg.timestamp\" > 1234 AND \"msg.timestamp\" < 5678)";
        String remappedSql1 = filterProvider.remapFilterSql("clp.default.table_1", metadataFilterSql1);
        assertEquals(remappedSql1, "(end_timestamp > 1234 AND begin_timestamp < 5678)");

        String metadataFilterSql2 = "(\"msg.timestamp\" >= 1234 AND \"msg.timestamp\" <= 5678)";
        String remappedSql2 = filterProvider.remapFilterSql("clp.default.table_1", metadataFilterSql2);
        assertEquals(remappedSql2, "(end_timestamp >= 1234 AND begin_timestamp <= 5678)");

        String metadataFilterSql3 = "(\"msg.timestamp\" > 1234 AND \"msg.timestamp\" <= 5678)";
        String remappedSql3 = filterProvider.remapFilterSql("clp.default.table_1", metadataFilterSql3);
        assertEquals(remappedSql3, "(end_timestamp > 1234 AND begin_timestamp <= 5678)");

        String metadataFilterSql4 = "(\"msg.timestamp\" >= 1234 AND \"msg.timestamp\" < 5678)";
        String remappedSql4 = filterProvider.remapFilterSql("clp.default.table_1", metadataFilterSql4);
        assertEquals(remappedSql4, "(end_timestamp >= 1234 AND begin_timestamp < 5678)");

        String metadataFilterSql5 = "(\"msg.timestamp\" = 1234)";
        String remappedSql5 = filterProvider.remapFilterSql("clp.default.table_1", metadataFilterSql5);
        assertEquals(remappedSql5, "((begin_timestamp <= 1234 AND end_timestamp >= 1234))");
    }

    @AfterMethod
    public void tearDown()
    {
        if (null != tempFile && tempFile.exists()) {
            tempFile.delete();
        }
    }
}
