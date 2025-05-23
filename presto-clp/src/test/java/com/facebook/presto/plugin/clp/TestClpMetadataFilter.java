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
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestClpMetadataFilter
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
                        "        \"filterName\": \"level\",\n" +
                        "        \"mustHave\": false\n" +
                        "      }\n" +
                        "    ]\n" +
                        "  },\n" +
                        "  \"clp.default\": {\n" +
                        "    \"filters\": [\n" +
                        "      {\n" +
                        "        \"filterName\": \"author\",\n" +
                        "        \"mustHave\": false\n" +
                        "      }\n" +
                        "    ]\n" +
                        "  },\n" +
                        "  \"clp.default.table_1\": {\n" +
                        "    \"filters\": [\n" +
                        "      {\n" +
                        "        \"filterName\": \"timestamp\",\n" +
                        "        \"mustHave\": false\n" +
                        "      },\n" +
                        "      {\n" +
                        "        \"filterName\": \"file_name\",\n" +
                        "        \"mustHave\": true\n" +
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
        assertEquals(ImmutableSet.of("level", "author", "timestamp", "file_name"), tableFilterNames);

        Set<String> mustHaveCatalogFilterNames = filterProvider.getMustHaveFilterNames("clp");
        assertTrue(mustHaveCatalogFilterNames.isEmpty());
        Set<String> mustHaveSchemaFilterNames = filterProvider.getMustHaveFilterNames("clp.default");
        assertTrue(mustHaveSchemaFilterNames.isEmpty());
        Set<String> mustHaveTableFilterNames = filterProvider.getMustHaveFilterNames("clp.default.table_1");
        assertEquals(ImmutableSet.of("file_name"), mustHaveTableFilterNames);
    }

    @AfterMethod
    public void tearDown()
    {
        if (null != tempFile && tempFile.exists()) {
            tempFile.delete();
        }
    }
}
