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
package com.facebook.presto.plugin.clp.split;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.plugin.clp.ClpConfig;
import com.facebook.presto.plugin.clp.TestClpQueryBase;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.sql.planner.TypeProvider;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static java.util.UUID.randomUUID;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestClpSplitMetadataConfig
        extends TestClpQueryBase
{
    private String splitMetadataConfigPath;

    @BeforeMethod
    public void setUp() throws IOException, URISyntaxException
    {
        URL resource = getClass().getClassLoader().getResource("test-mysql-split-metadata.json");
        if (resource == null) {
            throw new FileNotFoundException("test-mysql-split-metadata.json not found in resources");
        }

        splitMetadataConfigPath = Paths.get(resource.toURI()).toAbsolutePath().toString();
    }

    @Test
    public void checkRequiredColumns()
    {
        SessionHolder sessionHolder = new SessionHolder();
        ClpConfig config = new ClpConfig();
        config.setSplitMetadataConfigPath(splitMetadataConfigPath);
        ClpSplitMetadataConfig splitMetadataConfig = new ClpSplitMetadataConfig(config, functionAndTypeManager);
        SchemaTableName schemaTableName = new SchemaTableName("default", "table_1");

        ClpMySqlSplitMetadataExpressionConverter converter = new ClpMySqlSplitMetadataExpressionConverter(
                functionAndTypeManager,
                standardFunctionResolution,
                splitMetadataConfig.getExposedToOriginalMapping(schemaTableName),
                splitMetadataConfig.getDataColumnRangeMapping(schemaTableName),
                splitMetadataConfig.getRequiredColumns(schemaTableName));

        TypeProvider typeProvider = TypeProvider.viewOf(ImmutableMap.of("level", BIGINT, "msg.timestamp", BIGINT));
        assertThrows(PrestoException.class, ()
                -> converter.transform(getRowExpression("(\"level\" >= 1 AND \"level\" <= 3)", typeProvider, sessionHolder)));

        converter.transform(
                getRowExpression("(\"msg.timestamp\" > 1234 AND \"msg.timestamp\" < 5678)", typeProvider, sessionHolder));
    }

    @Test
    public void getMetadataColumnNames()
    {
        ClpConfig config = new ClpConfig();
        config.setSplitMetadataConfigPath(splitMetadataConfigPath);
        ClpSplitMetadataConfig splitMetadataConfig = new ClpSplitMetadataConfig(config, functionAndTypeManager);

        Map<String, Type> columns = splitMetadataConfig.getMetadataColumns(new SchemaTableName("other", "test"));
        assertEquals(ImmutableMap.of("level", BIGINT), columns);
        columns = splitMetadataConfig.getMetadataColumns(new SchemaTableName("default", "table_2"));
        assertEquals(ImmutableMap.of("level", BIGINT, "author", VARCHAR), columns);
        columns = splitMetadataConfig.getMetadataColumns(new SchemaTableName("default", "table_1"));
        assertEquals(columns, ImmutableMap.of(
                "level", BIGINT,
                "author", VARCHAR,
                "begin_timestamp", BIGINT,
                "end_timestamp", BIGINT,
                "file_name", VARCHAR));
        Set<String> dataColumnsWithRangeBounds =
                splitMetadataConfig.getDataColumnsWithRangeBounds(new SchemaTableName("default", "table_1"));
        assertEquals(dataColumnsWithRangeBounds, ImmutableSet.of("msg.timestamp"));
    }

    @Test
    public void handleEmptyAndInvalidSplitMetadataConfig()
    {
        ClpConfig config = new ClpConfig();

        // Empty config
        ClpSplitMetadataConfig splitMetadataConfig = new ClpSplitMetadataConfig(config, functionAndTypeManager);
        assertTrue(splitMetadataConfig.getMetadataColumns(new SchemaTableName("default", "clp")).isEmpty());
        assertTrue(splitMetadataConfig.getMetadataColumns(new SchemaTableName("abc", "xyz")).isEmpty());
        assertTrue(splitMetadataConfig.getMetadataColumns(new SchemaTableName("abc.opq", "xyz")).isEmpty());

        // Invalid config
        config.setSplitMetadataConfigPath(randomUUID().toString());
        assertThrows(PrestoException.class, () -> new ClpSplitMetadataConfig(config, functionAndTypeManager));
    }
}
