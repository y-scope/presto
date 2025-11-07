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
package com.facebook.presto.plugin.clp.split.filter;

import com.facebook.presto.plugin.clp.ClpConfig;
import com.facebook.presto.plugin.clp.TestClpQueryBase;
import com.facebook.presto.plugin.clp.split.ClpMySqlSplitMetadataExpressionConverter;
import com.facebook.presto.plugin.clp.split.ClpSplitMetadataConfig;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.sql.planner.TypeProvider;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestClpMySqlSplitMetadataConfig
        extends TestClpQueryBase
{
    private String splitMetadataConfigPath;
    private TypeProvider typeProvider;
    private ClpMySqlSplitMetadataExpressionConverter converter;

    @BeforeMethod
    public void setUp() throws IOException, URISyntaxException
    {
        URL resource = getClass().getClassLoader().getResource("test-mysql-split-metadata.json");
        if (resource == null) {
            throw new FileNotFoundException("test-mysql-split-metadata.json not found in resources");
        }

        splitMetadataConfigPath = Paths.get(resource.toURI()).toAbsolutePath().toString();
        typeProvider = TypeProvider.viewOf(
                ImmutableMap.of("msg.timestamp", BIGINT,"begin_timestamp", BIGINT, "end_timestamp", BIGINT));

        ClpConfig config = new ClpConfig();
        config.setSplitMetadataConfigPath(splitMetadataConfigPath);
        ClpSplitMetadataConfig splitMetadataConfig = new ClpSplitMetadataConfig(config, functionAndTypeManager);

        SchemaTableName schemaTableName = new SchemaTableName("default", "table_1");
        converter = new ClpMySqlSplitMetadataExpressionConverter(
                functionAndTypeManager,
                standardFunctionResolution,
                splitMetadataConfig.getExposedToOriginalMapping(schemaTableName),
                splitMetadataConfig.getDataColumnRangeMapping(schemaTableName),
                splitMetadataConfig.getRequiredColumns(schemaTableName));
    }

    @Test
    public void remapSplitFilterPushDownExpression()
    {
        // Integer
        testRange(1234, 5678);
        testRange(-5678, -1234);

        // Decimal
        testRange(1234.001, 5678.999);
        testRange(-5678.999, -1234.001);

        // Scientific
        testRange("1.234E3", "5.678e3", 1234.0, 5678.0);
        testRange("-1.234e-3", "-5.678E-3", -0.001234, -0.005678);
    }

    private <T> void testRange(T lowerBound, T upperBound) {
        testRange(lowerBound, upperBound, lowerBound, upperBound);
    }

    private <T> void testRange(T lowerBound, T upperBound, T expectedLowerBound, T expectedUpperBound)
    {
        SessionHolder sessionHolder = new SessionHolder();

        String remappedSql1 = converter.transform(
                getRowExpression(
                        format("(\"msg.timestamp\" > %s AND \"msg.timestamp\" < %s)", lowerBound, upperBound),
                        typeProvider,
                        sessionHolder));
        assertEquals(
                remappedSql1,
                format("(end_timestamp > %s) AND (begin_timestamp < %s)", expectedLowerBound, expectedUpperBound));

        String remappedSql2 = converter.transform(
                getRowExpression(
                        format("(\"msg.timestamp\" >= %s AND \"msg.timestamp\" <= %s)", lowerBound, upperBound),
                        typeProvider,
                        sessionHolder));
        assertEquals(
                remappedSql2,
                format("(end_timestamp >= %s) AND (begin_timestamp <= %s)", expectedLowerBound, expectedUpperBound));

        String remappedSql3 = converter.transform(
                getRowExpression(
                        format("(\"msg.timestamp\" > %s AND \"msg.timestamp\" <= %s)", lowerBound, upperBound),
                        typeProvider,
                        sessionHolder));
        assertEquals(
                remappedSql3,
                format("(end_timestamp > %s) AND (begin_timestamp <= %s)", expectedLowerBound, expectedUpperBound));

        String remappedSql4 = converter.transform(
                getRowExpression(
                        format("(\"msg.timestamp\" >= %s AND \"msg.timestamp\" < %s)", lowerBound, upperBound),
                        typeProvider,
                        sessionHolder));
        assertEquals(
                remappedSql4,
                format("(end_timestamp >= %s) AND (begin_timestamp < %s)", expectedLowerBound, expectedUpperBound));

        String remappedSql5 = converter.transform(
                getRowExpression(format("(\"msg.timestamp\" = %s)", lowerBound), typeProvider, sessionHolder));
        assertEquals(
                remappedSql5,
                format("(begin_timestamp <= %s) AND (end_timestamp >= %s)", expectedLowerBound, expectedLowerBound));
    }
}
