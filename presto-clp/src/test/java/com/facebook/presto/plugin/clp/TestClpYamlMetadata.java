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

import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.plugin.clp.metadata.ClpMetadataProvider;
import com.facebook.presto.plugin.clp.metadata.ClpYamlMetadataProvider;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.plugin.clp.ClpConfig.MetadataProviderType.YAML;
import static com.facebook.presto.plugin.clp.ClpMetadata.DEFAULT_SCHEMA_NAME;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static org.testng.Assert.assertEquals;

public class TestClpYamlMetadata
{
    private static final String PINOT_BROKER_URL = "jdbc:pinot://localhost:8099";
    private static final String METADATA_YAML_PATH = "/home/xiaochong-dev/presto-e2e/pinot/tables-schema.yaml";
    private static final String TABLE_NAME = "cockroachdb";
    private ClpMetadata metadata;

    @BeforeTest
    public void setUp()
    {
        ClpConfig config = new ClpConfig()
                .setPolymorphicTypeEnabled(true)
                .setMetadataDbUrl(PINOT_BROKER_URL)
                .setMetadataProviderType(YAML)
                .setMetadataYamlPath(METADATA_YAML_PATH);
        ClpMetadataProvider metadataProvider = new ClpYamlMetadataProvider(config);
        metadata = new ClpMetadata(config, metadataProvider);
    }

    @Test
    public void testListSchemaNames()
    {
        assertEquals(metadata.listSchemaNames(SESSION), ImmutableList.of(DEFAULT_SCHEMA_NAME));
    }

    @Test
    public void testListTables()
    {
        ImmutableSet.Builder<SchemaTableName> builder = ImmutableSet.builder();
        builder.add(new SchemaTableName(DEFAULT_SCHEMA_NAME, TABLE_NAME));
        assertEquals(new HashSet<>(metadata.listTables(SESSION, Optional.empty())), builder.build());
    }

    @Test
    public void testGetTableMetadata()
    {
        ClpTableHandle clpTableHandle = (ClpTableHandle) metadata.getTableHandle(SESSION, new SchemaTableName(DEFAULT_SCHEMA_NAME, TABLE_NAME));
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(SESSION, clpTableHandle);
//        ImmutableSet<ColumnMetadata> columnMetadata = ImmutableSet.<ColumnMetadata>builder()
//                .add(ColumnMetadata.builder()
//                        .setName("a_bigint")
//                        .setType(BIGINT)
//                        .setNullable(true)
//                        .build())
//                .add(ColumnMetadata.builder()
//                        .setName("a_varchar")
//                        .setType(VARCHAR)
//                        .setNullable(true)
//                        .build())
//                .add(ColumnMetadata.builder()
//                        .setName("b_double")
//                        .setType(DOUBLE)
//                        .setNullable(true)
//                        .build())
//                .add(ColumnMetadata.builder()
//                        .setName("b_varchar")
//                        .setType(VARCHAR)
//                        .setNullable(true)
//                        .build())
//                .add(ColumnMetadata.builder()
//                        .setName("c")
//                        .setType(RowType.from(ImmutableList.of(
//                                RowType.field("d", BOOLEAN),
//                                RowType.field("e", VARCHAR))))
//                        .setNullable(true)
//                        .build())
//                .add(ColumnMetadata.builder()
//                        .setName("f")
//                        .setType(RowType.from(ImmutableList.of(
//                                RowType.field("g",
//                                        RowType.from(ImmutableList.of(
//                                                RowType.field("h", new ArrayType(VARCHAR))))))))
//                        .setNullable(true)
//                        .build())
//                .build();
//        assertEquals(columnMetadata, ImmutableSet.copyOf(tableMetadata.getColumns()));
        ImmutableSet<ColumnMetadata> actual = ImmutableSet.copyOf(tableMetadata.getColumns());
        System.out.println("Hello world");
    }
}
