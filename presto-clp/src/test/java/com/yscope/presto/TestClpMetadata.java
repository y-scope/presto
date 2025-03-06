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
package com.yscope.presto;

import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableList;
import com.yscope.presto.metadata.ClpNodeType;
import org.apache.commons.math3.util.Pair;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestClpMetadata
{
    private ClpMetadata metadata;
    private String metadataDbUrl;

    private static final String TABLE_NAME = "test";
    private static final String TABLE_SCHEMA = "default";

    @BeforeMethod
    public void setUp()
    {
        metadataDbUrl = "jdbc:h2:file:/tmp/testdb;MODE=MySQL;DATABASE_TO_UPPER=FALSE";
        String metadataDbTablePrefix = "clp_";
        String columnMetadataTablePrefix = "column_metadata_";
        ClpConfig config = new ClpConfig().setPolymorphicTypeEnabled(true)
                .setMetadataDbUrl(metadataDbUrl)
                .setMetadataDbUser("sa")
                .setMetadataDbPassword("")
                .setMetadataTablePrefix(metadataDbTablePrefix);
        metadata = new ClpMetadata(new ClpClient(config));

        try (Connection conn = DriverManager.getConnection(metadataDbUrl, "sa", "");
                Statement stmt = conn.createStatement()) {
            String createTable = "CREATE TABLE IF NOT EXISTS " + metadataDbTablePrefix + columnMetadataTablePrefix
                    + TABLE_NAME + " (name VARCHAR(512) NOT NULL, type TINYINT NOT NULL, PRIMARY KEY (name, type))";
            stmt.execute(createTable);

            List<Pair<String, ClpNodeType>> records = Arrays.asList(
                    new Pair<>("a", ClpNodeType.Integer),
                    new Pair<>("a", ClpNodeType.VarString),
                    new Pair<>("b", ClpNodeType.Float),
                    new Pair<>("b", ClpNodeType.ClpString),
                    new Pair<>("c", ClpNodeType.Float),
                    new Pair<>("c.d", ClpNodeType.Boolean),
                    new Pair<>("c.e", ClpNodeType.VarString));

            String insertSQL = "INSERT INTO " + metadataDbTablePrefix + columnMetadataTablePrefix + TABLE_NAME
                    + " (name, type) VALUES (?, ?)";
            try (PreparedStatement pstmt = conn.prepareStatement(insertSQL)) {
                for (Pair<String, ClpNodeType> record : records) {
                    pstmt.setString(1, record.getFirst());
                    pstmt.setByte(2, record.getSecond().getType());
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }
        }
        catch (SQLException e) {
            fail(e.getMessage());
        }
    }

    @AfterMethod
    public void tearDown()
    {
        File dbFile = new File("/tmp/testdb.mv.db");
        File lockFile = new File("/tmp/testdb.trace.db"); // Optional, H2 sometimes creates this
        if (dbFile.exists()) {
            dbFile.delete();
            System.out.println("Deleted database file: " + dbFile.getAbsolutePath());
        }
        if (lockFile.exists()) {
            lockFile.delete();
        }
    }

    @Test
    public void testListSchemaNames()
    {
        assertEquals(metadata.listSchemaNames(SESSION), ImmutableList.of(TABLE_SCHEMA));
    }

    @Test
    public void testListTables()
    {
        HashSet<SchemaTableName> tables = new HashSet<>();
        tables.add(new SchemaTableName(TABLE_SCHEMA, TABLE_NAME));
        assertEquals(new HashSet<>(metadata.listTables(SESSION, Optional.empty())), tables);
    }

    @Test
    public void testGetTableMetadata()
    {
        ClpTableHandle clpTableHandle =
                (ClpTableHandle) metadata.getTableHandle(SESSION, new SchemaTableName(TABLE_SCHEMA, TABLE_NAME));
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(SESSION, clpTableHandle);
        HashSet<ColumnMetadata> columnMetadata = new HashSet<>();
        columnMetadata.add(ColumnMetadata.builder()
                .setName("a_bigint")
                .setType(BigintType.BIGINT)
                .setNullable(true)
                .build());
        columnMetadata.add(ColumnMetadata.builder()
                .setName("a_varchar")
                .setType(VarcharType.VARCHAR)
                .setNullable(true)
                .build());
        columnMetadata.add(ColumnMetadata.builder()
                .setName("b_double")
                .setType(DoubleType.DOUBLE)
                .setNullable(true)
                .build());
        columnMetadata.add(ColumnMetadata.builder()
                .setName("b_varchar")
                .setType(VarcharType.VARCHAR)
                .setNullable(true)
                .build());
        columnMetadata.add(ColumnMetadata.builder()
                .setName("c.d")
                .setType(BooleanType.BOOLEAN)
                .setNullable(true)
                .build());
        columnMetadata.add(ColumnMetadata.builder()
                .setName("c.e")
                .setType(VarcharType.VARCHAR)
                .setNullable(true)
                .build());
        columnMetadata.add(ColumnMetadata.builder().setName("c").setType(DoubleType.DOUBLE).setNullable(true).build());
        assertEquals(columnMetadata, new HashSet<>(tableMetadata.getColumns()));
    }
}
