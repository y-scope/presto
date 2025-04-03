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

import com.facebook.presto.spi.SchemaTableName;
import com.yscope.presto.split.ClpMySQLSplitProvider;
import com.yscope.presto.split.ClpSplitProvider;
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
import java.util.List;
import java.util.Optional;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestClpSplit
{
    private ClpConfig config;
    private static final String TABLE_NAME_1 = "test_1";
    private static final String TABLE_NAME_2 = "test_2";
    private static final String TABLE_NAME_3 = "test_3";
    private static final String TABLE_SCHEMA = "default";
    private static final List<String> TABLE_NAME_LIST = Arrays.asList(TABLE_NAME_1, TABLE_NAME_2, TABLE_NAME_3);
    private static final int NUM_SPLITS = 10;

    @BeforeMethod
    public void setUp()
    {
        final String metadataDbUrl = "jdbc:h2:file:/tmp/split_testdb;MODE=MySQL;DATABASE_TO_UPPER=FALSE";
        final String metadataDbUser = "sa";
        final String metadataDbPassword = "";
        final String metadataDbTablePrefix = "clp_";
        final String tableMetadataSuffix = "table_metadata";
        final String archiveTableSuffix = "_archives";

        this.config = new ClpConfig().setPolymorphicTypeEnabled(true)
                .setMetadataDbUrl(metadataDbUrl)
                .setMetadataDbUser("sa")
                .setMetadataDbPassword("")
                .setMetadataTablePrefix(metadataDbTablePrefix);

        final String tableMetadataTableName = metadataDbTablePrefix + tableMetadataSuffix;
        final String archiveTableFormat = metadataDbTablePrefix + "%s" + archiveTableSuffix;

        final String createTableMetadataSQL = String.format(
                "CREATE TABLE IF NOT EXISTS %s (" +
                        " table_name VARCHAR(512) PRIMARY KEY," +
                        " table_path VARCHAR(1024) NOT NULL)", tableMetadataTableName);

        try (Connection conn = DriverManager.getConnection(metadataDbUrl, metadataDbUser, metadataDbPassword);
                Statement stmt = conn.createStatement()) {
            stmt.execute(createTableMetadataSQL);

            // Insert table metadata in batch
            String insertTableMetadataSQL = String.format("INSERT INTO %s (table_name, table_path) VALUES (?, ?)", tableMetadataTableName);
            try (PreparedStatement pstmt = conn.prepareStatement(insertTableMetadataSQL)) {
                for (String tableName : TABLE_NAME_LIST) {
                    pstmt.setString(1, tableName);
                    pstmt.setString(2, "/tmp/archives/" + tableName);
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }

            // Create and populate archive tables
            for (String tableName : TABLE_NAME_LIST) {
                String archiveTableName = String.format(archiveTableFormat, tableName);
                String createArchiveTableSQL = String.format("CREATE TABLE IF NOT EXISTS %s (id VARCHAR(128) PRIMARY KEY)", archiveTableName);
                stmt.execute(createArchiveTableSQL);

                String insertArchiveTableSQL = String.format("INSERT INTO %s (id) VALUES (?)", archiveTableName);
                try (PreparedStatement pstmt = conn.prepareStatement(insertArchiveTableSQL)) {
                    for (int i = 0; i < NUM_SPLITS; i++) {
                        pstmt.setString(1, "id_" + i);
                        pstmt.addBatch();
                    }
                    pstmt.executeBatch();
                }
            }
        }
        catch (SQLException e) {
            fail(e.getMessage());
        }
    }

    @AfterMethod
    public void tearDown()
    {
        File dbFile = new File("/tmp/split_testdb.mv.db");
        File lockFile = new File("/tmp/split_testdb.trace.db"); // Optional, H2 sometimes creates this
        if (dbFile.exists()) {
            dbFile.delete();
            System.out.println("Deleted database file: " + dbFile.getAbsolutePath());
        }
        if (lockFile.exists()) {
            lockFile.delete();
        }
    }

    @Test
    public void testListSplits()
    {
        ClpSplitProvider splitProvider = new ClpMySQLSplitProvider(config);
        for (String tableName : TABLE_NAME_LIST) {
            ClpTableLayoutHandle layoutHandle = new ClpTableLayoutHandle(new ClpTableHandle(new SchemaTableName(TABLE_SCHEMA, tableName)), Optional.empty());
            List<ClpSplit> splits = splitProvider.listSplits(layoutHandle);
            assertEquals(splits.size(), NUM_SPLITS);
            for (int i = 0; i < NUM_SPLITS; i++) {
                assertEquals(splits.get(i).getArchivePath(), "/tmp/archives/" + tableName + "/id_" + i);
                assertEquals(splits.get(i).getQuery(), Optional.empty());
            }
        }
    }
}
