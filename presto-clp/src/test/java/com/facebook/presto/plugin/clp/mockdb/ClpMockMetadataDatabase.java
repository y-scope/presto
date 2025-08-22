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
package com.facebook.presto.plugin.clp.mockdb;

import com.facebook.presto.plugin.clp.mockdb.table.ArchivesTableRows;
import com.facebook.presto.plugin.clp.mockdb.table.ColumnMetadataTableRows;
import com.facebook.presto.plugin.clp.mockdb.table.DatasetsTableRows;
import com.google.common.collect.ImmutableList;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class ClpMockMetadataDatabase
{
    public static Builder builder()
    {
        return new Builder();
    }

    private void addTableToDatasetsTableIfNotExist(List<String> tableNames)
    {
        try (Connection connection = DriverManager.getConnection(url, username, password)) {
            ImmutableList.Builder<String> repeatedArchiveStorageDirectory = ImmutableList.builder();
            for (String tableName : tableNames) {
                ArchivesTableRows.createTableIfNotExist(connection, tablePrefix, tableName);
                ColumnMetadataTableRows.createTableIfNotExist(connection, tablePrefix, tableName);
                repeatedArchiveStorageDirectory.add(archiveStorageDirectory);
            }
            DatasetsTableRows datasetsTableRows = new DatasetsTableRows(tableNames, repeatedArchiveStorageDirectory.build());
            datasetsTableRows.insertToTable(connection, tablePrefix);
        }
        catch (SQLException e) {
            fail(e.getMessage());
        }
    }

    private void addColumnMetadata(Map<String, ColumnMetadataTableRows> clpFields)
    {
        try (Connection connection = DriverManager.getConnection(url, username, password)) {
            for (Map.Entry<String, ColumnMetadataTableRows> entry : clpFields.entrySet()) {
                entry.getValue().insertToTable(connection, tablePrefix, entry.getKey());
            }
        }
        catch (SQLException e) {
            fail(e.getMessage());
        }
    }

    private void addSplits(Map<String, ArchivesTableRows> splits)
    {
        try (Connection connection = DriverManager.getConnection(url, username, password)) {
            for (Map.Entry<String, ArchivesTableRows> entry : splits.entrySet()) {
                entry.getValue().insertToTable(connection, tablePrefix, entry.getKey());
            }
        }
        catch (SQLException e) {
            fail(e.getMessage());
        }
    }

    public void teardown()
    {
        try (Connection connection = DriverManager.getConnection(url, username, password); Statement stmt = connection.createStatement()) {
            stmt.execute("DROP ALL OBJECTS"); // wipes all tables, views, etc.
        }
        catch (SQLException e) {
            fail(e.getMessage());
        }
        if (databaseFile.exists()) {
            try {
                FileUtils.delete(databaseFile);
            }
            catch (IOException e) {
                fail(e.getMessage());
            }
        }
    }

    public String getUrl()
    {
        return url;
    }

    public String getUsername()
    {
        return username;
    }

    public String getPassword()
    {
        return password;
    }

    public String getTablePrefix()
    {
        return tablePrefix;
    }

    private static final String MOCK_METADATA_DB_DEFAULT_USERNAME = "sa";
    private static final String MOCK_METADATA_DB_DEFAULT_PASSWORD = "";

    private static final String MOCK_METADATA_DB_DEFAULT_TABLE_PREFIX = "clp_";

    private static final String MOCK_METADATA_DB_URL_TEMPLATE = "jdbc:h2:file:%s;MODE=MySQL;DATABASE_TO_UPPER=FALSE";

    private ClpMockMetadataDatabase() {}

    private String url;
    private File databaseFile;
    private String archiveStorageDirectory;
    private String username;
    private String password;
    private String tablePrefix;

    public static final class Builder
    {
        private final ClpMockMetadataDatabase mockMetadataDatabase;
        private boolean isDatasetsTableCreated;

        private Builder()
        {
            mockMetadataDatabase = new ClpMockMetadataDatabase();
            setDatabaseUrl(format("/tmp/%s", UUID.randomUUID()));
            setUsername(MOCK_METADATA_DB_DEFAULT_USERNAME);
            setPassword(MOCK_METADATA_DB_DEFAULT_PASSWORD);
            setTablePrefix(MOCK_METADATA_DB_DEFAULT_TABLE_PREFIX);
        }

        public Builder setDatabaseUrl(String databaseFilePath)
        {
            mockMetadataDatabase.databaseFile = new File(databaseFilePath);
            mockMetadataDatabase.url = format(MOCK_METADATA_DB_URL_TEMPLATE, databaseFilePath);
            return this;
        }

        public Builder setArchiveStorageDirectory(String archiveStorageDirectory)
        {
            mockMetadataDatabase.archiveStorageDirectory = archiveStorageDirectory;
            return this;
        }

        public Builder setUsername(String username)
        {
            mockMetadataDatabase.username = username;
            return this;
        }

        public Builder setPassword(String password)
        {
            mockMetadataDatabase.password = password;
            return this;
        }

        public Builder setTablePrefix(String tablePrefix)
        {
            mockMetadataDatabase.tablePrefix = tablePrefix;
            return this;
        }

        public Builder createDatasetsTableIfNotExist()
        {
            if (!isDatasetsTableCreated) {
                DatasetsTableRows.createTableIfNotExist(
                        mockMetadataDatabase.url,
                        mockMetadataDatabase.username,
                        mockMetadataDatabase.password,
                        mockMetadataDatabase.tablePrefix);
                isDatasetsTableCreated = true;
            }
            return this;
        }

        public Builder addTables(List<String> tableNames)
        {
            validate();
            mockMetadataDatabase.addTableToDatasetsTableIfNotExist(tableNames);
            return this;
        }

        public Builder addColumnMetadata(Map<String, ColumnMetadataTableRows> clpFields)
        {
            validate();
            mockMetadataDatabase.addColumnMetadata(clpFields);
            return this;
        }

        public Builder addSplits(Map<String, ArchivesTableRows> splits)
        {
            validate();
            mockMetadataDatabase.addSplits(splits);
            return this;
        }

        public ClpMockMetadataDatabase build()
        {
            validate();
            return mockMetadataDatabase;
        }

        private void validate()
        {
            assertTrue(isDatasetsTableCreated, "createDatasetsTableIfNotExist might not be called");
            requireNonNull(mockMetadataDatabase.url, "url is null");
            requireNonNull(mockMetadataDatabase.archiveStorageDirectory, "archiveStorageDirectory is null");
            requireNonNull(mockMetadataDatabase.username, "username is null");
            requireNonNull(mockMetadataDatabase.password, "password is null");
            requireNonNull(mockMetadataDatabase.tablePrefix, "tablePrefix is null");
        }
    }
}
