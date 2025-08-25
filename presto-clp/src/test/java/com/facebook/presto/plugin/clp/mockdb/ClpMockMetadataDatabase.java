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

/**
 * File-backed H2 mock metadata database for CLP tests. Uses the same schema as the CLP package.
 * Provides a builder-driven setup and a single-call teardown that drops all objects and deletes
 * files.
 */
public class ClpMockMetadataDatabase
{
    private static final String MOCK_METADATA_DB_DEFAULT_USERNAME = "sa";
    private static final String MOCK_METADATA_DB_DEFAULT_PASSWORD = "";

    private static final String MOCK_METADATA_DB_DEFAULT_TABLE_PREFIX = "clp_";

    private static final String MOCK_METADATA_DB_URL_TEMPLATE = "jdbc:h2:file:%s;MODE=MySQL;DATABASE_TO_UPPER=FALSE";

    private String url;
    private String archiveStorageDirectory;
    private String username;
    private String password;
    private String tablePrefix;

    /**
     * Creates a new builder instance for constructing {@link ClpMockMetadataDatabase}.
     *
     * @return a new {@link Builder}
     */
    public static Builder builder()
    {
        return new Builder();
    }

    /**
     * Tears down the mock database by dropping all objects (tables, views, etc.) and deleting the
     * backing database file. Any exceptions during cleanup will cause the test to fail.
     */
    public void teardown()
    {
        try (Connection connection = DriverManager.getConnection(url, username, password); Statement stmt = connection.createStatement()) {
            stmt.execute("DROP ALL OBJECTS DELETE FILES");
        }
        catch (SQLException e) {
            fail(e.getMessage());
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

    private ClpMockMetadataDatabase() {}

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

        /**
         * Creates the datasets table if it does not already exist. Must be called before invoking
         * {@code addTables}, {@code addColumnMetadata}, or {@code addSplits}. Assumes
         * {@code setDatabaseUrl}, {@code setUsername}, {@code setPassword}, and
         * {@code setTablePrefix} have been set (defaults provided).
         *
         * @return this builder
         */
        public Builder createDatasetsTableIfNotExist()
        {
            validateDatasetsTableCreationRequirements();
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

        /**
         * Ensures all table names have been registered in the datasets table and for each table name
         * an archives table and a column metadata table have been created.
         *
         * @param tableNames list of table names to add
         * @return this builder
         */
        public Builder addTables(List<String> tableNames)
        {
            validateMockMetadataDatabase();
            mockMetadataDatabase.addTableToDatasetsTableIfNotExist(tableNames);
            return this;
        }

        /**
         * Inserts column metadata for the given fields into the column metadata table corresponding to
         * the given table name.
         *
         * @param clpFields mapping of table name to {@link ColumnMetadataTableRows}
         * @return this builder
         */
        public Builder addColumnMetadata(Map<String, ColumnMetadataTableRows> clpFields)
        {
            validateMockMetadataDatabase();
            mockMetadataDatabase.addColumnMetadata(clpFields);
            return this;
        }

        /**
         * Inserts split metadata into the archives table corresponding to the given table name.
         *
         * @param splits mapping of table name to {@link ArchivesTableRows}
         * @return this builder
         */
        public Builder addSplits(Map<String, ArchivesTableRows> splits)
        {
            validateMockMetadataDatabase();
            mockMetadataDatabase.addSplits(splits);
            return this;
        }

        /**
         * Builds and returns the configured {@link ClpMockMetadataDatabase} instance.
         *
         * @return the constructed {@link ClpMockMetadataDatabase}
         */
        public ClpMockMetadataDatabase build()
        {
            validateMockMetadataDatabase();
            return mockMetadataDatabase;
        }

        /**
         * Validates that all required parameters have been set and the datasets table has been
         * created.
         */
        private void validateMockMetadataDatabase()
        {
            assertTrue(isDatasetsTableCreated, "createDatasetsTableIfNotExist might not be called");
            requireNonNull(mockMetadataDatabase.archiveStorageDirectory, "archiveStorageDirectory is null");
            validateDatasetsTableCreationRequirements();
        }

        private void validateDatasetsTableCreationRequirements()
        {
            requireNonNull(mockMetadataDatabase.url, "url is null");
            requireNonNull(mockMetadataDatabase.username, "username is null");
            requireNonNull(mockMetadataDatabase.password, "password is null");
            requireNonNull(mockMetadataDatabase.tablePrefix, "tablePrefix is null");
        }
    }
}
