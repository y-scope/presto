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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.plugin.clp.ClpConfig;
import com.facebook.presto.plugin.clp.ClpSplit;
import com.facebook.presto.plugin.clp.ClpTableHandle;
import com.facebook.presto.plugin.clp.ClpTableLayoutHandle;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static java.lang.String.format;

public class ClpMySqlSplitProvider
        implements ClpSplitProvider
{
    // Column names
    public static final String ARCHIVES_TABLE_COLUMN_ID = "id";

    // Table suffixes
    public static final String ARCHIVES_TABLE_SUFFIX = "_archives";

    // SQL templates
    private static final String SQL_SELECT_ARCHIVES_TEMPLATE = format("SELECT `%s` FROM `%%s%%s%s` WHERE 1 = 1", ARCHIVES_TABLE_COLUMN_ID, ARCHIVES_TABLE_SUFFIX);

    private static final Logger log = Logger.get(ClpMySqlSplitProvider.class);

    private final ClpConfig config;

    @Inject
    public ClpMySqlSplitProvider(ClpConfig config)
    {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        }
        catch (ClassNotFoundException e) {
            log.error(e, "Failed to load MySQL JDBC driver");
            throw new RuntimeException("MySQL JDBC driver not found", e);
        }
        this.config = config;
    }

    @Override
    public List<ClpSplit> listSplits(ClpTableLayoutHandle clpTableLayoutHandle)
    {
        ImmutableList.Builder<ClpSplit> splits = new ImmutableList.Builder<>();
        ClpTableHandle clpTableHandle = clpTableLayoutHandle.getTable();
        String tablePath = clpTableHandle.getTablePath();
        String tableName = clpTableHandle.getSchemaTableName().getTableName();
        String archivePathQuery = format(SQL_SELECT_ARCHIVES_TEMPLATE, config.getMetadataTablePrefix(), tableName);

        if (clpTableLayoutHandle.getMetadataSql().isPresent()) {
            String metadataFilterQuery = clpTableLayoutHandle.getMetadataSql().get();
            archivePathQuery += " AND (" + metadataFilterQuery + ")";
        }
        log.debug("Query for archive: %s", archivePathQuery);

        try (Connection connection = getConnection()) {
            // Fetch archive IDs and create splits
            try (PreparedStatement statement = connection.prepareStatement(archivePathQuery); ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    final String archiveId = resultSet.getString(ARCHIVES_TABLE_COLUMN_ID);
                    final String archivePath = tablePath + "/" + archiveId;
                    splits.add(new ClpSplit(archivePath, clpTableLayoutHandle.getKqlQuery()));
                }
            }
        }
        catch (SQLException e) {
            log.warn("Database error while processing splits for %s: %s", tableName, e);
        }

        ImmutableList<ClpSplit> filteredSplits = splits.build();
        log.debug("Number of splits: %s", filteredSplits.size());
        return filteredSplits;
    }

    private Connection getConnection()
            throws SQLException
    {
        Connection connection = DriverManager.getConnection(config.getMetadataDbUrl(), config.getMetadataDbUser(), config.getMetadataDbPassword());
        String dbName = config.getMetadataDbName();
        if (dbName != null && !dbName.isEmpty()) {
            connection.createStatement().execute(format("USE `%s`", dbName));
        }
        return connection;
    }
}
