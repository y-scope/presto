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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.yscope.presto.schema.SchemaNode;
import javax.inject.Inject;

import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

public class ClpClient
{
    private static final String COLUMN_METADATA_PREFIX = "column_metadata_";
    private static final String ARCHIVE_TABLE_SUFFIX = "archives";
    private static final Logger log = Logger.get(ClpClient.class);
    private static final String QUERY_SELECT_COLUMNS = "SELECT * FROM %s" + COLUMN_METADATA_PREFIX + "?";
    private static final String QUERY_SHOW_TABLES = "SHOW TABLES";
    private static final String QUERY_SELECT_ARCHIVE_IDS = "SELECT id FROM %s" + ARCHIVE_TABLE_SUFFIX;

    private final ClpConfig config;
    private final String metadataDbUrl;
    private final ClpConfig.InputSource inputSource;
    private final LoadingCache<SchemaTableName, Set<ClpColumnHandle>> columnHandleCache;
    private final LoadingCache<String, Set<String>> tableNameCache;

    @Inject
    public ClpClient(ClpConfig config)
    {
        this.config = requireNonNull(config, "config is null");
        try {
            Class.forName("com.mysql.jdbc.Driver");
        }
        catch (ClassNotFoundException e) {
            log.error(e, "Failed to load MySQL JDBC driver");
        }
        this.metadataDbUrl = "jdbc:mysql://" + config.getMetadataDbHost() + ":" + config.getMetadataDbPort() + "/" + config.getMetadataDbName();
        this.inputSource = config.getInputSource();
        this.columnHandleCache = CacheBuilder.newBuilder()
                .expireAfterWrite(config.getMetadataExpireInterval(), SECONDS)
                .refreshAfterWrite(config.getMetadataRefreshInterval(), SECONDS)
                .build(CacheLoader.from(this::loadTableSchema));

        this.tableNameCache = CacheBuilder.newBuilder()
                .expireAfterWrite(config.getMetadataExpireInterval(), SECONDS)
                .refreshAfterWrite(config.getMetadataRefreshInterval(), SECONDS)
                .build(CacheLoader.from(this::loadTable));
    }

    public ClpConfig getConfig()
    {
        return config;
    }

    public Set<ClpColumnHandle> loadTableSchema(SchemaTableName schemaTableName)
    {
        String query = "SELECT * FROM " + config.getMetadataTablePrefix() + COLUMN_METADATA_PREFIX + schemaTableName.getTableName();

        Connection connection = null;
        LinkedHashSet<ClpColumnHandle> columnHandles = new LinkedHashSet<>();
        try {
            connection = DriverManager.getConnection(metadataDbUrl, config.getMetadataDbUser(), config.getMetadataDbPassword());
            Statement statement = connection.createStatement();

            ResultSet resultSet = statement.executeQuery(query);

            while (resultSet.next()) {
                String columnName = resultSet.getString("name");
                SchemaNode.NodeType columnType = SchemaNode.NodeType.fromType(resultSet.getByte("type"));
                Type prestoType = null;
                switch (columnType) {
                    case Integer:
                        prestoType = BigintType.BIGINT;
                        break;
                    case Float:
                        prestoType = DoubleType.DOUBLE;
                        break;
                    case ClpString:
                    case VarString:
                    case DateString:
                    case NullValue:
                        prestoType = VarcharType.VARCHAR;
                        break;
                    case UnstructuredArray:
                        prestoType = new ArrayType(VarcharType.VARCHAR);
                        break;
                    case Boolean:
                        prestoType = BooleanType.BOOLEAN;
                        break;
                    default:
                        break;
                }
                columnHandles.add(new ClpColumnHandle(columnName, prestoType, true));
            }
        }
        catch (SQLException e) {
            log.error(e, "Failed to connect to metadata database");
            return ImmutableSet.of();
        }
        finally {
            try {
                if (connection != null) {
                    connection.close();
                }
            }
            catch (SQLException ex) {
                log.warn(ex, "Failed to close metadata database connection");
            }
        }
        if (!config.isPolymorphicTypeEnabled()) {
            return columnHandles;
        }
        return handlePolymorphicType(columnHandles);
    }

    public Set<String> loadTable(String schemaName)
    {
        ImmutableSet.Builder<String> tableNames = ImmutableSet.builder();
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(metadataDbUrl, config.getMetadataDbUser(), config.getMetadataDbPassword());
            Statement statement = connection.createStatement();

            String query = "SHOW TABLES";
            ResultSet resultSet = statement.executeQuery(query);

            // Processing the results
            String databaseName = config.getMetadataDbName();
            String tableNamePrefix = config.getMetadataTablePrefix() + COLUMN_METADATA_PREFIX;
            while (resultSet.next()) {
                String tableName = resultSet.getString("Tables_in_" + databaseName);
                if (tableName.startsWith(config.getMetadataTablePrefix()) && tableName.length() > tableNamePrefix.length()) {
                    tableNames.add(tableName.substring(tableNamePrefix.length()));
                }
            }
        }
        catch (SQLException e) {
            log.error(e, "Failed to connect to metadata database");
        }
        finally {
            // Closing the connection
            try {
                if (connection != null) {
                    connection.close();
                }
            }
            catch (SQLException ex) {
                log.warn(ex, "Failed to close metadata database connection");
            }
        }
        return tableNames.build();
    }

    public Set<String> listTables(String schemaName)
    {
        return tableNameCache.getUnchecked(schemaName);
    }

    public List<String> listArchiveIds(String tableName)
    {
        if (inputSource == ClpConfig.InputSource.LOCAL) {
            Path tableDir = Paths.get(config.getClpArchiveDir(), tableName);
            if (!Files.exists(tableDir) || !Files.isDirectory(tableDir)) {
                return ImmutableList.of();
            }

            try (DirectoryStream<Path> stream = Files.newDirectoryStream(tableDir)) {
                ImmutableList.Builder<String> archiveIds = ImmutableList.builder();
                for (Path path : stream) {
                    if (Files.isDirectory(path)) {
                        archiveIds.add(path.getFileName().toString());
                    }
                }
                return archiveIds.build();
            }
            catch (Exception e) {
                return ImmutableList.of();
            }
        }
        else {
            Connection connection = null;
            try {
                connection = DriverManager.getConnection(metadataDbUrl, config.getMetadataDbUser(), config.getMetadataDbPassword());
                Statement statement = connection.createStatement();

                String query = "SELECT id FROM " + config.getMetadataTablePrefix() + ARCHIVE_TABLE_SUFFIX;
                ResultSet resultSet = statement.executeQuery(query);

                ImmutableList.Builder<String> archiveIds = ImmutableList.builder();
                while (resultSet.next()) {
                    archiveIds.add(resultSet.getString("id"));
                }
                return archiveIds.build();
            }
            catch (SQLException e) {
                log.error(e, "Failed to connect to metadata database");
                return ImmutableList.of();
            }
            finally {
                // Closing the connection
                try {
                    if (connection != null) {
                        connection.close();
                    }
                }
                catch (SQLException ex) {
                    log.warn(ex, "Failed to close metadata database connection");
                }
            }
        }
    }

    public Set<ClpColumnHandle> listColumns(SchemaTableName schemaTableName)
    {
        return columnHandleCache.getUnchecked(schemaTableName);
    }

    private Set<ClpColumnHandle> handlePolymorphicType(Set<ClpColumnHandle> columnHandles)
    {
        Map<String, List<ClpColumnHandle>> columnNameToColumnHandles = new HashMap<>();
        LinkedHashSet<ClpColumnHandle> polymorphicColumnHandles = new LinkedHashSet<>();

        for (ClpColumnHandle columnHandle : columnHandles) {
            columnNameToColumnHandles.computeIfAbsent(columnHandle.getColumnName(), k -> new ArrayList<>())
                    .add(columnHandle);
        }
        for (Map.Entry<String, List<ClpColumnHandle>> entry : columnNameToColumnHandles.entrySet()) {
            List<ClpColumnHandle> columnHandleList = entry.getValue();
            if (columnHandleList.size() == 1) {
                polymorphicColumnHandles.add(columnHandleList.get(0));
            }
            else {
                for (ClpColumnHandle columnHandle : columnHandleList) {
                    polymorphicColumnHandles.add(new ClpColumnHandle(
                            columnHandle.getColumnName() + "_" + columnHandle.getColumnType().getDisplayName(),
                            columnHandle.getColumnName(),
                            columnHandle.getColumnType(),
                            columnHandle.isNullable()));
                }
            }
        }
        return polymorphicColumnHandles;
    }
}
