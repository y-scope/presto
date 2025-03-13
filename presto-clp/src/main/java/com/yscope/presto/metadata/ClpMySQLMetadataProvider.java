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
package com.yscope.presto.metadata;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.SchemaTableName;
import com.yscope.presto.ClpColumnHandle;
import com.yscope.presto.ClpConfig;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

public class ClpMySQLMetadataProvider
        implements ClpMetadataProvider
{
    private static final Logger log = Logger.get(ClpMySQLMetadataProvider.class);

    public static final String COLUMN_METADATA_PREFIX = "column_metadata_";
    private static final String QUERY_SELECT_COLUMNS = "SELECT * FROM %s" + COLUMN_METADATA_PREFIX + "%s";
    private static final String TABLE_METADATA_TABLE_SUFFIX = "table_metadata";
    private static final String QUERY_SELECT_TABLES = "SELECT table_name FROM %s" + TABLE_METADATA_TABLE_SUFFIX;

    private final ClpConfig config;

    public ClpMySQLMetadataProvider(ClpConfig config)
    {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        }
        catch (ClassNotFoundException e) {
            log.error(e, "Failed to load MySQL JDBC driver");
        }
        this.config = config;
    }

    private Connection getConnection() throws SQLException
    {
        return DriverManager.getConnection(config.getMetadataDbUrl(), config.getMetadataDbUser(), config.getMetadataDbPassword());
    }

    // TODO(Rui): Consider move it to a util class
    private Type mapColumnType(byte type)
    {
        switch (ClpNodeType.fromType(type)) {
            case Integer:
                return BigintType.BIGINT;
            case Float:
                return DoubleType.DOUBLE;
            case ClpString:
            case VarString:
            case DateString:
            case NullValue:
                return VarcharType.VARCHAR;
            case UnstructuredArray:
                return new ArrayType(VarcharType.VARCHAR);
            case Boolean:
                return BooleanType.BOOLEAN;
            default:
                throw new IllegalArgumentException("Unknown column type: " + type);
        }
    }

    @Override
    public Set<ClpColumnHandle> listColumnHandles(SchemaTableName schemaTableName)
    {
        Set<ClpColumnHandle> columnHandles = new HashSet<>();
        String query = String.format(QUERY_SELECT_COLUMNS, config.getMetadataTablePrefix(), schemaTableName.getTableName());

        try (Connection connection = getConnection();
                PreparedStatement statement = connection.prepareStatement(query)) {
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    columnHandles.add(new ClpColumnHandle(
                            resultSet.getString("name"),
                            mapColumnType(resultSet.getByte("type")),
                            true));
                }
            }
        }
        catch (SQLException e) {
            log.error("Failed to load table schema for %s: %s" + schemaTableName.getTableName(), e);
        }
        return columnHandles;
    }

    @Override
    public Set<String> listTableNames(String schema)
    {
        Set<String> tableNames = new HashSet<>();

        String query = String.format(QUERY_SELECT_TABLES, config.getMetadataTablePrefix());
        try (Connection connection = getConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(query)) {
            while (resultSet.next()) {
                tableNames.add(resultSet.getString("table_name"));
            }
        }
        catch (SQLException e) {
            log.error("Failed to load table names: %s", e);
        }
        return tableNames;
    }
}
