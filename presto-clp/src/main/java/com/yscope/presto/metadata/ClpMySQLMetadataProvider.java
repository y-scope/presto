package com.yscope.presto.metadata;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.type.*;
import com.facebook.presto.spi.SchemaTableName;
import com.yscope.presto.ClpColumnHandle;
import com.yscope.presto.ClpConfig;

import java.sql.*;
import java.util.HashSet;
import java.util.Set;

public class ClpMySQLMetadataProvider implements ClpMetadataProvider{
    private static final Logger log = Logger.get(ClpMySQLMetadataProvider.class);

    private static final String COLUMN_METADATA_PREFIX = "column_metadata_";
    private static final String ARCHIVE_TABLE_SUFFIX = "archives";
    private static final String QUERY_SELECT_COLUMNS = "SELECT * FROM %s" + COLUMN_METADATA_PREFIX + "?";
    private static final String QUERY_SHOW_TABLES = "SHOW TABLES";

    private final ClpConfig config;

    public ClpMySQLMetadataProvider(ClpConfig config) {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        }
        catch (ClassNotFoundException e) {
            log.error(e, "Failed to load MySQL JDBC driver");
        }
        this.config = config;
    }

    private Connection getConnection() throws SQLException {
        return DriverManager.getConnection(config.getMetadataDbUrl(), config.getMetadataDbUser(), config.getMetadataDbPassword());
    }

    // TODO(Rui): Consider move it to a util class
    private Type mapColumnType(byte type) {
        switch (ClpNodeType.fromType(type)) {
            case Integer: return BigintType.BIGINT;
            case Float: return DoubleType.DOUBLE;
            case ClpString:
            case VarString:
            case DateString:
            case NullValue: return VarcharType.VARCHAR;
            case UnstructuredArray: return new ArrayType(VarcharType.VARCHAR);
            case Boolean: return BooleanType.BOOLEAN;
            default: throw new IllegalArgumentException("Unknown column type: " + type);
        }
    }

    @Override
    public Set<ClpColumnHandle> listTableSchema(SchemaTableName schemaTableName) {
        Set<ClpColumnHandle> columnHandles = new HashSet<>();
        String query = String.format(QUERY_SELECT_COLUMNS, config.getMetadataTablePrefix());

        try (Connection connection = getConnection();
             PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setString(1, schemaTableName.getTableName());

            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    columnHandles.add(new ClpColumnHandle(
                            resultSet.getString("name"),
                            mapColumnType(resultSet.getByte("type")),
                            true
                    ));
                }
            }
        } catch (SQLException e) {
            log.error("Failed to load table schema for: " + schemaTableName.getTableName(), e);
        }
        return columnHandles;
    }

    @Override
    public Set<String> listTables(String schema) {
        Set<String> tableNames = new HashSet<>();

        try (Connection connection = getConnection();
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(QUERY_SHOW_TABLES)) {

            while (resultSet.next()) {
                String tableName = resultSet.getString("Tables_in_" + config.getMetadataDbName());
                if (tableName.startsWith(config.getMetadataTablePrefix() + COLUMN_METADATA_PREFIX)) {
                    tableNames.add(tableName.substring((config.getMetadataTablePrefix() + COLUMN_METADATA_PREFIX).length()));
                }
            }
        } catch (SQLException e) {
            log.error("Failed to load table names", e);
        }

        return tableNames;
    }

}
