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
import com.github.luben.zstd.ZstdInputStream;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.yscope.presto.schema.SchemaNode;
import com.yscope.presto.schema.SchemaTree;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
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
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

public class ClpClient
{
    public static final String columnMetadataPrefix = "column_metadata_";
    public static final String archiveTableSuffix = "archives";
    private static final Logger log = Logger.get(ClpClient.class);
    private final ClpConfig config;
    private final ClpConfig.FileSource fileSource;
    private final Path executablePath;
    private final Path decompressDir;
    private final LoadingCache<String, Set<ClpColumnHandle>> columnHandleCache;
    private final LoadingCache<String, Set<String>> tableNameCache;

    @Inject
    public ClpClient(ClpConfig config)
    {
        this.config = requireNonNull(config, "config is null");
        this.fileSource = config.getFileSource();
        if (fileSource == ClpConfig.FileSource.LOCAL) {
            this.executablePath = getExecutablePath();
            this.decompressDir = Paths.get(System.getProperty("java.io.tmpdir"), "clp_decompress");
        }
        else {
            this.executablePath = null;
            this.decompressDir = null;
        }
        this.columnHandleCache = CacheBuilder.newBuilder()
                .expireAfterWrite(config.getMetadataExpireInterval(), SECONDS)
                .refreshAfterWrite(config.getMetadataRefreshInterval(), SECONDS)
                .build(CacheLoader.from(this::loadTableSchema));

        this.tableNameCache = CacheBuilder.newBuilder()
                .expireAfterWrite(config.getMetadataExpireInterval(), SECONDS)  // TODO: Configure
                .refreshAfterWrite(config.getMetadataRefreshInterval(), SECONDS)
                .build(CacheLoader.from(this::loadTable));
    }

    @PostConstruct
    public void start()
    {
        try {
            Files.createDirectories(decompressDir);
        }
        catch (IOException e) {
            log.error(e, "Failed to create decompression directory");
        }
    }

    @PreDestroy
    public void close()
    {
        try {
            Files.walkFileTree(decompressDir, new SimpleFileVisitor<Path>()
            {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException
                {
                    Files.delete(file);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException
                {
                    if (exc == null) {
                        Files.delete(dir);
                        return FileVisitResult.CONTINUE;
                    }
                    else {
                        throw exc; // Directory iteration failed
                    }
                }
            });
        }
        catch (IOException e) {
            log.error(e, "Failed to delete decompression directory");
        }
    }

    public ClpConfig getConfig()
    {
        return config;
    }

    private Path getExecutablePath()
    {
        String executablePathString = config.getClpExecutablePath();
        if (executablePathString == null || executablePathString.isEmpty()) {
            Path executablePath = getExecutablePathFromEnvironment();
            if (executablePath == null) {
                throw new RuntimeException("CLP executable path is not set");
            }
            return executablePath;
        }
        Path executablePath = Paths.get(executablePathString);
        if (!Files.exists(executablePath) || !Files.isRegularFile(executablePath)) {
            executablePath = getExecutablePathFromEnvironment();
            if (executablePath == null) {
                throw new RuntimeException("CLP executable path is not set");
            }
        }
        return executablePath;
    }

    private Path getExecutablePathFromEnvironment()
    {
        String executablePathString = System.getenv("CLP_EXECUTABLE_PATH");
        if (executablePathString == null || executablePathString.isEmpty()) {
            return null;
        }

        Path executablePath = Paths.get(executablePathString);
        if (!Files.exists(executablePath) || !Files.isRegularFile(executablePath)) {
            return null;
        }
        return executablePath;
    }

    public Set<ClpColumnHandle> loadTableSchema(String tableName)
    {
        Connection connection = null;
        LinkedHashSet<ClpColumnHandle> columnHandles = new LinkedHashSet<>();
        try {
            connection = DriverManager.getConnection(config.getMetadataDbUrl(), config.getMetadataDbUser(), config.getMetadataDbPassword());
            Statement statement = connection.createStatement();

            String query = "SELECT * FROM" + config.getMetadataTablePrefix() + columnMetadataPrefix + tableName;
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
            connection = DriverManager.getConnection(config.getMetadataDbUrl(), config.getMetadataDbUser(), config.getMetadataDbPassword());
            Statement statement = connection.createStatement();

            String query = "SHOW TABLES";
            ResultSet resultSet = statement.executeQuery(query);

            // Processing the results
            String databaseName = config.getMetadataDbUrl().substring(config.getMetadataDbUrl().lastIndexOf('/') + 1);
            String tableNamePrefix = config.getMetadataTablePrefix() + columnMetadataPrefix;
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

    public Set<String> listTables()
    {
        return tableNameCache.getUnchecked("default");
    }

    public List<String> listArchiveIds(String tableName)
    {
        if (fileSource == ClpConfig.FileSource.LOCAL) {
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
            String bucketName = config.getS3Bucket();
            Connection connection = null;
            try {
                connection = DriverManager.getConnection(config.getMetadataDbUrl(), config.getMetadataDbUser(), config.getMetadataDbPassword());
                Statement statement = connection.createStatement();

                String query = "SELECT id FROM " + config.getMetadataTablePrefix() + archiveTableSuffix;
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

    public Set<ClpColumnHandle> listColumns(String tableName)
    {
        return columnHandleCache.getUnchecked(tableName);
    }

    public ProcessBuilder getRecords(String tableName, String archiveId, Optional<String> query, List<String> columns)
    {
        if (!listTables().contains(tableName)) {
            return null;
        }

        return query.map(s -> searchTable(tableName, archiveId, s, columns))
                .orElseGet(() -> searchTable(tableName, archiveId, "*", columns));
    }

    private ProcessBuilder searchTable(String tableName, String archiveId, String query, List<String> columns)
    {
        if (fileSource == ClpConfig.FileSource.S3) {
            throw new RuntimeException("Cannot handle S3 source");
        }
        Path tableArchiveDir = Paths.get(config.getClpArchiveDir(), tableName);
        List<String> argumentList = new ArrayList<>();
        argumentList.add(executablePath.toString());
        argumentList.add("s");
        argumentList.add(tableArchiveDir.toString());
        argumentList.add("--archive-id");
        argumentList.add(archiveId);
        argumentList.add(query);
        if (!columns.isEmpty()) {
            argumentList.add("--projection");
            argumentList.addAll(columns);
        }
        log.info("Argument list: %s", argumentList.toString());
        return new ProcessBuilder(argumentList);
    }

    private boolean decompressRecords(String tableName)
    {
        if (fileSource == ClpConfig.FileSource.S3) {
            throw new RuntimeException("Cannot handle S3 source");
        }
        Path tableDecompressDir = decompressDir.resolve(tableName);
        Path tableArchiveDir = Paths.get(config.getClpArchiveDir(), tableName);

        try {
            ProcessBuilder processBuilder =
                    new ProcessBuilder(executablePath.toString(),
                            "x",
                            tableArchiveDir.toString(),
                            tableDecompressDir.toString());
            Process process = processBuilder.start();
            process.waitFor();
            return process.exitValue() == 0;
        }
        catch (IOException | InterruptedException e) {
            log.error(e, "Failed to decompress records for table %s", tableName);
            return false;
        }
    }

    private Set<ClpColumnHandle> parseSchemaTreeFile(Path schemaMapsFile)
    {
        SchemaTree schemaTree = new SchemaTree();
        try (InputStream fileInputStream = Files.newInputStream(schemaMapsFile);
                ZstdInputStream zstdInputStream = new ZstdInputStream(fileInputStream);
                DataInputStream dataInputStream = new DataInputStream(zstdInputStream)) {
            byte[] longBytes = new byte[8];
            byte[] intBytes = new byte[4];
            dataInputStream.readFully(longBytes);
            long numberOfNodes = ByteBuffer.wrap(longBytes).order(ByteOrder.nativeOrder()).getLong();
            for (int i = 0; i < numberOfNodes; i++) {
                dataInputStream.readFully(intBytes);
                int parentId = ByteBuffer.wrap(intBytes).order(ByteOrder.nativeOrder()).getInt();
                dataInputStream.readFully(longBytes);
                long stringSize = ByteBuffer.wrap(longBytes).order(ByteOrder.nativeOrder()).getLong();
                byte[] stringBytes = new byte[(int) stringSize];
                dataInputStream.readFully(stringBytes);
                String name = new String(stringBytes, StandardCharsets.UTF_8);
                SchemaNode.NodeType type = SchemaNode.NodeType.fromType(dataInputStream.readByte());
                schemaTree.addNode(parentId, name, type);
            }

            ArrayList<SchemaNode.NodeTuple> primitiveTypeFields = schemaTree.getPrimitiveFields();
            LinkedHashSet<ClpColumnHandle> columnHandles = new LinkedHashSet<>();
            for (SchemaNode.NodeTuple nodeTuple : primitiveTypeFields) {
                SchemaNode.NodeType nodeType = nodeTuple.getType();
                Type prestoType = null;
                switch (nodeType) {
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
                columnHandles.add(new ClpColumnHandle(nodeTuple.getName(), prestoType, true));
            }
            return columnHandles;
        }
        catch (IOException e) {
            return ImmutableSet.of();
        }
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
