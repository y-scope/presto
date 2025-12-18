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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.String.format;
import static org.testng.Assert.fail;

/**
 * Mock Pinot database for testing LASTWITHTIME deduplication queries.
 * Uses H2 as the backing store and an HTTP server to simulate the Pinot/Neutrino API.
 * Translates LASTWITHTIME queries into equivalent H2 SQL for deduplication.
 */
public class ClpMockPinotDatabase
{
    // DB_CLOSE_DELAY=-1 keeps the in-memory database alive as long as the JVM is running
    private static final String H2_URL_TEMPLATE = "jdbc:h2:mem:%s;MODE=MySQL;DATABASE_TO_UPPER=FALSE;DB_CLOSE_DELAY=-1";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final String h2Url;
    private final HttpServer httpServer;
    private final int port;
    private final String tableName;

    private ClpMockPinotDatabase(String h2Url, HttpServer httpServer, int port, String tableName)
    {
        this.h2Url = h2Url;
        this.httpServer = httpServer;
        this.port = port;
        this.tableName = tableName;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public void start()
    {
        httpServer.start();
    }

    public void stop()
    {
        httpServer.stop(0);
    }

    public int getPort()
    {
        return port;
    }

    public String getUrl()
    {
        return "http://localhost:" + port;
    }

    /**
     * Inserts a row into the mock Pinot table.
     * Multiple rows with the same tpath but different _timestampMillis simulate Pinot's append-only model.
     * All columns are stored as strings to match Uber's Pinot schema.
     */
    public void insertRow(String tpath, String hostname, String creationtime, String numMessages, String timestampMillis)
    {
        String sql = format(
                "INSERT INTO %s (tpath, hostname, creationtime, num_messages, _timestampMillis) VALUES (?, ?, ?, ?, ?)",
                tableName);
        try (Connection conn = DriverManager.getConnection(h2Url);
                PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, tpath);
            pstmt.setString(2, hostname);
            pstmt.setString(3, creationtime);
            pstmt.setString(4, numMessages);
            pstmt.setString(5, timestampMillis);
            pstmt.executeUpdate();
        }
        catch (SQLException e) {
            fail("Failed to insert row: " + e.getMessage());
        }
    }

    /**
     * Handles incoming HTTP requests, translating LASTWITHTIME queries to H2 SQL.
     */
    private void handleRequest(HttpExchange exchange) throws IOException
    {
        StringBuilder body = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(exchange.getRequestBody(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                body.append(line);
            }
        }

        String query = body.toString();
        String h2Query = translateToH2Query(query);

        try {
            String jsonResponse = executeQueryAndFormat(h2Query);
            byte[] responseBytes = jsonResponse.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, responseBytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(responseBytes);
            }
        }
        catch (SQLException e) {
            String error = "{\"error\": \"" + e.getMessage() + "\"}";
            byte[] responseBytes = error.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(500, responseBytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(responseBytes);
            }
        }
    }

    /**
     * Translates a Pinot query with LASTWITHTIME to an equivalent H2 query.
     * LASTWITHTIME(col, "_timestampMillis", 'string') is translated to a subquery that gets the latest value.
     */
    private String translateToH2Query(String pinotQuery)
    {
        // For H2, we simulate LASTWITHTIME by using a subquery to get the row with max _timestampMillis per tpath
        // Original: SELECT tpath, LASTWITHTIME(hostname, "_timestampMillis", 'string') AS hostname FROM table GROUP BY tpath
        // H2 equivalent: SELECT t.tpath, t.hostname FROM table t
        //                INNER JOIN (SELECT tpath, MAX(_timestampMillis) as max_ts FROM table GROUP BY tpath) m
        //                ON t.tpath = m.tpath AND t._timestampMillis = m.max_ts

        // Extract columns from LASTWITHTIME expressions
        Pattern lastWithTimePattern = Pattern.compile(
                "LASTWITHTIME\\s*\\(\\s*(\\w+)\\s*,\\s*\"_timestampMillis\"\\s*,\\s*'string'\\s*\\)\\s+AS\\s+(\\w+)",
                Pattern.CASE_INSENSITIVE);

        // Check if query has LASTWITHTIME
        if (!pinotQuery.toUpperCase().contains("LASTWITHTIME")) {
            return pinotQuery;
        }

        // Extract table name from query
        Pattern fromPattern = Pattern.compile("FROM\\s+([\\w.]+)", Pattern.CASE_INSENSITIVE);
        Matcher fromMatcher = fromPattern.matcher(pinotQuery);
        String queryTableName = tableName;
        if (fromMatcher.find()) {
            queryTableName = fromMatcher.group(1);
        }

        // Extract WHERE clause
        Pattern wherePattern = Pattern.compile("WHERE\\s+(.+?)\\s+GROUP BY", Pattern.CASE_INSENSITIVE);
        Matcher whereMatcher = wherePattern.matcher(pinotQuery);
        String whereClause = "1 = 1";
        if (whereMatcher.find()) {
            whereClause = whereMatcher.group(1);
        }

        // Extract columns
        List<String> columns = new ArrayList<>();
        columns.add("tpath");
        Matcher colMatcher = lastWithTimePattern.matcher(pinotQuery);
        while (colMatcher.find()) {
            columns.add(colMatcher.group(1));
        }

        // Build H2 query using subquery for deduplication
        StringBuilder h2Query = new StringBuilder();
        h2Query.append("SELECT ");
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                h2Query.append(", ");
            }
            h2Query.append("t.").append(columns.get(i));
        }
        h2Query.append(" FROM ").append(tableName).append(" t ");
        h2Query.append("INNER JOIN (SELECT tpath, MAX(_timestampMillis) as max_ts FROM ");
        h2Query.append(tableName).append(" WHERE ").append(whereClause);
        h2Query.append(" GROUP BY tpath) m ");
        h2Query.append("ON t.tpath = m.tpath AND t._timestampMillis = m.max_ts");

        return h2Query.toString();
    }

    /**
     * Executes the H2 query and formats the result as Neutrino JSON response.
     */
    private String executeQueryAndFormat(String query) throws SQLException, IOException
    {
        try (Connection conn = DriverManager.getConnection(h2Url);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(query)) {
            ObjectNode root = OBJECT_MAPPER.createObjectNode();
            ArrayNode columns = OBJECT_MAPPER.createArrayNode();
            ArrayNode data = OBJECT_MAPPER.createArrayNode();

            int columnCount = rs.getMetaData().getColumnCount();
            for (int i = 1; i <= columnCount; i++) {
                ObjectNode col = OBJECT_MAPPER.createObjectNode();
                col.put("name", rs.getMetaData().getColumnName(i));
                col.put("type", rs.getMetaData().getColumnTypeName(i));
                columns.add(col);
            }

            while (rs.next()) {
                ArrayNode row = OBJECT_MAPPER.createArrayNode();
                for (int i = 1; i <= columnCount; i++) {
                    Object value = rs.getObject(i);
                    if (value == null) {
                        row.addNull();
                    }
                    else if (value instanceof Number) {
                        row.add(((Number) value).longValue());
                    }
                    else {
                        row.add(value.toString());
                    }
                }
                data.add(row);
            }

            root.set("columns", columns);
            root.set("data", data);
            return OBJECT_MAPPER.writeValueAsString(root);
        }
    }

    public static final class Builder
    {
        private String tableName = "test_table";

        public Builder setTableName(String tableName)
        {
            this.tableName = tableName;
            return this;
        }

        public ClpMockPinotDatabase build() throws IOException, SQLException
        {
            String dbName = UUID.randomUUID().toString();
            String h2Url = format(H2_URL_TEMPLATE, dbName);

            // Create the table with all VARCHAR columns to match Uber's Pinot schema
            try (Connection conn = DriverManager.getConnection(h2Url);
                    Statement stmt = conn.createStatement()) {
                stmt.execute(format(
                        "CREATE TABLE %s (" +
                                "tpath VARCHAR(512) NOT NULL, " +
                                "hostname VARCHAR(256), " +
                                "creationtime VARCHAR(64), " +
                                "num_messages VARCHAR(64), " +
                                "_timestampMillis VARCHAR(64) NOT NULL)",
                        tableName));
            }

            // Create HTTP server
            HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
            int port = server.getAddress().getPort();

            ClpMockPinotDatabase db = new ClpMockPinotDatabase(h2Url, server, port, tableName);
            server.createContext("/v1/globalStatement", db::handleRequest);

            return db;
        }
    }
}
