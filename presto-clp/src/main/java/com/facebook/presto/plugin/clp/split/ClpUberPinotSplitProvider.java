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

import com.facebook.presto.plugin.clp.ClpConfig;
import com.facebook.presto.plugin.clp.ClpSplit;
import com.facebook.presto.plugin.clp.ClpTableHandle;
import com.facebook.presto.plugin.clp.ClpTableLayoutHandle;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.plugin.clp.ClpErrorCode.CLP_MANDATORY_COLUMN_NOT_IN_FILTER;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Uber-specific implementation of CLP Pinot split provider.
 * <p>
 * At Uber, Pinot is accessed through Neutrino, a cross-region routing and aggregation service
 * that provides a unified interface for querying distributed Pinot clusters. This implementation
 * customizes the SQL query endpoint URL to use Neutrino's global statements API instead of
 * the standard Pinot query endpoint.
 * </p>
 */
public class ClpUberPinotSplitProvider
        extends ClpPinotSplitProvider
{
    /**
     * Constructs an Uber CLP Pinot split provider with the given configuration.
     *
     * @param config the CLP configuration
     */
    @Inject
    public ClpUberPinotSplitProvider(
            ClpConfig config,
            FunctionMetadataManager functionManager,
            StandardFunctionResolution functionResolution,
            ClpSplitMetadataConfig metadataConfig)
    {
        super(config, functionManager, functionResolution, metadataConfig);
    }

    /**
     * Constructs the full split path by prepending the Terrablob storage URL prefix to the relative file path.
     *
     * @param relativePath the relative file path from the metadata database
     * @return the full split path with protocol, host, and port prefix
     * @throws IllegalArgumentException if the base URL is null, empty, or malformed
     */
    private String buildFullSplitPath(String relativePath)
    {
        String baseUrl = config.getUberTerrablobStorageBaseUrl();
        if (baseUrl == null || baseUrl.isEmpty()) {
            throw new IllegalArgumentException(
                    "Terrablob storage base URL (clp.uber-terrablob-storage-base-url) must be configured for Uber" +
                            " Pinot split provider");
        }

        try {
            URL url = new URL(baseUrl);
            int port = url.getPort();
            if (port == -1) {
                return url.getProtocol() + "://" + url.getHost() + relativePath;
            }
            return url.getProtocol() + "://" + url.getHost() + ":" + port + relativePath;
        }
        catch (MalformedURLException e) {
            throw new IllegalArgumentException(
                    format("Invalid Terrablob storage base URL: %s", baseUrl), e);
        }
    }

    @Override
    public List<ClpSplit> listSplits(ClpTableLayoutHandle clpTableLayoutHandle)
    {
        ClpTableHandle clpTableHandle = clpTableLayoutHandle.getTable();
        String tableName = inferMetadataTableName(clpTableHandle);
        Optional<String> metadataFilterQuery = Optional.empty();

        SchemaTableName schemaTableName = clpTableHandle.getSchemaTableName();
        if (clpTableLayoutHandle.getMetadataExpression() != null) {
            ClpUberPinotSplitMetadataExpressionConverter converter =
                    new ClpUberPinotSplitMetadataExpressionConverter(
                            functionManager,
                            functionResolution,
                            metadataConfig,
                            schemaTableName);
            metadataFilterQuery = Optional.of(converter.transform(clpTableLayoutHandle.getMetadataExpression()));
        }
        else if (!metadataConfig.getRequiredColumns(schemaTableName).isEmpty()) {
            throw new PrestoException(CLP_MANDATORY_COLUMN_NOT_IN_FILTER, "No required columns specified in the filter");
        }

        try {
            ImmutableList.Builder<ClpSplit> splits = new ImmutableList.Builder<>();
            List<String> metadataColumnNames = new ArrayList<>(
                    clpTableLayoutHandle.getOrInitializeSplitMetadataColumnNames());
            String splitQuery = buildSplitSelectionQuery(
                    tableName,
                    metadataColumnNames,
                    metadataFilterQuery.orElse("1 = 1"));
            List<Map<String, JsonNode>> splitRows = getQueryResult(splitQuery);

            for (Map<String, JsonNode> row : splitRows) {
                JsonNode tpathNode = row.get("tpath");
                if (tpathNode == null || tpathNode.isNull()) {
                    throw new RuntimeException("Missing required 'tpath' field in split metadata row");
                }
                String splitPath = buildFullSplitPath(tpathNode.asText());
                Map<String, Object> metadataColumns = extractMetadataColumns(row, metadataColumnNames, schemaTableName);

                splits.add(new ClpSplit(
                        splitPath,
                        determineSplitType(splitPath),
                        clpTableLayoutHandle.getKqlQuery(),
                        Optional.of(metadataColumns)));
            }

            List<ClpSplit> filteredSplits = splits.build();
            log.debug("Number of filtered splits: %s", filteredSplits.size());
            return filteredSplits;
        }
        catch (Exception e) {
            log.error(e, "Failed to list splits for table %s", tableName);
            throw new RuntimeException(format("Failed to list splits for table %s: %s", tableName, e.getMessage()), e);
        }
    }

    /**
     * Constructs the Neutrino SQL query endpoint URL for Uber's Pinot infrastructure.
     * <p>
     * Instead of using Pinot's standard {@code /query/sql} endpoint, this method constructs
     * a URL pointing to Neutrino's {@code /v1/globalStatements} endpoint, which provides
     * cross-region query routing and aggregation capabilities.
     * </p>
     *
     * @param config the CLP configuration containing the base Neutrino service URL
     * @return the Neutrino global statements endpoint URL
     * @throws MalformedURLException if the constructed URL is invalid
     */
    @Override
    protected URL buildPinotSqlQueryEndpointUrl(ClpConfig config) throws MalformedURLException
    {
        return new URL(config.getMetadataDbUrl() + "/v1/globalStatement");
    }

    /**
     * Infers the Uber-specific Pinot metadata table name from the CLP table handle.
     * <p>
     * At Uber, Pinot tables are organized under a specific namespace hierarchy.
     * All logging-related metadata tables are prefixed with {@code "rta.logging."}
     * to identify them within Uber's multi-tenant Pinot infrastructure. This prefix
     * represents:
     * <ul>
     *   <li><b>rta</b>: Real-Time Analytics platform namespace</li>
     *   <li><b>logging</b>: The logging subsystem within RTA</li>
     * </ul>
     * </p>
     * <p>
     * Unlike the standard Pinot implementation where schemas can affect table naming,
     * Uber's approach uses a flat namespace where all logging tables share the same
     * prefix regardless of the schema being queried.
     * </p>
     * <p>
     * Examples:
     * <ul>
     *   <li>Schema: "default", Table: "logs" → Pinot table: "rta.logging.logs"</li>
     *   <li>Schema: "production", Table: "events" → Pinot table: "rta.logging.events"</li>
     *   <li>Schema: "staging", Table: "metrics" → Pinot table: "rta.logging.metrics"</li>
     * </ul>
     * </p>
     *
     * @param tableHandle the CLP table handle containing schema and table information
     * @return the fully-qualified Pinot metadata table name with Uber's namespace prefix
     * @throws NullPointerException if tableHandle is null
     */
    @Override
    protected String inferMetadataTableName(ClpTableHandle tableHandle)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        SchemaTableName schemaTableName = tableHandle.getSchemaTableName();

        // Uber's Pinot tables use a fixed namespace prefix for all logging tables
        // Format: rta.logging.<table_name>
        String tableName = schemaTableName.getTableName();
        return buildUberTableName(tableName);
    }

    /**
     * Factory method for building Uber-specific table names.
     * Exposed for testing purposes.
     *
     * @param tableName the base table name
     * @return the fully-qualified Uber Pinot table name
     */
    @VisibleForTesting
    protected String buildUberTableName(String tableName)
    {
        return String.format("rta.logging.%s", tableName);
    }

    /**
     * Adds Uber-specific HTTP headers required for Neutrino service authentication.
     *
     * @param conn the HTTP connection to add headers to
     */
    protected void addCustomQueryRequestHeader(HttpURLConnection conn)
    {
        conn.setRequestProperty("RPC-Service", "neutrino-logging");
        conn.setRequestProperty("RPC-Caller", "logging-terrablob-connector");
        conn.setRequestProperty("Content-Type", "text/plain");
        conn.setRequestProperty("Accept", "text/plain");
    }

    /**
     * {@inheritDoc}
     * <p>
     * This Uber-specific implementation sends the SQL query as plain text to the Neutrino
     * endpoint and parses the response using {@link #parseQueryResponse(JsonNode)}.
     * </p>
     */
    @Override
    protected List<Map<String, JsonNode>> getQueryResult(String sql)
    {
        try {
            HttpURLConnection conn = (HttpURLConnection) pinotSqlQueryEndpointUrl.openConnection();
            conn.setRequestMethod("POST");
            conn.setDoOutput(true);
            conn.setConnectTimeout((int) SECONDS.toMillis(5));
            conn.setReadTimeout((int) SECONDS.toMillis(30));
            addCustomQueryRequestHeader(conn);

            log.info("Executing Pinot query: %s", sql);
            ObjectMapper mapper = new ObjectMapper();
            String body = sql;
            try (OutputStream os = conn.getOutputStream()) {
                os.write(body.getBytes(StandardCharsets.UTF_8));
            }

            int code = conn.getResponseCode();
            InputStream is = (code >= 200 && code < 300) ? conn.getInputStream() : conn.getErrorStream();
            if (is == null) {
                throw new IOException("Pinot HTTP " + code + " with empty body");
            }

            JsonNode root;
            try (InputStream in = is) {
                root = mapper.readTree(in);
            }

            return parseQueryResponse(root);
        }
        catch (IOException e) {
            log.error(e, "IO error executing Pinot query: %s", sql);
            return Collections.emptyList();
        }
        catch (Exception e) {
            log.error(e, "Unexpected error executing Pinot query: %s", sql);
            return Collections.emptyList();
        }
    }

    /**
     * Parses the JSON response from an Uber Neutrino query into a list of row maps.
     * <p>
     * The Uber Neutrino response format differs from standard Pinot:
     * <ul>
     *   <li><b>columns</b>: Array of column definitions, each containing "name", "type", and "typeSignature"</li>
     *   <li><b>data</b>: Array of row arrays (equivalent to Pinot's "rows")</li>
     * </ul>
     * </p>
     *
     * @param root the root JSON node of the query response
     * @return a list of maps where each map represents a row with column names as keys
     * @throws IllegalStateException if the response is missing required fields
     */
    @VisibleForTesting
    protected List<Map<String, JsonNode>> parseQueryResponse(JsonNode root)
    {
        JsonNode columnsNode = root.get("columns");
        if (columnsNode == null) {
            throw new IllegalStateException("Uber query response missing 'columns' field");
        }

        JsonNode dataNode = root.get("data");
        if (dataNode == null) {
            throw new IllegalStateException("Uber query response missing 'data' field");
        }

        ImmutableList.Builder<String> columnNamesBuilder = ImmutableList.builder();
        for (Iterator<JsonNode> it = columnsNode.elements(); it.hasNext(); ) {
            JsonNode columnDef = it.next();
            JsonNode nameNode = columnDef.get("name");
            if (nameNode == null) {
                throw new IllegalStateException("Column definition missing 'name' field");
            }
            columnNamesBuilder.add(nameNode.asText());
        }
        List<String> columnNames = columnNamesBuilder.build();

        ImmutableList.Builder<Map<String, JsonNode>> resultBuilder = ImmutableList.builder();
        for (Iterator<JsonNode> it = dataNode.elements(); it.hasNext(); ) {
            JsonNode row = it.next();
            ImmutableMap.Builder<String, JsonNode> rowBuilder = ImmutableMap.builder();
            for (int i = 0; i < columnNames.size(); i++) {
                JsonNode val = row.get(i);
                if (val != null) {
                    rowBuilder.put(columnNames.get(i), val);
                }
            }
            resultBuilder.add(rowBuilder.build());
        }

        List<Map<String, JsonNode>> results = resultBuilder.build();
        log.debug("Number of results: %s", results.size());
        return results;
    }
}
