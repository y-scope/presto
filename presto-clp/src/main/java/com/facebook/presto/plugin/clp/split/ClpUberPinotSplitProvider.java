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
import com.facebook.presto.plugin.clp.optimization.ClpTopNSpec;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.plugin.clp.ClpErrorCode.CLP_MANDATORY_COLUMN_NOT_IN_FILTER;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

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

    @Override
    public List<ClpSplit> listSplits(ClpTableLayoutHandle clpTableLayoutHandle)
    {
        ClpTableHandle clpTableHandle = clpTableLayoutHandle.getTable();
        Optional<ClpTopNSpec> topNSpecOptional = clpTableLayoutHandle.getTopN();
        String tableName = inferMetadataTableName(clpTableHandle);
        Optional<String> metadataFilterQuery = Optional.empty();

        SchemaTableName schemaTableName = clpTableHandle.getSchemaTableName();
        Map<String, Map<String, String>> dataColumnRangeMapping = metadataConfig.getDataColumnRangeMapping(schemaTableName);
        if (clpTableLayoutHandle.getMetadataExpression().isPresent()) {
            ClpUberPinotSplitMetadataExpressionConverter converter =
                    new ClpUberPinotSplitMetadataExpressionConverter(
                            functionManager,
                            functionResolution,
                            metadataConfig.getExposedToOriginalMapping(schemaTableName),
                            dataColumnRangeMapping,
                            metadataConfig.getRequiredColumns(schemaTableName));
            metadataFilterQuery = Optional.of(converter.transform(clpTableLayoutHandle.getMetadataExpression().get()));
        }
        else if (!metadataConfig.getRequiredColumns(schemaTableName).isEmpty()) {
            throw new PrestoException(CLP_MANDATORY_COLUMN_NOT_IN_FILTER, "No required columns specified in the filter");
        }

        try {
            ImmutableList.Builder<ClpSplit> splits = new ImmutableList.Builder<>();
            if (topNSpecOptional.isPresent()) {
                ClpTopNSpec topNSpec = topNSpecOptional.get();
                // Only handles one range metadata column for now
                ClpTopNSpec.Ordering ordering = topNSpec.getOrderings().get(0);
                String columnName = ordering.getColumn();
                String lowerBound = columnName;
                String upperBound = columnName;
                if (dataColumnRangeMapping.containsKey(columnName)) {
                    lowerBound = dataColumnRangeMapping.get(columnName).getOrDefault("lowerBound", lowerBound);
                    upperBound = dataColumnRangeMapping.get(columnName).getOrDefault("upperBound", upperBound);
                }

                String dir = (ordering.getOrder() == ClpTopNSpec.Order.ASC) ? "ASC" : "DESC";
                String splitMetaQuery = buildSplitMetadataQuery(tableName, metadataFilterQuery.orElse("1 = 1"), upperBound, dir);
                List<ArchiveMeta> archiveMetaList = fetchArchiveMeta(splitMetaQuery, ordering);
                List<ArchiveMeta> selected = selectTopNArchives(archiveMetaList, topNSpec.getLimit(), ordering.getOrder());

                for (ArchiveMeta a : selected) {
                    String splitPath = a.id;
                    splits.add(new ClpSplit(splitPath, determineSplitType(splitPath), clpTableLayoutHandle.getKqlQuery(), Optional.empty()));
                }
                List<ClpSplit> filteredSplits = splits.build();
                log.debug("Number of topN filtered splits: %s", filteredSplits.size());
                return filteredSplits;
            }
            List<String> metadataColumnNames = new ArrayList<>(
                    clpTableLayoutHandle.getSplitMetaColumnNames().orElse(new HashSet<>()));
            String splitQuery = buildSplitSelectionQuery(
                    tableName,
                    metadataColumnNames,
                    metadataFilterQuery.orElse("1 = 1"));
            List<JsonNode> splitRows = getQueryResult(pinotSqlQueryEndpointUrl, splitQuery);
            for (JsonNode row : splitRows) {
                String splitPath = row.elements().next().asText();
                splits.add(new ClpSplit(splitPath, determineSplitType(splitPath), clpTableLayoutHandle.getKqlQuery(), Optional.empty()));
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
        return new URL(config.getMetadataDbUrl() + "/v1/globalStatements");
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
}
