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
package com.facebook.presto.plugin.clp;

import com.facebook.presto.plugin.clp.split.ClpMySqlSplitProvider;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static com.facebook.presto.plugin.clp.ClpConnectorFactory.CONNECTOR_NAME;
import static com.facebook.presto.plugin.clp.ClpErrorCode.CLP_MANDATORY_METADATA_FILTER_NOT_VALID;
import static com.facebook.presto.plugin.clp.ClpErrorCode.CLP_METADATA_FILTER_CONFIG_NOT_FOUND;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Provides metadata filter configuration and utilities for remapping and validating metadata
 * filters based on a JSON configuration file.
 * <p>
 * The configuration file locates at {@code clp.metadata-filter-config} and defines metadata filters
 * for different scopes:
 * <ul>
 *   <li><b>Catalog-level</b>: applies to all schemas and tables under the catalog.</li>
 *   <li><b>Schema-level</b>: applies to all tables under the specified catalog and schema.</li>
 *   <li><b>Table-level</b>: applies to the fully qualified catalog.schema.table.</li>
 * </ul>
 * <p>
 * Each scope maps to a list of filter definitions. Each filter includes:
 * <ul>
 *   <li>{@code filterName}: must match a column name in the table's schema. Note that only numeric
 *   type column can be used as metadata filter now.</li>
 *   <li>{@code rangeMapping} (optional): specifies how the filter should be remapped when it
 *   targets metadata-only columns. Note that this option is valid only if the column is numeric
 *   type.
 *       For example, a condition like {@code "msg.timestamp" > 1234 AND "msg.timestamp" < 5678}
 *       will be rewritten as {@code end_timestamp > 1234 AND begin_timestamp < 5678} to ensure
 *       metadata-based filtering produces a superset of the actual result.</li>
 * </ul>
 * <p>
 * This provider is used by {@link ClpFilterToKqlConverter} to determine which columns are eligible
 * for metadata filter push down, and by {@link ClpMySqlSplitProvider} to construct metadata filter
 * queries that determine which splits to read.
 */
public class ClpMetadataFilterProvider
{
    private final Map<String, TableConfig> filterMap;

    @Inject
    public ClpMetadataFilterProvider(ClpConfig config)
    {
        requireNonNull(config, "config is null");

        ObjectMapper mapper = new ObjectMapper();
        try {
            filterMap = mapper.readValue(
                    new File(config.getMetadataFilterConfig()),
                    new TypeReference<Map<String, TableConfig>>() {});
        }
        catch (IOException e) {
            throw new PrestoException(CLP_METADATA_FILTER_CONFIG_NOT_FOUND, "Failed to metadata filter config file open.");
        }
    }

    public void checkContainsAllFilters(SchemaTableName schemaTableName, String metadataFilterKqlQuery)
    {
        boolean hasAllMetadataFilterColumns = true;
        String notFoundFilterColumnName = "";
        for (String columnName : getFilterNames(format("%s.%s", CONNECTOR_NAME, schemaTableName))) {
            if (!metadataFilterKqlQuery.contains(columnName)) {
                hasAllMetadataFilterColumns = false;
                notFoundFilterColumnName = columnName;
                break;
            }
        }
        if (!hasAllMetadataFilterColumns) {
            throw new PrestoException(CLP_MANDATORY_METADATA_FILTER_NOT_VALID, notFoundFilterColumnName + " is a mandatory metadata filter column but not valid");
        }
    }

    /**
     * Rewrites the input SQL string by remapping filter conditions based on the configured
     * metadata filter range mappings for the given scope.
     *
     * <p>The {@code scope} follows the format {@code catalog[.schema][.table]}, and determines
     * which filter mappings to apply. For each level of scope (catalog, schema, table), this
     * method collects all range mappings defined in the metadata filter configuration. Mappings
     * from more specific scopes (e.g., table-level) override or supplement those from broader
     * scopes (e.g., catalog-level).
     *
     * <p>This method performs regex-based replacements to convert numeric filter expressions such
     * as:
     *
     * <ul>
     *   <li>{@code "msg.timestamp" >= 1234} → {@code end_timestamp >= 1234}</li>
     *   <li>{@code "msg.timestamp" <= 5678} → {@code begin_timestamp <= 5678}</li>
     *   <li>{@code "msg.timestamp" = 4567} →
     *   {@code (begin_timestamp <= 4567 AND end_timestamp >= 4567)}</li>
     * </ul>
     *
     * @param scope the catalog.schema.table scope used to resolve applicable filter mappings
     * @param sql the original SQL expression to be remapped
     * @return the rewritten SQL string with metadata filter expressions remapped according to the
     * configured range mappings
     */
    public String remapFilterSql(String scope, String sql)
    {
        String[] splitScope = scope.split("\\.");
        if (0 == splitScope.length) {
            return sql;
        }

        Map<String, RangeMapping> mappings = new HashMap<>(getAllMappingsFromTableConfig(filterMap.get(splitScope[0])));

        if (1 < splitScope.length) {
            mappings.putAll(getAllMappingsFromTableConfig(filterMap.get(splitScope[0] + "." + splitScope[1])));
        }

        if (3 == splitScope.length) {
            mappings.putAll(getAllMappingsFromTableConfig(filterMap.get(scope)));
        }

        String remappedSql = sql;
        for (Map.Entry<String, RangeMapping> entry : mappings.entrySet()) {
            remappedSql = remappedSql.replaceAll(
                    format("\"(%s)\"\\s(>=?)\\s([0-9]*)", entry.getKey()),
                    format("%s $2 $3", entry.getValue().upperBound));
            remappedSql = remappedSql.replaceAll(
                    format("\"(%s)\"\\s(<=?)\\s([0-9]*)", entry.getKey()),
                    format("%s $2 $3", entry.getValue().lowerBound));
            remappedSql = remappedSql.replaceAll(
                    format("\"(%s)\"\\s(=)\\s([0-9]*)", entry.getKey()),
                    format("(%s <= $3 AND %s >= $3)", entry.getValue().lowerBound, entry.getValue().upperBound));
        }
        return remappedSql;
    }

    public Set<String> getFilterNames(String scope)
    {
        String[] splitScope = scope.split("\\.");
        if (0 == splitScope.length) {
            return ImmutableSet.of();
        }
        ImmutableSet.Builder<String> builder = ImmutableSet.builder();
        builder.addAll(getAllFilterNamesFromTableConfig(filterMap.get(splitScope[0])));

        if (1 < splitScope.length) {
            builder.addAll(getAllFilterNamesFromTableConfig(filterMap.get(splitScope[0] + "." + splitScope[1])));
        }

        if (3 == splitScope.length) {
            builder.addAll(getAllFilterNamesFromTableConfig(filterMap.get(scope)));
        }
        return builder.build();
    }

    private Set<String> getAllFilterNamesFromTableConfig(TableConfig tableConfig)
    {
        return null != tableConfig ? tableConfig.filters.stream()
                .map(filter -> filter.filterName)
                .collect(toImmutableSet()) : ImmutableSet.of();
    }

    private Map<String, RangeMapping> getAllMappingsFromTableConfig(TableConfig tableConfig)
    {
        return tableConfig != null
                ? tableConfig.filters.stream()
                .filter(filter -> null != filter.rangeMapping)
                .collect(toImmutableMap(
                        filter -> filter.filterName,
                        filter -> filter.rangeMapping))
                : ImmutableMap.of();
    }

    private static class TableConfig
    {
        public List<Filter> filters;
    }

    private static class RangeMapping
    {
        @JsonProperty("lowerBound")
        public String lowerBound;

        @JsonProperty("upperBound")
        public String upperBound;

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (!(o instanceof RangeMapping)) {
                return false;
            }
            RangeMapping that = (RangeMapping) o;
            return Objects.equals(lowerBound, that.lowerBound) &&
                    Objects.equals(upperBound, that.upperBound);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(lowerBound, upperBound);
        }
    }

    private static class Filter
    {
        @JsonProperty("filterName")
        public String filterName;

        @JsonProperty("rangeMapping")
        public RangeMapping rangeMapping;
    }
}
