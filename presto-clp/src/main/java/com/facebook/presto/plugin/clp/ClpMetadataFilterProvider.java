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

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
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
 * Loads and manages metadata filter configurations for the CLP connector.
 * <p>
 * The configuration file is specified by the {@code clp.metadata-filter-config} property
 * and defines metadata filters used to optimize query execution through split pruning.
 * Filters can be declared at different scopes:
 * <ul>
 *   <li><b>Catalog-level</b>: applies to all schemas and tables within a catalog.</li>
 *   <li><b>Schema-level</b>: applies to all tables within a specific catalog and schema.</li>
 *   <li><b>Table-level</b>: applies to a fully qualified table {@code catalog.schema.table}.</li>
 * </ul>
 *
 * <p>Each scope maps to a list of filter definitions. Each filter includes the following fields:
 * <ul>
 *   <li><b>{@code columnName}</b> (required): the name of a column in the table's logical schema.
 *       Only columns of numeric type are currently supported as metadata filters.</li>
 *
 *   <li><b>{@code rangeMapping}</b> (optional): remaps a logical filter to physical metadata-only columns.
 *       This field is valid only for numeric-type columns.
 *       For example, a condition such as:
 *       <pre>{@code
 *       "msg.timestamp" > 1234 AND "msg.timestamp" < 5678
 *       }</pre>
 *       will be rewritten as:
 *       <pre>{@code
 *       "end_timestamp" > 1234 AND "begin_timestamp" < 5678
 *       }</pre>
 *       This ensures the filter applies to a superset of the actual result set, enabling safe pruning.</li>
 *
 *   <li><b>{@code required}</b> (optional, default: {@code false}): indicates whether the filter must be present
 *       in the extracted metadata filter SQL query. If a required filter is missing or cannot be pushed down,
 *       the query will be rejected.</li>
 * </ul>
 */
public class ClpMetadataFilterProvider
{
    private final Map<String, List<Filter>> filterMap;

    @Inject
    public ClpMetadataFilterProvider(ClpConfig config)
    {
        requireNonNull(config, "config is null");

        ObjectMapper mapper = new ObjectMapper();
        try {
            filterMap = mapper.readValue(
                    new File(config.getMetadataFilterConfig()),
                    new TypeReference<Map<String, List<Filter>>>() {});
        }
        catch (IOException e) {
            throw new PrestoException(CLP_METADATA_FILTER_CONFIG_NOT_FOUND, "Failed to metadata filter config file open.");
        }
    }

    public void checkContainsRequiredFilters(SchemaTableName schemaTableName, String metadataFilterSql)
    {
        boolean hasRequiredMetadataFilterColumns = true;
        ImmutableList.Builder<String> notFoundListBuilder = ImmutableList.builder();
        for (String columnName : getRequiredFilterNames(format("%s.%s", CONNECTOR_NAME, schemaTableName))) {
            if (!metadataFilterSql.contains(columnName)) {
                hasRequiredMetadataFilterColumns = false;
                notFoundListBuilder.add(columnName);
            }
        }
        if (!hasRequiredMetadataFilterColumns) {
            throw new PrestoException(
                    CLP_MANDATORY_METADATA_FILTER_NOT_VALID,
                    notFoundListBuilder.build() + " is a mandatory metadata filter column but not valid");
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

        Map<String, RangeMapping> mappings = new HashMap<>(getAllMappingsFromFilters(filterMap.get(splitScope[0])));

        if (1 < splitScope.length) {
            mappings.putAll(getAllMappingsFromFilters(filterMap.get(splitScope[0] + "." + splitScope[1])));
        }

        if (3 == splitScope.length) {
            mappings.putAll(getAllMappingsFromFilters(filterMap.get(scope)));
        }

        String remappedSql = sql;
        for (Map.Entry<String, RangeMapping> entry : mappings.entrySet()) {
            String key = entry.getKey();
            RangeMapping value = entry.getValue();
            remappedSql = remappedSql.replaceAll(
                    format("\"(%s)\"\\s(>=?)\\s([0-9]*)", key),
                    format("%s $2 $3", value.upperBound));
            remappedSql = remappedSql.replaceAll(
                    format("\"(%s)\"\\s(<=?)\\s([0-9]*)", key),
                    format("%s $2 $3", value.lowerBound));
            remappedSql = remappedSql.replaceAll(
                    format("\"(%s)\"\\s(=)\\s([0-9]*)", key),
                    format("(%s <= $3 AND %s >= $3)", value.lowerBound, value.upperBound));
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
        builder.addAll(getAllFilterNamesFromFilters(filterMap.get(splitScope[0])));

        if (1 < splitScope.length) {
            builder.addAll(getAllFilterNamesFromFilters(filterMap.get(splitScope[0] + "." + splitScope[1])));
        }

        if (3 == splitScope.length) {
            builder.addAll(getAllFilterNamesFromFilters(filterMap.get(scope)));
        }
        return builder.build();
    }

    private Set<String> getAllFilterNamesFromFilters(List<Filter> filters)
    {
        return null != filters ? filters.stream()
                .map(filter -> filter.columnName)
                .collect(toImmutableSet()) : ImmutableSet.of();
    }

    private Set<String> getRequiredFilterNames(String scope)
    {
        String[] splitScope = scope.split("\\.");
        if (0 == splitScope.length) {
            return ImmutableSet.of();
        }
        ImmutableSet.Builder<String> builder = ImmutableSet.builder();
        builder.addAll(getRequiredFilterNamesFromFilters(filterMap.get(splitScope[0])));

        if (1 < splitScope.length) {
            builder.addAll(getRequiredFilterNamesFromFilters(filterMap.get(splitScope[0] + "." + splitScope[1])));
        }

        if (3 == splitScope.length) {
            builder.addAll(getRequiredFilterNamesFromFilters(filterMap.get(scope)));
        }
        return builder.build();
    }

    private Set<String> getRequiredFilterNamesFromFilters(List<Filter> filters)
    {
        return null != filters ? filters.stream()
                .filter(filter -> filter.required)
                .map(filter -> filter.columnName)
                .collect(toImmutableSet()) : ImmutableSet.of();
    }

    private Map<String, RangeMapping> getAllMappingsFromFilters(List<Filter> filters)
    {
        return filters != null
                ? filters.stream()
                .filter(filter -> null != filter.rangeMapping)
                .collect(toImmutableMap(
                        filter -> filter.columnName,
                        filter -> filter.rangeMapping))
                : ImmutableMap.of();
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
        @JsonProperty("columnName")
        public String columnName;

        @JsonProperty("rangeMapping")
        public RangeMapping rangeMapping;

        @JsonProperty("required")
        public boolean required;
    }
}
