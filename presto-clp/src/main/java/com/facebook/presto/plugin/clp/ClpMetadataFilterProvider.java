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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static com.facebook.presto.plugin.clp.ClpConnectorFactory.CONNECTOR_NAME;
import static com.facebook.presto.plugin.clp.ClpErrorCode.CLP_MANDATORY_METADATA_FILTER_NOT_VALID;
import static com.facebook.presto.plugin.clp.ClpErrorCode.CLP_METADATA_FILTER_CONFIG_NOT_FOUND;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

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

    public Map<String, TableConfig> getFilterMap()
    {
        return filterMap;
    }

    public void checkContainsAllFilters(SchemaTableName schemaTableName, String metadataFilterKqlQuery)
    {
        boolean hasAllMetadataFilterColumns = true;
        String notFoundFilterColumnName = "";
        for (String columnName : getFilterNames(
                CONNECTOR_NAME + "." + schemaTableName.toString())) {
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
                    "\"(" + entry.getKey() + ")\"\\s(>=?)\\s([0-9]*)", entry.getValue().upperBound + " $2 $3");
            remappedSql = remappedSql.replaceAll(
                    "\"(" + entry.getKey() + ")\"\\s(<=?)\\s([0-9]*)", entry.getValue().lowerBound + " $2 $3");
            remappedSql = remappedSql.replaceAll(
                    "\"(" + entry.getKey() + ")\"\\s(=)\\s([0-9]*)",
                    "(" + entry.getValue().lowerBound + " <= $3 AND " + entry.getValue().upperBound + " >= $3)");
        }
        return remappedSql;
    }

    public Set<String> getFilterNames(String scope)
    {
        String[] splitScope = scope.split("\\.");
        if (0 == splitScope.length) {
            return Collections.emptySet();
        }
        Set<String> filterNames = new HashSet<>(getAllFilerNamesFromTableConfig(filterMap.get(splitScope[0])));

        if (1 < splitScope.length) {
            filterNames.addAll(getAllFilerNamesFromTableConfig(filterMap.get(splitScope[0] + "." + splitScope[1])));
        }

        if (3 == splitScope.length) {
            filterNames.addAll(getAllFilerNamesFromTableConfig(filterMap.get(scope)));
        }
        return ImmutableSet.copyOf(filterNames);
    }

    private Set<String> getAllFilerNamesFromTableConfig(TableConfig tableConfig)
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
