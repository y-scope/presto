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
import com.google.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.plugin.clp.ClpErrorCode.CLP_METADATA_FILTER_CONFIG_NOT_FOUND;
import static java.util.Objects.requireNonNull;

/**
 * This class is to read a JSON config file from the path given by config file. Here is an example:
 * {
 *   "clp.default.table_1": {
 *     "filters": [
 *       {
 *         "filterName": "timestamp",
 *         "type": "numeric",
 *         "mustHave": false
 *       },
 *       {
 *         "filterName": "file_name",
 *         "type": "string",
 *         "mustHave": true
 *       }
 *     ]
 *   }
 * }
 *
 */
public class ClpMetadataFilterProvider
{
    private static final String NUMERIC_TYPE = "numeric";
    private static final String STRING_TYPE = "string";
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
        return filterNames;
    }

    public void checkContainsAllMustHaveFilters(SchemaTableName schemaTableName, String metadataFilterKqlQuery)
    {
        boolean hasAllMustHaveMetadataFilterColumns = true;
        String notFoundMandatoryFilterColumnName = "";
        for (String mustHaveColumnName : getMustHaveFilterNames(
                ClpConnectorFactory.CONNECTOR_NAME + "." + schemaTableName.toString())) {
            if (!metadataFilterKqlQuery.contains(mustHaveColumnName)) {
                hasAllMustHaveMetadataFilterColumns = false;
                notFoundMandatoryFilterColumnName = mustHaveColumnName;
                break;
            }
        }
        if (!hasAllMustHaveMetadataFilterColumns) {
            throw new PrestoException(ClpErrorCode.CLP_MANDATORY_METADATA_FILTER_NOT_VALID, notFoundMandatoryFilterColumnName + " is a mandatory metadata filter column but not valid");
        }
    }

    public Set<String> getMustHaveFilterNames(String scope)
    {
        String[] splitScope = scope.split("\\.");
        if (0 == splitScope.length) {
            return Collections.emptySet();
        }
        Set<String> filterNames = new HashSet<>(getAllMustHaveFilterNamesFromTableConfig(filterMap.get(splitScope[0])));

        if (1 < splitScope.length) {
            filterNames.addAll(getAllMustHaveFilterNamesFromTableConfig(filterMap.get(splitScope[0] + "." + splitScope[1])));
        }

        if (3 == splitScope.length) {
            filterNames.addAll(getAllMustHaveFilterNamesFromTableConfig(filterMap.get(scope)));
        }
        return filterNames;
    }

    private Set<String> getAllFilerNamesFromTableConfig(TableConfig tableConfig)
    {
        return null != tableConfig ? tableConfig.filters.stream()
                .map(filter -> filter.filterName)
                .collect(Collectors.toSet()) : Collections.emptySet();
    }

    private Set<String> getAllMustHaveFilterNamesFromTableConfig(TableConfig tableConfig)
    {
        return null != tableConfig ? tableConfig.filters.stream()
                .filter(filter -> filter.mustHave)
                .map(filter -> filter.filterName)
                .collect(Collectors.toSet()) : Collections.emptySet();
    }

    public static class TableConfig
    {
        public List<Filter> filters;
    }

    public static class Filter
    {
        @JsonProperty("filterName")
        public String filterName;

        @JsonProperty("mustHave")
        public boolean mustHave;
    }
}
