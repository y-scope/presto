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
package com.facebook.presto.plugin.clp.metadata.filter;

import com.facebook.presto.plugin.clp.ClpConfig;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static com.facebook.presto.plugin.clp.ClpConnectorFactory.CONNECTOR_NAME;
import static com.facebook.presto.plugin.clp.ClpErrorCode.CLP_MANDATORY_METADATA_FILTER_NOT_VALID;
import static com.facebook.presto.plugin.clp.ClpErrorCode.CLP_METADATA_FILTER_CONFIG_NOT_FOUND;
import static com.facebook.presto.plugin.clp.metadata.filter.ClpMySqlMetadataFilterProvider.ClpMySqlMetadataDatabaseSpecific;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Loads and manages metadata filter configurations for the CLP connector.
 * <p></p>
 * The configuration file is specified by the {@code clp.metadata-filter-config} property
 * and defines metadata filters used to optimize query execution through split pruning.
 * <p></p>
 * Each filter config indicates how a data column--a column in the Presto table--should be mapped
 * to a metadata column--a column in CLP’s metadata database.
 * <p></p>
 * Filter configs can be declared at either a catalog, schema, or table scope. Filter configs under
 * a particular scope will apply to all child scopes (e.g., schema-level filter configs will apply
 * to all tables within that schema).
 * <p></p>
 * For different type of metadata database, each filter config could include different following
 * fields. Here are some common fields:
 * <ul>
 *   <li><b>{@code columnName}</b>: the data column's name.</li>
 *
 *   <li><b>{@code metadataDatabaseSpecific}</b>: the metadata database specific sub-object.</li>
 *
 *   <li><b>{@code required}</b> (optional, defaults to {@code false}): indicates whether the
 *   filter must be present in the translated metadata filter SQL query. If a required filter
 *   is missing or cannot be pushed down, the query will be rejected.</li>
 * </ul>
 */
public abstract class ClpMetadataFilterProvider {

    protected final Map<String, List<Filter>> filterMap;

    public ClpMetadataFilterProvider(ClpConfig config)
    {
        requireNonNull(config, "config is null");

        if (null == config.getMetadataFilterConfig()) {
            filterMap = ImmutableMap.of();
            return;
        }
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

    public abstract String remapMetadataFilterPushDown(String scope, String pushDownExpression);

    public void checkContainsRequiredFilters(SchemaTableName schemaTableName, String metadataFilterSql)
    {
        boolean hasRequiredMetadataFilterColumns = true;
        ImmutableList.Builder<String> notFoundListBuilder = ImmutableList.builder();
        for (String columnName : getRequiredColumnNames(format("%s.%s", CONNECTOR_NAME, schemaTableName))) {
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

    public Set<String> getColumnNames(String scope)
    {
        return collectColumnNamesFromScopes(scope, this::getAllColumnNamesFromFilters);
    }

    private Set<String> getRequiredColumnNames(String scope)
    {
        return collectColumnNamesFromScopes(scope, this::getRequiredColumnNamesFromFilters);
    }

    private Set<String> collectColumnNamesFromScopes(String scope, Function<List<Filter>, Set<String>> extractor)
    {
        String[] splitScope = scope.split("\\.");
        ImmutableSet.Builder<String> builder = ImmutableSet.builder();

        builder.addAll(extractor.apply(filterMap.get(splitScope[0])));

        if (splitScope.length > 1) {
            builder.addAll(extractor.apply(filterMap.get(splitScope[0] + "." + splitScope[1])));
        }

        if (splitScope.length == 3) {
            builder.addAll(extractor.apply(filterMap.get(scope)));
        }

        return builder.build();
    }

    private Set<String> getAllColumnNamesFromFilters(List<Filter> filters)
    {
        return null != filters ? filters.stream()
                .map(filter -> filter.columnName)
                .collect(toImmutableSet()) : ImmutableSet.of();
    }

    private Set<String> getRequiredColumnNamesFromFilters(List<Filter> filters)
    {
        return null != filters ? filters.stream()
                .filter(filter -> filter.required)
                .map(filter -> filter.columnName)
                .collect(toImmutableSet()) : ImmutableSet.of();
    }

    protected static class Filter {
        @JsonProperty("columnName")
        public String columnName;

        @JsonProperty("metadataDatabaseSpecific")
        public MetadataDatabaseSpecific metadataDatabaseSpecific;

        @JsonProperty("required")
        public boolean required;
    }

    @JsonTypeInfo(
            use = JsonTypeInfo.Id.NAME,
            include = JsonTypeInfo.As.PROPERTY,
            property = "database"
    )
    @JsonSubTypes({
            @JsonSubTypes.Type(value = ClpMySqlMetadataDatabaseSpecific.class, name = "mysql")
    })
    protected interface MetadataDatabaseSpecific
    {}
}
