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
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
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
import static com.facebook.presto.plugin.clp.metadata.filter.ClpMetadataFilter.MetadataDatabaseSpecific;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Loads and manages metadata filter configurations for the CLP connector.
 * <p></p>
 * The configuration file is specified by the {@code clp.metadata-filter-config} property
 * and defines metadata filters used to optimize query execution through split pruning.
 * <p></p>
 * Filter configs can be declared at either a catalog, schema, or table scope. Filter configs under
 * a particular scope will apply to all child scopes (e.g., schema-level filter configs will apply
 * to all tables within that schema).
 * <p></p>
 * For different type of metadata database, each filter config could include different following
 * fields. See {@link ClpMetadataFilter} for the structure definition.
 */
public abstract class ClpMetadataFilterProvider
{
    protected final Map<String, List<ClpMetadataFilter>> filterMap;

    public ClpMetadataFilterProvider(ClpConfig config)
    {
        requireNonNull(config, "config is null");

        if (null == config.getMetadataFilterConfig()) {
            filterMap = ImmutableMap.of();
            return;
        }
        ObjectMapper mapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addDeserializer(
                MetadataDatabaseSpecific.class,
                new ClpMetadataDatabaseSpecificDeserializer(getMetadataDatabaseSpecificClass()));
        mapper.registerModule(module);
        try {
            filterMap = mapper.readValue(
                    new File(config.getMetadataFilterConfig()),
                    new TypeReference<Map<String, List<ClpMetadataFilter>>>() {});
        }
        catch (IOException e) {
            throw new PrestoException(CLP_METADATA_FILTER_CONFIG_NOT_FOUND, "Failed to metadata filter config file open.");
        }
    }

    /**
     * Rewrites the given pushed-down expression for metadata filtering to remap filter conditions
     * based on the configured range mappings for the given scope.
     * <p></p>
     * The {@code scope} follows the format {@code catalog[.schema][.table]}, and determines
     * which filter mappings to apply, since mappings from more specific scopes (e.g., table-level)
     * override or supplement those from broader scopes (e.g., catalog-level). For each scope
     * (catalog, schema, table), this method collects all range mappings defined in the metadata
     * filter configuration.
     *
     * @param scope the scope of the filter
     * @param pushDownExpression the pushed-down expression for metadata filtering that needs to
     *                           be rewritten
     * @return the rewritten pushed-down expression for metadata filtering
     */
    public abstract String remapMetadataFilterPushDown(String scope, String pushDownExpression);

    /**
     * Checks for the given table, if the given pushed-down expression for metadata filtering
     * contains all required fields.
     *
     * @param schemaTableName the table that is being queried
     * @param metadataFilterPushDownExpression the pushed-down expression for metadata filtering
     *                                         to be checked
     */
    public void checkContainsRequiredFilters(SchemaTableName schemaTableName, String metadataFilterPushDownExpression)
    {
        boolean hasRequiredMetadataFilterColumns = true;
        ImmutableList.Builder<String> notFoundListBuilder = ImmutableList.builder();
        for (String columnName : getRequiredColumnNames(format("%s.%s", CONNECTOR_NAME, schemaTableName))) {
            if (!metadataFilterPushDownExpression.contains(columnName)) {
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

    /**
     * Returns the {@link MetadataDatabaseSpecific} class implemented by the user. To respect to
     * our code style, we would recommend to implement a {@code protected static class} as an
     * inner class in the user-implemented {@link ClpMetadataFilterProvider} class.
     *
     * @return the user-implemented {@link MetadataDatabaseSpecific} class.
     */
    protected abstract Class<? extends MetadataDatabaseSpecific> getMetadataDatabaseSpecificClass();

    private Set<String> getRequiredColumnNames(String scope)
    {
        return collectColumnNamesFromScopes(scope, this::getRequiredColumnNamesFromFilters);
    }

    private Set<String> collectColumnNamesFromScopes(String scope, Function<List<ClpMetadataFilter>, Set<String>> extractor)
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

    private Set<String> getAllColumnNamesFromFilters(List<ClpMetadataFilter> filters)
    {
        return null != filters ? filters.stream()
                .map(filter -> filter.columnName)
                .collect(toImmutableSet()) : ImmutableSet.of();
    }

    private Set<String> getRequiredColumnNamesFromFilters(List<ClpMetadataFilter> filters)
    {
        return null != filters ? filters.stream()
                .filter(filter -> filter.required)
                .map(filter -> filter.columnName)
                .collect(toImmutableSet()) : ImmutableSet.of();
    }
}
