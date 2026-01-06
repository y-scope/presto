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

import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.plugin.clp.ClpConfig;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.inject.Inject;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.plugin.clp.ClpErrorCode.CLP_SPLIT_METADATA_CONFIG_NOT_FOUND;
import static java.util.Objects.requireNonNull;

/**
 * A class that loads and manages split-level metadata configuration.
 * <p></p>
 * The configuration file defines metadata columns and filtering rules that can be applied in query
 * planning to prune irrelevant splits based on metadata (e.g., timestamps, partitions, etc.).
 * <p></p>
 * The configuration supports a hierarchical namespace structure:
 * <ul>
 *     <li>Global defaults under an empty string key ("").</li>
 *     <li>Schema-level overrides under the schema name (e.g., <code>"logs"</code>).</li>
 *     <li>Table-level overrides under the full name (e.g., <code>"logs.events"</code>}).</li>
 * </ul>
 * <p></p>
 * Configurations from broader scopes are merged with more specific ones, with table-level
 * definitions overriding schema-level and global definitions.
 */
public class ClpSplitMetadataConfig
{
    private final Map<String, TableConfig> tableConfigs = new HashMap<>();
    private final TypeManager typeManager;

    /**
     * Represents a metadata column entry defined in the split metadata configuration file. Each
     * {@link MetaColumn} corresponds to one metadata column that can be exposed in query filters.
     */
    public static class MetaColumn
    {
        public final String name;
        public final String type;
        public final String exposedAs;
        public final String description;
        public final String asRangeBoundOf; // optional
        public final String boundType;      // "lower" or "upper"

        public MetaColumn(String name, JsonNode node)
        {
            this.name = name;
            this.type = node.path("type").asText();
            this.exposedAs = node.path("exposedAs").asText(name);
            this.description = node.path("description").asText(null);
            JsonNode filter = node.path("filter");
            this.asRangeBoundOf = filter.path("asRangeBoundOf").isMissingNode() ? null : filter.path("asRangeBoundOf").asText();
            this.boundType = filter.path("boundType").isMissingNode() ? null : filter.path("boundType").asText();
        }
    }

    /**
     * Represents a rule that defines how a metadata column or a data column should be used in
     * filtering. A rule may indicate that a column is required for query pruning and include an
     * explanation of why it is necessary.
     */
    public static class FilterRule
    {
        public final String column;
        public final boolean required;
        public final String reason;

        public FilterRule(JsonNode node)
        {
            this.column = node.path("column").asText();
            this.required = node.path("required").asBoolean(false);
            this.reason = node.path("reason").asText(null);
        }
    }

    public static class TableConfig
    {
        public final Map<String, MetaColumn> metaColumns = new HashMap<>();
        public final List<FilterRule> filterRules = new ArrayList<>();
    }

    @Inject
    public ClpSplitMetadataConfig(ClpConfig config, TypeManager typeManager)
    {
        requireNonNull(config, "config is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");

        if (null == config.getSplitMetadataConfigPath()) {
            return;
        }

        ObjectMapper mapper = new ObjectMapper();
        JsonNode root;
        try {
            root = mapper.readTree(Files.readAllBytes(Paths.get(config.getSplitMetadataConfigPath())));
        }
        catch (IOException e) {
            throw new PrestoException(CLP_SPLIT_METADATA_CONFIG_NOT_FOUND, "Failed to open split metadata config file", e);
        }

        for (Iterator<String> it = root.fieldNames(); it.hasNext(); ) {
            String namespace = it.next();
            JsonNode tableNode = root.get(namespace);

            TableConfig cfg = new TableConfig();
            if (tableNode.has("metaColumns")) {
                for (Iterator<String> m = tableNode.get("metaColumns").fieldNames(); m.hasNext(); ) {
                    String colName = m.next();
                    cfg.metaColumns.put(colName, new MetaColumn(colName, tableNode.get("metaColumns").get(colName)));
                }
            }
            if (tableNode.has("filterRules")) {
                for (JsonNode rule : tableNode.get("filterRules")) {
                    cfg.filterRules.add(new FilterRule(rule));
                }
            }
            tableConfigs.put(namespace, cfg);
        }
    }

    /**
     * Returns the mapping of exposed metadata column names to their Presto {@link Type}s for the
     * given table.
     *
     * @param name the {@link SchemaTableName} of the target table
     * @return a map from exposed column name → type
     */
    public Map<String, Type> getMetadataColumns(SchemaTableName name)
    {
        TableConfig cfg = getTableConfig(name);
        Map<String, Type> result = new LinkedHashMap<>();
        for (MetaColumn c : cfg.metaColumns.values()) {
            result.put(c.exposedAs, typeManager.getType(TypeSignature.parseTypeSignature(c.type)));
        }
        return result;
    }

    /**
     * Returns the set of metadata columns marked as required in the configuration.
     *
     * @param name the {@link SchemaTableName} of the target table
     * @return a set of column names that are required for filtering
     */
    public Set<String> getRequiredColumns(SchemaTableName name)
    {
        TableConfig cfg = getTableConfig(name);
        Set<String> requiredColumns = new LinkedHashSet<>();
        for (FilterRule rule : cfg.filterRules) {
            if (rule.required) {
                requiredColumns.add(rule.column);
            }
        }
        return requiredColumns;
    }

    /**
     * Returns a mapping of exposed metadata column names to their original internal names.
     *
     * @param name the {@link SchemaTableName} of the target table
     * @return a map from exposed column name → original column name
     */
    public Map<String, String> getExposedToOriginalMapping(SchemaTableName name)
    {
        TableConfig cfg = getTableConfig(name);
        Map<String, String> mapping = new HashMap<>();
        for (MetaColumn c : cfg.metaColumns.values()) {
            mapping.put(c.exposedAs, c.name);
        }
        return mapping;
    }

    /**
     * Returns the set of data column names that have associated range bounds defined in the
     * metadata configuration.
     *
     * @param name the {@link SchemaTableName} of the target table
     * @return a set of data column names that have range bounds
     */
    public Set<String> getDataColumnsWithRangeBounds(SchemaTableName name)
    {
        TableConfig cfg = getTableConfig(name);
        Set<String> result = new LinkedHashSet<>();
        for (MetaColumn c : cfg.metaColumns.values()) {
            if (c.asRangeBoundOf != null) {
                result.add(c.asRangeBoundOf);
            }
        }
        return result;
    }

    /**
     * Returns a mapping from data column names to their associated range bound metadata columns.
     * <p>
     * Each entry maps a data column name to another map with keys {@code "lower"} and/or
     * {@code "upper"}, representing the metadata column names that define those bounds.
     *
     * @param name the {@link SchemaTableName} of the target table
     * @return a nested mapping from data column name → ("lower"/"upper" → metadata column name)
     */
    public Map<String, Map<String, String>> getDataColumnRangeMapping(SchemaTableName name)
    {
        TableConfig cfg = getTableConfig(name);
        Map<String, Map<String, String>> mapping = new HashMap<>();
        for (MetaColumn c : cfg.metaColumns.values()) {
            if (c.asRangeBoundOf != null && c.boundType != null) {
                mapping.computeIfAbsent(c.asRangeBoundOf, k -> new HashMap<>())
                        .put(c.boundType, c.name);
            }
        }
        return mapping;
    }

    /**
     * Returns a mapping from exposed column names to their associated range bound metadata columns.
     * <p>
     * Each entry maps an exposed column name to another map with keys {@code "lower"} and/or
     * {@code "upper"}, representing the metadata column names that define those bounds.
     *
     * @param name the {@link SchemaTableName} of the target table
     * @return a nested mapping from exposed column name → ("lower"/"upper" → metadata column name)
     */
    public Map<String, Map<String, String>> getExposedColumnRangeBoundsMapping(SchemaTableName name)
    {
        TableConfig cfg = getTableConfig(name);
        Map<String, Map<String, String>> mapping = new HashMap<>();
        for (MetaColumn c : cfg.metaColumns.values()) {
            if (c.asRangeBoundOf != null || c.boundType != null) {
                mapping.computeIfAbsent(c.exposedAs, k -> new HashMap<>())
                        .put(c.boundType, c.name);
            }
        }
        return mapping;
    }

    /**
     * @param name the {@link SchemaTableName} of the target table
     * @return a set of metadata column names that are used as range bounds
     */
    public Set<String> getMetadataColumnsWithRangeBounds(SchemaTableName name)
    {
        TableConfig cfg = getTableConfig(name);
        Set<String> result = new LinkedHashSet<>();
        for (MetaColumn c : cfg.metaColumns.values()) {
            if (c.asRangeBoundOf != null || c.boundType != null) {
                result.add(c.name);
            }
        }
        return result;
    }

    /**
     * @param name the {@link SchemaTableName} of the target table
     * @return a map from exposed column name → range bound data column name
     */
    public Map<String, String> getExposedToRangeMapping(SchemaTableName name)
    {
        TableConfig cfg = getTableConfig(name);
        Map<String, String> mapping = new HashMap<>();
        for (MetaColumn c : cfg.metaColumns.values()) {
            if (c.asRangeBoundOf != null) {
                mapping.put(c.exposedAs, c.asRangeBoundOf);
            }
        }
        return mapping;
    }

    /**
     * @param name the {@link SchemaTableName} of the target table
     * @return a set of exposed column names that have range bounds configured
     */
    public Set<String> getExposedColumnsWithRangeBounds(SchemaTableName name)
    {
        TableConfig cfg = getTableConfig(name);
        Set<String> result = new LinkedHashSet<>();
        for (MetaColumn c : cfg.metaColumns.values()) {
            if (c.asRangeBoundOf != null || c.boundType != null) {
                result.add(c.exposedAs);
            }
        }
        return result;
    }

    /**
     * Merges and returns the effective {@link TableConfig} for the given table, taking into account
     * the hierarchical configuration structure: global → schema → table.
     *
     * @param name the {@link SchemaTableName} of the target table
     * @return the merged table configuration
     */
    private TableConfig getTableConfig(SchemaTableName name)
    {
        TableConfig merged = new TableConfig();

        List<String> namespaces = new ArrayList<>();
        namespaces.add("");
        namespaces.add(name.getSchemaName());
        namespaces.add(name.getSchemaName() + "." + name.getTableName());

        for (String ns : namespaces) {
            TableConfig cfg = tableConfigs.get(ns);
            if (cfg != null) {
                merged.metaColumns.putAll(cfg.metaColumns);

                for (FilterRule rule : cfg.filterRules) {
                    boolean exists = merged.filterRules.stream()
                            .anyMatch(r -> r.column.equals(rule.column));
                    if (!exists) {
                        merged.filterRules.add(rule);
                    }
                }
            }
        }

        return merged;
    }
}
