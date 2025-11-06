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

import static com.facebook.presto.plugin.clp.ClpErrorCode.CLP_SPLIT_FILTER_CONFIG_NOT_FOUND;
import static java.util.Objects.requireNonNull;

public class ClpSplitMetadataConfig
{
    private final Map<String, TableConfig> tableConfigs = new HashMap<>();

    public static class MetaColumn {
        public final String name;
        public final String type;
        public final String exposedAs;
        public final String description;
        public final String asRangeBoundOf; // optional
        public final String boundType;      // "lower" or "upper"

        public MetaColumn(String name, JsonNode node) {
            this.name = name;
            this.type = node.path("type").asText();
            this.exposedAs = node.path("exposedAs").asText(name);
            this.description = node.path("description").asText(null);
            JsonNode filter = node.path("filter");
            this.asRangeBoundOf = filter.path("asRangeBoundOf").isMissingNode() ? null : filter.path("asRangeBoundOf").asText();
            this.boundType = filter.path("boundType").isMissingNode() ? null : filter.path("boundType").asText();
        }
    }

    public static class FilterRule {
        public final String column;
        public final boolean required;
        public final String reason;

        public FilterRule(JsonNode node) {
            this.column = node.path("column").asText();
            this.required = node.path("required").asBoolean(false);
            this.reason = node.path("reason").asText(null);
        }
    }

    public static class TableConfig {
        public final Map<String, MetaColumn> metaColumns = new HashMap<>();
        public final List<FilterRule> filterRules = new ArrayList<>();
    }

    @Inject
    public ClpSplitMetadataConfig(ClpConfig config) {
        requireNonNull(config, "config is null");

        ObjectMapper mapper = new ObjectMapper();
        JsonNode root;
        try {
            root = mapper.readTree(Files.readAllBytes(Paths.get(config.getSplitFilterConfig())));
        }
        catch (IOException e) {
            throw new PrestoException(CLP_SPLIT_FILTER_CONFIG_NOT_FOUND, "Failed to open split filter config file", e);
        }

        for (Iterator<String> it = root.fieldNames(); it.hasNext();) {
            String namespace = it.next();
            JsonNode tableNode = root.get(namespace);

            TableConfig cfg = new TableConfig();
            if (tableNode.has("metaColumns")) {
                for (Iterator<String> m = tableNode.get("metaColumns").fieldNames(); m.hasNext();) {
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

    public Map<String, String> getMetadataColumns(SchemaTableName name) {
        TableConfig cfg = getTableConfig(name);
        Map<String, String> result = new LinkedHashMap<>();
        for (MetaColumn c : cfg.metaColumns.values()) {
            result.put(c.exposedAs, c.type);
        }
        return result;
    }

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

    public Map<String, String> getExposedToOriginalMapping(SchemaTableName name) {
        TableConfig cfg = getTableConfig(name);
        Map<String, String> mapping = new HashMap<>();
        for (MetaColumn c : cfg.metaColumns.values()) {
            mapping.put(c.exposedAs, c.name);
        }
        return mapping;
    }

    public Set<String> getDataColumnsWithRangeBounds(SchemaTableName name) {
        TableConfig cfg = getTableConfig(name);
        Set<String> result = new LinkedHashSet<>();
        for (MetaColumn c : cfg.metaColumns.values()) {
            if (c.asRangeBoundOf != null) {
                result.add(c.asRangeBoundOf);
            }
        }
        return result;
    }

    public Map<String, Map<String, String>> getDataColumnRangeMapping(SchemaTableName name) {
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

    private TableConfig getTableConfig(SchemaTableName name) {
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
