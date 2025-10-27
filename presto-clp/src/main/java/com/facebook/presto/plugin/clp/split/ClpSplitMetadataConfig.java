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
import com.facebook.presto.plugin.clp.ClpConfig;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class ClpSplitMetadataConfig
{
    /** Metadata column definition */
    public static class MetaColumn {
        public String type;           // STRING, DATE, BIGINT
        public String exposedAs;      // optional logical name
        public Filter filter;         // optional filter config
        public String name;           // original column name, assigned during load

        public static class Filter {
            public Boolean allowDirectFilter;   // default true
            public String asRangeBoundOf;       // optional: maps to data column
            public String boundType;            // lower/upper
        }

        public String getExposedName() {
            return exposedAs != null ? exposedAs : name;
        }
    }

    /** Filter rule (required filters, etc.) */
    public static class FilterRule {
        public String column;
        public Boolean required;    // default false
        public String reason;
    }

    /** Per-dataset configuration */
    public static class TableConfig {
        public String inherits;
        public Map<String, MetaColumn> metaColumns = new HashMap<>();
        public List<FilterRule> filterRules = new ArrayList<>();
    }

    private final Map<String, TableConfig> rawConfigs = new HashMap<>();
    private final Map<String, TableConfig> resolvedConfigs = new HashMap<>();

    public ClpSplitMetadataConfig(ClpConfig config) throws Exception {
        requireNonNull(config, "config is null");

        if (null == config.getSplitFilterConfig()) {
            return;
        }

        ObjectMapper mapper = new ObjectMapper();
        Map<String, TableConfig> map = mapper.readValue(new File(config.getSplitFilterConfig()), new TypeReference<>() {});
        rawConfigs.putAll(map);
        validateInheritance();
    }

    /** Detect cycles in inheritance */
    private void validateInheritance() {
        for (String key : rawConfigs.keySet()) {
            Set<String> visited = new HashSet<>();
            String current = key;
            while (rawConfigs.get(current).inherits != null) {
                current = rawConfigs.get(current).inherits;
                if (visited.contains(current)) {
                    throw new RuntimeException("Cycle detected in inheritance: " + key);
                }
                visited.add(current);
            }
        }
    }

    /** Resolve dataset config with nested inheritance */
    public TableConfig getConfig(String dataset) {
        if (resolvedConfigs.containsKey(dataset)) {
            return resolvedConfigs.get(dataset);
        }

        TableConfig config = rawConfigs.get(dataset);
        if (config == null) {
            // fallback to parent/grandparent by dataset name
            int lastDot = dataset.lastIndexOf('.');
            if (lastDot > 0) {
                return getConfig(dataset.substring(0, lastDot));
            }
            throw new RuntimeException("Dataset config not found: " + dataset);
        }

        TableConfig merged = new TableConfig();
        if (config.inherits != null) {
            TableConfig parent = getConfig(config.inherits);
            merged.metaColumns.putAll(parent.metaColumns);
            merged.filterRules.addAll(parent.filterRules);
        }

        // Override child metaColumns and filterRules
        merged.metaColumns.putAll(config.metaColumns);
        merged.filterRules.addAll(config.filterRules);

        // Set default allowDirectFilter = true if not specified
        merged.metaColumns.values().forEach(mc -> {
            mc.name = mc.name == null ? getKeyForMeta(mc, config) : mc.name;
            if (mc.filter == null) mc.filter = new MetaColumn.Filter();
            if (mc.filter.allowDirectFilter == null) mc.filter.allowDirectFilter = true;
        });

        resolvedConfigs.put(dataset, merged);
        return merged;
    }

    // Helper to assign original name
    private String getKeyForMeta(MetaColumn mc, TableConfig datasetConfig) {
        for (Map.Entry<String, MetaColumn> e : datasetConfig.metaColumns.entrySet()) {
            if (e.getValue() == mc) return e.getKey();
        }
        return null;
    }

    /** Convert metadata columns to Presto types with exposed names */
    public Map<String, Type> resolveMetadataSchema(String dataset) {
        TableConfig config = getConfig(dataset);
        Map<String, String> schema = new LinkedHashMap<>();
        for (MetaColumn mc : config.metaColumns.values()) {
            String prestoType;
            switch (mc.type.toUpperCase()) {
                case "STRING": prestoType = "VARCHAR"; break;
                case "DATE": prestoType = "DATE"; break;
                case "BIGINT": prestoType = "BIGINT"; break;
                default: prestoType = "VARCHAR"; break;
            }
            schema.put(mc.getExposedName(), prestoType);
        }
        return schema;
    }

    /** Generate metadata filter SQL */
    public String generateMetadataFilterSql(String dataset, Map<String, Object> filters) {
        TableConfig config = getConfig(dataset);
        List<String> conditions = new ArrayList<>();
        Set<String> requiredColumns = new HashSet<>();
        for (FilterRule rule : config.filterRules) {
            if (Boolean.TRUE.equals(rule.required)) requiredColumns.add(rule.column);
        }

        for (Map.Entry<String, Object> e : filters.entrySet()) {
            String col = e.getKey();
            Object val = e.getValue();

            // Translate msg.timestamp -> begin_timestamp / end_timestamp
            MetaColumn begin = null, end = null;
            for (MetaColumn mc : config.metaColumns.values()) {
                if (mc.filter != null && col.equals(mc.filter.asRangeBoundOf)) {
                    if ("lower".equals(mc.filter.boundType)) begin = mc;
                    if ("upper".equals(mc.filter.boundType)) end = mc;
                }
            }
            if (begin != null && end != null && val instanceof List && ((List<?>) val).size() == 2) {
                Object lowerVal = ((List<?>) val).get(0);
                Object upperVal = ((List<?>) val).get(1);
                conditions.add(begin.name + " >= " + lowerVal);
                conditions.add(end.name + " <= " + upperVal);
                requiredColumns.remove(col);
            } else {
                // direct filter on metadata
                if (config.metaColumns.containsKey(col)) {
                    conditions.add(col + " = '" + val + "'");
                    requiredColumns.remove(col);
                }
            }
        }

        if (!requiredColumns.isEmpty()) {
            throw new RuntimeException("Required filters missing: " + requiredColumns);
        }

        return String.join(" AND ", conditions);
    }
}
