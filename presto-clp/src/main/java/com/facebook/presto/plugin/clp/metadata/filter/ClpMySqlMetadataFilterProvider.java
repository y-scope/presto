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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.facebook.presto.plugin.clp.metadata.filter.ClpMetadataFilter.MetadataDatabaseSpecific;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.lang.String.format;

/**
 * Loads and manages metadata filter configurations of MySQL metadata database for the CLP
 * connector.
 */
public class ClpMySqlMetadataFilterProvider
        extends ClpMetadataFilterProvider
{
    @Inject
    public ClpMySqlMetadataFilterProvider(ClpConfig config)
    {
        super(config);
    }

    /**
     * This method performs regex-based replacements according to the {@code "rangeMapping"} field
     * in {@link ClpMySqlMetadataDatabaseSpecific} to convert numeric filter expressions. For
     * example:
     * <ul>
     *   <li>{@code "msg.timestamp" >= 1234} → {@code end_timestamp >= 1234}</li>
     *   <li>{@code "msg.timestamp" <= 5678} → {@code begin_timestamp <= 5678}</li>
     *   <li>{@code "msg.timestamp" = 4567} →
     *   {@code (begin_timestamp <= 4567 AND end_timestamp >= 4567)}</li>
     * </ul>
     *
     * @param scope the scope of the filter
     * @param pushDownExpression the MySQL SQL string that needs to be rewritten
     * @return the rewritten SQL string
     */
    @Override
    public String remapMetadataFilterPushDown(String scope, String pushDownExpression)
    {
        String[] splitScope = scope.split("\\.");

        Map<String, ClpMySqlMetadataDatabaseSpecific.RangeMapping> mappings = new HashMap<>(getAllMappingsFromFilters(filterMap.get(splitScope[0])));

        if (1 < splitScope.length) {
            mappings.putAll(getAllMappingsFromFilters(filterMap.get(splitScope[0] + "." + splitScope[1])));
        }

        if (3 == splitScope.length) {
            mappings.putAll(getAllMappingsFromFilters(filterMap.get(scope)));
        }

        String remappedSql = pushDownExpression;
        for (Map.Entry<String, ClpMySqlMetadataDatabaseSpecific.RangeMapping> entry : mappings.entrySet()) {
            String key = entry.getKey();
            ClpMySqlMetadataDatabaseSpecific.RangeMapping value = entry.getValue();
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

    @Override
    protected Class<? extends MetadataDatabaseSpecific> getMetadataDatabaseSpecificClass()
    {
        return ClpMySqlMetadataDatabaseSpecific.class;
    }

    private Map<String, ClpMySqlMetadataDatabaseSpecific.RangeMapping> getAllMappingsFromFilters(List<ClpMetadataFilter> filters)
    {
        return null != filters
                ? filters.stream()
                .filter(filter ->
                        filter.metadataDatabaseSpecific instanceof ClpMySqlMetadataDatabaseSpecific &&
                                ((ClpMySqlMetadataDatabaseSpecific) filter.metadataDatabaseSpecific).rangeMapping != null)
                .collect(toImmutableMap(
                        filter -> filter.columnName,
                        filter -> ((ClpMySqlMetadataDatabaseSpecific) filter.metadataDatabaseSpecific).rangeMapping))
                : ImmutableMap.of();
    }

    /**
     * The MySql-specific fields in the filter contains:
     * <ul>
     *   <li><b>{@code rangeMapping}</b> <i>(optional)</i>: an object only for numeric type filter
     *   with the following properties:
     *      <ul>
     *          <li>{@code lowerBound}: The metadata column that represents the lower bound of values
     *          in a split for the data column.</li>
     *          <li>{@code upperBound}: The metadata column that represents the upper bound of values
     *          in a split for the data column.</li>
     *      </ul>
     *   </li>
     * </ul>
     */
    protected static class ClpMySqlMetadataDatabaseSpecific
            implements MetadataDatabaseSpecific
    {
        @JsonProperty("rangeMapping")
        public RangeMapping rangeMapping;

        public static class RangeMapping
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
    }
}
