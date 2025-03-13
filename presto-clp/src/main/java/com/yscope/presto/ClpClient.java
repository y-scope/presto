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
package com.yscope.presto;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.yscope.presto.metadata.ClpMetadataProvider;
import com.yscope.presto.metadata.ClpMySQLMetadataProvider;
import com.yscope.presto.split.ClpMySQLSplitProvider;
import com.yscope.presto.split.ClpSplitProvider;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

public class ClpClient
{
    private static final Logger log = Logger.get(ClpClient.class);

    private final ClpConfig config;
    private final LoadingCache<SchemaTableName, Set<ClpColumnHandle>> columnHandleCache;
    private final LoadingCache<String, Set<String>> tableNameCache;
    private final ClpMetadataProvider clpMetadataProvider;
    private final ClpSplitProvider clpSplitProvider;

    @Inject
    public ClpClient(ClpConfig config)
    {
        this.config = requireNonNull(config, "config is null");
        if (config.getMetadataSource() == ClpConfig.MetadataSource.MYSQL) {
            clpMetadataProvider = new ClpMySQLMetadataProvider(config);
        }
        else {
            log.error("Unsupported metadata source: %s", config.getMetadataSource());
            throw new PrestoException(ClpErrorCode.CLP_UNSUPPORTED_METADATA_SOURCE, "Unsupported metadata source: " + config.getMetadataSource());
        }

        if (config.getSplitSource() == ClpConfig.SplitSource.MYSQL) {
            clpSplitProvider = new ClpMySQLSplitProvider(config);
        }
        else {
            log.error("Unsupported split source: %s", config.getSplitSource());
            throw new PrestoException(ClpErrorCode.CLP_UNSUPPORTED_SPLIT_SOURCE, "Unsupported split source: " + config.getSplitSource());
        }

        this.columnHandleCache = CacheBuilder.newBuilder()
                .expireAfterWrite(config.getMetadataExpireInterval(), SECONDS)
                .refreshAfterWrite(config.getMetadataRefreshInterval(), SECONDS)
                .build(CacheLoader.from(this::loadColumnHandles));

        this.tableNameCache = CacheBuilder.newBuilder()
                .expireAfterWrite(config.getMetadataExpireInterval(), SECONDS)
                .refreshAfterWrite(config.getMetadataRefreshInterval(), SECONDS)
                .build(CacheLoader.from(this::loadTableNames));
    }

    public Set<ClpColumnHandle> loadColumnHandles(SchemaTableName schemaTableName)
    {
        Set<ClpColumnHandle> columnHandles = clpMetadataProvider.listColumnHandles(schemaTableName);
        if (!config.isPolymorphicTypeEnabled()) {
            return columnHandles;
        }
        return handlePolymorphicType(columnHandles);
    }

    public Set<String> loadTableNames(String schemaName)
    {
        return clpMetadataProvider.listTableNames(schemaName);
    }

    public Set<String> listTables(String schemaName)
    {
        return tableNameCache.getUnchecked(schemaName);
    }

    public List<ClpSplit> listSplits(ClpTableLayoutHandle layoutHandle)
    {
        return clpSplitProvider.listSplits(layoutHandle);
    }

    public Set<ClpColumnHandle> listColumns(SchemaTableName schemaTableName)
    {
        return columnHandleCache.getUnchecked(schemaTableName);
    }

    private Set<ClpColumnHandle> handlePolymorphicType(Set<ClpColumnHandle> columnHandles)
    {
        Map<String, List<ClpColumnHandle>> columnNameToColumnHandles = new HashMap<>();
        HashSet<ClpColumnHandle> polymorphicColumnHandles = new HashSet<>();

        for (ClpColumnHandle columnHandle : columnHandles) {
            columnNameToColumnHandles.computeIfAbsent(columnHandle.getColumnName(), k -> new ArrayList<>())
                    .add(columnHandle);
        }
        for (Map.Entry<String, List<ClpColumnHandle>> entry : columnNameToColumnHandles.entrySet()) {
            List<ClpColumnHandle> columnHandleList = entry.getValue();
            if (columnHandleList.size() == 1) {
                polymorphicColumnHandles.add(columnHandleList.get(0));
            }
            else {
                for (ClpColumnHandle columnHandle : columnHandleList) {
                    polymorphicColumnHandles.add(new ClpColumnHandle(
                            columnHandle.getColumnName() + "_" + columnHandle.getColumnType().getDisplayName(),
                            columnHandle.getColumnName(),
                            columnHandle.getColumnType(),
                            columnHandle.isNullable()));
                }
            }
        }
        return polymorphicColumnHandles;
    }
}
