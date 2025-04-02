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
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

public class ClpClient
{
    private static final Logger log = Logger.get(ClpClient.class);

    private final LoadingCache<SchemaTableName, List<ClpColumnHandle>> columnHandleCache;
    private final LoadingCache<String, List<String>> tableNameCache;
    private final ClpMetadataProvider clpMetadataProvider;
    private final ClpSplitProvider clpSplitProvider;

    @Inject
    public ClpClient(ClpConfig config)
    {
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

    public List<ClpColumnHandle> loadColumnHandles(SchemaTableName schemaTableName)
    {
        return clpMetadataProvider.listColumnHandles(schemaTableName);
    }

    public List<String> loadTableNames(String schemaName)
    {
        return clpMetadataProvider.listTableNames(schemaName);
    }

    public List<String> listTables(String schemaName)
    {
        return tableNameCache.getUnchecked(schemaName);
    }

    public List<ClpSplit> listSplits(ClpTableLayoutHandle layoutHandle)
    {
        return clpSplitProvider.listSplits(layoutHandle);
    }

    public List<ClpColumnHandle> listColumns(SchemaTableName schemaTableName)
    {
        return columnHandleCache.getUnchecked(schemaTableName);
    }
}
