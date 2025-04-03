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

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.NodeProvider;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.spi.schedule.NodeSelectionStrategy.NO_PREFERENCE;

public class ClpSplit
        implements ConnectorSplit
{
    private final SchemaTableName schemaTableName;
    private final String archivePath;
    private final Optional<String> query;
    private final int archiveType;

    public enum ArchiveType
    {
        UNKNOWN(0, ""),
        DEFAULT_SFA(1, "clps"),
        IRV2(2, "clp.zst");

        private final int value;
        private final String extension;

        ArchiveType(int value, String extension) {
            this.value = value;
            this.extension = extension;
        }

        public int getValue() {
            return value;
        }

        public String getExtension() {
            return extension;
        }
    }

    public static int getArchiveTypeByArchiveId(String archiveId)
    {
        if (archiveId.endsWith(ArchiveType.DEFAULT_SFA.getExtension())) {
            return ArchiveType.DEFAULT_SFA.getValue();
        } else if (archiveId.endsWith(ArchiveType.IRV2.getExtension())) {
            return ArchiveType.IRV2.getValue();
        }
        return ArchiveType.UNKNOWN.getValue();
    }

    @JsonCreator
    public ClpSplit(@JsonProperty("schemaTableName") @Nullable SchemaTableName schemaTableName,
                    @JsonProperty("archivePath") @Nullable String archivePath,
                    @JsonProperty("query") Optional<String> query,
                    @JsonProperty("archiveType") @Nullable int archiveType)
    {
        this.schemaTableName = schemaTableName;
        this.archivePath = archivePath;
        this.query = query;
        this.archiveType = archiveType;
    }

    @JsonProperty
    @Nullable
    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName;
    }

    @JsonProperty
    public String getArchivePath()
    {
        return archivePath;
    }

    @JsonProperty
    public Optional<String> getQuery()
    {
        return query;
    }

    @JsonProperty
    public int getArchiveType()
    {
        return archiveType;
    }

    @Override
    public NodeSelectionStrategy getNodeSelectionStrategy()
    {
        return NO_PREFERENCE;
    }

    @Override
    public List<HostAddress> getPreferredNodes(NodeProvider nodeProvider)
    {
        return ImmutableList.of();
    }

    @Override
    public Object getInfo()
    {
        return this;
    }
}
