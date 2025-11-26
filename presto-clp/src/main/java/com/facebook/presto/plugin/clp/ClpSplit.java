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

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.NodeProvider;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;

import java.io.IOException;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.spi.schedule.NodeSelectionStrategy.NO_PREFERENCE;
import static java.util.Objects.requireNonNull;

public class ClpSplit
        implements ConnectorSplit
{
    private final String path;
    private final SplitType type;
    private final Optional<String> kqlQuery;
    private final String projectionNameValueEncoded;

    @JsonCreator
    public ClpSplit(
            @JsonProperty("path") String path,
            @JsonProperty("type") SplitType type,
            @JsonProperty("kqlQuery") Optional<String> kqlQuery,
            @JsonProperty("projectionNameValue") String projectionNameValueEncoded)
    {
        this.path = requireNonNull(path, "Split path is null");
        this.type = requireNonNull(type, "Split type is null");
        this.kqlQuery = kqlQuery;
        this.projectionNameValueEncoded = projectionNameValueEncoded != null ? projectionNameValueEncoded : "";
    }

    public ClpSplit(
            String path,
            SplitType type,
            Optional<String> kqlQuery,
            Optional<Map<String, Object>> projectionNameValue)
    {
        this.path = requireNonNull(path, "Split path is null");
        this.type = requireNonNull(type, "Split type is null");
        this.kqlQuery = kqlQuery;

        try {
            this.projectionNameValueEncoded = encodeProjectionNameValue(
                    projectionNameValue.orElse(new HashMap<>()));
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to encode projection name value", e);
        }
    }

    @JsonProperty
    public String getPath()
    {
        return path;
    }

    @JsonProperty
    public SplitType getType()
    {
        return type;
    }

    @JsonProperty
    public Optional<String> getKqlQuery()
    {
        return kqlQuery;
    }

    @JsonProperty("projectionNameValue")
    public String getProjectionNameValue()
    {
        return projectionNameValueEncoded;
    }

    // Serialize projectionColumns to MessagePack, then Base64 encode for JSON transport
    public String encodeProjectionNameValue(Map<String, Object> splitMetadataColumn) throws IOException
    {
        if (splitMetadataColumn.isEmpty()) {
            return "";
        }

        MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();

        packer.packMapHeader(splitMetadataColumn.size());
        for (Map.Entry<String, Object> entry : splitMetadataColumn.entrySet()) {
            packer.packString(entry.getKey());

            Object value = entry.getValue();
            if (value instanceof String) {
                packer.packString((String) value);
            }
            else if (value instanceof Integer) {
                packer.packInt((Integer) value);
            }
            else if (value instanceof Long) {
                packer.packLong((Long) value);
            }
            else if (value instanceof Double) {
                packer.packDouble((Double) value);
            }
            else if (value instanceof Float) {
                packer.packFloat((Float) value);
            }
            else {
                throw new IllegalArgumentException(
                        "Unsupported type for column " + entry.getKey() + ": " + value.getClass());
            }
        }

        byte[] bytes = packer.toByteArray();
        packer.close();

        // Base64 encode for JSON transport
        return Base64.getEncoder().encodeToString(bytes);
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
    public int hashCode()
    {
        return Objects.hash(path, type, kqlQuery);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ClpSplit other = (ClpSplit) obj;
        return this.type == other.type && this.path.equals(other.path) && this.kqlQuery.equals(other.kqlQuery);
    }

    @Override
    public Map<String, String> getInfo()
    {
        return ImmutableMap.of("path", path, "type", type.toString(), "kqlQuery", kqlQuery.orElse("<null>"));
    }

    public enum SplitType
    {
        ARCHIVE,
        IR,
    }
}
