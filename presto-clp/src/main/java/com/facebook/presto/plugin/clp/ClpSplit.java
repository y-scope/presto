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
    private final String metadataProjectionNameValueEncoded;

    /**
     * Invoked by Jackson; serializes a ClpSplit to JSON format
     *
     * @param path the path to the split
     * @param type the split type
     * @param kqlQuery optional KQL query pushed down to CLP-S
     * @param metadataProjectionNameValueEncoded Base64-encoded MessagePack representation of metadata projection in
     *                                           column name and value pairs
     */
    @JsonCreator
    public ClpSplit(
            @JsonProperty("path") String path,
            @JsonProperty("type") SplitType type,
            @JsonProperty("kqlQuery") Optional<String> kqlQuery,
            @JsonProperty("metadataProjectionNameValue") String metadataProjectionNameValueEncoded)
    {
        this.path = requireNonNull(path, "Split path is null");
        this.type = requireNonNull(type, "Split type is null");
        this.kqlQuery = kqlQuery;
        this.metadataProjectionNameValueEncoded = metadataProjectionNameValueEncoded != null ? metadataProjectionNameValueEncoded : "";
    }

    /**
     * Creates a ClpSplit for internal use
     *
     * @param path the path to the split
     * @param type the split type
     * @param kqlQuery optional KQL query pushed down to CLP-S
     * @param metadataProjectionNameValue optional map of metadata projection column names to their values
     * @throws RuntimeException if encoding metadata projection name-value pairs fails
     */
    public ClpSplit(
            String path,
            SplitType type,
            Optional<String> kqlQuery,
            Optional<Map<String, Object>> metadataProjectionNameValue)
    {
        this.path = requireNonNull(path, "Split path is null");
        this.type = requireNonNull(type, "Split type is null");
        this.kqlQuery = kqlQuery;

        try {
            this.metadataProjectionNameValueEncoded = encodeProjectionNameValue(
                    metadataProjectionNameValue.orElse(new HashMap<>()));
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

    @JsonProperty("metadataProjectionNameValue")
    public String getmetadataProjectionNameValue()
    {
        return metadataProjectionNameValueEncoded;
    }

    /**
     * @param metadataColumnNameValue map of metadata column names to their values.
     * @return Base64-encoded MessagePack representation of the metadataColumnNameValue map
     * @throws IOException if MessagePack serialization fails
     * @throws IllegalArgumentException if a value type is not String, Long, or Double
     */
    private String encodeProjectionNameValue(Map<String, Object> metadataColumnNameValue) throws IOException
    {
        if (metadataColumnNameValue.isEmpty()) {
            return "";
        }

        try (MessageBufferPacker packer = MessagePack.newDefaultBufferPacker()) {
            packer.packMapHeader(metadataColumnNameValue.size());
            for (Map.Entry<String, Object> entry : metadataColumnNameValue.entrySet()) {
                packer.packString(entry.getKey());

                Object value = entry.getValue();
                if (value instanceof String) {
                    packer.packString((String) value);
                }
                else if (value instanceof Long) {
                    packer.packLong((Long) value);
                }
                else if (value instanceof Double) {
                    packer.packDouble((Double) value);
                }
                else {
                    throw new IllegalArgumentException(
                            "Unsupported type for column " + entry.getKey() + ": " + value.getClass());
                }
            }
            return Base64.getEncoder().encodeToString(packer.toByteArray());
        }
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
