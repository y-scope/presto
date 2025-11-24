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

import com.facebook.presto.plugin.clp.optimization.ClpTopNSpec;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.relation.RowExpression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;

public class ClpTableLayoutHandle
        implements ConnectorTableLayoutHandle
{
    private final ClpTableHandle table;
    private final Optional<String> kqlQuery;
    private final RowExpression metadataExpression;
    private final boolean metadataQueryOnly;
    private final Optional<ClpTopNSpec> topN;

    private Optional<Set<String>> splitMetadataColumnNames;

    @JsonCreator
    public ClpTableLayoutHandle(
            @JsonProperty("table") ClpTableHandle table,
            @JsonProperty("kqlQuery") Optional<String> kqlQuery,
            @JsonProperty("metadataExpression") RowExpression metadataExpression,
            @JsonProperty("metadataQueryOnly") boolean metadataQueryOnly,
            @JsonProperty("splitMetadataColumnNames") Optional<Set<String>> splitMetadataColumnNames,
            @JsonProperty("topN") Optional<ClpTopNSpec> topN)
    {
        this.table = table;
        this.kqlQuery = kqlQuery;
        this.metadataExpression = metadataExpression;
        this.metadataQueryOnly = metadataQueryOnly;
        this.splitMetadataColumnNames = splitMetadataColumnNames;
        this.topN = topN;
    }

    public ClpTableLayoutHandle(
            @JsonProperty("table") ClpTableHandle table,
            @JsonProperty("kqlQuery") Optional<String> kqlQuery,
            @JsonProperty("metadataExpression") RowExpression metadataExpression)
    {
        this.table = table;
        this.kqlQuery = kqlQuery;
        this.metadataExpression = metadataExpression;
        this.metadataQueryOnly = false;
        this.splitMetadataColumnNames = Optional.empty();
        this.topN = Optional.empty();
    }

    public ClpTableLayoutHandle(
            @JsonProperty("table") ClpTableHandle table,
            @JsonProperty("splitMetadataColumnNames") Optional<Set<String>> splitMetadataColumnNames)
    {
        this.table = table;
        this.kqlQuery = Optional.empty();
        this.metadataExpression = null;
        this.metadataQueryOnly = false;
        this.splitMetadataColumnNames = splitMetadataColumnNames;
        this.topN = Optional.empty();
    }

    @JsonProperty
    public ClpTableHandle getTable()
    {
        return table;
    }

    @JsonProperty
    public Optional<String> getKqlQuery()
    {
        return kqlQuery;
    }

    @JsonProperty
    public RowExpression getMetadataExpression()
    {
        return metadataExpression;
    }

    @JsonProperty
    public boolean isMetadataQueryOnly()
    {
        return metadataQueryOnly;
    }

    @JsonProperty
    public Optional<Set<String>> getSplitMetadataColumnNames()
    {
        return splitMetadataColumnNames;
    }

    public Set<String> getOrInitializeSplitMetadataColumnNames()
    {
        if (!splitMetadataColumnNames.isPresent()) {
            splitMetadataColumnNames = Optional.of(new HashSet<>());
        }
        return splitMetadataColumnNames.get();
    }

    @JsonProperty
    public Optional<ClpTopNSpec> getTopN()
    {
        return topN;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ClpTableLayoutHandle that = (ClpTableLayoutHandle) o;
        return Objects.equals(table, that.table) &&
                Objects.equals(kqlQuery, that.kqlQuery) &&
                Objects.equals(metadataExpression, that.metadataExpression) &&
                Objects.equals(splitMetadataColumnNames, that.splitMetadataColumnNames) &&
                Objects.equals(topN, that.topN);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(
                table, kqlQuery, metadataExpression, metadataQueryOnly, topN);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("table", table)
                .add("kqlQuery", kqlQuery)
                .add("metadataExpression", metadataExpression)
                .add("metadataQueryOnly", metadataQueryOnly)
                .add("topN", topN)
                .toString();
    }
}
